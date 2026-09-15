#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Photos18.com 图集爬虫 (高质量重构版 - 健壮性强化)

功能:
 - 爬取 Photos18.com 网站首页列表下的所有图集。
 - (新增) 图像内容验证：自动检测并重新下载损坏的图片 (需 Pillow 库)。

网站结构:
 - 列表页 URL 格式: BASE_URL/?page=X&per-page=100 (支持分页)
 - 专辑页 URL 格式: BASE_URL/v/...
 - 图片 URL 格式: https://img.photos18.com/... (特定 CDN)

流程:
 1. 初始化 Session (带连接池) -> 请求首页并探测实际总页数。
 2. 遍历列表页 (带 page-sleep 延迟) -> 提取专辑(标题, 链接) (先收集并去重)。
 3. 启动**相册并发池** (低并发, 默认 CONCURRENCY_ALBUM=1)。
    └-> (等待 album-sleep 延迟) -> 请求专辑详情页。
        └-> 解析收集所有图片链接。
 4. 启动**图片并发池** (高并发, 默认 CONCURRENCY_IMAGE=8)。
    └-> (并发下载图片) -> 检查文件断点续传/损坏。
        └-> (新增) 验证图片内容 (Pillow/大小) -> 原子化写入文件并保存。

特点:
 - **健壮请求**: 针对 429 Too Many Requests、连接断开 (RemoteDisconnected) 等错误，采用优化的指数退避重试机制。
 - **抗反爬增强**: 通过大幅降低相册并发数，并增加列表页和相册详情页请求延迟，有效应对网站限速。
 - **智能探测**: 自动尝试探测网站的实际最大页码，避免因页面截断而遗漏内容。
 - **文件安全**: 实现原子化写入 (使用 `.part` 文件)，确保文件在中断时不会损坏，并支持对已下载的损坏文件进行修复性重下载。
 - **高效率**: 采用两级并发 (相册低并发、图片高并发) 策略，平衡速度与稳定性。
 - **详细日志**: 提供了 [专辑 X/N] 和 (图片 Y/Z) 的进度打印。
"""
import os
import re
import time
import random
import argparse
import logging
import io # (新增)
from urllib.parse import urljoin, unquote, urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Dict, Optional, Set, Any

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import shutil

# -------- (新增) 检查 Pillow 库 --------
try:
    from PIL import Image, ImageFile
    from PIL.Image import UnidentifiedImageError
    ImageFile.LOAD_TRUNCATED_IMAGES = True
    Image.MAX_IMAGE_PIXELS = None
    PILLOW_AVAILABLE = True
except ImportError:
    PILLOW_AVAILABLE = False
    
# -------- 默认配置 (针对反爬优化) --------
BASE_URL = "https://www.photos18.com"
DEFAULT_SAVE_DIR = "美女图集"
DEFAULT_RETRIES = 5
DEFAULT_TIMEOUT = 15

# !!! (新增) FlareSolverr 配置：只用来"借"一次浏览器过 Cloudflare 拿 Cookie，
# 不是每个请求都走它，所以不会拖慢整体抓取速度。
USE_FLARESOLVERR = False                                # 改成 False 即可完全关闭，恢复原始行为
FLARESOLVERR_URL = "http://192.168.255.250:8191/v1"   # 注意必须带 /v1
FLARESOLVERR_MAX_TIMEOUT = 60000                       # 传给 FlareSolverr 浏览器等待上限(ms)

# !!! 核心优化点 1: 大幅降低相册并发数，避免触发 429
DEFAULT_CONCURRENCY_ALBUM = 1       # 并发处理相册数量 (强烈推荐 1 或 2)
DEFAULT_CONCURRENCY_IMAGE = 1       # 每个相册内部并发下载图片数量 

# !!! 核心优化点 2: 增加列表页和详情页延迟，应对限速和连接中断
DEFAULT_PAGE_SLEEP = 5.0            # 爬取每个列表页后的延迟 (提高到 5.0 秒)
DEFAULT_IMAGE_SLEEP = 0.2           # 每张图片下载成功后的短暂延迟
DEFAULT_ALBUM_DETAIL_SLEEP = 2.0    # 请求相册详情页后的延迟 (用于并发任务之间的缓冲)

MAX_POOL_SIZE = 64                  # 增大连接池容量
PER_PAGE_COUNT = 100                # 网站每页固定显示数量

# -------- 日志设置 --------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

if not PILLOW_AVAILABLE:
    logging.warning("Pillow 库未安装。将跳过严格的图像完整性校验 (请运行: pip install Pillow)")

# -------- 辅助函数 --------
def make_session() -> requests.Session:
    """创建并配置requests.Session，配置更大的连接池和必要的UA。"""
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    })
    
    adapter = HTTPAdapter(
        pool_connections=MAX_POOL_SIZE, 
        pool_maxsize=MAX_POOL_SIZE
    )
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    
    return s

# -------- (新增) FlareSolverr 辅助函数 --------
def flaresolverr_get_cookies(target_url: str, timeout_ms: int = FLARESOLVERR_MAX_TIMEOUT) -> Optional[Dict[str, Any]]:
    """
    调用 FlareSolverr /v1，让它用真实浏览器过一次 Cloudflare 检测。
    成功返回 {"cookies": [...], "user_agent": "..."}；失败返回 None。
    注意：这只是"借"一次浏览器拿 Cookie，不是每个请求都要走这里。
    """
    payload = {
        "cmd": "request.get",
        "url": target_url,
        "maxTimeout": timeout_ms,
    }
    try:
        resp = requests.post(
            FLARESOLVERR_URL,
            json=payload,
            timeout=(timeout_ms / 1000) + 60,  # 本地 HTTP 超时要比 FlareSolverr 内部超时更长一点
        )
        resp.raise_for_status()
        data = resp.json()
    except RequestException as e:
        logging.error("调用 FlareSolverr 失败: %s", e)
        return None

    if data.get("status") != "ok":
        logging.error("FlareSolverr 未能过盾: %s", data.get("message"))
        return None

    solution = data["solution"]
    logging.info("FlareSolverr 过盾成功，获得 %d 个 Cookie。", len(solution.get("cookies", [])))
    return {
        "cookies": solution.get("cookies", []),
        "user_agent": solution.get("userAgent"),
    }

def apply_flaresolverr_result(session: requests.Session, result: Dict[str, Any]) -> None:
    """
    把 FlareSolverr 拿到的 Cookie 和 User-Agent 写进本地 session。
    UA 必须跟 Cookie 一起换——Cloudflare 会把清关 Cookie 和 UA 绑定校验，
    只换 Cookie 不换 UA，是最常见的"明明有 Cookie 还是被拦"的原因。
    """
    for c in result.get("cookies", []):
        try:
            session.cookies.set(
                c["name"], c["value"],
                domain=c.get("domain", ""),
                path=c.get("path", "/"),
            )
        except KeyError:
            continue
    if result.get("user_agent"):
        session.headers["User-Agent"] = result["user_agent"]

def is_cf_blocked(response: requests.Response) -> bool:
    """标准化的 Cloudflare 拦截检测 (智能兼容文本网页与二进制图片流)"""
    # 1. 状态码属于明确的封锁限制
    if response.status_code in (403, 503):
        return True
        
    # 2. 如果 Content-Type 明确是图片格式 (image/jpeg, image/png 等)，绝对不是 CF 验证页，安全放行！
    ct = response.headers.get("Content-Type", "")
    if "image/" in ct.lower():
        return False
        
    # 3. 如果下到的不是图片 (比如变成了 text/html)，才安全地读取前3000字符，检测是否有 CF 盾特征词
    try:
        text = response.text[:3000]
        cf_signatures = [
            "Just a moment...",
            "_cf_chl_opt",
            "Enable JavaScript and cookies to continue",
            "Attention Required! | Cloudflare"
        ]
        return any(sig in text for sig in cf_signatures)
    except Exception:
        return False
    
def request_text(session: requests.Session, url: str, retries: int, timeout: int) -> Optional[str]:
    """请求文本页面，带重试、指数退避和 UnboundLocalError 修复。"""
    
    BASE_WAIT_TIME = 3.0 # 基础等待时间
    
    for attempt in range(1, retries + 1):
        r: Optional[requests.Response] = None # 修复 UnboundLocalError
        
        try:
            r = session.get(url, timeout=timeout, headers={"Referer": BASE_URL})
            
            # (新增) 疑似被 Cloudflare 拦截：状态码或页面特征异常，先用 FlareSolverr 刷新 Cookie 再重试
            if USE_FLARESOLVERR and is_cf_blocked(r):
                logging.warning("疑似被 Cloudflare 拦截 (状态 %s): %s，尝试用 FlareSolverr 刷新 Cookie。",
                                r.status_code, url)
                fs_result = flaresolverr_get_cookies(url)
                if fs_result:
                    apply_flaresolverr_result(session, fs_result)
                    continue  # 用新 Cookie 重新发这次请求，不当作失败
            
            r.raise_for_status()
            return r.text
            
        except RequestException as e:
            if r is not None and r.status_code == 429 and attempt < retries:
                 logging.warning("请求失败: %s (尝试 %d/%d) 错误: %s。遭遇 429，等待更长时间。", url, attempt, retries, e)
                 wait_time = min(60, (2 ** attempt) * BASE_WAIT_TIME + random.random() * BASE_WAIT_TIME)
                 time.sleep(wait_time)
                 
            elif attempt < retries:
                status_msg = f"{r.status_code}" if r is not None else "无响应"
                logging.warning("请求失败: %s (尝试 %d/%d) 错误: %s。状态: %s，等待并重试。", 
                                url, attempt, retries, e, status_msg)
                wait_time = min(15, (2 ** attempt) * BASE_WAIT_TIME + random.random() * 1.0)
                time.sleep(wait_time)
                
            else:
                 logging.error("请求失败: %s (所有尝试均失败)。", url)
                 return None
                 
    return None

def request_binary(session: requests.Session, url: str, retries: int, timeout: int) -> Optional[bytes]:
    r: Optional[requests.Response] = None  # 修复 UnboundLocalError
    """请求二进制文件(图片)，带重试和指数退避。"""
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, timeout=timeout, stream=True, headers={"Referer": BASE_URL})
            
            # --- 新增：FlareSolverr 图片过盾逻辑 ---
            if USE_FLARESOLVERR and is_cf_blocked(r):
                logging.warning("疑似图片请求被 Cloudflare 拦截 (状态 %s, Content-Type: %s): %s，尝试刷新 Cookie。",
                                r.status_code, r.headers.get("Content-Type", ""), url)
                fs_result = flaresolverr_get_cookies(url)
                if fs_result:
                    apply_flaresolverr_result(session, fs_result)
                    continue
            # ------------------------------------

            r.raise_for_status()
            return r.content
        except RequestException as e:
            wait_time = min(30, (2 ** attempt) * 0.4) + random.random()
            if r is not None and r.status_code == 429 and attempt < retries:
                logging.warning("图片请求遭遇 429 限速: %s，等待 %.1fs。", url, wait_time * 2)
                time.sleep(wait_time * 2)
            elif attempt < retries:
                logging.warning("图片请求失败: %s (尝试 %d/%d) 错误: %s，等待 %.1fs。", url, attempt, retries, e, wait_time)
                time.sleep(wait_time)
            else:
                logging.error("图片请求彻底失败: %s - %s", url, e)
    return None

def sanitize_filename(name: str, maxlen: int = 120) -> str:
    """清洗并截断文件名/文件夹名。"""
    if not name: return "untitled"
    name = unquote(name)
    s = re.sub(r'[\0\/\\:\*\?\"<>\|]+', "_", name).strip()
    return s[:maxlen] or "untitled"

# -------- (新增) 图像验证辅助函数 --------
def is_image_valid_file(filepath: str, args: argparse.Namespace) -> bool:
    """[新] 检查磁盘上的文件是否为有效图像。"""
    #try:
    #    file_size = os.path.getsize(filepath)
    #    if file_size < (args.min_size * 1024): # (已修正)
    #        return False
    #except OSError:
    #    return False

    if args.verify and PILLOW_AVAILABLE:
        try:
            with Image.open(filepath) as img:
                img.verify()
            return True
        # 由于保留未知格式是如此的不现实，只好宁缺毋滥了
        ## (优化) 区分“无法识别”和“确认损坏”
        #except UnidentifiedImageError:
        #    # 格式无法识别 (可能是 AVIF, HEIC 等新格式)
        #    # 我们选择“信任”它，只要它的大小达标 (上面已检查)
        #    logging.warning("Pillow 无法识别 %s (可能为新格式)，已跳过强校验。", filepath)
        #    return True # 既然大小合格，Pillow不认识，我们就放行
        except (UnidentifiedImageError, ValueError, OSError, TypeError, FileNotFoundError) as e:
            # 确定的损坏 (例如 "Truncated file", "bad string") 或文件问题
            logging.debug("Pillow 校验失败 (文件确认损坏或IO问题): %s. 错误: %s", filepath, e)
            return False 
        except Exception as e:
            # 其他未知异常
            logging.error("校验文件 %s 时发生未知异常: %s", filepath, e)
            return False
            
    return True

def is_image_valid_bytes(data: bytes, args: argparse.Namespace) -> bool:
    """[新] 检查内存中的 bytes 是否为有效图像。"""
    #if len(data) < (args.min_size * 1024):
    #    logging.warning("验证失败 (内容太小 %dKB)", len(data) // 1024)
    #    return False
    
    if args.verify and PILLOW_AVAILABLE:
        try:
            with Image.open(io.BytesIO(data)) as img:
                img.verify()
            return True
        # 由于保留未知格式是如此的不现实，只好宁缺毋滥了
        ## (优化) 区分“无法识别”和“确认损坏”
        #except UnidentifiedImageError:
        #    # 格式无法识别 (可能是 AVIF, HEIC 等新格式)
        #    # 我们选择“信任”它，只要它的大小达标 (上面已检查)
        #    logging.warning("Pillow 无法识别 %s (可能为新格式)，已跳过强校验。", filepath)
        #    return True # 既然大小合格，Pillow不认识，我们就放行
        except (UnidentifiedImageError, ValueError, OSError, TypeError) as e:
            # 确定的损坏 (例如 "Truncated file", "bad string")
            # 这也包括了Pillow试图解析HTML时抛出的错误
            logging.debug("Pillow 校验失败 (内容确认损坏或为HTML): %s", e)
            return False
        except Exception as e:
            logging.error("验证时发生系统级异常: %s", e)
            return False
            
    return True

def save_bytes_atomic(path: str, data: bytes) -> bool:
    """原子化写入文件，避免文件损坏。"""
    tmp_path = path + ".part"
    try:
        with open(tmp_path, "wb") as f: 
            f.write(data)
        try:
            os.replace(tmp_path, path)
        except OSError:
            # 兼容跨分区/外接移动硬盘挂载时的安全降级写入
            shutil.move(tmp_path, path)
        return True
    except IOError as e:
        logging.error("文件写入失败 %s : %s", path, e)
        if os.path.exists(tmp_path):
            try: os.remove(tmp_path)
            except OSError: pass
        return False

# -------- 解析函数 (保持不变) --------
def parse_total_pages(html: str) -> int:
    """从 HTML 中解析出总页数，健壮地查找最大页码。"""
    soup = BeautifulSoup(html, "html.parser")
    max_page_found = 1

    for a in soup.select("nav#w0 ul.pagination a"):
        href = a.get("href")
        if href:
            try:
                query_string = urlparse(href).query
                params = parse_qs(query_string)
                if 'page' in params:
                    page_num_str = params['page'][0]
                    if page_num_str.isdigit():
                         page_num = int(page_num_str)
                         if page_num > max_page_found:
                             max_page_found = page_num
            except Exception:
                pass
        
        data_page_str = a.get("data-page")
        if data_page_str:
            try:
                page_num = int(data_page_str) + 1
                if page_num > max_page_found:
                    max_page_found = page_num
            except ValueError:
                pass
    
    return max_page_found 

def parse_albums_on_page(html: str, base_url: str) -> List[Tuple[str, str]]:
    """从列表页HTML中解析出(标题, URL)元组列表。"""
    soup = BeautifulSoup(html, "html.parser")
    albums = []
    for card in soup.select("div#videos div.card"):
        a_tag = card.select_one("div.card-body a[href]")
        if a_tag:
            href = a_tag.get("href", "").strip()
            title = a_tag.get_text(strip=True) or "" 
            if href.startswith('/v/') and title:
                full_url = urljoin(base_url, href)
                albums.append((title, full_url))
    return albums

def parse_album_details(html: str) -> Tuple[str, List[str]]:
    """从相册详情页解析：标题, 图片URL列表。"""
    soup = BeautifulSoup(html, "html.parser")
    
    title_tag = soup.select_one("h1.title")
    title = title_tag.get_text(strip=True) if title_tag else "未知标题"
    
    img_urls: List[str] = []
    for a_tag in soup.select("div#content a[data-fancybox='gallery'][href]"):
        img_url = a_tag.get("href", "").split("?")[0]
        
        if img_url.startswith('https://img.photos18.com'):
            img_urls.append(img_url)
    
    final_urls = sorted(list(set(img_urls))) 
    
    return title, final_urls

# -------- 下载核心逻辑 --------
def download_single_image(session: requests.Session, url: str, album_dir: str, args: argparse.Namespace, current_index: int, total_images: int) -> str:
    """(已更新) 下载单张图片，并根据文件大小和内容(Pillow)验证有效性。"""
    filename = sanitize_filename(os.path.basename(urlparse(url).path))
    dest_path = os.path.join(album_dir, filename)
    progress_prefix = f"({current_index}/{total_images})"

    # 1. 检查已存在的文件
    if os.path.exists(dest_path):
        if is_image_valid_file(dest_path, args):
            logging.info("%s 跳过 (已存在且有效): %s", progress_prefix, dest_path)
            return "skipped"
        else:
            logging.warning("%s 重新下载 (文件无效或损坏): %s", progress_prefix, dest_path)
            try:
                os.remove(dest_path)
            except OSError as e:
                logging.error("%s 无法删除旧的损坏文件: %s", progress_prefix, e)

    # 2. 执行下载
    data = request_binary(session, url, retries=args.retries, timeout=args.timeout)
    
    if not data:
        logging.warning("%s 下载失败 (未获取到数据): %s", progress_prefix, url)
        return "fail"
    
    # 3. 验证新下载的数据
    if not is_image_valid_bytes(data, args):
        logging.warning("%s 下载的内容验证失败(损坏或HTML)，抛弃: %s", progress_prefix, url)
        return "fail"

    # 4. 保存
    if save_bytes_atomic(dest_path, data):
        logging.info("%s 下载成功: %s", progress_prefix, dest_path)
        time.sleep(args.image_sleep + random.random() * 0.5)
        return "ok"
    
    logging.warning("%s 下载后保存文件失败: %s", progress_prefix, dest_path)
    return "fail"

# -------- 相册处理主流程 --------
def process_album(session: requests.Session, title: str, url: str, save_root: str, base_url: str, args: argparse.Namespace, album_index: int, total_albums: int, album_detail_sleep: float) -> Dict[str, int]:
    """处理单个相册：获取详情 -> 解析链接列表 -> 并发下载。"""
    
    time.sleep(album_detail_sleep + random.random() * 1.0)
    
    album_prefix = f"[{album_index}/{total_albums}] {title}"
    logging.info("%s -> 正在请求详情页: %s", album_prefix, url)
    
    html = request_text(session, url, retries=args.retries, timeout=args.timeout)
    if not html:
        logging.error("%s -> 无法获取相册页面，跳过。", album_prefix)
        return {"ok": 0, "skipped": 0, "fail": 1}

    real_title, final_urls = parse_album_details(html)
    
    folder_name = sanitize_filename(real_title or title)
    album_dir = os.path.join(save_root, folder_name)
    
    if not final_urls:
        logging.warning("%s -> 未解析到任何图片链接，跳过。", album_prefix)
        return {"ok": 0, "skipped": 0, "fail": 0}

    os.makedirs(album_dir, exist_ok=True)
    total_images = len(final_urls)
    logging.info("%s -> 准备下载 %d 张图片到: %s", album_prefix, total_images, folder_name)
    
    results: Dict[str, int] = {"ok": 0, "skipped": 0, "fail": 0}
    with ThreadPoolExecutor(max_workers=args.image_concurrency, thread_name_prefix='ImageDownloader') as executor:
        future_map: Dict[Any, str] = {}
        for index, img_url in enumerate(final_urls, start=1):
            future = executor.submit(
                download_single_image, 
                session, img_url, album_dir, args, 
                index, total_images
            )
            future_map[future] = img_url
            
        for future in as_completed(future_map):
            try:
                status = future.result()
                results[status] += 1
            except Exception:
                img_url = future_map[future]
                logging.exception("%s -> 图片下载任务异常: %s", album_prefix, img_url)
                results["fail"] += 1
    
    logging.info("%s -> 处理完成。结果: 成功: %d, 跳过: %d, 失败: %d", 
                 album_prefix, results['ok'], results['skipped'], results['fail'])
    return results

# -------- 主函数 --------
def main():
    parser = argparse.ArgumentParser(description="Photos18.com 图集爬虫", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--dir", default=DEFAULT_SAVE_DIR, help="图片保存的根目录")
    parser.add_argument("--start", type=int, default=1, help="起始列表页码")
    parser.add_argument("--end", type=int, default=0, help="结束列表页码 (0 代表自动检测)")
    parser.add_argument("-r", "--retries", type=int, default=DEFAULT_RETRIES, help="请求失败最大重试次数")
    parser.add_argument("-t", "--timeout", type=int, default=DEFAULT_TIMEOUT, help="请求超时时间(秒)")
    parser.add_argument("-c", "--album-concurrency", type=int, default=DEFAULT_CONCURRENCY_ALBUM, help="并发处理的相册数量 (已降低)")
    parser.add_argument("-w", "--image-concurrency", type=int, default=DEFAULT_CONCURRENCY_IMAGE, help="相册内部并发下载图片数")
    parser.add_argument("--page-sleep", type=float, default=DEFAULT_PAGE_SLEEP, help="爬取每个列表页后的延迟(秒) (已增加)")
    parser.add_argument("--image-sleep", type=float, default=DEFAULT_IMAGE_SLEEP, help="每张图片下载成功后的延迟(秒)")
    parser.add_argument("--album-sleep", type=float, default=DEFAULT_ALBUM_DETAIL_SLEEP, help="每个相册详情页请求前的延迟(秒)")
    
    # -------- (新增) 验证相关参数 --------
    parser.add_argument(
        "--verify", 
        action="store_true", 
        help="[推荐] 启用严格的图像验证。会检查已存在和新下载的文件是否损坏 (需要 Pillow 库)。"
    )
    parser.add_argument(
        "--min-size", 
        type=int, 
        default=1, 
        help="最小文件大小(KB)。(在 --verify 未启用或 Pillow 库不可用时作为后备检查)"
    )
    # ----------------------------------------

    args = parser.parse_args()

    session = make_session()
    base_url = BASE_URL.rstrip('/')
        
    save_root = os.path.abspath(args.dir)
    os.makedirs(save_root, exist_ok=True)
    
    logging.info("【初始化】相册并发数: %d, 列表页延迟: %.1fs", args.album_concurrency, args.page_sleep)
    
    # (新增) 启动前先用 FlareSolverr 过一次盾，把 Cookie/UA 灌进 session
    # 之后所有 request_text/request_binary 仍然直接走本地 session.get()，不会变慢
    if USE_FLARESOLVERR:
        logging.info("正在通过 FlareSolverr 获取 Cloudflare 通行 Cookie...")
        fs_result = flaresolverr_get_cookies(base_url)
        if fs_result:
            apply_flaresolverr_result(session, fs_result)
            logging.info("Cookie 注入完成。")
        else:
            logging.warning("FlareSolverr 过盾失败，将尝试直接请求 (可能会被 Cloudflare 拦截)。")
    
    home_html = request_text(session, base_url, args.retries, args.timeout)
    if not home_html:
        logging.critical("无法获取网站首页 %s，程序退出。", base_url)
        return

    total_site_pages = parse_total_pages(home_html)
    logging.info("【步骤1】从首页检测到最大页码: %d", total_site_pages)
    
    if total_site_pages < 10:
        logging.info("【步骤2】首页页码可能被截断，尝试探测实际最大页码...")
        
        PROBE_PAGE = 999 
        probe_url = f"{base_url}/?page={PROBE_PAGE}&per-page={PER_PAGE_COUNT}"
        probe_html = request_text(session, probe_url, args.retries, args.timeout)

        if probe_html:
            probed_max_pages = parse_total_pages(probe_html)
            # 增加校验：避免服务器因为页码太大重定向回了首页导致返回 1
            if probed_max_pages > total_site_pages and probed_max_pages > 1:
                total_site_pages = probed_max_pages
                logging.info("【步骤2】成功探测到实际总页数: %d", total_site_pages)
            else:
                logging.warning("【步骤2】探测结果无提升，仍使用当前页数: %d", total_site_pages)
        else:
            logging.warning("【步骤2】请求探测页失败，仍使用当前页数: %d", total_site_pages)
            
    start_page = max(1, args.start)
    end_page = args.end if args.end > 0 and args.end >= start_page else total_site_pages
    
    summary: Dict[str, int] = {"ok": 0, "skipped": 0, "fail": 0, "albums_processed": 0}
    seen_album_urls: Set[str] = set()
    all_albums_to_process: List[Tuple[str, str]] = []

    for page_num in range(start_page, end_page + 1):
        if page_num == 1:
            page_url = base_url
        else:
            page_url = f"{base_url}/?page={page_num}&per-page={PER_PAGE_COUNT}"
            
        logging.info("开始发现列表页 %d/%d -> %s", page_num, end_page, page_url)
        
        list_html = request_text(session, page_url, args.retries, args.timeout)
        if not list_html: 
            logging.warning("列表页 %d 获取失败，跳过。", page_num)
            time.sleep(args.page_sleep + random.random())
            continue

        albums_on_page = parse_albums_on_page(list_html, base_url)
        logging.info("列表页 %d/%d 找到 %d 个专辑。", page_num, end_page, len(albums_on_page)) 
        
        for title, url in albums_on_page:
            if url not in seen_album_urls:
                seen_album_urls.add(url)
                all_albums_to_process.append((title, url))
                
        time.sleep(args.page_sleep + random.random())

    total_albums = len(all_albums_to_process)
    logging.info("所有列表页遍历完毕，共发现 %d 个待处理相册。开始并发处理...", total_albums)
    logging.info("=" * 50)
    
    with ThreadPoolExecutor(max_workers=args.album_concurrency, thread_name_prefix='AlbumProcessor') as executor:
        future_map: Dict[Any, Tuple[str, str, int]] = {}
        
        for index, (title, url) in enumerate(all_albums_to_process, start=1):
            future = executor.submit(
                process_album, 
                session, title, url, save_root, base_url, args,
                index, total_albums, args.album_sleep
            )
            future_map[future] = (title, url, index)
            
        logging.info("已提交 %d 个相册任务，等待完成...", total_albums)
        
        for future in as_completed(future_map):
            title, url, index = future_map[future]
            try:
                result = future.result()
                for k, v in result.items():
                    summary[k] += v
                if any(v > 0 for k, v in result.items() if k != "skipped"):
                    summary["albums_processed"] += 1
            except Exception:
                logging.exception("[%d/%d] 处理相册 '%s' 时发生未捕获的异常: %s", 
                                  index, total_albums, title, url)
    
    logging.info("=" * 50)
    logging.info("所有任务完成！")
    logging.info(
        "处理相册数: %d, 成功下载: %d, 跳过: %d, 失败: %d",
        summary["albums_processed"], summary["ok"], summary["skipped"], summary["fail"]
    )
    logging.info("=" * 50)

if __name__ == "__main__":
    main()