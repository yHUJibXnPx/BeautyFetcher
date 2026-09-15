#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
meiru.neocities.org 图集爬虫 (高质量重构版 - 健壮性强化)

功能:
  - 爬取 meiru.neocities.org 网站的图片集。
  - (新增) 图像内容验证：自动检测并重新下载损坏的图片 (需 Pillow 库)。

网站结构:
  - 主站分页 -> 列表页 -> 专辑链接
  - 专辑页内部也存在分页
  - 图片从第三方图床下载

流程:
  主页 -> 获取主站总页数 -> 遍历所有列表页 -> 提取专辑链接 (先收集)
     └-> (并发处理专辑) -> (延迟) -> 进入专辑页 -> 获取专辑标题和内部总页数
          └-> 遍历专辑内所有分页 -> 收集所有图片文件名
               └-> (并发下载图片) -> (新增) 验证图片内容 -> 尝试从多个图床下载并保存

特点:
  - 两级并发: 并发处理多个专辑，并在每个专辑内部并发下载所有图片。
  - 健壮的请求与重试: 
    - 修复 UnboundLocalError。
    - 针对 429 Too Many Requests 和 RemoteDisconnected 优化的指数退避。
  - 防护增强: 
    - 增加连接池 (HTTPAdapter)。
    - 增加列表页爬取延迟 (page-sleep)。
    - 新增专辑详情页请求延迟 (album-sleep) 以防止 429。
  - 原子化写入/断点续传: 自动跳过已完整下载的文件。
  - (新增) 图像验证: 
    - 通过 --verify 启用，可识别并修复已存在的损坏文件。
    - 验证新下载的内容，防止 HTML 错误页被保存。
  - 多图床支持: 自动轮询多个备用图片CDN，提高下载成功率。
  - 统一日志: 实现了 [专辑 X/N] 和 (图片 Y/Z) 进度打印。
"""
import os
import re
import time
import random
import argparse
import logging
import io # (新增)
from urllib.parse import urljoin, unquote
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
    # 允许 PIL 尝试加载截断的文件
    ImageFile.LOAD_TRUNCATED_IMAGES = True
    Image.MAX_IMAGE_PIXELS = None # 禁用解压炸弹检查
    PILLOW_AVAILABLE = True
except ImportError:
    PILLOW_AVAILABLE = False
    # 日志尚未配置，暂不打印
    
# -------- 默认配置 (针对反爬优化) --------
BASE_URL = "https://meiru.neocities.org"
# 可靠的图片CDN列表，程序会自动按顺序尝试
TG_URLS = [
    "https://teleimgs.netlib.re/file",
    "https://telegraph-image.pages.dev/file",
    "https://im.gurl.eu.org/file"
]
DEFAULT_SAVE_DIR = "美女图集"
DEFAULT_RETRIES = 5
DEFAULT_TIMEOUT = 20
DEFAULT_CONCURRENCY_ALBUM = 4       # 并发处理专辑数量
DEFAULT_CONCURRENCY_IMAGE = 1       # 每个专辑内部并发下载图片数量
DEFAULT_PAGE_SLEEP = 3.0            # (强化) 爬取每个列表页后的延迟
DEFAULT_ALBUM_DETAIL_SLEEP = 2.0    # (新增) 请求每个专辑详情页前的延迟
DEFAULT_IMAGE_SLEEP = 0.2           # 每张图片下载成功后的短暂延迟
DEFAULT_POOL_SIZE = 64              # (新增) 连接池大小

# !!! (新增) FlareSolverr 配置：只用来"借"一次浏览器过 Cloudflare 拿 Cookie，
# 不是每个请求都走它，所以不会拖慢整体抓取速度。
USE_FLARESOLVERR = False                                # 改成 False 即可完全关闭，恢复原始行为
FLARESOLVERR_URL = "http://192.168.255.250:8191/v1"   # 注意必须带 /v1
FLARESOLVERR_MAX_TIMEOUT = 60000                       # 传给 FlareSolverr 浏览器等待上限(ms)

# -------- 跳过已知问题的URL --------
SKIP_URLS = {
    "https://meiru.neocities.org/view/aqua-kiara-sessyoin",
}

# -------- 日志设置 --------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# (新增) 在日志配置后检查 Pillow
if not PILLOW_AVAILABLE:
    logging.warning("Pillow 库未安装。将跳过严格的图像完整性校验 (请运行: pip install Pillow)")

# -------- 辅助函数 (强化版) --------
def make_session() -> requests.Session:
    """(强化) 创建一个配置好 User-Agent 和 HTTPAdapter 的 requests.Session 对象"""
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64 x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Referer": BASE_URL
    })
    
    adapter = HTTPAdapter(
        pool_connections=DEFAULT_POOL_SIZE,
        pool_maxsize=DEFAULT_POOL_SIZE
    )
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    
    return s

# -------- (新增) FlareSolverr 辅助函数 --------
def flaresolverr_get_cookies(target_url: str, timeout_ms: int = FLARESOLVERR_MAX_TIMEOUT) -> Optional[Dict[str, Any]]:
    """
    调用 FlareSolverr /v1，让它用真实浏览器过一次 Cloudflare 检测。
    成功返回 {"cookies": [...], "user_agent": "..."}；失败返回 None。
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
            timeout=(timeout_ms / 1000) + 60,
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
    UA 必须跟 Cookie 一起换，否则 Cloudflare 会认为 Cookie 和当前浏览器不匹配。
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
    
def request_text(session: requests.Session, url: str, retries: int = DEFAULT_RETRIES, timeout: int = DEFAULT_TIMEOUT) -> Optional[str]:
    """(强化) 请求文本页面，带重试、指数退避、429/UnboundLocalError 修复，并保证正确解码。"""
    r: Optional[requests.Response] = None # 修复 UnboundLocalError
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, timeout=timeout)
            
            # (新增) 疑似被 Cloudflare 拦截：先用 FlareSolverr 刷新 Cookie 再重试
            if USE_FLARESOLVERR and is_cf_blocked(r):
                logging.warning("疑似被 Cloudflare 拦截 (状态 %s): %s，尝试用 FlareSolverr 刷新 Cookie。",
                                r.status_code, url)
                fs_result = flaresolverr_get_cookies(url)
                if fs_result:
                    apply_flaresolverr_result(session, fs_result)
                    continue
            
            r.raise_for_status()  # 遇到 4xx/5xx 错误时会抛出异常
            # (保留) 自动识别编码，优先用网页自带声明，其次 fallback 到 UTF-8
            r.encoding = r.apparent_encoding or "utf-8"
            return r.text
            
        except RequestException as e:
            wait_time = min(30, (2 ** attempt) * 0.5) + random.random()
            status_msg = f"{r.status_code}" if r is not None else "无响应"

            if r is not None and r.status_code == 429 and attempt < retries:
                logging.warning("请求失败: %s (尝试 %d/%d) 错误: %s。遭遇 429，等待 %.1fs。", 
                                url, attempt, retries, e, wait_time * 2)
                time.sleep(wait_time * 2)
            elif attempt < retries:
                logging.warning("请求失败: %s (尝试 %d/%d) 错误: %s。状态: %s，等待 %.1fs 并重试。", 
                                url, attempt, retries, e, status_msg, wait_time)
                time.sleep(wait_time)
            else:
                logging.error("请求失败: %s (所有尝试均失败)。错误: %s", url, e)
    return None

def request_binary(session: requests.Session, url: str, retries: int = DEFAULT_RETRIES, timeout: int = DEFAULT_TIMEOUT) -> Optional[bytes]:
    """(强化) 请求二进制文件(图片)，带重试、指数退避和 429/UnboundLocalError 修复。"""
    r: Optional[requests.Response] = None # 修复 UnboundLocalError
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, timeout=timeout, stream=True)
            
            # (新增) 疑似被 Cloudflare 拦截：期望拿到图片，却收到 403/503 或 HTML 页面
            # 图片可能来自第三方图床(teleimgs/telegraph-image/gurl.eu.org)，用它自己的 URL 去过盾
            if USE_FLARESOLVERR and is_cf_blocked(r):
                logging.warning("疑似图片请求被 Cloudflare 拦截 (状态 %s, Content-Type: %s): %s，尝试刷新 Cookie。",
                                r.status_code, r.headers.get("Content-Type", ""), url)
                fs_result = flaresolverr_get_cookies(url)
                if fs_result:
                    apply_flaresolverr_result(session, fs_result)
                    continue
            
            r.raise_for_status() # 检查 4xx/5xx 错误
            return r.content
            
        except RequestException as e:
            wait_time = min(30, (2 ** attempt) * 0.4) + random.random()
            status_msg = f"{r.status_code}" if r is not None else "无响应"

            if r is not None and r.status_code == 429 and attempt < retries:
                logging.warning("图片请求失败: %s (尝试 %d/%d) 错误: %s。遭遇 429，等待 %.1fs。", 
                                url, attempt, retries, e, wait_time * 2)
                time.sleep(wait_time * 2)
            elif attempt < retries:
                logging.warning("图片请求失败: %s (尝试 %d/%d) 错误: %s。状态: %s，等待 %.1fs 并重试。", 
                                url, attempt, retries, e, status_msg, wait_time)
                time.sleep(wait_time)
            else:
                logging.error("图片请求失败: %s (所有尝试均失败)。错误: %s", url, e)
    return None

def sanitize_filename(name: str, maxlen: int = 150) -> str:
    """清洗并截断文件名，避免乱码或非法字符。"""
    if not name:
        return "untitled"
    # 去掉前后空格
    name = name.strip()
    # 替换掉不允许的字符
    s = re.sub(r'[\0\/\\:\*\?\"<>\|]+', "_", name)
    # 限制长度，防止路径过长
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

    # 2. 强校验 (Pillow)
    if args.verify and PILLOW_AVAILABLE:
        try:
            with Image.open(filepath) as img:
                img.verify() # 检查文件是否截断或损坏
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
    # 1. 基础检查: 大小
    #if len(data) < (args.min_size * 1024):
    #    logging.warning("验证失败 (内容太小 %dKB)", len(data) // 1024)
    #    return False
    
    # 2. 强校验 (Pillow)
    if args.verify and PILLOW_AVAILABLE:
        try:
            # 从 bytes 打开
            with Image.open(io.BytesIO(data)) as img:
                img.verify() # 检查文件是否截断或损坏
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

# -------- 解析函数 --------
def parse_max_page_number(soup: BeautifulSoup) -> int:
    """从分页导航(div#pagination)中解析出最大页码。"""
    page_numbers = {1}
    for link in soup.select("div#pagination a[href]"):
        text = link.get_text(strip=True)
        if text.isdigit():
            page_numbers.add(int(text))
        else:
            match = re.search(r'[=/](\d+)', link.get('href', ''))
            if match:
                page_numbers.add(int(match.group(1)))
    return max(page_numbers)

def parse_albums_on_listing_page(html: str) -> List[str]:
    """从主站的列表页HTML中解析出所有专辑的URL。"""
    soup = BeautifulSoup(html, "html.parser")
    links = soup.select('div.text-center.font-semibold a[href]')
    return [urljoin(BASE_URL, link['href']) for link in links]

def parse_album_info(html: str) -> Tuple[str, int]:
    """从专辑首页HTML中解析出专辑标题和内部总页数。"""
    soup = BeautifulSoup(html, "html.parser")
    title_tag = soup.find("h1")
    title = sanitize_filename(title_tag.get_text(strip=True)) if title_tag else "未命名专辑"
    total_pages = parse_max_page_number(soup)
    return title, total_pages

def parse_images_on_album_page(html: str) -> List[str]:
    """从专辑的单个分页HTML中解析出所有图片的原始文件名。"""
    soup = BeautifulSoup(html, "html.parser")
    filenames = []
    gallery_div = soup.find("div", id="gallery")
    if gallery_div:
        for img in gallery_div.select("img.block.my-2.mx-auto[src]"):
            src = img.get("src", "")
            # 文件名规则：.jpg -> .png
            filename = os.path.basename(src).replace(".jpg", ".png")
            if filename:
                filenames.append(filename)
    return filenames

# -------- 下载核心逻辑 --------
def download_single_image(session: requests.Session, filename: str, album_dir: str, args: argparse.Namespace, current_index: int, total_images: int) -> str:
    """(已更新) 下载单张图片，带进度日志，包含多源重试逻辑和内容验证。返回状态。"""
    dest_path = os.path.join(album_dir, sanitize_filename(filename))
    
    progress_prefix = f"({current_index}/{total_images})"
    
    # 1. 检查已存在的文件
    if os.path.exists(dest_path):
        if is_image_valid_file(dest_path, args):
            logging.info("%s 跳过 (已存在且有效): %s", progress_prefix, dest_path)
            return "skipped"
        else:
            logging.warning("%s 重新下载 (文件无效或损坏): %s", progress_prefix, dest_path)
            try:
                os.remove(dest_path) # 删除损坏的旧文件
            except OSError as e:
                logging.error("%s 无法删除旧的损坏文件: %s", progress_prefix, e)

    # 2. 执行下载 (多源)
    img_content = None
    for tg_url_base in TG_URLS:
        img_url = f"{tg_url_base}/{filename}"
        img_content = request_binary(session, img_url, retries=2, timeout=args.timeout)
        if img_content:
            break
    
    if not img_content:
        logging.warning("%s 下载失败 (已尝试所有图床): %s", progress_prefix, filename)
        return "fail"
    
    # 3. 验证新下载的数据
    if not is_image_valid_bytes(img_content, args):
        logging.warning("%s 下载的内容验证失败(损坏或HTML)，抛弃: %s", progress_prefix, filename)
        return "fail"
        
    # 4. 保存 (数据已验证)
    if save_bytes_atomic(dest_path, img_content):
        logging.info("%s 下载成功: %s", progress_prefix, dest_path)
        time.sleep(args.image_sleep + random.random() * 0.2)
        return "ok"
    else:
        logging.error("%s 文件写入失败: %s", progress_prefix, dest_path)
        return "fail"

# -------- 专辑处理主流程 --------
def process_album(session: requests.Session, album_url: str, save_root: str, args: argparse.Namespace, album_index: int, total_albums: int, album_detail_sleep: float) -> Dict[str, int]:
    """(强化) 处理单个专辑：(延迟) -> 获取所有图片链接 -> 并发下载。"""
    
    # (新增) 核心优化：相册并发任务之间的延迟
    time.sleep(album_detail_sleep + random.random() * 0.5)
    
    log_prefix = f"[专辑 {album_index}/{total_albums}]" # 标题稍后获取
    logging.info("%s -> 开始处理: %s", log_prefix, album_url)
    
    # 1. 获取专辑首页信息
    first_page_html = request_text(session, album_url, retries=args.retries, timeout=args.timeout)
    if not first_page_html:
        logging.error("%s -> 无法获取专辑首页: %s", log_prefix, album_url)
        return {"ok": 0, "skipped": 0, "fail": 1} # 计为1次失败

    album_title, total_album_pages = parse_album_info(first_page_html)
    
    # (强化) 更新日志前缀
    log_prefix = f"[专辑 {album_index}/{total_albums}] {album_title}"
    
    album_dir = os.path.join(save_root, album_title)
    os.makedirs(album_dir, exist_ok=True)
    logging.info("%s -> 专辑共 %d 页", log_prefix, total_album_pages)

    # 2. 收集专辑内所有分页的图片文件名
    all_image_filenames = set()
    all_image_filenames.update(parse_images_on_album_page(first_page_html))

    for page_num in range(2, total_album_pages + 1):
        page_url = f"{album_url.rstrip('/')}/{page_num}/"
        page_html = request_text(session, page_url, retries=args.retries, timeout=args.timeout)
        if page_html:
            filenames = parse_images_on_album_page(page_html)
            all_image_filenames.update(filenames)
            logging.debug("%s -> 解析专辑分页: %s (找到 %d 张图片)", log_prefix, page_url, len(filenames))
        else:
            logging.warning("%s -> 获取专辑分页失败: %s", log_prefix, page_url)

    if not all_image_filenames:
        logging.warning("%s -> 未解析到任何图片", log_prefix)
        return {"ok": 0, "skipped": 0, "fail": 0}
        
    # 3. 对所有图片进行并发下载
    total_images = len(all_image_filenames)
    logging.info("%s -> 准备下载 %d 张图片...", log_prefix, total_images)
    results = {"ok": 0, "skipped": 0, "fail": 0}
    
    # (强化) 准备带序号的图片URL列表
    indexed_image_filenames = list(enumerate(all_image_filenames, start=1))
    
    with ThreadPoolExecutor(max_workers=args.image_concurrency) as executor:
        future_map = {
            executor.submit(
                download_single_image, 
                session, fname, album_dir, args,
                index, total_images # 传递进度
            ): fname 
            for index, fname in indexed_image_filenames
        }
        
        for future in as_completed(future_map):
            try:
                status = future.result()
                results[status] += 1
            except Exception:
                fname = future_map[future]
                logging.exception("%s -> 图片下载任务异常 [%s]", log_prefix, fname)
                results["fail"] += 1
    
    logging.info("%s -> 处理完成。结果: 成功: %d, 跳过: %d, 失败: %d", 
                 log_prefix, results['ok'], results['skipped'], results['fail'])
    return results

# -------- 主函数 --------
def main():
    parser = argparse.ArgumentParser(description="Meiru.neocities.org 图集爬虫 (高质量重构版 - 健壮性强化)", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--dir", default=DEFAULT_SAVE_DIR, help="图片保存的根目录")
    parser.add_argument("--start", type=int, default=1, help="起始列表页码")
    parser.add_argument("--end", type=int, default=0, help="结束列表页码 (0 代表自动检测)")
    parser.add_argument("-r", "--retries", type=int, default=DEFAULT_RETRIES, help="请求失败最大重试次数")
    parser.add_argument("-t", "--timeout", type=int, default=DEFAULT_TIMEOUT, help="请求超时时间(秒)")
    parser.add_argument("-c", "--album-concurrency", type=int, default=DEFAULT_CONCURRENCY_ALBUM, help="并发处理的专辑数量")
    parser.add_argument("-w", "--image-concurrency", type=int, default=DEFAULT_CONCURRENCY_IMAGE, help="每个专辑内部并发下载图片数")
    parser.add_argument("--page-sleep", type=float, default=DEFAULT_PAGE_SLEEP, help="爬取每个列表页后的基础延迟(秒)")
    parser.add_argument("--album-sleep", type=float, default=DEFAULT_ALBUM_DETAIL_SLEEP, help="(新增)每个相册详情页请求前的延迟(秒)")
    parser.add_argument("--image-sleep", type=float, default=DEFAULT_IMAGE_SLEEP, help="每张图片下载成功后的延迟(秒)")
    
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

    save_root = os.path.abspath(args.dir)
    os.makedirs(save_root, exist_ok=True)
    session = make_session()

    # (新增) 启动前先用 FlareSolverr 过一次盾，把 Cookie/UA 灌进 session
    if USE_FLARESOLVERR:
        logging.info("正在通过 FlareSolverr 获取 Cloudflare 通行 Cookie...")
        fs_result = flaresolverr_get_cookies(BASE_URL)
        if fs_result:
            apply_flaresolverr_result(session, fs_result)
            logging.info("Cookie 注入完成。")
        else:
            logging.warning("FlareSolverr 过盾失败，将尝试直接请求 (可能会被 Cloudflare 拦截)。")

    logging.info("请求首页以获取主站总页数...")
    home_html = request_text(session, BASE_URL, retries=args.retries, timeout=args.timeout)
    if not home_html:
        logging.critical("无法获取网站首页，程序退出。")
        return
        
    total_site_pages = parse_max_page_number(BeautifulSoup(home_html, 'html.parser'))
    logging.info("检测到主站总页数: %d", total_site_pages)

    start_page = max(1, args.start)
    end_page = args.end if args.end > 0 and args.end >= start_page else total_site_pages
    
    summary = {"ok": 0, "skipped": 0, "fail": 0, "albums_processed": 0}
    seen_album_urls: Set[str] = set()
    
    # (强化) 1. "Collect First"
    all_albums_to_process: List[str] = []
    
    for page_num in range(start_page, end_page + 1):
        page_url = f"{BASE_URL}/page/{page_num}/" if page_num > 1 else BASE_URL
        logging.info("开始发现列表页 %d/%d -> %s", page_num, end_page, page_url)
        
        list_html = request_text(session, page_url, retries=args.retries, timeout=args.timeout)
        if not list_html:
            logging.warning("获取列表页失败: %s", page_url)
            time.sleep(args.page_sleep + random.random()) # 失败也要延迟
            continue
        
        albums_on_page = parse_albums_on_listing_page(list_html)
        logging.info("列表页 %d/%d 找到 %d 个专辑。", page_num, end_page, len(albums_on_page))
        
        for album_url in albums_on_page:
            if album_url in SKIP_URLS:
                logging.info("根据配置跳过: %s", album_url)
                continue
            if album_url not in seen_album_urls:
                seen_album_urls.add(album_url)
                all_albums_to_process.append(album_url)
        
        time.sleep(args.page_sleep + random.random())

    # (强化) 2. "Process Second"
    total_albums = len(all_albums_to_process)
    if total_albums == 0:
        logging.warning("未找到任何新专辑，程序结束。")
        return
        
    logging.info("所有列表页遍历完毕，共发现 %d 个待处理专辑。开始并发处理...", total_albums)
    logging.info("=" * 50)
    
    with ThreadPoolExecutor(max_workers=args.album_concurrency, thread_name_prefix='AlbumProcessor') as executor:
        future_map: Dict[Any, Tuple[str, int]] = {}
        
        for index, album_url in enumerate(all_albums_to_process, start=1):
            future = executor.submit(
                process_album, 
                session, album_url, save_root, args,
                index, total_albums, args.album_sleep # 传递进度和延迟
            )
            future_map[future] = (album_url, index)

        logging.info("已提交 %d 个相册任务，等待完成...", total_albums)
        
        for future in as_completed(future_map):
            album_url, index = future_map[future]
            try:
                result = future.result()
                summary["ok"] += result["ok"]
                summary["skipped"] += result["skipped"]
                summary["fail"] += result["fail"]
                summary["albums_processed"] += 1
            except Exception:
                logging.exception("[%d/%d] 处理专辑 [%s] 时发生未捕获的异常", index, total_albums, album_url)
                summary["albums_processed"] += 1 # 即使异常也计为已处理
    
    logging.info("=" * 50)
    logging.info("所有任务完成！")
    logging.info(
        "处理专辑数: %d / %d, 成功下载: %d, 跳过: %d, 失败: %d",
        summary["albums_processed"], total_albums, summary["ok"], summary["skipped"], summary["fail"]
    )
    logging.info("=" * 50)


if __name__ == "__main__":
    main()