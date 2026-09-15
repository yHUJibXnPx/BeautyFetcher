#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
24FA/OK/II/ME/FAA.cc 系列网站图集爬虫 (高质量重构版 - 修复 temp_ 链接错误)

功能:
  - 爬取指定分类下的所有图集。
  - (新增) 图像内容验证：自动检测并重新下载损坏的图片 (需 Pillow 库)。
  - (修复) 自动修正带有 temp_ 前缀的错误图片链接，解决 500 无法下载的问题。

网站结构:
  - 域名可能频繁更换，内置多个备用域名。
  - 分类页(c49.aspx) -> 列表分页(c49p2.aspx...) -> 专辑页(n...aspx)
  - 专辑页内部也有分页(n...p2.aspx...)
  - 图片URL格式为 .../123.jpg_gzip.aspx

流程:
  自动寻找可用域名 -> 请求分类首页 -> 获取列表总页数
      └-> 遍历列表页 -> 提取专辑(标题, 链接) (先收集)
          └-> (并发处理专辑) -> (延迟) -> 进入专辑页 -> 获取内部总页数
                └-> (并发获取所有分页HTML) -> 解析收集所有图片链接
                    └-> (并发下载图片) -> (新增) 验证图片内容 -> 清洗文件名并保存

特点:
  - 域名自动切换: 启动时自动检测可用的BASE_URL，提高抗失效能力。
  - 高效三级并发(已保留)。
  - 健壮的请求与解析: 
    - 修复 UnboundLocalError。
    - 针对 429 Too Many Requests 和 RemoteDisconnected 优化的指数退避。
  - 防护增强: 
    - (已存在) 增加连接池 (HTTPAdapter)。
    - 增加列表页爬取延迟 (page-sleep)。
    - 新增专辑详情页请求延迟 (album-sleep) 以防止 429。
  - 原子化写入/断点续传: 保证文件安全，支持任务中断后继续。
  - (新增) 图像验证: 
    - 通过 --verify 启用，可识别并修复已存在的损坏文件。
    - 验证新下载的内容，防止 HTML 错误页被保存。
  - 详细日志: 已实现 [专辑 X/N] 和 (图片 Y/Z) 进度打印。
  - (修复) 自动移除链接中的 temp_ 前缀。
  - (新增) 下载失败时，日志会额外打印所属专辑的标题和链接，方便排查。
"""
import os
import re
import time
import random
import argparse
import logging
import io 
from urllib.parse import urljoin, urlparse, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Dict, Optional, Set, Any

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import shutil

# -------- 检查 Pillow 库 --------
try:
    from PIL import Image, ImageFile
    from PIL.Image import UnidentifiedImageError
    ImageFile.LOAD_TRUNCATED_IMAGES = True
    Image.MAX_IMAGE_PIXELS = None
    PILLOW_AVAILABLE = True
except ImportError:
    PILLOW_AVAILABLE = False


# -------- 默认配置 --------
BASE_URLS = [
    "https://www.24fa.com/",
    "https://www.24ta.cc/",
    "https://www.24faa.cc/",
    "https://www.24me.cc/",
    "https://www.24ii.cc/",
    "https://www.24ok.cc/",
]
ALBUM_CATEGORY_PATH = "c49.aspx"
DEFAULT_SAVE_DIR = "美女图集"
DEFAULT_RETRIES = 5
DEFAULT_TIMEOUT = 20
DEFAULT_CONCURRENCY_ALBUM = 2
DEFAULT_CONCURRENCY_PAGE = 1
DEFAULT_CONCURRENCY_IMAGE = 1
DEFAULT_PAGE_SLEEP = 4.0
DEFAULT_ALBUM_DETAIL_SLEEP = 3.0
DEFAULT_IMAGE_SLEEP = 0.4
DEFAULT_POOL_SIZE = 64

# !!! (新增) FlareSolverr 配置：只用来"借"一次浏览器过 Cloudflare 拿 Cookie，
# 不是每个请求都走它，所以不会拖慢整体抓取速度。
USE_FLARESOLVERR = False                                # 改成 False 即可完全关闭，恢复原始行为
FLARESOLVERR_URL = "http://192.168.255.250:8191/v1"   # 注意必须带 /v1
FLARESOLVERR_MAX_TIMEOUT = 60000                       # 传给 FlareSolverr 浏览器等待上限(ms)

# -------- 日志设置 --------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

if not PILLOW_AVAILABLE:
    logging.warning("Pillow 库未安装。将跳过严格的图像完整性校验 (请运行: pip install Pillow)")

# -------- 辅助函数 --------
def find_active_base_url(session: requests.Session, urls: List[str], timeout: int) -> Optional[str]:
    for url in urls:
        try:
            logging.info("正在测试域名: %s", url)
            response = session.head(url, timeout=timeout)
            if response.status_code < 400:
                logging.info("域名 %s 可用", url)
                return url
        except requests.RequestException:
            logging.warning("域名 %s 测试失败", url)
    return None

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    })
    adapter = HTTPAdapter(pool_connections=DEFAULT_POOL_SIZE, pool_maxsize=DEFAULT_POOL_SIZE)
    s.mount('http://', adapter)
    s.mount('https://', adapter)
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
    r: Optional[requests.Response] = None
    for attempt in range(1, retries + 1):
        try:
            url = urljoin(BASE_URLS[0], url)  # 确保给 session.get 和 FlareSolverr 的是一条完整 URL
            r = session.get(url, timeout=timeout)

            # (新增) 疑似被 Cloudflare 拦截：先用 FlareSolverr 刷新 Cookie 再重试
            if USE_FLARESOLVERR and is_cf_blocked(r):
                logging.warning("疑似被 Cloudflare 拦截 (状态 %s): %s，尝试用 FlareSolverr 刷新 Cookie。",
                                r.status_code, url)
                fs_result = flaresolverr_get_cookies(url)
                if fs_result:
                    apply_flaresolverr_result(session, fs_result)
                    continue

            r.raise_for_status()
            if "window.location.href" in r.text and "goback" in r.text:
                logging.warning("检测到JS重定向或错误页面: %s", url)
                return None
            return r.text
        except RequestException as e:
            wait_time = min(30, (2 ** attempt) * 0.5) + random.random()
            status_msg = f"{r.status_code}" if r is not None else "无响应"
            if r is not None and r.status_code == 429 and attempt < retries:
                logging.warning("请求失败: %s (尝试 %d/%d) 错误: %s。遭遇 429，等待 %.1fs。", url, attempt, retries, e, wait_time * 2)
                time.sleep(wait_time * 2)
            elif attempt < retries:
                logging.warning("请求失败: %s (尝试 %d/%d) 错误: %s。状态: %s，等待 %.1fs 并重试。", url, attempt, retries, e, status_msg, wait_time)
                time.sleep(wait_time)
            else:
                logging.error("请求失败: %s (所有尝试均失败)。错误: %s", url, e)
    return None

def request_binary(session: requests.Session, url: str, retries: int, timeout: int, headers: Optional[Dict[str, str]] = None) -> Optional[bytes]:
    r: Optional[requests.Response] = None  # 修复 UnboundLocalError
    for attempt in range(1, retries + 1):
        try:
            url = urljoin(BASE_URLS[0], url)  # 确保给 session.get 和 FlareSolverr 的是一条完整 URL
            # 关键修复：加入 headers=headers 传递
            r = session.get(url, timeout=timeout, stream=True, headers=headers, verify=False)
            
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

def sanitize_filename(name: str, maxlen: int = 150) -> str:
    if not name: return "untitled"
    name = unquote(name).replace(".jpg_gzip.aspx", ".jpg")
    s = re.sub(r'[\0\/\\:\*\?\"<>\|]+', "_", name).strip()
    return s[:maxlen] or "untitled"

def is_image_valid_file(filepath: str, args: argparse.Namespace) -> bool:
    if args.verify and PILLOW_AVAILABLE:
        try:
            with Image.open(filepath) as img:
                img.verify()
            return True
        except (UnidentifiedImageError, ValueError, OSError, TypeError, FileNotFoundError) as e:
            logging.debug("Pillow 校验失败 (文件确认损坏或IO问题): %s. 错误: %s", filepath, e)
            return False 
        except Exception as e:
            logging.error("校验文件 %s 时发生未知异常: %s", filepath, e)
            return False
    return True

def is_image_valid_bytes(data: bytes, args: argparse.Namespace) -> bool:
    if args.verify and PILLOW_AVAILABLE:
        try:
            with Image.open(io.BytesIO(data)) as img:
                img.verify()
            return True
        except (UnidentifiedImageError, ValueError, OSError, TypeError) as e:
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
def parse_total_pages(soup: BeautifulSoup) -> int:
    pager_div = soup.find("div", class_="pager")
    if not pager_div: return 1
    page_numbers = {1}
    for li in pager_div.find_all("li"):
        text = li.get_text(strip=True)
        if text.isdigit():
            page_numbers.add(int(text))
    return max(page_numbers)

def parse_albums_on_listing_page(html: str, base_url: str) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    albums = []
    album_links = soup.select('a[href^="n"][href$=".aspx"]')
    for a_tag in album_links:
        h5_tag = a_tag.find("h5")
        if h5_tag:
            title_text = h5_tag.get_text(strip=True)
            if title_text: 
                title = sanitize_filename(title_text)
                url = urljoin(base_url, a_tag['href'])
                albums.append((title, url))
    return albums

def parse_images_on_album_page(html: str, base_url: str) -> Set[str]:
    soup = BeautifulSoup(html, "html.parser")
    image_urls = set()
    content_div = soup.find("div", id="content")
    if content_div:
        for img in content_div.find_all("img", src=True):
            src = img['src']
            if "temp_" in src:
                src = src.replace("temp_", "")
            if src.startswith("upload/") and src.endswith(".jpg_gzip.aspx"):
                image_urls.add(urljoin(base_url, src))
    return image_urls

# -------- 下载核心逻辑 (已修改) --------
def download_single_image(
    session: requests.Session, 
    url: str, 
    album_dir: str, 
    args: argparse.Namespace, 
    current_index: int, 
    total_images: int,
    album_title: str,  # [新增] 接收专辑标题
    album_url: str     # [新增] 接收专辑URL
) -> str:
    """
    下载单张图片，并在失败时打印专辑来源。
    """
    filename = sanitize_filename(os.path.basename(urlparse(url).path))
    dest_path = os.path.join(album_dir, filename)
    
    progress_prefix = f"({current_index}/{total_images})"

    # 1. 检查已存在
    if os.path.exists(dest_path):
        if is_image_valid_file(dest_path, args):
            logging.info("%s 跳过 (已存在): %s", progress_prefix, dest_path)
            return "skipped"
        else:
            # 增加来源打印
            logging.warning("%s [来源: %s] 重新下载 (文件损坏): %s", progress_prefix, album_title, dest_path)
            try:
                os.remove(dest_path)
            except OSError as e:
                logging.error("%s 无法删除损坏文件: %s", progress_prefix, e)

    # 2. 执行下载
    data = request_binary(session, url, retries=args.retries, timeout=args.timeout)
    
    if not data:
        # [修改] 打印更详细的错误信息
        logging.warning("%s [来源: %s] 下载失败 (无数据) | 链接: %s | 专辑: %s", 
                        progress_prefix, album_title, url, album_url)
        return "fail"
    
    # 3. 验证内容
    if not is_image_valid_bytes(data, args):
        # [修改] 打印更详细的错误信息
        logging.warning("%s [来源: %s] 验证失败 (损坏/HTML) 抛弃 | 链接: %s", 
                        progress_prefix, album_title, url)
        return "fail"

    # 4. 保存
    if save_bytes_atomic(dest_path, data):
        logging.info("%s 下载成功: %s", progress_prefix, dest_path)
        time.sleep(args.image_sleep + random.random() * 0.5)
        return "ok"
    else:
        logging.warning("%s [来源: %s] 保存文件失败: %s", progress_prefix, album_title, dest_path)
        return "fail"

# -------- 专辑处理主流程 (已修改) --------
def process_album(
    session: requests.Session, 
    title: str, 
    url: str, 
    save_root: str, 
    base_url: str, 
    args: argparse.Namespace, 
    album_index: int, 
    total_albums: int, 
    album_detail_sleep: float
) -> Dict[str, int]:
    
    time.sleep(album_detail_sleep + random.random() * 0.5)
    
    log_prefix = f"[专辑 {album_index}/{total_albums}] {title}"
    logging.info("%s -> 正在请求专辑首页: %s", log_prefix, url)

    first_page_html = request_text(session, url, retries=args.retries, timeout=args.timeout)
    if not first_page_html:
        logging.error("%s 无法获取专辑首页。", log_prefix)
        return {"ok": 0, "skipped": 0, "fail": 1}
    
    soup = BeautifulSoup(first_page_html, "html.parser")
    total_album_pages = parse_total_pages(soup)
    all_image_urls = parse_images_on_album_page(first_page_html, base_url)
    first_page_count = len(all_image_urls)
    
    logging.info("%s -> 专辑共 %d 页。首页解析 %d 张。开始并发获取其余分页...", 
                 log_prefix, total_album_pages, first_page_count)
    
    page_urls_to_fetch = [url.rsplit(".", 1)[0] + f"p{p}.aspx" for p in range(2, total_album_pages + 1)]
    
    with ThreadPoolExecutor(max_workers=args.page_concurrency, thread_name_prefix='PageFetcher') as executor:
        future_to_url = {executor.submit(request_text, session, page_url, args.retries, args.timeout): page_url for page_url in page_urls_to_fetch}
        for future in as_completed(future_to_url):
            page_url = future_to_url[future]
            page_html = future.result()
            if page_html:
                all_image_urls.update(parse_images_on_album_page(page_html, base_url))
            else:
                logging.warning("%s -> 获取相册分页失败: %s", log_prefix, page_url)

    if not all_image_urls:
        logging.warning("%s 未解析到任何图片。", log_prefix)
        return {"ok": 0, "skipped": 0, "fail": 0}

    total_images = len(all_image_urls)
    logging.info("%s -> 收集完成，共 %d 张图片。开始下载...", log_prefix, total_images) 

    album_dir = os.path.join(save_root, title)
    os.makedirs(album_dir, exist_ok=True)
    results = {"ok": 0, "skipped": 0, "fail": 0}
    indexed_image_urls = list(enumerate(all_image_urls, start=1))

    with ThreadPoolExecutor(max_workers=args.image_concurrency, thread_name_prefix='ImageDownloader') as executor:
        future_map = {
            executor.submit(
                download_single_image, 
                session, 
                img_url, 
                album_dir, 
                args, 
                index, 
                total_images,
                title, # [修改] 传递专辑标题
                url    # [修改] 传递专辑URL
            ): img_url 
            for index, img_url in indexed_image_urls
        }
        
        for future in as_completed(future_map):
            try:
                status = future.result()
                results[status] += 1
            except Exception:
                img_url = future_map[future]
                logging.exception("图片下载任务异常 [来源: %s] 图片: %s", title, img_url)
                results["fail"] += 1
    
    logging.info("%s -> 处理完成。成功: %d, 跳过: %d, 失败: %d", 
                 log_prefix, results['ok'], results['skipped'], results['fail'])
    return results

# -------- 主函数 --------
def main():
    parser = argparse.ArgumentParser(description="24FA系列网站图集爬虫 (最终版)", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--dir", default=DEFAULT_SAVE_DIR, help="图片保存的根目录")
    parser.add_argument("--start", type=int, default=1, help="起始列表页码")
    parser.add_argument("--end", type=int, default=0, help="结束列表页码 (0 代表自动检测)")
    parser.add_argument("-r", "--retries", type=int, default=DEFAULT_RETRIES, help="请求失败最大重试次数")
    parser.add_argument("-t", "--timeout", type=int, default=DEFAULT_TIMEOUT, help="请求超时时间(秒)")
    parser.add_argument("-c", "--album-concurrency", type=int, default=DEFAULT_CONCURRENCY_ALBUM, help="并发处理的专辑数量")
    parser.add_argument("-p", "--page-concurrency", type=int, default=DEFAULT_CONCURRENCY_PAGE, help="专辑内部并发获取分页数")
    parser.add_argument("-w", "--image-concurrency", type=int, default=DEFAULT_CONCURRENCY_IMAGE, help="专辑内部并发下载图片数")
    parser.add_argument("--page-sleep", type=float, default=DEFAULT_PAGE_SLEEP, help="爬取每个列表页后的延迟")
    parser.add_argument("--album-sleep", type=float, default=DEFAULT_ALBUM_DETAIL_SLEEP, help="每个相册详情页请求前的延迟")
    parser.add_argument("--image-sleep", type=float, default=DEFAULT_IMAGE_SLEEP, help="每张图片下载成功后的延迟")
    parser.add_argument("--verify", action="store_true", help="启用严格的图像验证(需Pillow)")
    parser.add_argument("--min-size", type=int, default=1, help="最小文件大小(KB)")
    
    args = parser.parse_args()
    session = make_session()
    base_url = find_active_base_url(session, BASE_URLS, args.timeout)
    if not base_url: return

    # --- 新增：FlareSolverr 初始握手 ---
    if USE_FLARESOLVERR:
        logging.info("正在通过 FlareSolverr 获取 Cloudflare 通行 Cookie...")
        fs_result = flaresolverr_get_cookies(base_url)
        if fs_result:
            apply_flaresolverr_result(session, fs_result)
            logging.info("Cookie 和 UA 注入完成。")
        else:
            logging.warning("FlareSolverr 过盾失败，将尝试直接请求 (可能会被 Cloudflare 拦截)。")
    # -----------------------------------

    save_root = os.path.abspath(args.dir)
    os.makedirs(save_root, exist_ok=True)
    
    main_list_url = urljoin(base_url, ALBUM_CATEGORY_PATH)
    logging.info("请求分类首页: %s", main_list_url)
    home_html = request_text(session, main_list_url, retries=args.retries, timeout=args.timeout)
    if not home_html: return

    total_site_pages = parse_total_pages(BeautifulSoup(home_html, "html.parser"))
    start_page = max(1, args.start)
    end_page = args.end if args.end > 0 and args.end >= start_page else total_site_pages
    
    summary = {"ok": 0, "skipped": 0, "fail": 0, "albums_processed": 0}
    seen_album_urls: Set[str] = set()
    all_albums_to_process = [] 

    for page_num in range(start_page, end_page + 1):
        page_url = main_list_url if page_num == 1 else main_list_url.rsplit(".", 1)[0] + f"p{page_num}.aspx"
        logging.info("列表页进度 %d/%d : %s", page_num, end_page, page_url)
        list_html = request_text(session, page_url, args.retries, args.timeout)
        if not list_html:
            time.sleep(args.page_sleep)
            continue

        current_page_albums = parse_albums_on_listing_page(list_html, base_url)
        for title, url in current_page_albums:
            if url not in seen_album_urls:
                seen_album_urls.add(url)
                all_albums_to_process.append((title, url))
        time.sleep(args.page_sleep + random.random())

    total_albums = len(all_albums_to_process)
    logging.info("共发现 %d 个待处理专辑，开始处理...", total_albums)

    with ThreadPoolExecutor(max_workers=args.album_concurrency, thread_name_prefix='AlbumProcessor') as executor:
        future_map = {
            executor.submit(process_album, session, title, url, save_root, base_url, args, i, total_albums, args.album_sleep): (title, url)
            for i, (title, url) in enumerate(all_albums_to_process, start=1)
        }
        
        for future in as_completed(future_map):
            try:
                result = future.result() 
                summary["ok"] += result["ok"]
                summary["skipped"] += result["skipped"]
                summary["fail"] += result["fail"]
                summary["albums_processed"] += 1
            except Exception as e:
                title, url = future_map[future]
                logging.error("专辑处理异常 [%s]: %s", title, e)
                
    logging.info("任务完成。成功: %d, 跳过: %d, 失败: %d", summary["ok"], summary["skipped"], summary["fail"])

if __name__ == "__main__":
    main()