#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
mitaku.net 图集爬虫 (专业级重构版 - 健壮性强化)

功能:
  - 爬取 mitaku.net 网站的图集。
  - (新增) 图像内容验证：自动检测并重新下载损坏的图片 (需 Pillow 库)。

流程:
  域名测试 -> 获取总页数 -> 遍历列表页 -> 提取所有专辑链接 (先收集)
  └→ (并发处理) -> (延迟) -> 进入专辑页 -> 解析图片总数和首图URL
    └→ 根据模式生成所有图片URL
      └→ (并发下载) -> (新增) 验证图片内容 -> 下载图片并保存

特点:
  - 域名测试: 自动寻找可用的域名。
  - 特殊解析: 根据首图URL和总数进行图片URL推导。
  - 两级并发: 并发处理多个图集，同时在每个图集内部并发下载图片。
  - (新增) 图像验证: 
    - 通过 --verify 启用，可识别并修复已存在的损坏文件。
    - 验证新下载的内容，防止 HTML 错误页被保存。
  - 防护增强: 
    - 增加连接池 (HTTPAdapter)。
    - 增加列表页爬取延迟 (page-sleep)。
    - 新增专辑详情页请求延迟 (album-sleep) 以防止 429。
  - 详细日志: 已实现 [专辑 X/N] 和 (图片 Y/Z) 进度打印。
"""
import os
import re
import time
import random
import argparse
import logging
import io # (新增)
from urllib.parse import urljoin, urlparse, unquote
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
BASE_URLS = [
    "https://mitaku.net",
    # 可以添加其他可能的镜像站
]
DEFAULT_SAVE_DIR = "美女图集"
DEFAULT_RETRIES = 5
DEFAULT_TIMEOUT = 15
DEFAULT_CONCURRENCY_ALBUM = 4       # 并发处理相册数量
DEFAULT_CONCURRENCY_IMAGE = 1       # 每个相册内部并发下载图片数量
DEFAULT_PAGE_SLEEP = 3.0            # (强化) 爬取每个列表页后的延迟
DEFAULT_ALBUM_DETAIL_SLEEP = 2.0    # (新增) 请求每个专辑详情页前的延迟
DEFAULT_IMAGE_SLEEP = 0.4           # 每张图片下载成功后的短暂延迟
DEFAULT_POOL_SIZE = 64              # (新增) 连接池大小

# !!! (新增) FlareSolverr 配置：只用来"借"一次浏览器过 Cloudflare 拿 Cookie，
# 不是每个请求都走它，所以不会拖慢整体抓取速度。
USE_FLARESOLVERR = False                                # 改成 False 即可完全关闭，恢复原始行为
FLARESOLVERR_URL = "http://192.168.255.250:8191/v1"   # 注意必须带 /v1
FLARESOLVERR_MAX_TIMEOUT = 60000                       # 传给 FlareSolverr 浏览器等待上限(ms)

# -------- 日志设置 --------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

if not PILLOW_AVAILABLE:
    logging.warning("Pillow 库未安装。将跳过严格的图像完整性校验 (请运行: pip install Pillow)")

# -------- 辅助函数 (强化版) --------
def find_active_base_url(session: requests.Session, urls: List[str], timeout: int) -> Optional[str]:
    """测试URL列表，返回第一个可用的URL。"""
    for url in urls:
        try:
            logging.info("正在测试域名: %s", url)
            response = session.head(url, timeout=timeout, allow_redirects=True)
            if response.status_code < 400:
                logging.info("域名 %s 可用", response.url)
                return response.url.rstrip('/')
        except requests.RequestException:
            logging.warning("域名 %s 测试失败", url)
    return None

def make_session() -> requests.Session:
    """(强化) 创建并配置requests.Session (含HTTPAdapter)。"""
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
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
    
def request_text(session: requests.Session, url: str, retries: int, timeout: int) -> Optional[str]:
    """(强化) 请求文本页面，带重试、指数退避和 429/UnboundLocalError 修复。"""
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
            
            r.raise_for_status() # 检查 4xx/5xx 错误
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

def request_binary(session: requests.Session, url: str, retries: int, timeout: int) -> Optional[bytes]:
    """(强化) 请求二进制文件(图片)，带重试、指数退避和 429/UnboundLocalError 修复。"""
    r: Optional[requests.Response] = None # 修复 UnboundLocalError
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, timeout=timeout, stream=True)
            
            # (新增) 疑似被 Cloudflare 拦截：期望拿到图片，却收到 403/503 或 HTML 页面
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


# -------- 解析函数 --------
def parse_total_pages(html: str) -> int:
    """从首页HTML中稳健地解析出总页数。"""
    soup = BeautifulSoup(html, "html.parser")
    # 策略1: 'Page 1 of N'
    span = soup.select_one("div.wp-pagenavi span.pages")
    if span:
        match = re.search(r'of\s+(\d+)', span.get_text(), re.I)
        if match:
            return int(match.group(1))
    # 策略2: 最后一个页码链接 `a.last`
    a_last = soup.select_one("div.wp-pagenavi a.last[href]")
    if a_last:
        match = re.search(r'/page/(\d+)/', a_last["href"])
        if match:
            return int(match.group(1))
    # 策略3: 所有页码链接中的最大值
    page_numbers = {1}
    for a in soup.select("div.wp-pagenavi a[href]"):
        match = re.search(r'/page/(\d+)/', a["href"])
        if match:
            page_numbers.add(int(match.group(1)))
    return max(page_numbers)

def parse_albums_on_page(html: str, base_url: str) -> List[Tuple[str, str]]:
    """从列表页HTML中解析出(标题, URL)元组列表。"""
    soup = BeautifulSoup(html, "html.parser")
    albums = []
    for art in soup.select("article"):
        # 优先从特色图片链接中获取
        a_tag = art.select_one(".featured-image a[href]") or art.select_one("h2.entry-title a[href]")
        if a_tag:
            href = a_tag.get("href", "").strip()
            # 优先使用 title 属性，否则使用文本内容
            title = a_tag.get("title") or a_tag.get_text(strip=True) or ""
            if href:
                albums.append((title.strip(), urljoin(base_url, href)))
    return albums

def parse_album_details(html: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[int]]:
    """从相册详情页解析：post_id, 标题, 首图URL, 图片总数。"""
    soup = BeautifulSoup(html, "html.parser")
    post_id, title, first_img_url, total = None, None, None, None
    # Post ID
    art = soup.find("article", id=re.compile(r'post-\d+'))
    if art: post_id = art["id"].replace('post-', '')
    # Title
    title_tag = soup.select_one("h1.entry-title")
    if title_tag: title = title_tag.get_text(strip=True)
    # First Image URL
    a_tag = soup.select_one("a.msacwl-img-link[data-mfp-src], a[data-mfp-src]")
    if a_tag:
        first_img_url = a_tag.get("data-mfp-src", "").split("?")[0]
    else:
        # 尝试从第一张 img 标签中获取
        img_tag = soup.select_one("div.msacwl-img-wrap img[src], article img[src]")
        if img_tag: first_img_url = img_tag.get("src", "").split("?")[0]
    
    # Total Images (从文本中获取: "Content: 91 Pics and 12 Videos" 或 "Image: 15 Pics")
    text_match = soup.find(string=re.compile(r"Image\s*:\s*\d+\s*Pics|Content\s*:\s*\d+\s*Pics", re.I))
    if text_match:
        match = re.search(r"(\d+)\s*Pics", text_match, re.I)
        if match: total = int(match.group(1))
    
    return post_id, title, first_img_url, total

def build_image_list_from_pattern(first_url: str, total: int) -> List[str]:
    """根据首图URL和总数，通过模式匹配生成图片URL列表。"""
    # 尝试匹配格式: .../xxx-1.jpg
    match = re.match(r'(.+?)-1(\.[A-Za-z0-9]+)$', first_url)
    if not match: 
        logging.warning("首图URL '%s' 不符合生成模式 '...-1.ext'", first_url)
        return []
        
    prefix, suffix = match.groups()
    
    # 检查 total 是否合理
    if total <= 0:
        logging.warning("图片总数不合理: %d", total)
        return []
        
    # 从 1 到 total 生成 URL 列表
    return [f"{prefix}-{i}{suffix}" for i in range(1, total + 1)]

def fallback_parse_images(html: str) -> List[str]:
    """
    【修补后的逻辑】
    直接解析 HTML，从 <a class="msacwl-img-link"> 标签的 data-mfp-src 属性中提取所有图片 URL。
    这种方法最可靠，因为它获取的是网页实际加载的完整链接，不受文件后缀不一致的影响。
    """
    soup = BeautifulSoup(html, 'html.parser')
    
    # 使用CSS选择器或属性查找来定位所有图片链接元素
    # 目标：查找所有具有 data-mfp-src 属性的 <a> 标签
    # 根据滚动源码.txt，img_link 类也是一个明确的标识
    img_urls = set()
    
    # 查找所有符合条件的 <a> 标签
    for a_tag in soup.find_all('a', class_='msacwl-img-link', attrs={'data-mfp-src': True}):
        url = a_tag.get('data-mfp-src')
        if url:
            img_urls.add(url) # 使用 set 自动去重
            
    # 返回一个排序后的列表
    return sorted(list(img_urls))


# -------- 下载核心逻辑 --------
def download_single_image(session: requests.Session, url: str, album_dir: str, args: argparse.Namespace, current_index: int, total_images: int) -> str:
    """
    (已更新) 下载单张图片，并根据文件大小和内容(Pillow)验证有效性。
    """
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
                os.remove(dest_path) # 删除损坏的旧文件
            except OSError as e:
                logging.error("%s 无法删除旧的损坏文件: %s", progress_prefix, e)

    # 2. 执行下载 (文件不存在 或 文件损坏)
    data = request_binary(session, url, retries=args.retries, timeout=args.timeout)
    
    if not data:
        logging.warning("%s 下载失败 (未获取到数据): %s", progress_prefix, url)
        return "fail"
    
    # 3. 验证新下载的数据 (在保存前)
    if not is_image_valid_bytes(data, args):
        logging.warning("%s 下载的内容验证失败(损坏或HTML)，抛弃: %s", progress_prefix, url)
        return "fail"

    # 4. 保存 (数据已验证)
    if save_bytes_atomic(dest_path, data):
        logging.info("%s 下载成功: %s", progress_prefix, filename)
        time.sleep(args.image_sleep + random.random() * 0.5)
        return "ok"
    
    logging.warning("%s 下载后保存文件失败: %s", progress_prefix, dest_path)
    return "fail"

# -------- 相册处理主流程 --------
# -------- 相册处理主流程 --------
def process_album(session: requests.Session, title: str, url: str, save_root: str, base_url: str, args: argparse.Namespace, album_index: int, total_albums: int, album_detail_sleep: float) -> Dict[str, int]:
    """
    (强化) 处理单个相册：(延迟) -> 获取详情 -> 生成链接列表 -> 并发下载。
    """
    time.sleep(album_detail_sleep + random.random() * 0.5)
    
    # 1. 确保日志前缀使用正确的变量 (album_index, total_albums)
    album_prefix = f"[{album_index}/{total_albums}] {title}"
    logging.info("%s -> 正在请求详情页: %s", album_prefix, url)
    
    # 2. 【关键】获取 HTML 文本，并赋值给 html 变量
    html = request_text(session, url, retries=args.retries, timeout=args.timeout)
    if not html:
        logging.error("%s -> 无法获取相册页面，跳过。", album_prefix)
        return {"ok": 0, "skipped": 0, "fail": 1}

    # 3. 解析相册元数据（为了命名文件夹）
    post_id, real_title, first_img, total = parse_album_details(html)
    folder_name = f"{post_id} - {sanitize_filename(real_title or title)}" if post_id else sanitize_filename(real_title or title)
    album_dir = os.path.join(save_root, folder_name)

    # 4. 【核心修复】直接使用健壮的 HTML 解析，不再推导链接
    logging.info("%s -> 正在从 HTML 源码中提取所有图片链接...", album_prefix)
    img_urls = fallback_parse_images(html) # 使用已定义的 html 变量
    
    # 原有的模式推导逻辑 (L412-L422) 已被移除/注释
    
    final_urls: List[str] = sorted(list({urljoin(base_url, u) for u in img_urls})) # 去重并排序
    
    if not final_urls:
        logging.warning("%s -> 未解析到任何图片，跳过。", album_prefix)
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
                index, total_images # 进度参数
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
    parser = argparse.ArgumentParser(description="Mitaku.net 图集爬虫 (专业级重构版 - 健壮性强化)", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--dir", default=DEFAULT_SAVE_DIR, help="图片保存的根目录")
    parser.add_argument("--start", type=int, default=1, help="起始列表页码")
    parser.add_argument("--end", type=int, default=0, help="结束列表页码 (0 代表自动检测)")
    parser.add_argument("-r", "--retries", type=int, default=DEFAULT_RETRIES, help="请求失败最大重试次数")
    parser.add_argument("-t", "--timeout", type=int, default=DEFAULT_TIMEOUT, help="请求超时时间(秒)")
    parser.add_argument("-c", "--album-concurrency", type=int, default=DEFAULT_CONCURRENCY_ALBUM, help="并发处理的相册数量")
    parser.add_argument("-w", "--image-concurrency", type=int, default=DEFAULT_CONCURRENCY_IMAGE, help="相册内部并发下载图片数")
    parser.add_argument("--page-sleep", type=float, default=DEFAULT_PAGE_SLEEP, help="爬取每个列表页后的延迟(秒)")
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

    session = make_session()
    base_url = find_active_base_url(session, BASE_URLS, args.timeout)
    if not base_url:
        logging.critical("所有备用域名都无法访问，程序退出。")
        return

    # (新增) 确定可用域名后，先用 FlareSolverr 过一次盾，把 Cookie/UA 灌进 session
    if USE_FLARESOLVERR:
        logging.info("正在通过 FlareSolverr 获取 Cloudflare 通行 Cookie...")
        fs_result = flaresolverr_get_cookies(base_url)
        if fs_result:
            apply_flaresolverr_result(session, fs_result)
            logging.info("Cookie 注入完成。")
        else:
            logging.warning("FlareSolverr 过盾失败，将尝试直接请求 (可能会被 Cloudflare 拦截)。")
        
    save_root = os.path.abspath(args.dir)
    os.makedirs(save_root, exist_ok=True)
    
    home_html = request_text(session, base_url, args.retries, args.timeout)
    if not home_html:
        logging.critical("无法获取网站首页，程序退出。")
        return

    total_site_pages = parse_total_pages(home_html)
    logging.info("检测到网站总页数: %d", total_site_pages)

    start_page = max(1, args.start)
    end_page = args.end if args.end > 0 and args.end >= start_page else total_site_pages
    
    summary: Dict[str, int] = {"ok": 0, "skipped": 0, "fail": 0, "albums_processed": 0}
    seen_album_urls: Set[str] = set()
    all_albums_to_process: List[Tuple[str, str]] = [] # 收集所有专辑

    # 1. 发现阶段：串行遍历列表页，收集所有专辑链接
    for page_num in range(start_page, end_page + 1):
        page_url = f"{base_url}/page/{page_num}/" if page_num > 1 else base_url
        logging.info("开始发现列表页 %d/%d -> %s", page_num, end_page, page_url)
        
        list_html = request_text(session, page_url, args.retries, args.timeout)
        if not list_html:
            logging.warning("获取列表页失败: %s", page_url)
            time.sleep(args.page_sleep + random.random()) # 失败也要延迟
            continue

        albums_on_page = parse_albums_on_page(list_html, base_url)
        logging.info("列表页 %d/%d 找到 %d 个专辑。", page_num, end_page, len(albums_on_page)) 
        
        for title, url in albums_on_page:
            if url not in seen_album_urls:
                seen_album_urls.add(url)
                all_albums_to_process.append((title, url))
                
        time.sleep(args.page_sleep + random.random())

    # 2. 处理阶段：并发处理收集到的所有专辑
    total_albums = len(all_albums_to_process)
    if total_albums == 0:
        logging.warning("未找到任何新相册，程序结束。")
        return
        
    logging.info("所有列表页遍历完毕，共发现 %d 个待处理相册。开始并发处理...", total_albums)
    logging.info("=" * 50)
    
    with ThreadPoolExecutor(max_workers=args.album_concurrency, thread_name_prefix='AlbumProcessor') as executor:
        future_map: Dict[Any, Tuple[str, str, int]] = {}
        
        for index, (title, url) in enumerate(all_albums_to_process, start=1):
            future = executor.submit(
                process_album, 
                session, title, url, save_root, base_url, args,
                index, total_albums, args.album_sleep # 传递全局进度和延迟
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
                logging.exception("[%d/%d] 处理相册 '%s' 时发生未捕获的异常: %s", index, total_albums, title, url)
                summary["albums_processed"] += 1
    
    logging.info("=" * 50)
    logging.info("所有任务完成！")
    logging.info(
        "处理相册数: %d / %d, 成功下载: %d, 跳过: %d, 失败: %d",
        summary["albums_processed"], total_albums, summary["ok"], summary["skipped"], summary["fail"]
    )
    logging.info("=" * 50)

if __name__ == "__main__":
    main()