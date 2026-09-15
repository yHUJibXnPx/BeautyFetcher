#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
凸凹吧 (24ao.cc / nnao.cc) 图集爬虫
  — 基于 24FA 系列爬虫重构，适配新站点结构 —

【主要变更点（相对旧版24fa脚本）】
  1. BASE_URLS         → 新站备用域名
  2. 列表分页URL        → ?Page=N 参数格式（旧：c49p2.aspx）
  3. 列表页总数探测     → 依赖 » 按钮逐步探测（旧：直接读最大页码）
  4. 专辑列表解析       → a.index-imgcontent-title（旧：a[href^="n"] + h5）
  5. 专辑详情分页URL    → Content/2631_2.html（旧：np2.aspx）
  6. 详情页图片解析     → #pageContainer img[src^="/LoadImage.ashx/"]
  7. sanitize_filename → 去掉 .jpg_gzip.aspx 处理，改为处理 /LoadImage.ashx/ 路径

【不变部分】
  - 三级并发框架（专辑/分页/图片）
  - 指数退避重试 + 429 特判
  - 原子写入 / 断点续传
  - Pillow 图像验证（--verify）
  - 所有 CLI 参数
"""
import base64
import os
import re
import time
import random
import argparse
import logging
import io
import hashlib
from urllib.parse import urljoin, urlparse, urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Dict, Optional, Set, Any

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import shutil

# -------- Pillow 可选依赖 --------
try:
    from PIL import Image, ImageFile
    from PIL.Image import UnidentifiedImageError
    ImageFile.LOAD_TRUNCATED_IMAGES = True
    Image.MAX_IMAGE_PIXELS = None
    PILLOW_AVAILABLE = True
except ImportError:
    PILLOW_AVAILABLE = False


# ======================================================================
# 配置区 — 只需改这里就能适配域名/分类变化
# ======================================================================

BASE_URLS = [
    "https://www.24ao.cc/",
    "https://www.nnao.cc/",
]

# 分类路径（对应文件分析中的 5 个栏目）：
#   最新:    Articles
#   无圣光:  Articles/Categories/1
#   凸凹图:  Articles/Categories/2
#   视频:    Articles/Categories/3
#   写真集:  Articles/Categories/4
# 分类映射字典：编号 -> (栏目名称, URL路径)
CATEGORY_MAP = {
    0: ("最新", "Articles"),
    1: ("无圣光", "Articles/Categories/1"),
    2: ("凸凹图", "Articles/Categories/2"),
    3: ("靓人体", "Articles/Categories/3"),
    4: ("写真集", "Articles/Categories/4"),
}

DEFAULT_CATEGORY = 0  # 默认爬取 0: 最新

DEFAULT_SAVE_DIR       = "美女图集"
DEFAULT_RETRIES        = 5
DEFAULT_TIMEOUT        = 20
DEFAULT_CONCURRENCY_ALBUM = 2      # 并发专辑数（新站反爬更严，调低）
DEFAULT_CONCURRENCY_IMAGE = 1      # 专辑内并发下载图片数
DEFAULT_PAGE_SLEEP        = 4.0    # 列表页爬取间隔
DEFAULT_ALBUM_DETAIL_SLEEP = 3   # 专辑详情页请求前延迟
DEFAULT_IMAGE_SLEEP       = 0.4    # 单图下载后延迟
DEFAULT_POOL_SIZE         = 64

# !!! (新增) FlareSolverr 配置：只用来"借"一次浏览器过 Cloudflare 拿 Cookie，
# 不是每个请求都走它，所以不会拖慢整体抓取速度。
USE_FLARESOLVERR = False                                # 改成 False 即可完全关闭，恢复原始行为
FLARESOLVERR_URL = "http://192.168.255.250:8191/v1"   # 注意必须带 /v1
FLARESOLVERR_MAX_TIMEOUT = 60000                       # 传给 FlareSolverr 浏览器等待上限(ms)

# ======================================================================
# 日志
# ======================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

if not PILLOW_AVAILABLE:
    logging.warning("Pillow 未安装，将跳过图像完整性校验 (pip install Pillow)")


# ======================================================================
# 网络工具
# ======================================================================

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8", # 必须加上，模拟真实浏览器
        "Connection": "keep-alive",
    })
    # 增加重试机制和连接池
    adapter = HTTPAdapter(pool_connections=DEFAULT_POOL_SIZE, pool_maxsize=DEFAULT_POOL_SIZE)
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
    
def find_active_base_url(session: requests.Session,
                         urls: List[str],
                         timeout: int) -> Optional[str]:
    for url in urls:
        try:
            logging.info("测试域名: %s", url)
            r = session.head(url, timeout=timeout, verify=False, allow_redirects=True)
            if r.status_code < 400:
                logging.info("可用域名: %s", url)
                return url
        except RequestException:
            logging.warning("域名不可用: %s", url)
    return None


def request_text(session: requests.Session,
                 url: str,
                 retries: int,
                 timeout: int) -> Optional[str]:
    r: Optional[requests.Response] = None
    for attempt in range(1, retries + 1):
        try:
            url = urljoin(BASE_URLS[0], url)  # 确保给 session.get 和 FlareSolverr 的是一条完整 URL
            r = session.get(url, timeout=timeout, verify=False)

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
                logging.warning("JS重定向页面，跳过: %s", url)
                return None
            return r.text
        except RequestException as e:
            wait = min(30, (2 ** attempt) * 0.5) + random.random()
            status = f"{r.status_code}" if r is not None else "无响应"
            if r is not None and r.status_code == 429 and attempt < retries:
                logging.warning("[%d/%d] 429 限速，等待 %.1fs: %s",
                                attempt, retries, wait * 2, url)
                time.sleep(wait * 2)
            elif attempt < retries:
                logging.warning("[%d/%d] 请求失败 %s，等待 %.1fs: %s",
                                attempt, retries, status, wait, url)
                time.sleep(wait)
            else:
                logging.error("请求彻底失败: %s — %s", url, e)
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


# ======================================================================
# 文件工具
# ======================================================================

def sanitize_filename(name: str, maxlen: int = 150) -> str:
    """清洗文件名/文件夹名。"""
    if not name:
        return "untitled"
    # 新站图片路径形如 /LoadImage.ashx/Abc...xyz123.jpg
    # 取最后一段作为文件名
    name = name.split("/")[-1]
    # 去掉可能的查询参数
    name = name.split("?")[0]
    s = re.sub(r'[\0\/\\:\*\?\"<>\|]+', "_", name).strip()
    return s[:maxlen] or "untitled"


def image_filename_from_url(url: str) -> str:
    """
    从 LoadImage.ashx URL 生成稳定文件名。
    URL 末尾自带哈希段（如 ...3257cd46802b.jpg），直接用它做文件名。
    若解析不到合理扩展名，回退到 URL 的 md5 前缀 + .jpg。
    """
    path = urlparse(url).path          # /LoadImage.ashx/Abc...802b.jpg
    basename = path.split("/")[-1]     # Abc...802b.jpg
    # 检查是否有图片扩展名
    if re.search(r'\.(jpe?g|png|gif|webp)$', basename, re.I):
        return sanitize_filename(basename)
    # 回退：用URL哈希
    h = hashlib.md5(url.encode()).hexdigest()[:12]
    return f"{h}.jpg"


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

# ======================================================================
# 图像验证
# ======================================================================

def is_image_valid_file(filepath: str, args: argparse.Namespace) -> bool:
    if args.verify and PILLOW_AVAILABLE:
        try:
            with Image.open(filepath) as img:
                img.verify()
            return True
        except Exception as e:
            logging.debug("Pillow 校验失败 %s: %s", filepath, e)
            return False
    return True


def is_image_valid_bytes(data: bytes, args: argparse.Namespace) -> bool:
    if args.verify and PILLOW_AVAILABLE:
        try:
            with Image.open(io.BytesIO(data)) as img:
                img.verify()
            return True
        except Exception as e:
            logging.debug("Pillow 字节校验失败: %s", e)
            return False
    return True


# ======================================================================
# ★ 解析函数 — 这里是相对旧版改动最大的部分 ★
# ======================================================================

def has_next_page(soup: BeautifulSoup) -> bool:
    """
    【新】列表页分页探测。
    新站的列表页只显示有限页码 + » 号，不显示末页总数。
    只要存在 aria-label="Next" 的链接，就说明还有更多页。
    """
    pagination = soup.find("ul", class_="pagination")
    if not pagination:
        return False
    next_li = pagination.find("a", {"aria-label": "Next"})
    return next_li is not None


# ======================================================================
# 优化后的解析函数
# ======================================================================

def parse_albums_on_listing_page(html: str,
                                 base_url: str) -> List[Tuple[str, str]]:
    """
    【新】从列表页解析 (标题, URL) 列表。
    新站结构:
      <a href="/Articles/Content/2631.html" class="index-imgcontent-title">标题</a>
    """
    soup = BeautifulSoup(html, "html.parser")
    albums = []
    seen_urls: Set[str] = set()

    for a in soup.select("a.index-imgcontent-title"):
        href = a.get("href", "").strip()
        title_text = a.get_text(strip=True)
        if not href or not title_text:
            continue
        # 只处理详情页链接
        if not re.match(r"^/Articles/Content/\d+\.html$", href):
            continue
        url = urljoin(base_url, href)
        if url in seen_urls:
            continue
        seen_urls.add(url)
        albums.append((sanitize_filename(title_text), url))
    logging.debug("列表页找到 %d 个专辑链接", len(albums))
    return albums

def parse_images_from_imgdata(html: str, base_url: str) -> Set[str]:
    """
    从隐藏字段 <input id="imgData"> 中解码图片URL。
    该字段存储 Base64(图片路径) 的管道符分隔列表。
    """
    soup = BeautifulSoup(html, "html.parser")
    image_urls: Set[str] = set()

    imgdata_input = soup.find("input", {"id": "imgData"})
    if not imgdata_input:
        logging.debug("未找到 imgData 字段")
        return image_urls

    raw_value = imgdata_input.get("value", "").strip()
    if not raw_value:
        return image_urls

    for b64_part in raw_value.split("|"):
        b64_part = b64_part.strip()
        if not b64_part:
            continue
        try:
            decoded = base64.b64decode(b64_part).decode("utf-8")
            if "/loadimage.ashx/" in decoded.lower():
                image_urls.add(urljoin(base_url, decoded))
        except Exception as e:
            logging.debug("imgData 解码失败: %s — %s", b64_part[:20], e)

    return image_urls

def build_list_page_url(base_category_url: str, page: int) -> str:
    """
    【新】构造列表分页URL。
    第1页: https://www.24ao.cc/Articles/Categories/2
    第N页: https://www.24ao.cc/Articles/Categories/2?Page=N
    """
    if page <= 1:
        return base_category_url
    return f"{base_category_url}?Page={page}"


# ======================================================================
# 修复下载函数中的变量错误
# ======================================================================

def download_single_image(session: requests.Session,
                          url: str,
                          album_dir: str,
                          args: argparse.Namespace,
                          current_index: int,
                          total_images: int,
                          album_url: str) -> str:
    filename = image_filename_from_url(url)
    dest_path = os.path.join(album_dir, filename)
    prefix = f"({current_index}/{total_images})"

    if os.path.exists(dest_path) and is_image_valid_file(dest_path, args):
        return "skipped"

    # --- 修复：Referer 使用当前专辑的 URL，而不是 base_url ---
    img_headers = {
        "Referer": album_url, 
        "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }
    
    data = request_binary(session, url, args.retries, args.timeout, headers=img_headers)
    if not data:
        logging.warning("%s 下载失败(无数据): %s", prefix, url)
        return "fail"

    if not is_image_valid_bytes(data, args):
        logging.warning("%s 内容校验失败(非图片): %s", prefix, url)
        return "fail"

    if save_bytes_atomic(dest_path, data):
        logging.info("%s 成功: %s", prefix, filename)
        time.sleep(args.image_sleep + random.random() * 0.3)
        return "ok"
    return "fail"


# ======================================================================
# 专辑处理流程
# ======================================================================

def process_album(session, title, url, save_root, base_url, args,
                  album_index, total_albums, album_detail_sleep):
    time.sleep(album_detail_sleep + random.random() * 0.5)
    prefix = f"[专辑 {album_index}/{total_albums}] {title}"
    logging.info("%s → 请求首页: %s", prefix, url)

    first_html = request_text(session, url, args.retries, args.timeout)
    if not first_html:
        logging.error("%s 首页获取失败", prefix)
        return {"ok": 0, "skipped": 0, "fail": 1}

    # ★ imgData 已包含全部图片，无需翻子页
    all_image_urls = parse_images_from_imgdata(first_html, base_url)
    total_images = len(all_image_urls)

    logging.info("%s → 从 imgData 解析到 %d 张图", prefix, total_images)

    if not total_images:
        logging.warning("%s 未解析到任何图片", prefix)
        return {"ok": 0, "skipped": 0, "fail": 0}

    album_dir = os.path.join(save_root, title)
    os.makedirs(album_dir, exist_ok=True)

    results = {"ok": 0, "skipped": 0, "fail": 0}
    indexed = list(enumerate(all_image_urls, start=1))

    with ThreadPoolExecutor(max_workers=args.image_concurrency,
                            thread_name_prefix="ImageDL") as ex:
        future_map = {
            ex.submit(download_single_image, session, img_url,
                      album_dir, args, idx, total_images, url): img_url
            for idx, img_url in indexed
        }
        for future in as_completed(future_map):
            try:
                results[future.result()] += 1
            except Exception:
                logging.exception("图片任务异常: %s", future_map[future])
                results["fail"] += 1

    logging.info("%s → 完成。成功:%d 跳过:%d 失败:%d",
                 prefix, results["ok"], results["skipped"], results["fail"])
    ## --- 添加以下两行调试代码 ---
    ## 运行脚本后打开 debug.html，如果里面全是脚本或者提示“请开启 Javascript”，那就说明 requests 已经无法直接抓取该站
    #with open("debug.html", "w", encoding="utf-8") as f:
    #    f.write(first_html)
    # --------------------------
    # 日志调试
    #if first_html:
    #    soup = BeautifulSoup(first_html, "html.parser")
    #    print("=== LoadImage 数量 ===", first_html.count("LoadImage.ashx"))
    #    print("=== data-original 数量 ===", first_html.count("data-original"))
    #    
    #    # 打印所有可能的图片相关标签
    #    imgs = soup.find_all("img")
    #    print(f"找到 {len(imgs)} 个 <img> 标签")
    #    for i, img in enumerate(imgs[:10]):   # 只打印前10个
    #        print(f"IMG {i}: data-original={img.get('data-original')} | src={img.get('src')}")
    #    
    #    # 查找 script 中是否包含图片数据
    #    scripts = soup.find_all("script")
    #    for script in scripts:
    #        if script.string and "LoadImage" in script.string:
    #            print("发现包含 LoadImage 的 script 标签！")
    #            print(script.string[:500])  # 打印前500字符
    #            break
    return results


# ======================================================================
# 主函数
# ======================================================================

def main():
    parser = argparse.ArgumentParser(
        description="凸凹吧 (24ao.cc) 图集爬虫",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-d", "--dir", default=DEFAULT_SAVE_DIR,
                        help="图片保存根目录")
    parser.add_argument(
        "-c", "--category", 
        type=int, 
        default=DEFAULT_CATEGORY,
        help="指定爬取栏目: 0=最新, 1=无圣光, 2=凸凹图, 3=靓人体, 4=写真集, -1=全部栏目"
    )
    parser.add_argument("--start", type=int, default=1,
                        help="起始列表页码")
    parser.add_argument("--end", type=int, default=0,
                        help="结束列表页码（0=自动探测到末页）")
    parser.add_argument("-r", "--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("-t", "--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("-a", "--album-concurrency", type=int,
                        default=DEFAULT_CONCURRENCY_ALBUM,
                        help="并发处理的专辑数量")
    parser.add_argument("-w", "--image-concurrency", type=int,
                        default=DEFAULT_CONCURRENCY_IMAGE)
    parser.add_argument("--page-sleep", type=float, default=DEFAULT_PAGE_SLEEP)
    parser.add_argument("--album-sleep", type=float,
                        default=DEFAULT_ALBUM_DETAIL_SLEEP)
    parser.add_argument("--image-sleep", type=float, default=DEFAULT_IMAGE_SLEEP)
    parser.add_argument("--verify", action="store_true",
                        help="启用 Pillow 图像完整性校验（需安装 Pillow）")
    parser.add_argument("--min-size", type=int, default=1,
                        help="最小文件大小(KB)，--verify 未启用时后备检查")
    args = parser.parse_args()

    session = make_session()
    base_url = find_active_base_url(session, BASE_URLS, args.timeout)
    if base_url:
        session.headers.update({"Referer": base_url})
    if not base_url:
        logging.critical("所有备用域名均不可用，退出。")
        return

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

    # ----------------------------------------------------------------
    # 列表页遍历 (支持单栏目或全栏目遍历并全局去重)
    # 新站列表页用 » 号探测是否有下一页，不能预知总页数，
    # 所以这里用"爬到没有 » 为止"的策略；
    # 若用户指定了 --end，则以用户指定为准。
    # ----------------------------------------------------------------
    all_albums_to_process: List[Tuple[str, str]] = []
    seen_album_urls: Set[str] = set()

    # 路由选择逻辑
    if args.category == -1:
        # 提取字典中所有的 (栏目名, 路径) 元组
        target_categories = list(CATEGORY_MAP.values())
        logging.info("已启用全栏目遍历模式 (共 %d 个栏目)，将自动处理跨栏目重复图集。", len(target_categories))
    else:
        # 输入合法性校验
        if args.category not in CATEGORY_MAP:
            logging.critical("无效的栏目编号: %d。请查看 --help 获取支持的编号。", args.category)
            return
        # 将单选包装成列表，统一后续的 for 循环逻辑
        target_categories = [CATEGORY_MAP[args.category]]

    # 开始遍历选定的栏目
    for cat_name, cat_path in target_categories:
        category_url = urljoin(base_url, cat_path)
        logging.info("========== 开始收集栏目: [%s] ==========", cat_name)
        
        page_num = args.start
        while True:
            if args.end > 0 and page_num > args.end:
                break

            page_url = build_list_page_url(category_url, page_num)
            logging.info("正在获取 [%s] 列表页 %d: %s", cat_name, page_num, page_url)

            list_html = request_text(session, page_url, args.retries, args.timeout)
            if not list_html:
                logging.warning("[%s] 列表页获取失败，停止当前栏目翻页: %s", cat_name, page_url)
                break

            soup = BeautifulSoup(list_html, "html.parser")
            current_albums = parse_albums_on_listing_page(list_html, base_url)

            newly_added = 0
            for title, url in current_albums:
                # 全局去重：只收集没见过的专辑 URL
                if url not in seen_album_urls:
                    seen_album_urls.add(url)
                    all_albums_to_process.append((title, url))
                    newly_added += 1

            logging.info("[%s] 列表页 %d 收集完成，新增 %d 个专辑（当前全局累计待处理 %d 个）",
                         cat_name, page_num, newly_added, len(all_albums_to_process))

            # 判断是否继续翻页
            if args.end == 0 and not has_next_page(soup):
                logging.info("[%s] 已到达最后一页（无 » 按钮），当前栏目收集完毕。", cat_name)
                break

            page_num += 1
            time.sleep(args.page_sleep + random.random())

    # ----------------------------------------------------------------
    # 并发处理所有专辑
    # ----------------------------------------------------------------
    total_albums = len(all_albums_to_process)
    if total_albums == 0:
        logging.warning("未找到任何专辑，程序结束。")
        return

    logging.info("=" * 60)
    logging.info("列表遍历完毕，共 %d 个专辑，开始并发处理…", total_albums)
    logging.info("=" * 60)

    summary = {"ok": 0, "skipped": 0, "fail": 0, "albums_processed": 0}

    with ThreadPoolExecutor(max_workers=args.album_concurrency,
                            thread_name_prefix="AlbumProcessor") as executor:
        future_map: Dict[Any, Tuple[str, str, int]] = {}
        for index, (title, url) in enumerate(all_albums_to_process, start=1):
            f = executor.submit(
                process_album,
                session, title, url, save_root, base_url, args,
                index, total_albums, args.album_sleep
            )
            future_map[f] = (title, url, index)

        logging.info("已提交 %d 个专辑任务，等待完成…", total_albums)

        for future in as_completed(future_map):
            album_title, album_url, idx = future_map[future]
            try:
                result = future.result()
                for k in ("ok", "skipped", "fail"):
                    summary[k] += result[k]
            except Exception as e:
                logging.error("[%d/%d] 专辑异常 [%s]: %s",
                              idx, total_albums, album_title, e)
            summary["albums_processed"] += 1

    logging.info("=" * 70)
    logging.info("任务完成汇总:")
    logging.info("  处理专辑: %d / %d", summary["albums_processed"], total_albums)
    logging.info("  成功下载: %d 张", summary["ok"])
    logging.info("  跳过(已存在): %d 张", summary["skipped"])
    logging.info("  失败: %d 张", summary["fail"])
    logging.info("=" * 70)


if __name__ == "__main__":
    main()