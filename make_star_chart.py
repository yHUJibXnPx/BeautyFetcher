import time
import os
import re
import getpass
import requests
import logging
logging.getLogger('matplotlib.font_manager').setLevel(logging.ERROR)

from datetime import datetime

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator

USERNAME = "yHUJibXnPx"
README_FILE = "README.md"

# ================= 动态图表配置清单 =================
# 你可以定义任意数量的图表，脚本会自动匹配 README 中的对应锚点
CHARTS_CONFIG = [
    {
        "anchor": "STAR_HISTORY_SELF",           # 匹配 <!-- START_STAR_HISTORY_SELF -->
        "output": "star_history_self.png",
        "title": f"Current Repo Star History",
        "repos": [
            "BeautyFetcher"                 # 单个项目 (或当前项目)
        ]
    },
    {
        "anchor": "STAR_HISTORY_ALL",            # 匹配 <!-- START_STAR_HISTORY_ALL -->
        "output": "star_history_all.png",
        "title": f"All Projects Overview",
        "repos": [                               # 多项目对比
        ]
    }
]
# ===================================================

# 内存缓存，防止同一个 repo 的 API 被重复请求
REPO_CACHE = {}

def get_secure_token():
    """获取 Token：优先读取环境变量，其次再手动输入"""
    # 1. 优先尝试从环境变量读取 (Bash 脚本里 export 的变量会在这里被捕获)
    env_token = os.environ.get("METADATA_TOKEN")
    if env_token and env_token.strip():
        # 为了保持日志整洁，读取到环境变量时可以直接返回，不打印废话
        return env_token.strip()

    # 2. 如果环境变量里没有，才退回到原本的手动输入模式
    print("注意: \n1. 本脚本需要申请 fine-grained token \n  Repository access: All repositories\n  Permissions: Contents -> Access -> Read and write ; Metadata -> Access -> Read only")
    print("\n2. 当需要单项目统计时需要 README.md 定义锚点 \n<!-- START_STAR_HISTORY_SELF -->\n\n<!-- END_STAR_HISTORY_SELF -->\n")
    print("安全提示: 输入过程不会显示字符。")
    return getpass.getpass(prompt="请输入 GitHub Token: ")

def fetch_stargazers(repo, token):
    """带缓存的 API 请求"""
    if repo in REPO_CACHE:
        return REPO_CACHE[repo]
        
    print(f"正在获取 {repo} 的 Star 数据...")
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.star+json",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    
    star_dates = []
    url = f"https://api.github.com/repos/{USERNAME}/{repo}/stargazers"
    page = 1
    
    while True:
        try:
            response = requests.get(f"{url}?per_page=100&page={page}", headers=headers)
            response.raise_for_status()
            data = response.json()
            if not data:
                break
            for item in data:
                dt = datetime.strptime(item['starred_at'], "%Y-%m-%dT%H:%M:%SZ")
                star_dates.append(dt)
            if 'next' not in response.links:
                break
            page += 1            
            # [新增] 每次分页请求后稍微停顿半秒，极致温柔地对待 GitHub 服务器
            time.sleep(0.5)
        except requests.exceptions.RequestException as e:
            # 捕获 HTTP 错误，并尝试读取 GitHub 返回的详细错误信息
            print(f"! 获取 {repo} 数据失败: {e}")
            if e.response is not None:
                 print(f"   GitHub API 返回: {e.response.text}")
            break
            
    sorted_dates = sorted(star_dates)
    REPO_CACHE[repo] = sorted_dates # 存入缓存
    return sorted_dates


def generate_dark_chart(repos, title, output_file, token, max_display=15):
    """
    高度复刻 Star History 原版手绘风格的图表生成函数 (已增加 Top 15 过滤与防遮挡排版)
    """
    all_data = {}
    repo_star_counts = {}

    # 1. 抓取所有请求数据，并记录每个项目的最终 Star 数量
    for repo in repos:
        dates = fetch_stargazers(repo, token)
        all_data[repo] = dates
        repo_star_counts[repo] = len(dates)

    # 2. 核心截断逻辑：按 Star 数降序排序，只取前 max_display (默认 15) 名
    sorted_repos = sorted(repos, key=lambda r: repo_star_counts[r], reverse=True)
    is_truncated = len(sorted_repos) > max_display
    top_repos = sorted_repos[:max_display]

    BG_COLOR = "#0d1117"
    TEXT_COLOR = "#ffffff"
    GREEN_COLOR = "#3fb950"

    # 开启手绘风格与环境清理
    plt.xkcd(scale=1, length=100, randomness=2)
    plt.rcParams['path.effects'] = []
    plt.rcParams['axes.linewidth'] = 1.5
    plt.rcParams['font.family'] = [
        'Comic Neue', 'Comic Sans MS', 'xkcd', 'DejaVu Sans', 
        'Noto Sans CJK SC', 'sans-serif'
    ]

    fig, ax = plt.subplots(figsize=(10, 6), facecolor=BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color(TEXT_COLOR)
    ax.spines['bottom'].set_color(TEXT_COLOR)

    colors = [
        "#f85149", "#58a6ff", "#d29922", "#f0883e", 
        "#3fb950", "#bc8cff", "#79c0ff", "#a5d6ff"
    ]
    
    has_any_data = False
    max_stars = 0
    all_dates = []

    # 3. 仅绘制过滤后的 Top N 项目
    for i, repo in enumerate(top_repos):
        dates = all_data[repo]
        color = colors[i % len(colors)]
        full_repo_name = f"{USERNAME}/{repo}"
        
        if dates:
            has_any_data = True
            counts = list(range(1, len(dates) + 1))
            max_stars = max(max_stars, counts[-1])
            all_dates.extend(dates)
            
            ax.step(dates, counts, where="post", label=full_repo_name, color=color, linewidth=2)
            ax.plot(dates[-1], counts[-1], marker="o", color=color, markersize=5)
        else:
            ax.plot([], [], marker="s", linestyle="none", color=color, label=full_repo_name)

    # 坐标轴文本与刻度设置
    ax.set_ylabel("GitHub Stars", color=TEXT_COLOR, fontsize=12, labelpad=10)
    ax.set_xlabel("Timeline", color=TEXT_COLOR, fontsize=12, labelpad=10)

    if has_any_data:
        ax.set_ylim(bottom=0, top=max(max_stars * 1.15, 1))
        # 智能限制 X 轴最多显示 6-8 个时间节点，防止拥挤
        ax.xaxis.set_major_locator(MaxNLocator(nbins=6))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        # 【新增】将 X 轴日期倾斜 45 度，并靠右对齐，彻底解决重叠遮挡！
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    else:
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_xticks([])
        ax.set_yticks([1])
        ax.set_yticklabels(["0"], color=TEXT_COLOR, fontsize=12)

    # ================= 排版防遮挡核心修改区 =================

    # 4. 优化标题位置：使用 ax.set_title 而不是绝对坐标，它会自动避让图例和标签
    # 如果项目被截断，在标题自动追加 " (Top 15)" 提示
    title_suffix = f" (Top {max_display})" if is_truncated else ""
    ax.set_title(
        f"★ Star History - {title}{title_suffix}", 
        color=GREEN_COLOR, fontsize=16, fontweight="bold", pad=20
    )
    
    # 底部水印依然放在右下角 ★ star-history.com
    fig.text(0.90, 0.03, "★ star-history", color=GREEN_COLOR, fontsize=10, ha="right")

    # 5. 图例样式：回到左上角内部，并开启 80% 的不透明度
    legend = ax.legend(
        loc="upper left", 
        # 删除了 bbox_to_anchor，让它自然呆在画布内
        frameon=True, 
        facecolor=BG_COLOR, 
        edgecolor=TEXT_COLOR, 
        fontsize=9,
        framealpha=0.8 # 【新增】加入半透明效果，防止遮挡背景的折线
    )
    for text in legend.get_texts():
        text.set_color(TEXT_COLOR)

    # ========================================================
    
    ax.grid(False)
    ax.tick_params(colors=TEXT_COLOR)

    # bbox_inches="tight" 会自动把移到外面的图例包含在图片范围内，不会被切断
    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches="tight", facecolor=BG_COLOR)
    plt.close(fig)
    plt.rcdefaults()
    
    print(f"渲染成功: {output_file}")
    return True

def inject_to_readme(anchor_name, image_path):
    """根据指定的锚点名称精准替换内容，并在任何情况下保留锚点"""
    if not os.path.exists(README_FILE):
        return

    with open(README_FILE, "r", encoding="utf-8") as f:
        readme_text = f.read()

    # 动态构建正则锚点匹配
    pattern = rf"(<!-- START_{anchor_name} -->\n).*?(\n<!-- END_{anchor_name} -->)"
    
    if re.search(pattern, readme_text, flags=re.DOTALL):
        new_content = f"![Star History Chart](./{image_path})"
        updated_text = re.sub(
            pattern, 
            rf"\g<1>{new_content}\g<2>", 
            readme_text, 
            flags=re.DOTALL
        )
        with open(README_FILE, "w", encoding="utf-8") as f:
            f.write(updated_text)
        print(f"已将 [{image_path}] 插入到锚点 <!-- START_{anchor_name} --> 中。")
    else:
        # 如果 README 里没有这个锚点，直接跳过，不做报错中断
        print(f"¡ README 未找到 <!-- START_{anchor_name} --> 锚点，忽略该图表插入。")

if __name__ == "__main__":
    token = get_secure_token()
    if not token.strip():
        print("错误: Token 不能为空。")
        exit(1)

    # 遍历所有配置的图表
    for config in CHARTS_CONFIG:
        anchor = config["anchor"]
        output = config["output"]
        title = config["title"]
        repos = config["repos"]
        # [新增] 每个项目请求完毕后停顿 1 秒
        time.sleep(1)
        # 检查 README 中是否有这个锚点，只有包含锚点时才去抓 API 和渲染图片（省去无效的 API 请求）
        with open(README_FILE, "r", encoding="utf-8") as f:
            content = f.read()
            if f"<!-- START_{anchor} -->" not in content:
                print(f"跳过图表 [{anchor}]：README 中未预留此锚点。")
                continue

        # 渲染图表
        generate_dark_chart(repos, title, output, token)
        # 注入 README
        inject_to_readme(anchor, output)
