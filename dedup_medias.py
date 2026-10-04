#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dedup_medias.py  (v3) — 只处理“绝对重复”：字节完全相同的文件

这个脚本不做任何“相似”判断：不抽帧、不解码、不算感知哈希。
一个文件只有在满足下面全部条件时才会被认定为另一个文件的重复：
  1. 文件大小完全相同
  2. 头 / 中 / 尾若干段内容的指纹相同（只是为了避免对大量文件做整文件比较的初筛）
  3. 两个文件从头到尾逐字节比较，完全一致        ← 最终裁决，不依赖任何哈希
字节完全相同 => 画面、声音、元数据必然完全相同，不存在“看起来像但其实不同”的误判。

安全设计
  * 默认只生成报告（duplicates_report.txt），不移动任何文件；加 --apply 才会真正移动
  * 被判定为重复的文件不会被删除，而是移到隔离区，并写入 actions.jsonl，可用 --undo 还原
  * 报告里每一组都写明：保留哪一个（KEEP）、移走哪些（MOVE），不会再出现“不知道为什么没了”
  * 硬链接（同一个文件的多个路径）不算重复，不会处理
  * 只依赖 Python 标准库（装了 tqdm 会显示进度条）

用法
  python3 dedup_medias.py normal_video sex_video picture            # 只生成报告，不动任何文件
  python3 dedup_medias.py normal_video sex_video picture --apply    # 确认报告没问题后，真正移动
  python3 dedup_medias.py picture --scope dir                       # 只在【同一个目录内】找重复
  python3 dedup_medias.py --undo --quarantine ./quarantine_duplicates               # 还原全部
  python3 dedup_medias.py --undo --undo-kind exact --quarantine ./quarantine_duplicates
  python3 dedup_medias.py --undo --undo-kind near  --quarantine ./quarantine_duplicates   # 还原旧版“近似”移走的文件
一个例子
  cd /media/gmktecm6/TOSHIBABLACK2T/mixed
  # 第一步：只出报告，不动任何文件
  python3 dedup_medias.py normal_video sex_video picture
  # 第二步：打开 duplicates_report.txt 看过没问题后，再加 --apply
  python3 dedup_medias.py normal_video sex_video picture --apply

保留规则（同一组完全相同的文件里留哪一个）
  1) 命令行里靠前的目录优先；2) 修改时间更早的优先；3) 路径更短的优先；4) 路径字典序。
"""
import argparse
import hashlib
import json
import os
import shutil
import sys
import time
from collections import defaultdict
from typing import Dict, List, NamedTuple, Optional

try:
    from tqdm import tqdm
except ImportError:                               # 没装 tqdm 也能跑，只是没有进度条
    def tqdm(it=None, **kwargs):
        return it

# --------- Config ----------
QUARANTINE_NAME = "quarantine_duplicates"
ACTIONS_NAME    = "actions.jsonl"
REPORT_NAME     = "duplicates_report.txt"
EMPTY_LOG       = "empty_files.log"

IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".avif")
VIDEO_EXTS = (".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm", ".ts", ".m4v")
SKIP_DIRS  = {"$RECYCLE.BIN", "System Volume Information", "@eaDir", "lost+found", "__MACOSX"}

# 旧版本的参数。它们已不存在；遇到时明确报错，避免有人以为它们还在起作用。
REMOVED_FLAGS = ("--near", "--near-images", "--exact-only", "--delete-bad", "--ssim_threshold",
                 "--workers", "--video_workers", "--frames", "--hash_alg", "--max_hamming",
                 "--image_hamming", "--min_match_ratio", "--pos_tol", "--duration_tol", "--db")


class F(NamedTuple):
    path: str
    mtime: int
    size: int
    dev: int
    ino: int
    ridx: int          # 所属目录在命令行里的序号（越小越优先保留）


# --------- Scanning ----------
def gather(roots: List[str], qdir: str, exts: tuple):
    """扫描所有目录；返回 (文件列表, 0 字节文件列表, 被忽略的硬链接数)。"""
    files: List[F] = []
    empties: List[str] = []
    for ridx, root in enumerate(roots):
        stack = [root]
        while stack:
            d = stack.pop()
            try:
                it = os.scandir(d)
            except OSError as e:
                print(f"  [warn] 无法读取目录 {d}: {e}")
                continue
            with it:
                for e in it:
                    name = e.name
                    try:
                        if e.is_dir(follow_symlinks=False):
                            if name.startswith(".") or name in SKIP_DIRS or e.path == qdir:
                                continue
                            stack.append(e.path)
                        elif e.is_file(follow_symlinks=False):
                            if name.startswith("._"):                 # macOS AppleDouble 元数据，不是真文件
                                continue
                            if os.path.splitext(name)[1].lower() not in exts:
                                continue
                            st = e.stat(follow_symlinks=False)
                            if st.st_size == 0:
                                empties.append(e.path)
                                continue
                            files.append(F(e.path, int(st.st_mtime), st.st_size, st.st_dev, st.st_ino, ridx))
                    except OSError as ex:
                        print(f"  [warn] 读取失败 {e.path}: {ex}")

    files.sort(key=lambda f: (f.ridx, f.path))
    uniq, seen, hardlinks = [], set(), 0
    for f in files:                                                   # 同一个 inode 的多个路径 = 同一个文件，不是重复
        if f.ino:
            k = (f.dev, f.ino)
            if k in seen:
                hardlinks += 1
                continue
            seen.add(k)
        uniq.append(f)
    return uniq, sorted(empties), hardlinks


# --------- Exact comparison ----------
def quick_fingerprint(path: str, size: int, chunk: int = 1 << 20) -> bytes:
    """初筛用：大小 + 头/1/3处/2/3处/尾各 1MB。只用来缩小范围，不用来下结论。"""
    h = hashlib.blake2b(digest_size=16)
    h.update(str(size).encode())
    with open(path, "rb") as fh:
        if size <= 4 * chunk:
            h.update(fh.read())
        else:
            for off in (0, size // 3, 2 * size // 3, size - chunk):
                fh.seek(off)
                h.update(fh.read(chunk))
    return h.digest()


def files_identical(a: str, b: str, bufsize: int = 8 << 20) -> bool:
    """逐字节比较整个文件。任何一个字节不同立即返回 False。"""
    with open(a, "rb") as fa, open(b, "rb") as fb:
        while True:
            x, y = fa.read(bufsize), fb.read(bufsize)
            if x != y:
                return False
            if not x:
                return True


def keep_key(f: F):
    return (f.ridx, f.mtime, len(f.path), f.path)


def find_duplicate_groups(files: List[F], scope: str, errors: List[str]) -> List[List[F]]:
    """返回若干组“逐字节完全相同”的文件，每组至少 2 个，组内已按保留优先级排序（第 0 个保留）。"""
    buckets: Dict[tuple, List[F]] = defaultdict(list)
    for f in files:
        key = (f.size, os.path.dirname(f.path)) if scope == "dir" else (f.size,)
        buckets[key].append(f)
    cands = [g for g in buckets.values() if len(g) > 1]
    print(f"  大小相同的候选组: {len(cands):,}（涉及 {sum(len(g) for g in cands):,} 个文件）")

    groups: List[List[F]] = []
    for g in tqdm(cands, desc="Comparing", unit="grp"):
        by_fp: Dict[bytes, List[F]] = defaultdict(list)
        for f in g:
            try:
                by_fp[quick_fingerprint(f.path, f.size)].append(f)
            except OSError as e:
                errors.append(f"读取失败（已跳过）: {f.path}  [{e}]")
        for same_fp in by_fp.values():
            if len(same_fp) < 2:
                continue
            classes: List[List[F]] = []                               # 把指纹相同的文件按“逐字节相同”分类
            for f in same_fp:
                for cls in classes:
                    try:
                        same = files_identical(cls[0].path, f.path)
                    except OSError as e:
                        errors.append(f"比较失败（已跳过）: {f.path}  [{e}]")
                        same = False
                        break
                    if same:
                        cls.append(f)
                        break
                else:
                    classes.append([f])
            groups.extend(sorted(c, key=keep_key) for c in classes if len(c) > 1)
    groups.sort(key=lambda g: g[0].path)
    return groups


# --------- Quarantine ----------
def unique_dest(dest: str) -> str:
    if not os.path.lexists(dest):
        return dest
    base, ext = os.path.splitext(dest)
    n = 1
    while os.path.lexists(f"{base}.dup{n}{ext}"):
        n += 1
    return f"{base}.dup{n}{ext}"


def move_file(src: str, dest: str) -> str:
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    dest = unique_dest(dest)                                          # 永不覆盖隔离区里已有的文件
    try:
        os.rename(src, dest)
    except OSError:                                                   # 跨文件系统：复制后再删除
        shutil.move(src, dest)
    return dest


def root_labels(roots: List[str]) -> List[str]:
    used, labels = set(), []
    for r in roots:
        base = os.path.basename(r.rstrip(os.sep)) or "root"
        label, n = base, 1
        while label in used:
            n += 1
            label = f"{base}_{n}"
        used.add(label)
        labels.append(label)
    return labels


def write_action(qdir: str, rec: dict):
    line = json.dumps(rec, ensure_ascii=False)
    try:
        line.encode("utf-8")
    except UnicodeEncodeError:                                        # 文件名含无法用 UTF-8 表示的字节
        line = json.dumps(rec, ensure_ascii=True)
    with open(os.path.join(qdir, ACTIONS_NAME), "a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def apply_groups(groups: List[List[F]], roots: List[str], qdir: str, errors: List[str]):
    labels = root_labels(roots)
    moved = reclaimed = 0
    todo = sum(len(g) - 1 for g in groups)
    bar = tqdm(total=todo, desc="Moving", unit="file")
    for g in groups:
        keep = g[0]
        for f in g[1:]:
            rel = f.path[len(roots[f.ridx]) + 1:]
            dest = os.path.join(qdir, labels[f.ridx], rel)
            try:
                dest = move_file(f.path, dest)
            except OSError as e:
                errors.append(f"移动失败: {f.path}  [{e}]")
                continue
            write_action(qdir, {"t": time.time(), "kind": "exact", "keep": keep.path, "remove": f.path,
                                "dest": dest, "size": f.size})
            moved += 1
            reclaimed += f.size
            if bar is not None and hasattr(bar, "update"):
                bar.update(1)
    if bar is not None and hasattr(bar, "close"):
        bar.close()
    return moved, reclaimed


# --------- Report ----------
def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return f"{n:.1f} TB"


def write_report(groups: List[List[F]], path: str):
    with open(path, "w", encoding="utf-8", errors="replace") as fh:
        fh.write(f"# 字节完全相同的文件组，共 {len(groups)} 组（KEEP = 保留，MOVE = 将被移到隔离区）\n\n")
        for i, g in enumerate(groups, 1):
            fh.write(f"[{i}] size={g[0].size} ({human(g[0].size)})  copies={len(g)}\n")
            fh.write(f"  KEEP  {g[0].path}\n")
            for f in g[1:]:
                fh.write(f"  MOVE  {f.path}\n")
            fh.write("\n")


# --------- Undo ----------
def undo(qdir: str, kind: Optional[str] = None):
    fp = os.path.join(qdir, ACTIONS_NAME)
    if not os.path.exists(fp):
        print(f"找不到 {fp}")
        return
    with open(fp, "r", encoding="utf-8") as fh:
        recs = [json.loads(line) for line in fh if line.strip()]
    if kind:
        recs = [r for r in recs if r.get("kind") == kind]
    restored = conflict = missing = 0
    for r in tqdm(list(reversed(recs)), desc="Restoring", unit="file"):
        src, dst = r["dest"], r["remove"]
        if not os.path.exists(src):
            missing += 1                       # 隔离区里已经没有这个文件（可能之前已还原或被手动处理）
        elif os.path.exists(dst):
            conflict += 1                      # 原位置已有同名文件，不覆盖
        else:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.move(src, dst)
            restored += 1
    print(f"已还原 {restored} 个（范围：{kind or '全部'}，共 {len(recs)} 条记录；"
          f"隔离区里已不存在 {missing}，原位置已有同名文件 {conflict}）")


# --------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser(description="只处理字节完全相同的重复文件（默认只出报告，--apply 才移动）")
    p.add_argument("roots", nargs="*", help="要扫描的目录（可多个；靠前的目录里的文件优先保留）")
    p.add_argument("--apply", action="store_true", help="真正把重复文件移到隔离区（不加则只生成报告）")
    p.add_argument("--scope", choices=("all", "dir"), default="all",
                   help="all=所有目录之间互相比较（默认）；dir=只在同一个目录内找重复")
    p.add_argument("--ext", default=None, help="要处理的扩展名，逗号分隔，如 .mkv,.mp4（默认：常见图片+视频）")
    p.add_argument("--quarantine", default=None,
                   help=f"隔离区目录（默认：第一个目录的上一级/{QUARANTINE_NAME}，与源文件同盘以便直接 rename）")
    p.add_argument("--report", default=REPORT_NAME, help=f"报告文件路径（默认 ./{REPORT_NAME}）")
    p.add_argument("--dry-run", action="store_true", help=argparse.SUPPRESS)     # 兼容旧习惯：现在默认就是只出报告
    p.add_argument("--undo", action="store_true", help="按隔离区的 actions.jsonl 把文件还原回原位置")
    p.add_argument("--undo-kind", choices=("exact", "near", "corrupt"), default=None,
                   help="配合 --undo：只还原某一类（exact=字节相同；near/corrupt 是旧版本留下的记录）")
    args = p.parse_args()
    if not args.undo and not args.roots:
        p.error("需要至少一个目录")
    return args


def reject_removed_flags(argv: List[str]):
    hit = sorted({a.split("=")[0] for a in argv if a.split("=")[0] in REMOVED_FLAGS})
    if hit:
        print("这些参数在 v3 里已经不存在：", " ".join(hit))
        print("v3 只处理“字节完全相同”的文件，不再有近似判断、抽帧、SSIM、损坏检测，也不再自动删除任何文件。")
        print("请去掉它们后重新运行（默认只出报告，加 --apply 才会移动到隔离区）。")
        sys.exit(2)


def main():
    reject_removed_flags(sys.argv[1:])
    args = parse_args()

    if args.undo:
        undo(os.path.abspath(args.quarantine or QUARANTINE_NAME), args.undo_kind)
        return

    roots: List[str] = []
    for r in args.roots:
        rp = os.path.realpath(r)
        if not os.path.isdir(rp):
            print("Invalid root:", r)
            sys.exit(1)
        if rp not in roots:
            roots.append(rp)                                          # 保持命令行顺序（决定保留优先级）
    roots = [r for i, r in enumerate(roots)
             if not any(r.startswith(o + os.sep) for o in roots if o != r)]   # 去掉被其它目录包含的子目录
    qdir = os.path.abspath(args.quarantine) if args.quarantine \
        else os.path.join(os.path.dirname(roots[0]), QUARANTINE_NAME)

    exts = tuple(e.strip().lower() if e.strip().startswith(".") else "." + e.strip().lower()
                 for e in args.ext.split(",") if e.strip()) if args.ext else IMAGE_EXTS + VIDEO_EXTS

    print("扫描目录 …")
    files, empties, hardlinks = gather(roots, qdir, exts)
    print(f"Found {len(files):,} files  (roots: {', '.join(roots)})")
    if hardlinks:
        print(f"  已忽略 {hardlinks:,} 个硬链接路径（与另一个路径是同一个文件，不算重复）")
    if empties:
        with open(EMPTY_LOG, "w", encoding="utf-8", errors="replace") as fh:
            fh.write("\n".join(empties) + "\n")
        print(f"  发现 {len(empties):,} 个 0 字节文件（脚本不会处理它们，列表见 {EMPTY_LOG}）")

    errors: List[str] = []
    groups = find_duplicate_groups(files, args.scope, errors)
    n_move = sum(len(g) - 1 for g in groups)
    reclaim = sum(f.size for g in groups for f in g[1:])
    write_report(groups, args.report)

    print(f"\n字节完全相同的重复组: {len(groups):,}；其中可移走 {n_move:,} 个文件，共 {human(reclaim)}")
    print(f"完整报告（每组写明保留哪个、移走哪些）: {os.path.abspath(args.report)}")
    for g in groups[:5]:
        print(f"  例: KEEP {g[0].path}")
        for f in g[1:]:
            print(f"      MOVE {f.path}")

    if not groups:
        pass
    elif not args.apply:
        print("\n[未移动任何文件] 请先查看报告；确认无误后加 --apply 再运行一次。")
    else:
        os.makedirs(qdir, exist_ok=True)
        moved, reclaimed = apply_groups(groups, roots, qdir, errors)
        print(f"\n已移动 {moved:,} 个文件到隔离区（{human(reclaimed)}）: {qdir}")
        print(f"还原: python3 {os.path.basename(sys.argv[0])} --undo --quarantine {qdir}")
        print("隔离区的文件不会被自动删除；确认无误后，你可以自己删除隔离区释放空间。")

    if errors:
        print(f"\n有 {len(errors)} 个文件读取/比较/移动失败（未做任何处理）：")
        for e in errors[:50]:
            print("  ", e)
        if len(errors) > 50:
            print(f"  … 其余 {len(errors) - 50} 条省略")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n已中断。已经移动的文件都记录在隔离区的 actions.jsonl 里，可用 --undo 还原。")
        sys.exit(130)