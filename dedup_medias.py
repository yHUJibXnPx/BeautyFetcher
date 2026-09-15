#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stream-frame pHash video dedupe with SQLite cache.
Unified image + video deduplication:
- Parallel image hashing (ThreadPoolExecutor)
- Correct batch SQLite inserts (no per-row SELECT after executemany)
- find_candidates rewritten: one GROUP BY query instead of N single queries
- JPEG temp frames, ffmpeg -an to avoid opus errors
- dry-run, quarantine, actions.log, bad_videos.log

Install:
  python3 -m pip install imagehash pillow opencv-python numpy tqdm scikit-image ffmpeg-python
  sudo apt install ffmpeg

Usage:
  python3 dedup_medias.py "./sex_video"
  python3 dedup_medias.py "./normal_video" --workers 1 --dry-run
  python3 dedup_medias.py "美女图集"
  # 需要精确验证时
  python3 dedup_medias.py "美女图集" --ssim_threshold 0.85
  python3 dedup_medias.py "美女图集" --workers 1 --dry-run
"""
import os,io
import sys
import argparse
import sqlite3
import subprocess
import time
import shutil
from pathlib import Path
from typing import List, Tuple, Optional, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

from PIL import Image
import imagehash
import numpy as np
from tqdm import tqdm

try:
    from skimage.metrics import structural_similarity as ssim
except Exception:
    ssim = None

# --------- Config ----------
DB_FILE        = "media_frames.db"
ACTIONS_LOG    = "actions.log"
QUARANTINE_DIR = "quarantine_duplicates"
#TMP_FRAME_DIR  = ".dedup_tmp_frames"
TMP_FRAME_DIR  = "/dev/shm/.dedup_tmp_frames" if os.path.exists("/dev/shm") else "/tmp/.dedup_tmp_frames"
BAD_LOG        = "bad_medias.log"
DB_BATCH_SIZE  = 500

IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".avif")
VIDEO_EXTS = (".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm", ".ts", ".m4v")

# --------- Logging ----------
def log_bad(path: str, reason: str):
    try:
        with open(BAD_LOG, "a", encoding="utf-8") as f:
            f.write(f"{time.time()},{path},{reason}\n")
    except Exception:
        pass

def print_bad_summary(delete_bad: bool = False):
    """
    Print bad-media summary, write delete_bad.sh, and optionally delete the files.
    Only files whose reason starts with hash_failed are treated as truly corrupt
    and included in the delete script / auto-deletion.
    Other reasons (stat_failed, move_failed) are listed but left alone.
    """
    DELETE_SCRIPT = "delete_bad.sh"

    if not os.path.exists(BAD_LOG):
        print("No bad media logged.")
        return

    all_bad: dict = {}   # path -> last reason
    corrupt: list = []   # hash_failed only, in order

    with open(BAD_LOG, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(",", 2)
            if len(parts) >= 3:
                _, fpath, reason = parts[0], parts[1], parts[2]
            elif len(parts) == 2:
                fpath, reason = parts[1], ""
            else:
                continue
            if fpath not in all_bad:
                all_bad[fpath] = reason
                if reason.startswith("hash_failed"):
                    corrupt.append(fpath)

    print(f"\n=== 有问题的媒体文件（共 {len(all_bad)} 个，详见 {BAD_LOG}）===")
    for fpath, reason in all_bad.items():
        print(f"  [{reason[:40]}]  {fpath}")

    if corrupt:
        with open(DELETE_SCRIPT, "w", encoding="utf-8") as sh:
            sh.write("#!/usr/bin/env bash\n")
            sh.write(f"# 由 dedup_medias.py 自动生成 — 共 {len(corrupt)} 个无法解码的损坏文件\n")
            sh.write("# 确认无误后执行：bash delete_bad.sh\n\n")
            for p in corrupt:
                quoted = "'" + p.replace("'", "'\''") + "'"
                sh.write(f"rm -fv {quoted}\n")
        os.chmod(DELETE_SCRIPT, 0o755)
        print(f"\n已生成删除脚本：{DELETE_SCRIPT}（{len(corrupt)} 个损坏文件）")
        print(f"  确认后执行：bash {DELETE_SCRIPT}")

        if delete_bad:
            print(f"\n--delete-bad 已启用，开始删除 {len(corrupt)} 个损坏文件 …")
            deleted, failed = 0, 0
            for p in corrupt:
                try:
                    os.unlink(p)
                    deleted += 1
                except Exception as e:
                    print(f"  删除失败: {p}  ({e})")
                    failed += 1
            print(f"  删除完成：成功 {deleted} 个，失败 {failed} 个")
    else:
        print("  无损坏文件（hash_failed），无需生成删除脚本。")

# --------- DB ----------
def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA synchronous=NORMAL")
    cur.execute("PRAGMA cache_size=-65536")
    cur.execute("PRAGMA temp_store=MEMORY")
    cur.execute("""CREATE TABLE IF NOT EXISTS media (
                    id         INTEGER PRIMARY KEY,
                    path       TEXT UNIQUE,
                    mtime      INTEGER,
                    size       INTEGER,
                    media_type TEXT
                   )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS frames (
                    id       INTEGER PRIMARY KEY,
                    media_id INTEGER,
                    t_sec    REAL,
                    hash     TEXT,
                    w        INTEGER,
                    h        INTEGER,
                    FOREIGN KEY(media_id) REFERENCES media(id)
                   )""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_frames_hash     ON frames(hash)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_frames_media_id ON frames(media_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_path      ON media(path)")
    conn.commit()

# --------- Helpers ----------
def file_meta(path: Path) -> Tuple[int, int]:
    st = path.stat()
    return int(st.st_mtime), int(st.st_size)

def is_image(path: Path) -> bool:
    return path.suffix.lower() in IMAGE_EXTS

def is_video(path: Path) -> bool:
    return path.suffix.lower() in VIDEO_EXTS

def gather_media(root: Path) -> List[Path]:
    items = []
    for p in root.rglob("*"):
        if p.is_file() and (is_image(p) or is_video(p)):
            items.append(p)
    return sorted(items)

def hash_image(path: Path, alg: str, hash_size: int) -> Tuple[Optional[str], Optional[int], Optional[int]]:
    """Open image once; return (hash_hex, width, height)."""
    try:
        with Image.open(path) as im:
            im = im.convert("RGB")
            w, h = im.size
            if alg == "phash":
                hv = imagehash.phash(im, hash_size=hash_size)
            elif alg == "dhash":
                hv = imagehash.dhash(im, hash_size=hash_size)
            else:
                hv = imagehash.average_hash(im, hash_size=hash_size)
            return str(hv), w, h
    except Exception as e:
        log_bad(str(path), f"hash_failed:{e}")
        return None, None, None

def cleanup_tmp_frames():
    d = Path(TMP_FRAME_DIR)
    if not d.exists():
        return
    for f in d.glob("*"):
        try:
            f.unlink()
        except Exception:
            pass

# --------- ffmpeg ----------
def extract_frames(video_path: Path, fps: float, width: int) -> List[Path]:
    out_dir = Path(TMP_FRAME_DIR)
    out_dir.mkdir(exist_ok=True)
    stamp   = int(time.time() * 1_000_000)
    pattern = str(out_dir / f"f{stamp}_%06d.jpg")
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-an", "-i", str(video_path),
        "-vf", f"fps={fps},scale={width}:-1:flags=lanczos",
        "-q:v", "5", "-y", pattern,
    ]
    try:
        res = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if res.returncode != 0:
            err = (res.stderr or b"").decode(errors="ignore").strip()
            log_bad(str(video_path), f"ffmpeg_failed:{err[:300]}")
            return []
    except Exception as e:
        log_bad(str(video_path), f"ffmpeg_exception:{e}")
        return []
    frames = sorted(out_dir.glob(f"f{stamp}_*.jpg"))
    if not frames:
        log_bad(str(video_path), "no_frames_extracted")
    return frames

def extract_frame_hashes_in_memory(video_path: Path, fps: float, width: int, hash_alg: str, hash_size: int) -> List[Tuple[str, int, int]]:
    """FFmpeg 抽帧全在内存中进行，不写入任何磁盘临时文件"""
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-an", "-i", str(video_path),
        "-vf", f"fps={fps},scale={width}:-1:flags=lanczos",
        "-f", "image2pipe", "-vcodec", "mjpeg", "-"  # 输出到 stdout 管道
    ]
    try:
        res = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if res.returncode != 0:
            err = (res.stderr or b"").decode(errors="ignore").strip()
            log_bad(str(video_path), f"ffmpeg_failed:{err[:300]}")
            return []
    except Exception as e:
        log_bad(str(video_path), f"ffmpeg_exception:{e}")
        return []

    # 从内存二进制流中切割 JPEG 图像块并计算哈希
    data = res.stdout
    frame_hashes = []
    start = 0
    while True:
        soi = data.find(b'\xff\xd8', start)  # JPEG 文件头
        if soi == -1: break
        eoi = data.find(b'\xff\xd9', soi)   # JPEG 文件尾
        if eoi == -1: break
        
        jpg_bytes = data[soi:eoi + 2]
        start = eoi + 2
        
        try:
            with Image.open(io.BytesIO(jpg_bytes)) as im:
                im = im.convert("RGB")
                w, h = im.size
                if hash_alg == "phash":
                    hv = imagehash.phash(im, hash_size=hash_size)
                elif hash_alg == "dhash":
                    hv = imagehash.dhash(im, hash_size=hash_size)
                else:
                    hv = imagehash.average_hash(im, hash_size=hash_size)
                frame_hashes.append((str(hv), w, h))
        except Exception:
            pass
            
    if not frame_hashes:
        log_bad(str(video_path), "no_frames_extracted")
    return frame_hashes
    
# --------- Worker ----------
def _hash_worker(args_tuple):
    p, alg, hash_size = args_tuple
    try:
        mtime, size = file_meta(p)
    except Exception as e:
        log_bad(str(p), f"stat_failed:{e}")
        return None
    hv, w, h = hash_image(p, alg, hash_size)
    if hv is None:
        return None
    return (str(p), mtime, size, hv, w, h)

# --------- Indexing ----------
def _parallel_hash(paths: List[Path], alg: str, hash_size: int, workers: int, desc: str) -> List[Tuple]:
    results = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_hash_worker, (p, alg, hash_size)): p for p in paths}
        with tqdm(total=len(paths), desc=desc, unit="file", leave=False) as pb:
            for fut in as_completed(futs):
                pb.update(1)
                try:
                    r = fut.result()
                    if r:
                        results.append(r)
                except Exception as e:
                    log_bad(str(futs[fut]), f"worker_exc:{e}")
    return results

def _batch_insert_images(cur, conn, hashed: List[Tuple], mid_map: Optional[Dict[str,int]] = None):
    """
    Insert or update image media rows and their frame hashes in batches.
    mid_map: if provided, maps path_str -> existing media_id (update mode).
             if None, new inserts.
    """
    for i in range(0, len(hashed), DB_BATCH_SIZE):
        batch = hashed[i : i + DB_BATCH_SIZE]

        if mid_map is None:
            # New rows: INSERT then batch SELECT to get assigned ids
            cur.executemany(
                "INSERT OR IGNORE INTO media (path, mtime, size, media_type) VALUES (?,?,?,'image')",
                [(r[0], r[1], r[2]) for r in batch],
            )
            conn.commit()
            placeholders = ",".join("?" * len(batch))
            cur.execute(
                f"SELECT path, id FROM media WHERE path IN ({placeholders})",
                [r[0] for r in batch],
            )
            path_to_id = dict(cur.fetchall())
            frame_rows = [
                (path_to_id[r[0]], None, r[3], r[4], r[5])
                for r in batch if r[0] in path_to_id
            ]
        else:
            # Changed rows: delete old frames, update mtime/size, re-insert frames
            mids = [(mid_map[r[0]],) for r in batch if r[0] in mid_map]
            if mids:
                cur.executemany("DELETE FROM frames WHERE media_id=?", mids)
            cur.executemany(
                "UPDATE media SET mtime=?, size=? WHERE id=?",
                [(r[1], r[2], mid_map[r[0]]) for r in batch if r[0] in mid_map],
            )
            frame_rows = [
                (mid_map[r[0]], None, r[3], r[4], r[5])
                for r in batch if r[0] in mid_map
            ]

        if frame_rows:
            cur.executemany(
                "INSERT INTO frames (media_id, t_sec, hash, w, h) VALUES (?,?,?,?,?)",
                frame_rows,
            )
        conn.commit()


# --------- Stale DB cleanup ----------
def cleanup_stale_db(conn: sqlite3.Connection, current_paths: List[Path]):
    """
    Remove DB entries for files that no longer exist on disk.
    Called once per run with the result of gather_media(), so no extra stat() calls needed.
    Uses the same temp-table trick to avoid SQLite 999-variable limit.
    """
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM media")
    total_before = cur.fetchone()[0]
    if total_before == 0:
        return

    # Build set of current paths (already stat'd by gather_media)
    current_set = {str(p) for p in current_paths}

    # Load all DB paths, find which are missing
    cur.execute("SELECT id, path FROM media")
    stale = [(row[0],) for row in cur.fetchall() if row[1] not in current_set]

    if not stale:
        print(f"  DB stale check: all {total_before:,} entries still on disk, nothing to clean.")
        return

    print(f"  DB stale cleanup: removing {len(stale):,} entries no longer on disk …")
    # Batch delete frames then media
    for i in range(0, len(stale), DB_BATCH_SIZE):
        batch = stale[i : i + DB_BATCH_SIZE]
        cur.executemany("DELETE FROM frames WHERE media_id=?", batch)
        cur.executemany("DELETE FROM media  WHERE id=?",       batch)
    conn.commit()
    print(f"  DB stale cleanup done. Entries before: {total_before:,}  removed: {len(stale):,}")

def index_media(conn: sqlite3.Connection, media_paths: List[Path],
                fps: float, hash_alg: str, hash_size: int,
                frame_width: int, workers: int):
    cur = conn.cursor()

    cur.execute("SELECT id, path, mtime, size FROM media")
    existing: Dict[str, Tuple[int,int,int]] = {
        row[1]: (row[0], row[2], row[3]) for row in cur.fetchall()
    }

    images = [p for p in media_paths if is_image(p)]
    videos = [p for p in media_paths if is_video(p)]
    print(f"  Images: {len(images):,}   Videos: {len(videos):,}")

    # Partition images
    new_images:     List[Path]             = []
    changed_images: List[Tuple[Path, int]] = []
    for p in images:
        ps = str(p)
        if ps in existing:
            mid, ex_mt, ex_sz = existing[ps]
            try:
                mt, sz = file_meta(p)
            except Exception:
                continue
            if ex_mt != mt or ex_sz != sz:
                changed_images.append((p, mid))
        else:
            new_images.append(p)

    if new_images:
        print(f"  New images: {len(new_images):,}")
        hashed = _parallel_hash(new_images, hash_alg, hash_size, workers, "  Hashing new images")
        _batch_insert_images(cur, conn, hashed, mid_map=None)

    if changed_images:
        print(f"  Changed images: {len(changed_images):,}")
        paths_only = [p for p, _ in changed_images]
        mid_map    = {str(p): mid for p, mid in changed_images}
        hashed     = _parallel_hash(paths_only, hash_alg, hash_size, workers, "  Re-hashing changed images")
        _batch_insert_images(cur, conn, hashed, mid_map=mid_map)

    # Videos
    for p in tqdm(videos, desc="Videos", unit="vid"):
        ps = str(p)
        try:
            mtime, size = file_meta(p)
        except Exception as e:
            log_bad(ps, f"stat_failed:{e}")
            continue

        if ps in existing:
            mid, ex_mt, ex_sz = existing[ps]
            if ex_mt == mtime and ex_sz == size:
                continue
            cur.execute("DELETE FROM frames WHERE media_id=?", (mid,))
            cur.execute("UPDATE media SET mtime=?, size=? WHERE id=?", (mtime, size, mid))
            conn.commit()
            media_id = mid
        else:
            cur.execute(
                "INSERT OR IGNORE INTO media (path, mtime, size, media_type) VALUES (?,?,?,'video')",
                (ps, mtime, size),
            )
            conn.commit()
            media_id = cur.lastrowid

        #frame_paths = extract_frames(p, fps, frame_width)
        #if not frame_paths:
        #    continue

        #idx_map   = {str(fp): idx for idx, fp in enumerate(frame_paths)}
        #hashed    = _parallel_hash(frame_paths, hash_alg, hash_size,
        #                           min(workers, len(frame_paths)), "  Frames")
        #frame_rows = [
        #    (media_id, idx_map.get(r[0], 0) / max(fps, 1e-6), r[3], r[4], r[5])
        #    for r in hashed
        #]
        ## Clean up temp frame files
        #for fp in frame_paths:
        #    try: fp.unlink()
        #    except Exception: pass

        #if frame_rows:
        #    cur.executemany(
        #        "INSERT INTO frames (media_id, t_sec, hash, w, h) VALUES (?,?,?,?,?)",
        #        frame_rows,
        #    )
        #    conn.commit()
        hashed = extract_frame_hashes_in_memory(p, fps, frame_width, hash_alg, hash_size)
        if not hashed:
            continue

        frame_rows = [
            (media_id, idx / max(fps, 1e-6), r[0], r[1], r[2])
            for idx, r in enumerate(hashed)
        ]

        if frame_rows:
            cur.executemany(
                "INSERT INTO frames (media_id, t_sec, hash, w, h) VALUES (?,?,?,?,?)",
                frame_rows,
            )
            conn.commit()
    conn.commit()

# --------- Candidate search — GROUP BY, no N×single queries ----------
def find_candidates(conn: sqlite3.Connection, min_match_ratio: float):
    cur = conn.cursor()

    # One query: all hashes shared by >1 distinct media_id
    print("  Querying shared hashes …")
    cur.execute("""
        SELECT hash, GROUP_CONCAT(DISTINCT media_id), COUNT(DISTINCT media_id)
        FROM frames
        WHERE hash IS NOT NULL
        GROUP BY hash
        HAVING COUNT(DISTINCT media_id) > 1
    """)
    rows = cur.fetchall()
    print(f"  Shared-hash groups: {len(rows):,}")

    pair_counts: Dict[Tuple[int,int], int] = {}
    print(f"  Building pairs from {len(rows):,} hash groups (may take a while) ...")
    skipped_large = 0
    for _, mids_str, cnt in tqdm(rows, desc="  Pairing", unit="group"):
        if cnt > 500:  # skip runaway groups (common background hash shared by thousands)
            skipped_large += 1
            continue
        mids = list(map(int, mids_str.split(",")))
        for i, a in enumerate(mids):
            for b in mids[i+1:]:
                key = (a, b) if a < b else (b, a)
                pair_counts[key] = pair_counts.get(key, 0) + 1

    print(f"  Raw pairs: {len(pair_counts):,}  (skipped {skipped_large} oversized groups)")
    if not pair_counts:
        return [], {}

    # Collect all relevant media ids
    all_mids = set()
    for a, b in pair_counts:
        all_mids.add(a); all_mids.add(b)

    # Use a temp table to avoid SQLite's 999-variable IN() limit
    cur.execute("CREATE TEMP TABLE IF NOT EXISTS _mids (id INTEGER PRIMARY KEY)")
    cur.execute("DELETE FROM _mids")
    cur.executemany("INSERT OR IGNORE INTO _mids VALUES (?)", [(m,) for m in all_mids])
    conn.commit()

    cur.execute("""
        SELECT f.media_id, COUNT(*)
        FROM frames f
        JOIN _mids m ON f.media_id = m.id
        GROUP BY f.media_id
    """)
    frame_counts: Dict[int, int] = dict(cur.fetchall())

    candidates = []
    for (a, b), match_count in pair_counts.items():
        ratio = match_count / max(frame_counts.get(a, 1), frame_counts.get(b, 1))
        if ratio >= min_match_ratio:
            candidates.append((a, b, match_count, ratio))

    cur.execute("SELECT media.id, media.path FROM media JOIN _mids m ON media.id = m.id")
    media_map = dict(cur.fetchall())

    return candidates, media_map

# --------- Act ----------
def decide_and_act(conn, candidates, media_map, args):
    actions = []
    os.makedirs(args.quarantine, exist_ok=True)
    for a, b, match_count, ratio in tqdm(candidates, desc="Verifying & acting"):
        try:
            path_a = Path(media_map[a])
            path_b = Path(media_map[b])
        except Exception:
            continue
        try:
            meta_a = (path_a.stat().st_size, path_a.stat().st_mtime)
            meta_b = (path_b.stat().st_size, path_b.stat().st_mtime)
        except Exception as e:
            log_bad(str(path_a), f"stat_failed:{e}")
            continue

        keep, remove = (path_a, path_b) if meta_a >= meta_b else (path_b, path_a)

        ssim_score = None
        if args.ssim_threshold and ssim is not None:
            tmp_dir = Path(TMP_FRAME_DIR)
            tmp_dir.mkdir(exist_ok=True)
            tmp_a, tmp_b = tmp_dir / f"s_a_{a}.jpg", tmp_dir / f"s_b_{b}.jpg"
            try:
                for src, dst in [(path_a, tmp_a), (path_b, tmp_b)]:
                    if is_image(src):
                        shutil.copy2(src, dst)
                    else:
                        subprocess.run(
                            ["ffmpeg", "-hide_banner", "-loglevel", "error",
                             "-an", "-i", str(src), "-vf", "fps=1,scale=256:-1",
                             "-vframes", "1", "-y", str(dst)], check=False)
                if tmp_a.exists() and tmp_b.exists():
                    ia = np.array(Image.open(tmp_a).convert("L"), dtype=np.float32)
                    ib = np.array(Image.open(tmp_b).convert("L"), dtype=np.float32)
                    if ia.shape != ib.shape:
                        import cv2
                        ib = cv2.resize(ib, (ia.shape[1], ia.shape[0]))
                    ssim_score = float(ssim(ia, ib))
            except Exception:
                ssim_score = None
            finally:
                for t in (tmp_a, tmp_b):
                    try: t.unlink()
                    except Exception: pass
            if ssim_score is not None and ssim_score < args.ssim_threshold:
                continue

        rel  = os.path.relpath(str(remove), start=args.root)
        dest = os.path.join(args.quarantine, rel)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        actions.append({"keep": str(keep), "remove": str(remove),
                         "ratio": ratio, "ssim": ssim_score, "dest": dest})
        if args.dry_run:
            print(f"[DRY-RUN] {remove} -> {dest}")
        else:
            try:
                try:    os.replace(remove, dest)
                except: shutil.move(remove, dest)
                with open(ACTIONS_LOG, "a", encoding="utf-8") as f:
                    f.write(f"{time.time()},{keep},{remove},{dest},{ratio},{ssim_score}\n")
            except Exception as e:
                log_bad(str(remove), f"move_failed:{e}")
    return actions

# --------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser(description="Image+video dedupe — parallel & batched")
    p.add_argument("root")
    p.add_argument("--fps",             type=float, default=0.5)
    p.add_argument("--frame-width",     type=int,   default=128)
    p.add_argument("--hash_alg",        choices=("phash","dhash","ahash"), default="phash")
    p.add_argument("--hash_size",       type=int,   default=16)
    p.add_argument("--min_match_ratio", type=float, default=0.25)
    p.add_argument("--ssim_threshold",  type=float, default=0.0,
                   help="SSIM threshold, 0=disabled (default). Use 0.85 only on small sets")
    p.add_argument("--workers",         type=int,   default=os.cpu_count() or 4)
    p.add_argument("--dry-run",         action="store_true")
    p.add_argument("--quarantine",      default=QUARANTINE_DIR)
    p.add_argument("--delete-bad",     action="store_true",
                   help="自动删除 hash_failed 的损坏文件（同时生成 delete_bad.sh）")
    return p.parse_args()

def main():
    args = parse_args()
    root = Path(args.root)
    if not root.exists() or not root.is_dir():
        print("Invalid root:", args.root); sys.exit(1)

    cleanup_tmp_frames()
    conn = sqlite3.connect(DB_FILE)
    init_db(conn)

    media = gather_media(root)
    print(f"Found {len(media):,} media files")

    cleanup_stale_db(conn, media)
    index_media(conn, media, args.fps, args.hash_alg, args.hash_size, args.frame_width, args.workers)

    print("Searching candidates …")
    candidates, media_map = find_candidates(conn, args.min_match_ratio)
    print(f"Candidate pairs: {len(candidates):,}")

    args.root = str(root)
    actions = decide_and_act(conn, candidates, media_map, args)
    print(f"Done. Actions: {len(actions)}")

    cleanup_tmp_frames()
    print_bad_summary(delete_bad=args.delete_bad)

if __name__ == "__main__":
    main()