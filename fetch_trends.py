# fetch_trends.py  —— 带重试/退避/限速
import os
import time
import random
from typing import List
import pandas as pd
from pytrends.request import TrendReq

KW_FILE = "keywords.txt"
OUT_DIR = "data"
os.makedirs(OUT_DIR, exist_ok=True)

# --- 可按需调整 ---
GEO = os.getenv("GEO", "US")           # 国家/地区
TIMEFRAME = os.getenv("TF", "today 12-m")  # 时间范围，想更轻量可以用 "now 7-d"
BATCH_SIZE = 3                          # 每次请求的关键词个数（越小越安全）
BASE_SLEEP = 8                          # 每次请求后固定等待（秒）
MAX_RETRIES = 6                         # 单批次最大重试次数
BACKOFF = 1.7                           # 指数退避系数
# ------------------

def load_keywords(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        kws = [x.strip() for x in f if x.strip()]
    if not kws:
        raise ValueError("keywords.txt 为空，请至少放一个关键词")
    return kws

def chunked(lst: List[str], size: int) -> List[List[str]]:
    return [lst[i:i+size] for i in range(0, len(lst), size)]

def pull_one_batch(py: TrendReq, batch: List[str]) -> pd.DataFrame:
    # 带重试 + 指数退避
    wait = BASE_SLEEP
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            py.build_payload(batch, cat=0, timeframe=TIMEFRAME, geo=GEO, gprop="")
            df = py.interest_over_time().reset_index()
            if "isPartial" in df.columns:
                df = df.drop(columns=["isPartial"])
            return df
        except Exception as e:
            if attempt >= MAX_RETRIES:
                raise
            # 429/网络问题时退避等待
            jitter = random.uniform(0, 2.0)   # 抖动，避免“同频”
            sleep_s = wait + jitter
            print(f"[warn] batch={batch} 第{attempt}次失败：{e}\n   等待 {sleep_s:.1f}s 后重试…")
            time.sleep(sleep_s)
            wait *= BACKOFF
    # 理论不会走到这里
    raise RuntimeError("重试仍失败")

def pull_trends(kws: List[str]) -> pd.DataFrame:
    # retries/backoff 也在 TrendReq 层配置一份
    pytrends = TrendReq(hl="en-US", tz=0, retries=MAX_RETRIES, backoff_factor=0.5, timeout=(10, 30))
    batches = chunked(kws, BATCH_SIZE)
    frames = []
    for i, batch in enumerate(batches, 1):
        print(f"[info] 拉取批次 {i}/{len(batches)}: {batch}")
        df = pull_one_batch(pytrends, batch)
        frames.append(df)
        # 固定等待 + 随机抖动，降低被限概率
        sleep_s = BASE_SLEEP + random.uniform(0, 2.0)
        print(f"[info] 批次 {i} 完成，等待 {sleep_s:.1f}s 再继续…")
        time.sleep(sleep_s)
    # 合并所有批次（按日期对齐）
    out = frames[0]
    for f in frames[1:]:
        out = out.merge(f, on="date", how="outer")
    out = out.sort_values("date")
    return out

if __name__ == "__main__":
    keywords = load_keywords(KW_FILE)
    print(f"[info] 共 {len(keywords)} 个关键词：{keywords}")
    df = pull_trends(keywords)
    out_path = os.path.join(OUT_DIR, "trends.csv")
    df.to_csv(out_path, index=False)
    print(f"[ok] 已保存 -> {out_path}，行数={len(df)}")
