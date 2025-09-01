# fetch_trends.py
from datetime import date
from pytrends.request import TrendReq
import pandas as pd
import os

KW_FILE = "keywords.txt"
OUT_DIR = "data"
os.makedirs(OUT_DIR, exist_ok=True)

def load_keywords():
    with open(KW_FILE, "r", encoding="utf-8") as f:
        return [k.strip() for k in f if k.strip()]

def pull_trends(kws, geo="US", timeframe="today 12-m"):
    pytrends = TrendReq(hl="en-US", tz=0)
    batches = [kws[i:i+5] for i in range(0, len(kws), 5)]  # Google Trends 每次最多 5 个
    frames = []
    for batch in batches:
        pytrends.build_payload(batch, cat=0, timeframe=timeframe, geo=geo, gprop="")
        df = pytrends.interest_over_time().reset_index()
        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"])
        frames.append(df)
    # 合并所有结果
    out = frames[0]
    for f in frames[1:]:
        out = out.merge(f, on="date", how="outer")
    out = out.sort_values("date")
    return out

if __name__ == "__main__":
    keywords = load_keywords()
    df = pull_trends(keywords, geo=os.getenv("GEO", "US"), timeframe=os.getenv("TF", "today 12-m"))
    out_path = os.path.join(OUT_DIR, "trends.csv")
    df.to_csv(out_path, index=False)
    print(f"✅ Trends saved -> {out_path}")
