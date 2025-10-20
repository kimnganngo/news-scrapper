import re, json, time
from datetime import datetime, timedelta
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
from readability import Document
from dateutil import parser, tz

TZ = tz.gettz("Asia/Bangkok")
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; StockHarvester/1.0)"}

SOURCES = [
    {
        "name": "CafeF",
        "list_urls": [f"https://cafef.vn/thi-truong-chung-khoan.chn?p={p}" for p in range(1, 9)],
        "allow": lambda href: href and (href.endswith(".chn") or "/.chn" in href),
    },
    {
        "name": "Vietstock",
        "list_urls": [f"https://vietstock.vn/chung-khoan.htm?page={p}" for p in range(1, 9)],
        "allow": lambda href: href and href.endswith(".htm"),
    },
]

def get_html(url):
    for _ in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 200:
                return r.text
        except Exception:
            time.sleep(0.5)
    return ""

def parse_date(html, url):
    soup = BeautifulSoup(html, "lxml")
    cand = (
        soup.select_one("meta[property='article:published_time']") or
        soup.select_one("meta[name='pubdate']") or
        soup.select_one("meta[name='publishdate']") or
        soup.select_one("time")
    )
    if cand:
        val = cand.get("content") or cand.get("datetime")
        if val:
            try:
                dt = parser.parse(val)
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=TZ)
                return dt.astimezone(TZ)
            except Exception:
                pass
    # Vietstock fallback: /YYYY/MM/DD/
    m = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", url)
    if m:
        y, mth, d = map(int, m.groups())
        return datetime(y, mth, d, 8, 0, tzinfo=TZ)
    return None

def extract_content(html):
    doc = Document(html)
    title = doc.short_title()
    content_html = doc.summary()
    soup = BeautifulSoup(content_html, "lxml")
    text = soup.get_text(" ", strip=True)
    return title, text

def harvest(days=30):
    cutoff = datetime.now(TZ) - timedelta(days=days)
    out = []
    seen = set()
    for src in SOURCES:
        for list_url in src["list_urls"]:
            lst_html = get_html(list_url)
            if not lst_html: 
                continue
            lst = BeautifulSoup(lst_html, "lxml")
            for a in lst.select("a[href]"):
                href = a.get("href", "")
                if not href:
                    continue
                full = urljoin(list_url, href)  # nhận cả link tương đối
                if not src["allow"](full):
                    continue
                if full in seen:
                    continue
                seen.add(full)

                art_html = get_html(full)
                if not art_html:
                    continue
                dt = parse_date(art_html, full)
                if dt and dt < cutoff:
                    continue
                title, text = extract_content(art_html)
                if not text or len(text) < 200:
                    continue
                out.append({
                    "url": full,
                    "source": src["name"],
                    "title": title,
                    "date": dt.isoformat() if dt else None,
                    "content": text[:20000]
                })
                time.sleep(0.2)  # throttle nhẹ
    return out

if __name__ == "__main__":
    data = harvest(days=30)
    stamp = datetime.now(TZ).strftime("%Y-%m-%d")
    # đảm bảo thư mục data/ tồn tại
    import os
    os.makedirs("data", exist_ok=True)
    with open(f"data/news_{stamp}.jsonl", "w", encoding="utf-8") as f:
        for it in data:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")
    print(f"Wrote {len(data)} articles.")
