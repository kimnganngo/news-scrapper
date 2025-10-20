import re, json, time, os, argparse
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from readability import Document
from dateutil import parser, tz

# --- cấu hình chung ---
TZ = tz.gettz("Asia/Bangkok")
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; VN-NewsHarvester/1.1)"}

def build_list_pages(base_url: str, max_pages: int):
    """
    Trả về danh sách URL list page với nhiều pattern phân trang phổ biến.
    Tự động bỏ qua URL lỗi/404 khi request.
    """
    patterns = [
        "?page={p}",
        "?trang={p}",
        "/trang-{p}.htm",
        "/p{p}",
        "/?p={p}",
    ]
    urls = [base_url]
    for patt in patterns:
        for p in range(2, max_pages + 1):
            urls.append(base_url.rstrip("/") + patt.format(p=p))
    # de-dup theo thứ tự
    out, seen = [], set()
    for u in urls:
        if u not in seen:
            out.append(u); seen.add(u)
    return out

# --- định nghĩa nguồn ---
def mk_sources(max_pages: int):
    return [
        # CafeF
        {
            "name": "CafeF",
            "lists": build_list_pages("https://cafef.vn/thi-truong-chung-khoan.chn", max_pages),
            "allow": lambda href: href and href.endswith(".chn") and "cafef.vn" in urlparse(href).netloc,
        },
        # Vietstock
        {
            "name": "Vietstock",
            "lists": build_list_pages("https://vietstock.vn/chung-khoan.htm", max_pages),
            "allow": lambda href: href and href.endswith(".htm") and "vietstock.vn" in urlparse(href).netloc,
        },
        # Tin nhanh chứng khoán - chuyên mục Chứng khoán
        {
            "name": "TNCK-ChungKhoan",
            "lists": build_list_pages("https://www.tinnhanhchungkhoan.vn/chung-khoan/", max_pages),
            "allow": lambda href: href and href.endswith(".html") and "tinnhanhchungkhoan.vn" in urlparse(href).netloc,
        },
        # Tin nhanh chứng khoán - chuyên mục Doanh nghiệp
        {
            "name": "TNCK-DoanhNghiep",
            "lists": build_list_pages("https://www.tinnhanhchungkhoan.vn/doanh-nghiep/", max_pages),
            "allow": lambda href: href and href.endswith(".html") and "tinnhanhchungkhoan.vn" in urlparse(href).netloc,
        },
        # Người Quan Sát - Chứng khoán
        {
            "name": "NguoiQuanSat",
            "lists": build_list_pages("https://nguoiquansat.vn/chung-khoan", max_pages),
            "allow": lambda href: href and (href.endswith(".htm") or href.endswith(".html")) and "nguoiquansat.vn" in urlparse(href).netloc,
        },
        # Báo Mới - Chứng khoán (bản tin .epi)
        {
            "name": "BaoMoi",
            "lists": build_list_pages("https://baomoi.com/chung-khoan.epi", max_pages),
            "allow": lambda href: href and href.endswith(".epi") and "baomoi.com" in urlparse(href).netloc,
        },
    ]

def get_html(url):
    for _ in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 200 and r.text:
                return r.text
        except Exception:
            time.sleep(0.6)
    return ""

# parse ngày: nhiều khả năng meta khác nhau + fallback URL /YYYY/MM/DD/
DATE_META_SELECTORS = [
    "meta[property='article:published_time']::content",
    "meta[property='article:modified_time']::content",
    "meta[name='pubdate']::content",
    "meta[name='publishdate']::content",
    "meta[itemprop='datePublished']::content",
    "time::datetime",
    "span[class*=date]",
    "span[class*=time]",
]

def _first_meta_datetime(soup: BeautifulSoup):
    for sel in DATE_META_SELECTORS:
        if "::" in sel:
            css, attr = sel.split("::", 1)
        else:
            css, attr = sel, None
        for node in soup.select(css):
            val = node.get(attr) if attr else node.get_text(" ", strip=True)
            if not val:
                continue
            try:
                dt = parser.parse(val)
                return dt
            except Exception:
                pass
    return None

def parse_date(html, url):
    soup = BeautifulSoup(html, "lxml")
    dt = _first_meta_datetime(soup)
    if not dt:
        m = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", url)
        if m:
            y, mth, d = map(int, m.groups())
            dt = datetime(y, mth, d, 8, 0)
    if not dt:
        return None
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=TZ)
    return dt.astimezone(TZ)

def extract_content(html):
    # readability để bóc phần thân sạch
    doc = Document(html)
    title = (doc.short_title() or "").strip()
    content_html = doc.summary()
    soup = BeautifulSoup(content_html, "lxml")
    text = soup.get_text(" ", strip=True)
    return title, text

def harvest(days=30, max_pages=8):
    cutoff = datetime.now(TZ) - timedelta(days=days)
    out = []
    seen = set()
    for src in mk_sources(max_pages):
        for list_url in src["lists"]:
            lst_html = get_html(list_url)
            if not lst_html:
                continue
            lst = BeautifulSoup(lst_html, "lxml")
            for a in lst.select("a[href]"):
                href = a.get("href", "")
                if not href:
                    continue
                full = urljoin(list_url, href)  # xử lý link tương đối
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
                if not title:
                    # fallback: thẻ <title>
                    soup = BeautifulSoup(art_html, "lxml")
                    if soup.title:
                        title = soup.title.get_text(" ", strip=True)
                if not text or len(text) < 200:
                    continue
                out.append({
                    "url": full,
                    "source": src["name"],
                    "title": title,
                    "date": dt.isoformat() if dt else None,
                    "content": text[:20000]
                })
                time.sleep(0.25)  # throttle nhẹ
    return out

if __name__ == "__main__":
    parser_arg = argparse.ArgumentParser()
    parser_arg.add_argument("--days", type=int, default=int(os.getenv("DAYS", "30")))
    parser_arg.add_argument("--max-pages", type=int, default=int(os.getenv("MAX_PAGES", "8")))
    args = parser_arg.parse_args()

    data = harvest(days=args.days, max_pages=args.max_pages)
    stamp = datetime.now(TZ).strftime("%Y-%m-%d")
    os.makedirs("data", exist_ok=True)

    # JSONL
    jsonl_path = f"data/news_{stamp}.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for it in data:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")

    # CSV (để tiện dùng nếu cần)
    import csv
    csv_path = f"data/news_{stamp}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as cf:
        w = csv.DictWriter(cf, fieldnames=["url","source","title","date","content"])
        w.writeheader()
        for it in data:
            w.writerow(it)

    print(f"Wrote {len(data)} articles → {jsonl_path} & {csv_path}")
