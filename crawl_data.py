import requests
import csv
import time
import random
from datetime import datetime

LIST_API = "https://gateway.chotot.com/v1/public/ad-listing?region_v2=12000&cg=1000&limit=20&o={offset}"
DETAIL_API = "https://gateway.chotot.com/v2/public/ad-listing/{id}?include_expired_ads=true"
OUTPUT = "BDS.csv"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def safe_request(url):
    for _ in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            print("[WARN]", e)
        time.sleep(1 + random.random())
    return None


def format_date(value):
    """Chuẩn hóa timestamp từ Chợ Tốt (giây hoặc mili-giây), chỉ lấy ngày"""
    if not value:
        return datetime.now().strftime("%Y-%m-%d")
    try:
        val = int(value)
        if val > 1e12:  # mili-giây
            val = val / 1000
        return datetime.fromtimestamp(val).strftime("%Y-%m-%d")
    except:
        return datetime.now().strftime("%Y-%m-%d")

def crawl(limit_pages=2):
    all_rows = []

    for page in range(limit_pages):
        offset = page * 20
        print(f"[LIST] Page {page+1}: {LIST_API.format(offset=offset)}")
        data = safe_request(LIST_API.format(offset=offset))
        if not data or "ads" not in data:
            print("[WARN] Không có dữ liệu trang này.")
            break

        for ad in data["ads"]:
            ad_id = ad.get("list_id")
            detail = safe_request(DETAIL_API.format(id=ad_id))
            if not detail or "ad" not in detail:
                continue

            ad = detail["ad"]
            date_value = ad.get("time") or ad.get("date")

            row = {
                "ad_type_name": ad.get("category_name") or ad.get("ad_type_name") or "",
                "area": ad.get("area"),
                "price": ad.get("price"),
                "address": ad.get("address") or ad.get("region_name") or "",
                "property_legal_document": ad.get("property_legal_document"),
                "rooms": ad.get("rooms"),
                "toilets": ad.get("toilets"),
            }
            all_rows.append(row)

        time.sleep(random.uniform(1, 2))

    if all_rows:
        with open(OUTPUT, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=all_rows[0].keys())
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"[DONE] Đã lưu {len(all_rows)} dòng vào {OUTPUT}")
    else:
        print("[ERROR] Không có dữ liệu để lưu.")

if __name__ == "__main__":
    crawl(limit_pages=200)
