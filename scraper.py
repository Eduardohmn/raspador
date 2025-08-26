#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import csv, json, re, sys, time
from datetime import datetime, timezone
from pathlib import Path
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent
URLS_CSV = ROOT / "urls.csv"
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}

def norm(s): return re.sub(r"\s+", " ", s or "").strip()

def jsonld_price(soup):
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "{}")
        except Exception:
            continue
        objs = data if isinstance(data, list) else [data]
        for obj in objs:
            offers = obj.get("offers")
            if isinstance(offers, dict):
                price = offers.get("price") or offers.get("priceSpecification", {}).get("price")
                if price: return norm(str(price))
            if isinstance(offers, list):
                for off in offers:
                    price = off.get("price") or off.get("priceSpecification", {}).get("price")
                    if price: return norm(str(price))
            if obj.get("price"): return norm(str(obj["price"]))
    return None

def meta_price(soup):
    for sel in [
        ('meta', {'property': 'product:price:amount'}),
        ('meta', {'itemprop': 'price'}),
        ('meta', {'name': 'og:price:amount'}),
        ('meta', {'name': 'twitter:data1'}),
    ]:
        tag = soup.find(*sel)
        if tag and tag.get("content") and any(ch.isdigit() for ch in tag["content"]):
            return norm(tag["content"])
    return None

def common_price(soup):
    # Mercado Livre
    for sel in [
        "span.andes-money-amount__fraction",
        ".ui-pdp-price__second-line .andes-money-amount__fraction",
        ".andes-money-amount__fraction",
    ]:
        el = soup.select_one(sel)
        if el and norm(el.get_text()):
            cents = soup.select_one("span.andes-money-amount__cents")
            if cents and norm(cents.get_text()).isdigit():
                return f"R$ {norm(el.get_text())},{norm(cents.get_text())}"
            return norm(el.get_text())

    # Shopee (pode falhar se SPA)
    for sel in [
        "div[class*='product-price__current-price']",
        "div[data-sqe='price'] span",
        "div[class*='pmmxKc']",
    ]:
        el = soup.select_one(sel)
        if el and norm(el.get_text()):
            return norm(el.get_text())

    # Fallback: padrão "R$ 9,99"
    m = re.search(r"R\$\s*\d{1,3}(?:\.\d{3})*,\d{2}", soup.get_text(" ", strip=True))
    return norm(m.group(0)) if m else None

def extract_price(html, selector=""):
    soup = BeautifulSoup(html, "html.parser")
    if selector:
        el = soup.select_one(selector)
        if el and norm(el.get_text()):
            return norm(el.get_text())
    for fn in (jsonld_price, meta_price, common_price):
        p = fn(soup)
        if p: return p
    return ""

def fetch(url):
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text

def main():
    rows = []
    with open(URLS_CSV, newline="", encoding="utf-8") as f:
        for line in csv.DictReader(f):
            pid = norm(line.get("id","")); url = norm(line.get("url","")); selector = norm(line.get("selector",""))
            if not pid or not url: continue
            try:
                html = fetch(url)
                price = extract_price(html, selector)
                rows.append({"id": pid, "price": price, "url": url, "ts_utc": datetime.now(timezone.utc).isoformat(), "error": ""})
            except Exception as e:
                rows.append({"id": pid, "price": "", "url": url, "ts_utc": datetime.now(timezone.utc).isoformat(), "error": str(e)})
            time.sleep(1)

    # CSV + JSON
    (DATA_DIR / "latest_prices.csv").write_text(
        "id,price,url,ts_utc,error\n" + "\n".join(
            f"{r['id']},{r['price']},{r['url']},{r['ts_utc']},{r['error']}" for r in rows
        ),
        encoding="utf-8"
    )
    (DATA_DIR / "latest_prices.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[ok] {len(rows)} itens atualizados em data/latest_prices.*")

if __name__ == "__main__":
    sys.exit(main())
