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

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

# ---- helpers para ML (juntar parte inteira + centavos) ----
def join_fraction_cents(container: BeautifulSoup) -> str:
    if not container: 
        return ""
    # parte inteira
    frac = container.select_one(".andes-money-amount__fraction, .price-tag-fraction")
    # centavos (inclui o caso da sua imagem: __cents-superscript)
    cents = container.select_one(
        ".andes-money-amount__cents, "
        ".andes-money-amount__cents-superscript, "
        ".price-tag-cents"
    )
    if frac:
        f = norm(frac.get_text())
        c = norm(cents.get_text()) if cents else ""
        if c.isdigit() and c:
            return f"R$ {f},{c}"
        return f"R$ {f}"
    return ""

# ---------- JSON-LD (regular) ----------
def jsonld_price(soup: BeautifulSoup):
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
            if obj.get("price"): 
                return norm(str(obj["price"]))
    return None

# ---------- META (regular) ----------
def meta_price(soup: BeautifulSoup):
    for name, attrs in [
        ('meta', {'property': 'product:price:amount'}),
        ('meta', {'itemprop': 'price'}),
        ('meta', {'name': 'og:price:amount'}),
        ('meta', {'name': 'twitter:data1'}),
    ]:
        tag = soup.find(name, attrs)
        if tag and tag.get("content") and any(ch.isdigit() for ch in tag["content"]):
            return norm(tag["content"])
    return None

# ---------- fallback genérico ----------
def common_price(soup: BeautifulSoup):
    # tente explicitamente o container promo primeiro
    cont = soup.select_one(".ui-pdp-price__second-line")
    joined = join_fraction_cents(cont)
    if joined:
        return joined

    # se não achou o container, tente fraction + cents em geral
    for sel in [
        ".ui-pdp-price__main-container .andes-money-amount__fraction",
        ".andes-money-amount__fraction",
    ]:
        el = soup.select_one(sel)
        if el and norm(el.get_text()):
            # procurar cents próximo (sup/normal)
            cents = el.find_next("span", class_=re.compile(r"andes-money-amount__cents"))
            if not cents:
                cents = el.find_next("span", class_=re.compile(r"andes-money-amount__cents-superscript"))
            if cents and norm(cents.get_text()).isdigit():
                return f"R$ {norm(el.get_text())},{norm(cents.get_text())}"
            return f"R$ {norm(el.get_text())}"

    # Shopee (quando renderiza)
    for sel in [
        "div[data-sqe='price'] span",
        "div[class*='product-price__current-price']",
    ]:
        el = soup.select_one(sel)
        if el and norm(el.get_text()):
            return norm(el.get_text())

    # regex final
    m = re.search(r"R\$\s*\d{1,3}(?:\.\d{3})*,\d{2}", soup.get_text(" ", strip=True))
    return norm(m.group(0)) if m else None

def extract_price(html: str, selector: str = ""):
    soup = BeautifulSoup(html, "html.parser")

    # 1) Seletor manual (pode apontar para o CONTAINER promo)
    if selector:
        el = soup.select_one(selector)
        if el:
            joined = join_fraction_cents(el)
            if joined:
                return joined
            txt = norm(el.get_text())
            m = re.search(r"R\$\s*\d{1,3}(?:\.\d{3})*,\d{2}", txt)
            if m:
                return norm(m.group(0))
            if txt:
                return txt

    # 2) Preferir DOM promo do ML antes de JSON-LD/meta
    promo = common_price(soup)
    if promo:
        return promo

    # 3) Fallbacks (podem ser preço de lista)
    for fn in (jsonld_price, meta_price):
        p = fn(soup)
        if p: 
            return p

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
            if not pid or not url: 
                continue
            try:
                html = fetch(url)
                price = extract_price(html, selector)
                rows.append({"id": pid, "price": price, "url": url, "ts_utc": datetime.now(timezone.utc).isoformat(), "error": ""})
            except Exception as e:
                rows.append({"id": pid, "price": "", "url": url, "ts_utc": datetime.now(timezone.utc).isoformat(), "error": str(e)})
            time.sleep(1)

    (DATA_DIR / "latest_prices.csv").write_text(
        "id,price,url,ts_utc,error\n" + "\n".join(
            f"{r['id']},{r['price']},{r['url']},{r['ts_utc']},{r['error']}" for r in rows
        ),
        encoding="utf-8"
    )
    (DATA_DIR / "latest_prices.json").write_text(
        json.dumps(rows, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"[ok] {len(rows)} itens atualizados em data/latest_prices.*")

if __name__ == "__main__":
    sys.exit(main())
