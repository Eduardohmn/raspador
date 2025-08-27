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

# ---- helpers ----
def join_fraction_cents(container):
    if not container: return ""
    frac = container.select_one(".andes-money-amount__fraction, .price-tag-fraction")
    cents = container.select_one(".andes-money-amount__cents, .andes-money-amount__cents-superscript, .price-tag-cents, [class*='andes-money-amount__cents']")
    if frac:
        f = norm(frac.get_text())
        c = norm(cents.get_text()) if cents else ""
        if c and c.isdigit(): return f"R$ {f},{c}"
        return f"R$ {f}"
    return ""

def format_brl_from_meta(v):
    # meta itemprop="price" vem como 47.22 -> "R$ 47,22"
    if not v: return ""
    v = str(v).strip()
    if v.startswith("R$"): return v
    # aceita 47.22 ou 47,22
    if re.fullmatch(r"\d+(?:\.\d+)?", v):
        inteiro, _, dec = v.partition(".")
        dec = (dec + "00")[:2]
        return f"R$ {inteiro},{dec}"
    if re.search(r"\d", v):  # já é algo tipo 47,22
        return f"R$ {v}"
    return ""

# ---- detectores ----
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
            if obj.get("price"):
                return norm(str(obj["price"]))
    return None

def meta_price_brl(soup):
    # tenta meta price (microdata) e devolve BRL formatado
    tag = soup.find("meta", {"itemprop": "price"})
    if tag and tag.get("content"):
        brl = format_brl_from_meta(tag["content"])
        if brl: return brl
    # outros metas comuns (se vierem já em R$)
    for name, attrs in [
        ('meta', {'property': 'product:price:amount'}),
        ('meta', {'name': 'og:price:amount'}),
        ('meta', {'name': 'twitter:data1'}),
    ]:
        t = soup.find(name, attrs)
        if t and t.get("content"):
            c = norm(t["content"])
            if re.search(r"\d", c):
                return c if c.startswith("R$") else f"R$ {c}"
    return None

def ml_promo(soup):
    # 1) alvo direto do seu print
    el = soup.select_one("span.andes-money-amount[itemprop='offers']")
    if el:
        j = join_fraction_cents(el)
        if j: return j
        txt = norm(el.get_text())
        m = re.search(r"R\$\s*\d{1,3}(?:\.\d{3})*,\d{2}", txt)
        if m: return norm(m.group(0))
    # 2) container promo comum
    cont = soup.select_one(".ui-pdp-price__second-line")
    if cont:
        j = join_fraction_cents(cont)
        if j: return j
    # 3) fraction+cents soltos
    el = soup.select_one(".ui-pdp-price__main-container .andes-money-amount__fraction, .andes-money-amount__fraction")
    if el:
        cents = el.find_next("span", class_=re.compile(r"andes-money-amount__cents"))
        if not cents:
            cents = el.find_next("span", class_=re.compile(r"andes-money-amount__cents-superscript"))
        if cents and norm(cents.get_text()).isdigit():
            return f"R$ {norm(el.get_text())},{norm(cents.get_text())}"
        return f"R$ {norm(el.get_text())}"
    return None

def generic_shopee(soup):
    for sel in ["div[data-sqe='price'] span", "div[class*='product-price__current-price']"]:
        el = soup.select_one(sel)
        if el and norm(el.get_text()):
            return norm(el.get_text())
    m = re.search(r"R\$\s*\d{1,3}(?:\.\d{3})*,\d{2}", soup.get_text(" ", strip=True))
    return norm(m.group(0)) if m else None

# ---- pipeline ----
def extract_price(html: str, selector: str = ""):
    soup = BeautifulSoup(html, "html.parser")

    # 0) seletor manual (pode ser o próprio span do preço)
    if selector:
        el = soup.select_one(selector)
        if el:
            j = join_fraction_cents(el)
            if j: return j
            txt = norm(el.get_text())
            m = re.search(r"R\$\s*\d{1,3}(?:\.\d{3})*,\d{2}", txt)
            if m: return norm(m.group(0))
            if txt: return txt

    # 1) ML (promo) — prioridade
    p = ml_promo(soup)
    if p: return p

    # 2) META/JSON-LD como fallback
    p = meta_price_brl(soup)
    if p: return p
    p = jsonld_price(soup)
    if p: return p if p.startswith("R$") else f"R$ {p}"

    # 3) Shopee / genérico
    p = generic_shopee(soup)
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
