#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import csv, json, re, sys, time, random
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent
URLS_CSV = ROOT / "urls.csv"
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

# Aceita qualquer subdomínio que termine em mercadolivre.com.br
ML_DOMAIN_RE = re.compile(r"(?:^|\.)mercadolivre\.com\.br$", re.I)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.google.com/",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

def norm(s): 
    return re.sub(r"\s+", " ", s or "").strip()

def is_ml_domain(url: str) -> bool:
    try:
        host = urlparse(url).hostname or ""
    except Exception:
        return False
    return bool(ML_DOMAIN_RE.search(host))

def format_brl_from_meta(v: str) -> str:
    if not v: 
        return ""
    v = str(v).strip()
    if v.startswith("R$"):
        return v
    if re.fullmatch(r"\d+(?:\.\d+)?", v):
        inteiro, _, dec = v.partition(".")
        dec = (dec + "00")[:2]
        return f"R$ {inteiro},{dec}"
    if re.search(r"\d", v):
        return f"R$ {v}"
    return ""

def join_fraction_cents(container) -> str:
    if not container:
        return ""
    frac = container.select_one(".andes-money-amount__fraction, .price-tag-fraction")
    cents = container.select_one(".andes-money-amount__cents, .andes-money-amount__cents--superscript-36, .andes-money-amount__cents-superscript")
    if frac:
        f = norm(frac.get_text())
        c = norm(cents.get_text()) if cents else ""
        if c and re.fullmatch(r"\d{1,2}", c):
            return f"R$ {f},{c.zfill(2)}"
        return f"R$ {f}"
    return ""

def parse_aria_label_price(txt: str) -> str:
    if not txt:
        return ""
    m = re.search(r"(\d+)[^\d]+(\d{1,2})", txt)
    if m:
        inteiro, cents = m.group(1), m.group(2)
        return f"R$ {int(inteiro)},{int(cents):02d}"
    m = re.search(r"(\d+)", txt)
    if m:
        return f"R$ {int(m.group(1))}"
    return ""

def jsonld_price_brl(soup: BeautifulSoup) -> str:
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
                if price:
                    p = norm(str(price))
                    return p if p.startswith("R$") else f"R$ {p}"
            if isinstance(offers, list):
                for off in offers:
                    price = off.get("price") or off.get("priceSpecification", {}).get("price")
                    if price:
                        p = norm(str(price))
                        return p if p.startswith("R$") else f"R$ {p}"
            if obj.get("price"):
                p = norm(str(obj["price"]))
                return p if p.startswith("R$") else f"R$ {p}"
    return ""

def ml_extract_price(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    # 1) Alvo exato: <span class="andes-money-amount" itemprop="offers" role="img"> ... <meta itemprop="price" content="47.22">
    container = soup.select_one("span.andes-money-amount[role='img'][itemprop='offers']")
    if container:
        meta_price = container.select_one("meta[itemprop='price'][content]")
        if meta_price and meta_price.get("content"):
            brl = format_brl_from_meta(meta_price["content"])
            if brl:
                return brl
        j = join_fraction_cents(container)
        if j:
            return j
        aria = container.get("aria-label")
        if aria:
            p = parse_aria_label_price(aria)
            if p:
                return p

    # 2) Meta global comum no ML
    tag = soup.find("meta", {"itemprop": "price"})
    if tag and tag.get("content"):
        brl = format_brl_from_meta(tag["content"])
        if brl:
            return brl

    # 3) Containers usuais do PDP
    for sel in [
        ".ui-pdp-price__second-line",
        ".ui-pdp-price__main-container",
        "div.price-tag",
    ]:
        cont = soup.select_one(sel)
        if cont:
            j = join_fraction_cents(cont)
            if j:
                return j

    # 4) JSON-LD (fallback)
    p = jsonld_price_brl(soup)
    if p:
        return p

    # 5) Regex geral (último recurso)
    m = re.search(r"R\$\s*\d{1,3}(?:\.\d{3})*,\d{2}", soup.get_text(" ", strip=True))
    return norm(m.group(0)) if m else ""

def fetch_follow(url: str, session: requests.Session, retries: int = 3, backoff: float = 1.2):
    """
    Faz GET seguindo redirecionamentos (afiliados), retorna (html, final_url).
    Mantém o mesmo controle de 403/429 com backoff leve.
    """
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
            final_url = r.url
            if r.status_code in (403, 429) and attempt < retries:
                time.sleep(backoff * attempt + random.uniform(0, 0.6))
                continue
            r.raise_for_status()
            return r.text, final_url
        except Exception as e:
            last_exc = e
            if attempt < retries:
                time.sleep(backoff * attempt + random.uniform(0, 0.6))
            else:
                raise last_exc
    raise last_exc

def main():
    rows = []
    session = requests.Session()

    with open(URLS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for line in reader:
            pid = norm(line.get("id", ""))
            url = norm(line.get("url", ""))
            if not pid or not url:
                continue

            ts = datetime.now(timezone.utc).isoformat()

            try:
                html, final_url = fetch_follow(url, session=session)
                # Só aceita se o DOMÍNIO FINAL for Mercado Livre BR
                if not is_ml_domain(final_url):
                    raise ValueError(f"destino final não é Mercado Livre BR: {final_url}")
                price = ml_extract_price(html)
                if not price:
                    raise ValueError("não foi possível extrair o preço (ML)")
                rows.append({"id": pid, "price": price, "url": url, "ts_utc": ts, "error": ""})
            except Exception as e:
                rows.append({"id": pid, "price": "", "url": url, "ts_utc": ts, "error": str(e)})

            time.sleep(1)  # pausa leve

    # CSV (quoting correto por causa da vírgula em R$ x,yy)
    csv_path = DATA_DIR / "latest_prices.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "price", "url", "ts_utc", "error"])
        for r in rows:
            writer.writerow([r["id"], r["price"], r["url"], r["ts_utc"], r["error"]])

    # JSON
    json_path = DATA_DIR / "latest_prices.json"
    json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[ok] {len(rows)} itens atualizados em {csv_path.name} e {json_path.name}")

if __name__ == "__main__":
    sys.exit(main())
