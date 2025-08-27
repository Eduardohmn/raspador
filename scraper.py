#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import csv, json, re, sys, time, random
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, urljoin
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent
URLS_CSV = ROOT / "urls.csv"
DATA_DIR = ROOT / "data"
SNAP_DIR = DATA_DIR / "snapshots"
DATA_DIR.mkdir(exist_ok=True)
SNAP_DIR.mkdir(exist_ok=True)

# Aceita qualquer subdomínio que termine em mercadolivre.com.br
ML_HOST_RE = re.compile(r"(?:^|\.)mercadolivre\.com\.br$", re.I)

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

def is_ml(url: str) -> bool:
    try:
        host = urlparse(url).hostname or ""
    except Exception:
        return False
    return bool(ML_HOST_RE.search(host))

def format_brl_from_meta(v: str) -> str:
    # "47.22" -> "R$ 47,22"
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
    cents = container.select_one(".andes-money-amount__cents, .andes-money-amount__cents--superscript, .andes-money-amount__cents--superscript-36, .price-tag-cents, [class*='andes-money-amount__cents']")
    if frac:
        f = norm(frac.get_text())
        c = norm(cents.get_text()) if cents else ""
        if c and re.fullmatch(r"\d{1,2}", c):
            return f"R$ {f},{c.zfill(2)}"
        return f"R$ {f}"
    return ""

def parse_aria_label_price(txt: str) -> str:
    # "47 reais com 22 centavos" -> "R$ 47,22"
    if not txt:
        return ""
    m = re.search(r"(\d+)[^\d]+(\d{1,2})", txt)
    if m:
        inteiro, cents = m.group(1), m.group(2)
        return f"R$ {int(inteiro)},{int(cents):02d}"
    m = re.search(r"(\d+)", txt)
    return f"R$ {int(m.group(1))}" if m else ""

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

def preloaded_state_price(html: str) -> str:
    """
    Fallback: pega preço de scripts tipo window.__PRELOADED_STATE__ ou blocos JSON
    com 'prices'/'amount' e BRL.
    """
    # Busca padrão de "amount": 123.45 próximo a "BRL"
    for m in re.finditer(r'(?s)currency[_\s]*id"\s*:\s*"BRL".{0,300}?"(?:amount|price)"\s*:\s*([0-9]+(?:\.[0-9]{1,2})?)', html):
        val = m.group(1)
        brl = format_brl_from_meta(val)
        if brl:
            return brl
    # Fallback mais amplo
    m = re.search(r'"(?:amount|price)"\s*:\s*([0-9]+(?:\.[0-9]{1,2})?)', html)
    return format_brl_from_meta(m.group(1)) if m else ""

def ml_extract_price(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    # 0) Meta global (às vezes fora do container)
    tag = soup.find("meta", {"itemprop": "price"})
    if tag and tag.get("content"):
        brl = format_brl_from_meta(tag["content"])
        if brl:
            return brl

    # 1) Container "exato" que você mostrou (role=img + itemprop=offers)
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

    # 2) Mesmo container SEM os atributos (ML varia bastante)
    container = soup.select_one("span.andes-money-amount.ui-pdp-price__part") or soup.select_one("span.andes-money-amount")
    if container:
        # tenta meta dentro
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

    # 3) Containers usuais do PDP
    for sel in [".ui-pdp-price__second-line", ".ui-pdp-price__main-container", "div.price-tag"]:
        cont = soup.select_one(sel)
        if cont:
            j = join_fraction_cents(cont)
            if j:
                return j
            # meta por perto
            meta_price = cont.select_one("meta[itemprop='price'][content]")
            if meta_price and meta_price.get("content"):
                brl = format_brl_from_meta(meta_price["content"])
                if brl:
                    return brl

    # 4) JSON-LD
    p = jsonld_price_brl(soup)
    if p:
        return p

    # 5) Estado pré-carregado (scripts)
    p = preloaded_state_price(html)
    if p:
        return p

    # 6) Regex geral (último recurso)
    m = re.search(r"R\$\s*\d{1,3}(?:\.\d{3})*,\d{2}", soup.get_text(" ", strip=True))
    return norm(m.group(0)) if m else ""

def find_meta_refresh(html: str, base_url: str):
    """Se o afiliado fizer meta refresh, segue o URL."""
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.find("meta", attrs={"http-equiv": re.compile(r"refresh", re.I)})
    if not tag:
        return None
    content = tag.get("content") or ""
    m = re.search(r"url\s*=\s*([^;]+)", content, re.I)
    if not m:
        return None
    return urljoin(base_url, m.group(1).strip())

def fetch_follow(url: str, session: requests.Session, retries: int = 3, backoff: float = 1.2):
    """
    GET com redirects + tentativa de meta refresh (afiliados que não usam 30x).
    Retorna (html, final_url).
    """
    last_exc = None
    current_url = url
    for attempt in range(1, retries + 1):
        try:
            r = session.get(current_url, headers=HEADERS, timeout=30, allow_redirects=True)
            final_url = r.url
            # Alguns 403/429: espera e tenta de novo
            if r.status_code in (403, 429) and attempt < retries:
                time.sleep(backoff * attempt + random.uniform(0, 0.6))
                continue
            r.raise_for_status()
            html = r.text

            # Meta refresh (alguns afiliados usam isso)
            if not is_ml(final_url):
                nxt = find_meta_refresh(html, final_url)
                if nxt and nxt != final_url:
                    # segue uma vez o refresh
                    r2 = session.get(nxt, headers=HEADERS, timeout=30, allow_redirects=True)
                    r2.raise_for_status()
                    return r2.text, r2.url

            return html, final_url
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
            pid = norm(line.get("id",""))
            url = norm(line.get("url",""))
            selector = norm(line.get("selector",""))  # mantido p/ compat
            if not pid or not url:
                continue

            ts = datetime.now(timezone.utc).isoformat()

            try:
                html, final_url = fetch_follow(url, session=session)
                if not is_ml(final_url):
                    raise ValueError(f"destino final não é Mercado Livre BR: {final_url}")

                # Se o CSV passou seletor, tenta primeiro (compat com seu fluxo antigo)
                price = ""
                if selector:
                    soup = BeautifulSoup(html, "html.parser")
                    el = soup.select_one(selector)
                    if el:
                        price = join_fraction_cents(el) or norm(el.get_text())
                        if price and not price.startswith("R$"):
                            m = re.search(r"R\$\s*\d{1,3}(?:\.\d{3})*,\d{2}", price)
                            price = m.group(0) if m else price

                if not price:
                    price = ml_extract_price(html)

                if not price:
                    # salva snapshot para debug
                    (SNAP_DIR / f"{pid}.html").write_text(html, encoding="utf-8")
                    raise ValueError("não foi possível extrair o preço (ML)")

                rows.append({"id": pid, "price": price, "url": url, "ts_utc": ts, "error": ""})

            except Exception as e:
                rows.append({"id": pid, "price": "", "url": url, "ts_utc": ts, "error": str(e)})

            time.sleep(1)

    # CSV (usa writer por causa da vírgula em R$ x,yy)
    csv_path = DATA_DIR / "latest_prices.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "price", "url", "ts_utc", "error"])
        for r in rows:
            w.writerow([r["id"], r["price"], r["url"], r["ts_utc"], r["error"]])

    # JSON
    json_path = DATA_DIR / "latest_prices.json"
    json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[ok] {len(rows)} itens atualizados em {csv_path.name} e {json_path.name}")

if __name__ == "__main__":
    sys.exit(main())
