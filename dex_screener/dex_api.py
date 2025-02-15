# dex_screener/dex_api.py

import requests
import json
from utils.common import parse_float

def get_pairs_data(chain, pair_addresses):
    if not pair_addresses:
        return []
    joined = ",".join(pair_addresses)
    url = f"https://api.dexscreener.com/latest/dex/pairs/{chain}/{joined}"
    print(f"==> DexScreener request: {url}")
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        if "pairs" in data and data["pairs"] is not None:
            return data["pairs"]
        if "pair" in data and data["pair"] is not None:
            return [data["pair"]]
        return []
    except Exception as e:
        print(f"⚠️ Error get_pairs_data => {e}")
        return []

def search_pairs(chain, query):
    url = f"https://api.dexscreener.com/latest/dex/search?q={query}&chain={chain}"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        return data.get("pairs", [])
    except Exception as e:
        print(f"⚠️ Error search_pairs => {e}")
        return []

def extract_pair_fields(pair_json):
    """Extrae algunos campos 'básicos' (priceUsd, etc.) para ca_tracking."""
    try:
        price_str = pair_json.get('priceUsd', "0")
        price = parse_float(price_str)
        return {
            "price": price if price else 0.0,
            "symbol": pair_json.get('baseToken', {}).get('symbol', ''),
            "dex_id": pair_json.get('dexId',''),
            "pair_created_at": pair_json.get('pairCreatedAt',''),
            "liquidity": parse_float(str(pair_json.get('liquidity',{}).get('usd',0))),
            "volume_24h": parse_float(str(pair_json.get('volume',{}).get('h24',0))),
            "fdv": parse_float(str(pair_json.get('fdv',0))),
            "market_cap": parse_float(str(pair_json.get('marketCap',0))),
            "txns_24h": (
                pair_json.get('txns',{}).get('h24',{}).get('buys',0)
                + pair_json.get('txns',{}).get('h24',{}).get('sells',0)
            )
        }
    except Exception as e:
        print(f"⚠️ Error extract_pair_fields => {e}")
        return None

def extract_all_columns(pair_json):
    """
    Devuelve un dict con todas las columnas relevantes.
    """
    base = pair_json.get('baseToken', {})
    quote = pair_json.get('quoteToken', {})
    txns = pair_json.get('txns', {})
    volume = pair_json.get('volume', {})
    price_change = pair_json.get('priceChange', {})
    liquidity = pair_json.get('liquidity', {})
    info = pair_json.get('info', {})
    return {
        "chainId": pair_json.get("chainId",""),
        "dexId": pair_json.get("dexId",""),
        "url": pair_json.get("url",""),
        "pairAddress": pair_json.get("pairAddress",""),
        "baseTokenAddress": base.get("address",""),
        "baseTokenName": base.get("name",""),
        "baseTokenSymbol": base.get("symbol",""),
        "quoteTokenAddress": quote.get("address",""),
        "quoteTokenName": quote.get("name",""),
        "quoteTokenSymbol": quote.get("symbol",""),
        "priceNative": pair_json.get("priceNative",""),
        "priceUsd": pair_json.get("priceUsd",""),
        "txns_m5_buys": txns.get("m5",{}).get("buys",0),
        "txns_m5_sells": txns.get("m5",{}).get("sells",0),
        "txns_h1_buys": txns.get("h1",{}).get("buys",0),
        "txns_h1_sells": txns.get("h1",{}).get("sells",0),
        "txns_h6_buys": txns.get("h6",{}).get("buys",0),
        "txns_h6_sells": txns.get("h6",{}).get("sells",0),
        "txns_h24_buys": txns.get("h24",{}).get("buys",0),
        "txns_h24_sells": txns.get("h24",{}).get("sells",0),
        "volume_m5": volume.get("m5",0),
        "volume_h1": volume.get("h1",0),
        "volume_h6": volume.get("h6",0),
        "volume_h24": volume.get("h24",0),
        "priceChangeM5": price_change.get("m5",0),
        "priceChangeH1": price_change.get("h1",0),
        "priceChangeH6": price_change.get("h6",0),
        "priceChangeH24": price_change.get("h24",0),
        "liquidityUsd": liquidity.get("usd",0),
        "liquidityBase": liquidity.get("base",0),
        "liquidityQuote": liquidity.get("quote",0),
        "fdv": pair_json.get("fdv",0),
        "marketCap": pair_json.get("marketCap",0),
        "pairCreatedAt": pair_json.get("pairCreatedAt",""),
        "infoImageUrl": info.get("imageUrl",""),
        "infoHeader": info.get("header",""),
        "infoOpenGraph": info.get("openGraph",""),
        "infoWebsites": json.dumps(info.get("websites", [])),
        "infoSocials": json.dumps(info.get("socials", []))
    }

def extract_all_data_as_json(pair_json):
    """
    Devuelve TODO el contenido devuelto por DexScreener en un string JSON,
    para guardarlo en la hoja individual (col 'raw_api_data').
    """
    try:
        return json.dumps(pair_json, ensure_ascii=False)
    except:
        return "{}"
