# utils/common.py

import datetime

def parse_float(num_str):
    try:
        return float(num_str.replace(",", ".")) if num_str else 0.0
    except:
        return 0.0

def compute_profit_percent(current_price, initial_price):
    if initial_price <= 0:
        return 0.0
    return round(((current_price - initial_price)/initial_price)*100, 2)

def current_timestamp_str():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Diccionario de emojis por chain
EMOJIS = {
    "solana": "🔵",   # Ejemplo
    "bsc": "🟢",
    "ethereum": "🟣",
    # agrega más si necesitas
}

def sheet_name_for_chain_symbol(chain, symbol):
    """
    Retorna un nombre de hoja => "<emoji> <symbol>".
    Ej: "🔵 BROCCOLI" para solana
    """
    c = chain.lower()
    emoji = EMOJIS.get(c, "❓")
    # Normalizar symbol (si quieres uppercase o algo)
    sym_up = symbol.strip()
    return f"{emoji} {sym_up}"
