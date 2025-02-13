import time
import requests
import google.generativeai as genai
import gspread
import re
import asyncio
import datetime
from google.oauth2.service_account import Credentials
from telethon import TelegramClient, events
from collections import defaultdict

#############################################
#  🚀 CONFIGURACIÓN PRINCIPAL
#############################################

API_ID = 28644650
API_HASH = "e963f9b807bcf9d665b1d20de66f7c69"
GEMINI_API_KEY = "AIzaSyDTlAcI4qNx_QAKcTli2sc5jc_xl53qPZA"

SHEET_ID = "1K7p3Yeu6k1CzFrfJGUUD3DQocUNdPjgIGQl8YNtjAjQ"
CREDENTIALS_FILE = "credentials.json"

INITIAL_SHEET_ROWS = 5000
INITIAL_SHEET_COLS = 30
UPDATE_INTERVAL = 30  # Segundos para la actualización periódica

groups = {
    -1001669758312: "zin alpha entries",
@@ -165,11 +165,15 @@
        return 0.0

#############################################
#  🤖 GEMINI: CLASIFICACIÓN IA
#############################################

def gemini_classify(text):
    """Clasifica con Gemini si es relevante."""
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel("gemini-pro")
@@ -179,14 +183,16 @@
        print(f"🔍 Gemini classify => {is_relevant}")
        return is_relevant
    except Exception as e:
        print(f"⚠️ Error Gemini: {e}")
        return False

#############################################
#  💾 ALMACÉN DE PARES (tracked_pairs)
#############################################

tracked_pairs = {}
def load_tracked_pairs():
    """Carga la info (CA, symbol, initial_price, row_index) desde ca_tracking."""
    try:
@@ -224,7 +230,7 @@
def get_dexscreener_data_for_pairs(chain, pair_addresses):
    """
    Llama a /latest/dex/pairs/{chain}/{addr1},{addr2}...
    Retorna un dict con la info de cada par.
    """
    joined = ",".join(pair_addresses)
    url = f"https://api.dexscreener.com/latest/dex/pairs/{chain}/{joined}"
@@ -233,32 +239,31 @@
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        # data puede contener 'pairs': [...] o 'pair': ...
        # Para múltiples direcciones, DexScreener retorna 'pairs': []
        pairs = data.get('pairs')
        if pairs is not None:
            # Caso de batch con 'pairs': [...]
            return pairs
        # Caso de single pair con 'pair': ...
        single = data.get('pair')
        if single:
            return [single]
        # Caso de no encontrar nada
        return []
    except Exception as e:
        print(f"⚠️ Error en get_dexscreener_data_for_pairs: {e}")
        return []

def extract_data_fields(pair_json):
    """Extrae los campos: price, liquidity, volume_24h, fdv, txns_24h, market_cap, pair_created_at, dex_id, base_token."""
    try:
        price_str = pair_json.get('priceUsd', "0")
        price = parse_float(price_str) or 0.0
        liq = parse_float(str(pair_json.get('liquidity', {}).get('usd', 0))) or 0.0
        vol = parse_float(str(pair_json.get('volume', {}).get('h24', 0))) or 0.0
        fdv = parse_float(str(pair_json.get('fdv', 0))) or 0.0
        txns_24 = (pair_json.get('txns', {}).get('h24', {}).get('buys', 0)
                   + pair_json.get('txns', {}).get('h24', {}).get('sells', 0))
        mc = parse_float(str(pair_json.get('marketCap', 0))) or 0.0
        pair_created = pair_json.get('pairCreatedAt', None)
        dex_id = pair_json.get('dexId', "")
@@ -284,8 +289,8 @@

async def update_price_history():
    """
    Cada 30s (o el tiempo que definas) actualiza los precios en 'ca_tracking'
    y agrega histórico en la hoja individual del token.
    """
    while True:
        try:
@@ -297,35 +302,29 @@
            all_pair_addresses = list(tracked_pairs.keys())
            print(f"🔄 Actualizando {len(all_pair_addresses)} pares en DexScreener...")

            # Dividir en lotes de hasta 30 direcciones
            chunk_size = 30
            updates_ca_tracking = []
            now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            for i in range(0, len(all_pair_addresses), chunk_size):
                batch = all_pair_addresses[i:i+chunk_size]
                # Llamada al endpoint /pairs/solana/dir1,dir2,...
                results = get_dexscreener_data_for_pairs("solana", batch)

                # 'results' es una lista de pares
                for pair_data in results:
                    pair_addr = pair_data.get('pairAddress', "")
                    if pair_addr not in tracked_pairs:
                        # Pudo aparecer un par no registrado
                        continue
                    # Extraer data
                    extracted = extract_data_fields(pair_data)
                    if not extracted or extracted["price"] <= 0:
                        print(f"⚠️ Datos inválidos en {pair_addr}")
                        continue

                    # Actualizar ca_tracking
                    info_tracked = tracked_pairs[pair_addr]
                    init_price = info_tracked["initial_price"]
                    profit = compute_profit_percent(extracted["price"], init_price)
                    row_idx = info_tracked["row_index"]

                    # update range H..K => [price, profit, liquidez, volumen]
                    updates_ca_tracking.append({
                        "range": f"H{row_idx}:K{row_idx}",
                        "values": [[
@@ -336,10 +335,10 @@
                        ]]
                    })

                    # Agregar histórico en la hoja individual
                    ws_symb = ensure_crypto_sheet(info_tracked["symbol"])
                    if ws_symb:
                        new_hist_row = [
                            now_str,
                            extracted["price"],
                            profit,
@@ -351,9 +350,8 @@
                            extracted["dex_id"],
                            extracted["symbol"]
                        ]
                        await asyncio.to_thread(safe_append_row, ws_symb, new_hist_row)

            # Aplicar updates en ca_tracking (solo si hay algo)
            if updates_ca_tracking:
                await asyncio.to_thread(ws_ca_tracking.batch_update, updates_ca_tracking)
                print(f"🔄 {len(updates_ca_tracking)} actualizaciones en ca_tracking.")
@@ -378,10 +376,8 @@
     - source="DEX" => address extraído de link DexScreener
    """
    found = []
    # CA
    for match in CA_REGEX.findall(text):
        found.append(("CA", match))
    # Dex link
    for match in DEX_LINK_REGEX.findall(text):
        found.append(("DEX", match))
    return found
@@ -410,7 +406,7 @@
        row[group_to_col_index[group_name]] = msg_text
    await asyncio.to_thread(safe_append_row, ws_messages, row)

    # Filtro con Gemini
    relevant = await asyncio.to_thread(gemini_classify, msg_text)
    if not relevant:
        print("⚠️ Mensaje irrelevante IA.")
@@ -423,97 +419,76 @@
        return
    print(f"🔍 Direcciones halladas => {candidates}")

    # Manejo por CA
    #  - Búsqueda en /search, filtrar baseToken.address=CA, best pair
    # Manejo por DEX link
    #  - Intentar /search con la address, filtrar pairAddress == link
    for (src, addr) in candidates:
        if src == "CA":
            # Buscar sus pares => /search?q=addr&chain=solana
            try:
                best_pair_data = find_best_pair_for_ca(addr)
                if not best_pair_data:
                    print(f"⚠️ No se pudo hallar par con liquidez > 0 para CA {addr}")
                    continue
                # Registrar
                register_new_pair(best_pair_data, addr, group_name, ts_str)
            except Exception as e:
                print(f"❌ Error manejando CA {addr} => {e}")
        else:
            # Link DexScreener => se asume que 'addr' es pairAddress
            # Hacemos fallback => /search?q=addr => filtrar pairAddress
            try:
                pair_data = find_pair_by_address(addr)
                if not pair_data:
                    print(f"⚠️ No se pudo hallar info para par {addr}")
                    continue
                # Registrar
                register_new_pair(pair_data, addr, group_name, ts_str)
            except Exception as e:
                print(f"❌ Error manejando pair {addr} => {e}")

def find_best_pair_for_ca(ca):
    """
    Llama a /search?q=<CA>&chain=solana, filtra baseToken.address=CA,
    escoge el par con mayor liquidez y lo retorna.
    """
    url = f"https://api.dexscreener.com/latest/dex/search?q={ca}&chain=solana"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()
    pairs = data.get("pairs", [])
    # Filtrar
    valid = [p for p in pairs if p.get('baseToken', {}).get('address', '').lower() == ca.lower()]
    if not valid:
        return None
    best = max(valid, key=lambda x: float(x.get('liquidity', {}).get('usd', 0) or 0))
    return best

def find_pair_by_address(pair_addr):
    """
    Llama /search?q=<pair_addr>&chain=solana, retorna el par exacto
    si se encuentra en pairs con pairAddress=pair_addr
    """
    url = f"https://api.dexscreener.com/latest/dex/search?q={pair_addr}&chain=solana"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()
    pairs = data.get('pairs', [])
    for p in pairs:
        if p.get('pairAddress', '').lower() == pair_addr.lower():
            return p
    return None

def register_new_pair(pair_data, raw_addr, group_name, ts_str):
    """
    Dado un pair_data de DexScreener y la dirección (CA o pair) extraída, registra en ca_tracking.
    """
    pair_addr = pair_data.get("pairAddress")
    if not pair_addr:
        print(f"⚠️ pairAddress faltante en {raw_addr}")
        return
    # Verificar duplicado
    if duplicate_checker.is_duplicate(pair_addr):
        print(f"ℹ️ Par duplicado => {pair_addr}")
        return

    # Extraer fields
    extracted = extract_data_fields(pair_data)
    if not extracted or extracted["price"] <= 0:
        print(f"⚠️ Datos inválidos para {raw_addr}")
        return

    # Insertar fila en ca_tracking
    row = [
        ts_str,
        group_name,
        raw_addr,                # CA real, o pair if venía del link
        extracted["dex_id"],
        extracted["symbol"],
        pair_addr,
        extracted["price"],
        extracted["price"],     # current == initial
        0.0,                    # profit from call
        extracted["liquidity"],
        extracted["volume_24h"],
        extracted["fdv"],
@@ -526,7 +501,6 @@
        print("❌ Falló la inserción en ca_tracking.")
        return

    # Guardar en tracked_pairs
    tracked_pairs[pair_addr] = {
        "ca": raw_addr,
        "symbol": extracted["symbol"],
@@ -543,83 +517,82 @@

async def immediate_update():
    """
    Actualiza una vez antes de arrancar el bot,
    usando /pairs/solana/<joined_addresses> en lotes,
    para que los datos de ca_tracking tengan valor actual.
    """
    print("⏳ Immediate update...")
    try:
        if not tracked_pairs:
            print("🔄 No hay pares para actualizar en immediate_update.")
            return

        all_pairs = list(tracked_pairs.keys())
        chunk_size = 30
        updates = []
        for i in range(0, len(all_pairs), chunk_size):
            batch = all_pairs[i:i+chunk_size]
            results = get_dexscreener_data_for_pairs("solana", batch)
            for pair_json in results:
                pair_addr = pair_json.get("pairAddress", "")
                if pair_addr not in tracked_pairs:
                    continue
                extracted = extract_data_fields(pair_json)
                if not extracted or extracted["price"] <= 0:
                    continue
                init_price = tracked_pairs[pair_addr]["initial_price"]
                profit = compute_profit_percent(extracted["price"], init_price)
                row_idx = tracked_pairs[pair_addr]["row_index"]
                updates.append({
                    "range": f"H{row_idx}:K{row_idx}",
                    "values": [[
                        extracted["price"],
                        profit,
                        extracted["liquidity"],
                        extracted["volume_24h"]
                    ]]
                })
        if updates:
            ws_ca_tracking.batch_update(updates)
            print(f"🔄 immediate_update => {len(updates)} filas actualizadas.")
        else:
            print("ℹ️ immediate_update => sin updates.")
    except Exception as e:
        print(f"❌ Error immediate_update => {e}")

#############################################
#  🏁 BOT TELEGRAM
#############################################

client = TelegramClient("session_name", API_ID, API_HASH)

@client.on(events.NewMessage(chats=list(groups.keys())))
async def handler(event):
    print(f"📥 Mensaje en {event.chat_id}")
    await process_message(event)

async def main():
    print("🚀 Iniciando Bot DexScreener + IA + Sheets...")
    # Cargar pares
    await asyncio.to_thread(load_tracked_pairs)
    print(f"⚙️ Se han cargado {len(tracked_pairs)} pares en memoria.")

    # Actualización inicial
    await immediate_update()

    # Conectar Telegram
    await client.start()
    print("✅ Bot conectado a Telegram.")

    # Tarea de actualización periódica
    asyncio.create_task(update_price_history())

    print(f"🤖 Bot en marcha, actualizando cada {UPDATE_INTERVAL}s.")
    await client.run_until_disconnected()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"❌ Error fatal => {e}")
    finally:
        print("🛑 Bot detenido.")
