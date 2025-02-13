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

UPDATE_INTERVAL = 30  # segundos para actualización periódica

#############################################
#  🗂️ Configuración de grupos a escuchar
#############################################

groups = {
    -1001669758312: "zin alpha entries",
    -1001756488143: "sol",
    -1001593046999: "vulturecalls",
    -1002124780831: "printor gambles",
    -1002161891429: "printor calls",
    -1001198046393: "pows gems calls",
    -1001870127953: "watisdes",
    -1002390818052: "obitcalls",
    -1002234182572: "ketchums gambles",
    -1001355642881: "crypto eus gems",
    -1002360457432: "Marshall Calls"
}

#############################################
#  📡 CONEXIÓN A GOOGLE SHEETS
#############################################

creds = Credentials.from_service_account_file(
    CREDENTIALS_FILE,
    scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
)
gs_client = gspread.authorize(creds)
spreadsheet = gs_client.open_by_key(SHEET_ID)
print("✅ Conectado a Google Sheets")

def safe_append_row(sheet, row_data):
    """Añade una fila nueva en la hoja, expandiéndola si es necesario."""
    for attempt in range(3):
        try:
            current_count = len(sheet.get_all_values())
            if current_count + 1 >= sheet.row_count:
                sheet.add_rows(500)
                print("➕ Se añadieron 500 filas extra.")
            sheet.append_row(row_data)
            print(f"✅ Fila añadida en posición {current_count + 1}.")
            return current_count + 1
        except Exception as e:
            print(f"❌ Error añadiendo fila (intento {attempt+1}/3): {e}")
            time.sleep(1)
    return None

def get_or_create_worksheet(spreadsheet, sheet_name, headers):
    """Obtiene o crea una hoja con las cabeceras dadas."""
    try:
        ws = spreadsheet.worksheet(sheet_name)
        existing_headers = ws.row_values(1)
        if existing_headers != headers:
            print(f"⚠️ Cabeceras distintas en '{sheet_name}'. Se limpiará la hoja.")
            ws.clear()
            ws.append_row(headers)
        print(f"✅ Hoja '{sheet_name}' lista.")
        return ws
    except gspread.WorksheetNotFound:
        print(f"📄 Creando nueva hoja: {sheet_name}")
        ws = spreadsheet.add_worksheet(
            title=sheet_name,
            rows=5000,
            cols=30
        )
        ws.append_row(headers)
        return ws
    except Exception as e:
        print(f"❌ Error crítico con hoja '{sheet_name}': {e}")
        exit()

#############################################
#  Configuración HOJAS
#############################################

# Hoja raw_messages
raw_headers = ["Timestamp"] + list(groups.values())
ws_messages = get_or_create_worksheet(spreadsheet, "raw_messages", raw_headers)

# Hoja ca_tracking (overview)
# Se mantiene lo esencial: 
#   - Timestamp (cuando se registra la call)
#   - Grupo (Telegram)
#   - CA (token)
#   - PairAddress (liquidez)
#   - Símbolo
#   - Initial Price
#   - Current Price
#   - Profit (%)
ca_tracking_headers = [
    "Timestamp",
    "Grupo",
    "CA",
    "PairAddress",
    "Símbolo",
    "Initial Price USD",
    "Current Price USD",
    "Profit (%)"
]
ws_ca_tracking = get_or_create_worksheet(spreadsheet, "ca_tracking", ca_tracking_headers)
group_to_col_index = {name: i+1 for i, name in enumerate(groups.values())}

# Hojas individuales (por token)
# Incluimos el Grupo, y todos los datos (Price, Liquidez, FDV, etc.)
# También convertimos PairCreated a timestamp humano.
crypto_sheet_headers = [
    "Timestamp",
    "Grupo",
    "Price USD",
    "Profit (%)",
    "Liquidity USD",
    "Volume 24h",
    "FDV",
    "Market Cap",
    "Pair Created At",
    "Dex ID",
    "Token Symbol"
]

crypto_sheets = {}

def ensure_crypto_sheet(symbol):
    if symbol in crypto_sheets:
        return crypto_sheets[symbol]
    try:
        ws = get_or_create_worksheet(spreadsheet, symbol, crypto_sheet_headers)
        crypto_sheets[symbol] = ws
        print(f"✅ Hoja '{symbol}' verificada/creada.")
        return ws
    except Exception as e:
        print(f"❌ Error creando/verificando hoja para '{symbol}': {e}")
        return None

#############################################
#  🕵️ DETECCIÓN DE DUPLICADOS
#############################################

class DuplicateChecker:
    def __init__(self):
        self.existing_pairs = set()
        self.load_existing_pairs()
    
    def load_existing_pairs(self):
        try:
            records = ws_ca_tracking.get_all_records()
            self.existing_pairs = {row['PairAddress'] for row in records if row.get('PairAddress')}
            print(f"✅ DuplicateChecker: {len(self.existing_pairs)} pares cargados.")
        except Exception as e:
            print(f"❌ Error cargando duplicados: {e}")
    
    def is_duplicate(self, pair_address):
        self.load_existing_pairs()
        return pair_address in self.existing_pairs

duplicate_checker = DuplicateChecker()

#############################################
#  AUXILIARES
#############################################

def parse_float(num_str):
    try:
        return float(num_str.replace(",", ".")) if num_str else None
    except:
        return 0.0

def compute_profit_percent(current_price, initial_price):
    if not initial_price:
        return 0.0
    return round(((current_price - initial_price)/initial_price) * 100, 2)

#############################################
#  🤖 GEMINI CLASIFY (con fallback)
#############################################

def gemini_classify(text):
    """
    Si la API falla, devolvemos True (procesar).
    """
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel("gemini-pro")
        prompt = f"Clasifica como RELEVANTE sólo si menciona nuevo token:\n'{text}'"
        resp = model.generate_content(prompt)
        return ("relevante" in resp.text.strip().lower())
    except Exception as e:
        print(f"⚠️ Error Gemini: {e}. Fallback => True")
        return True

#############################################
#  tracked_pairs: { pairAddress: {...} }
#############################################

tracked_pairs = {}

def load_tracked_pairs():
    try:
        values = ws_ca_tracking.get_all_values()
        if len(values) <= 1:
            print("⚠️ ca_tracking sin registros.")
            return
        headers = values[0]
        for i, row in enumerate(values[1:], start=2):
            try:
                pair_addr = row[headers.index("PairAddress")]
                if pair_addr:
                    init_price = parse_float(row[headers.index("Initial Price USD")])
                    symbol = row[headers.index("Símbolo")]
                    tracked_pairs[pair_addr] = {
                        "symbol": symbol,
                        "initial_price": init_price,
                        "row_index": i
                    }
                    # No guardamos CA ni Grupo, ya no se usan para updates
                    ensure_crypto_sheet(symbol)
            except Exception as e:
                print(f"⚠️ Fila {i} => {e}")
        print(f"✅ Se cargaron {len(tracked_pairs)} pares en memoria.")
    except Exception as e:
        print(f"❌ Error en load_tracked_pairs => {e}")

#############################################
#  DexScreener
#############################################

def get_pairs_data_solana(pair_addrs):
    """
    Llama /latest/dex/pairs/solana/<addr1>,<addr2>...
    Devuelve una lista con la info de cada par.
    """
    joined = ",".join(pair_addrs)
    url = f"https://api.dexscreener.com/latest/dex/pairs/solana/{joined}"
    print(f"==> DexScreener request: {url}")
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        # Puede ser 'pairs' o 'pair'
        if "pairs" in data and data["pairs"] is not None:
            return data["pairs"]
        if "pair" in data and data["pair"] is not None:
            return [data["pair"]]
        return []
    except Exception as e:
        print(f"⚠️ Error get_pairs_data_solana => {e}")
        return []

def dex_extract_fields(pair_json):
    """Extrae campos: price, liquidity, volume24h, fdv, marketCap, etc."""
    try:
        price_str = pair_json.get('priceUsd', "0")
        price = parse_float(price_str)
        liquidity = parse_float(str(pair_json.get('liquidity', {}).get('usd', 0)))
        volume_24h = parse_float(str(pair_json.get('volume', {}).get('h24', 0)))
        fdv = parse_float(str(pair_json.get('fdv', 0)))
        txns_24h = (pair_json.get('txns', {}).get('h24', {}).get('buys', 0)
                    + pair_json.get('txns', {}).get('h24', {}).get('sells', 0))
        market_cap = parse_float(str(pair_json.get('marketCap', 0)))
        pair_created = pair_json.get('pairCreatedAt', None)  # milisegundos?

        # Convertimos el pairCreated en formato legible si viene en milis
        pair_created_str = ""
        if isinstance(pair_created, int):
            # Asumimos que es milisegundos
            dt_obj = datetime.datetime.utcfromtimestamp(pair_created/1000.0)
            pair_created_str = dt_obj.strftime("%Y-%m-%d %H:%M:%S UTC")
        elif isinstance(pair_created, str) and pair_created.isdigit():
            # Convertir string a int
            try:
                pair_created_int = int(pair_created)
                dt_obj = datetime.datetime.utcfromtimestamp(pair_created_int/1000.0)
                pair_created_str = dt_obj.strftime("%Y-%m-%d %H:%M:%S UTC")
            except:
                pair_created_str = ""
        else:
            # No es milisegundos
            pair_created_str = ""

        dex_id = pair_json.get('dexId', '')
        symbol = pair_json.get('baseToken', {}).get('symbol', '')

        return {
            "price": price if price else 0.0,
            "liquidity": liquidity if liquidity else 0.0,
            "volume_24h": volume_24h if volume_24h else 0.0,
            "fdv": fdv if fdv else 0.0,
            "txns_24h": txns_24h,
            "market_cap": market_cap if market_cap else 0.0,
            "pair_created_at": pair_created_str,
            "dex_id": dex_id,
            "symbol": symbol
        }
    except Exception as e:
        print(f"⚠️ Error dex_extract_fields => {e}")
        return None

#############################################
#  UPDATE LOOP
#############################################

async def update_price_loop():
    while True:
        try:
            if not tracked_pairs:
                print("⚠️ No hay pares en tracked_pairs.")
                await asyncio.sleep(UPDATE_INTERVAL)
                continue

            all_pairs = list(tracked_pairs.keys())
            print(f"🔄 Actualizando {len(all_pairs)} pares en DexScreener...")
            chunk_size = 30
            updates_ca = []
            now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            for i in range(0, len(all_pairs), chunk_size):
                batch = all_pairs[i:i+chunk_size]
                data_list = get_pairs_data_solana(batch)
                for pair_info in data_list:
                    pair_addr = pair_info.get("pairAddress", "")
                    if pair_addr not in tracked_pairs:
                        continue
                    extracted = dex_extract_fields(pair_info)
                    if not extracted or extracted["price"] <= 0:
                        continue
                    init_price = tracked_pairs[pair_addr]["initial_price"]
                    profit = compute_profit_percent(extracted["price"], init_price)
                    row_idx = tracked_pairs[pair_addr]["row_index"]
                    updates_ca.append({
                        "range": f"F{row_idx}:H{row_idx}",  # columns [Current Price, Profit(%)]
                        "values": [[
                            extracted["price"],
                            profit
                        ]]
                    })

                    # Actualizar la hoja individual
                    symb = tracked_pairs[pair_addr]["symbol"]
                    ws = ensure_crypto_sheet(symb)
                    if ws:
                        # [Timestamp, Grupo, Price, Profit, Liquidity, Vol, FDV, MC, Created, DexID, Symbol]
                        row_hist = [
                            now_str,
                            "?",  # No la tenemos en tracked_pairs, la recordamos en register_new_pair
                            extracted["price"],
                            profit,
                            extracted["liquidity"],
                            extracted["volume_24h"],
                            extracted["fdv"],
                            extracted["market_cap"],
                            extracted["pair_created_at"],
                            extracted["dex_id"],
                            extracted["symbol"]
                        ]
                        await asyncio.to_thread(safe_append_row, ws, row_hist)

            if updates_ca:
                await asyncio.to_thread(ws_ca_tracking.batch_update, updates_ca)
                print(f"🔄 {len(updates_ca)} actualizaciones en ca_tracking.")
            else:
                print("ℹ️ No hay actualizaciones para ca_tracking esta ronda.")
        except Exception as e:
            print(f"❌ Error en update_price_loop => {e}")

        await asyncio.sleep(UPDATE_INTERVAL)

#############################################
#  DETECT CA/DEX
#############################################

CA_REGEX = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")
DEX_LINK_REGEX = re.compile(r"https?://dexscreener\.com/solana/([^/\s\?]+)")

def detect_addresses(text):
    addresses = []
    for match in CA_REGEX.findall(text):
        addresses.append(("CA", match))
    for match in DEX_LINK_REGEX.findall(text):
        addresses.append(("DEX", match))
    return addresses

#############################################
#  PROCESAMIENTO MENSAJES
#############################################

processed_msg_ids = set()

async def handle_message(event):
    # Evitar duplicados
    msg_id = event.message.id
    if msg_id in processed_msg_ids:
        return
    processed_msg_ids.add(msg_id)

    chat_id = event.chat_id
    group_name = groups.get(chat_id, "Desconocido")
    msg_text = event.message.message
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Guardar en raw_messages
    row = [now_str] + [""]*(len(raw_headers)-1)
    if group_name in group_to_col_index:
        row[group_to_col_index[group_name]] = msg_text
    await asyncio.to_thread(safe_append_row, ws_messages, row)

    # IA: fallback => True si error
    is_relevant = await asyncio.to_thread(gemini_classify, msg_text)
    if not is_relevant:
        print("⚠️ Mensaje irrelevante IA.")
        return

    # Extraer CA/DEX
    candidates = detect_addresses(msg_text)
    if not candidates:
        print("❌ No se hallaron direcciones CA/DEX en el mensaje.")
        return

    print(f"🔍 Detectado => {candidates}")
    for (src, addr) in candidates:
        if src == "CA":
            # /search => filtrar baseToken.address=CA => mayor liquidez
            try:
                pair_data = find_best_pair_for_ca(addr)
                if not pair_data:
                    print(f"⚠️ No se encontró par liquidez>0 para CA {addr}")
                    continue
                register_pair(pair_data, addr, group_name, now_str)
            except Exception as e:
                print(f"❌ Error CA {addr} => {e}")
        else:
            # Link DexScreener => interpretado como pairAddress
            try:
                pair_data = find_pair_by_address(addr)
                if not pair_data:
                    print(f"⚠️ No se halló info en /search para pair {addr}")
                    continue
                register_pair(pair_data, addr, group_name, now_str)
            except Exception as e:
                print(f"❌ Error pair {addr} => {e}")

def find_best_pair_for_ca(ca):
    url = f"https://api.dexscreener.com/latest/dex/search?q={ca}&chain=solana"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    js = r.json()
    pairs = js.get("pairs", [])
    # filtrar baseToken.address == ca
    valid = [p for p in pairs if p.get('baseToken', {}).get('address','').lower() == ca.lower()]
    if not valid:
        return None
    best = max(valid, key=lambda x: float(x.get('liquidity',{}).get('usd',0) or 0))
    return best

def find_pair_by_address(pair_addr):
    url = f"https://api.dexscreener.com/latest/dex/search?q={pair_addr}&chain=solana"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    js = r.json()
    pairs = js.get("pairs", [])
    for p in pairs:
        if p.get('pairAddress','').lower() == pair_addr.lower():
            return p
    return None

def register_pair(pair_data, raw_ca_or_pair, group_name, ts_str):
    pair_addr = pair_data.get("pairAddress")
    if not pair_addr:
        return
    # check duplicado
    if duplicate_checker.is_duplicate(pair_addr):
        print(f"ℹ️ Par duplicado => {pair_addr}")
        return

    # extraer fields
    extracted = dex_extract_fields(pair_data)
    if not extracted or extracted["price"] <=0:
        print(f"⚠️ Datos inválidos => {raw_ca_or_pair}")
        return

    # Insertar fila en ca_tracking
    # [Timestamp, Grupo, CA, Pair, Símbolo, InitPrice, CurrPrice, Profit%]
    row = [
        ts_str,
        group_name,
        raw_ca_or_pair,
        pair_addr,
        extracted["symbol"],
        extracted["price"],  # init
        extracted["price"],  # current
        0.0                  # profit
    ]
    row_idx = safe_append_row(ws_ca_tracking, row)
    if not row_idx:
        print("❌ No se pudo registrar en ca_tracking.")
        return

    tracked_pairs[pair_addr] = {
        "symbol": extracted["symbol"],
        "initial_price": extracted["price"],
        "row_index": row_idx
    }
    duplicate_checker.existing_pairs.add(pair_addr)
    ensure_crypto_sheet(extracted["symbol"])
    print(f"🆕 Nuevo par => {extracted['symbol']} / {pair_addr}")

#############################################
#  ACTUALIZACIÓN INICIAL
#############################################

async def immediate_update():
    print("⏳ Iniciando actualización inicial de pares previos...")
    if not tracked_pairs:
        print("⚠️ Ningún par previo en ca_tracking.")
        return
    try:
        all_pairs = list(tracked_pairs.keys())
        chunk = 30
        updates = []
        for i in range(0,len(all_pairs), chunk):
            batch = all_pairs[i:i+chunk]
            pairs_data = get_pairs_data_solana(batch)
            for pair_info in pairs_data:
                pair_addr = pair_info.get('pairAddress','')
                if pair_addr not in tracked_pairs:
                    continue
                extracted = dex_extract_fields(pair_info)
                if not extracted or extracted["price"]<=0:
                    continue
                init_price = tracked_pairs[pair_addr]["initial_price"]
                profit = compute_profit_percent(extracted["price"], init_price)
                row_idx = tracked_pairs[pair_addr]["row_index"]
                updates.append({
                    "range": f"F{row_idx}:H{row_idx}",
                    "values": [[
                        extracted["price"],
                        profit
                    ]]
                })
        if updates:
            ws_ca_tracking.batch_update(updates)
            print(f"🔄 immediate_update => {len(updates)} actualizaciones.")
        else:
            print("ℹ️ immediate_update => sin updates.")
    except Exception as e:
        print(f"❌ Error immediate_update => {e}")

#############################################
#  TELEGRAM BOT
#############################################

client = TelegramClient("session_name", API_ID, API_HASH)

@client.on(events.NewMessage(chats=list(groups.keys())))
async def handler(event):
    await handle_message(event)

async def main():
    print("🚀 Iniciando Bot DexScreener + IA + Sheets...")
    # Cargar pares
    await asyncio.to_thread(load_tracked_pairs)
    # Update inicial
    await immediate_update()
    # Conectar
    await client.start()
    print("✅ Bot conectado a Telegram.")
    # Tarea de actualización
    asyncio.create_task(update_price_loop())
    print(f"🤖 Bot corriendo. Intervalo update: {UPDATE_INTERVAL}s")
    await client.run_until_disconnected()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"❌ Error fatal => {e}")
    finally:
        print("🛑 Bot detenido.")
