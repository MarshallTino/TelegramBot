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
UPDATE_INTERVAL = 30  # Intervalo de actualización en segundos

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
    """Añade una fila, expandiendo la hoja si hace falta."""
    for attempt in range(3):
        try:
            current_count = len(sheet.get_all_values())
            if current_count + 1 >= sheet.row_count:
                sheet.add_rows(500)
                print("➕ Añadidas 500 filas extra.")
            sheet.append_row(row_data)
            print(f"✅ Fila añadida en posición {current_count + 1}.")
            return current_count + 1
        except Exception as e:
            print(f"❌ Error añadiendo fila (intento {attempt+1}/3): {e}")
            time.sleep(1)
    return None

def get_or_create_worksheet(spreadsheet, sheet_name, headers):
    """Obtiene o crea una hoja con cabeceras predefinidas."""
    try:
        ws = spreadsheet.worksheet(sheet_name)
        if ws.row_values(1) != headers:
            print(f"⚠️ Cabeceras distintas en '{sheet_name}'. Se limpiará la hoja.")
            ws.clear()
            ws.append_row(headers)
        print(f"✅ Hoja '{sheet_name}' lista.")
        return ws
    except gspread.WorksheetNotFound:
        print(f"📄 Creando nueva hoja: {sheet_name}")
        ws = spreadsheet.add_worksheet(
            title=sheet_name,
            rows=INITIAL_SHEET_ROWS,
            cols=INITIAL_SHEET_COLS
        )
        ws.append_row(headers)
        return ws
    except Exception as e:
        print(f"❌ Error crítico con hoja '{sheet_name}': {e}")
        exit()

#############################################
#  🛠️ CONFIGURACIÓN DE HOJAS PRINCIPALES
#############################################

raw_headers = ["Timestamp"] + list(groups.values())
ws_messages = get_or_create_worksheet(spreadsheet, "raw_messages", raw_headers)

ca_tracking_headers = [
    "Timestamp", "Grupo", "CA", "DEX", "Símbolo", "PairAddress",
    "Initial Price USD", "Current Price USD", "Profit from Call (%)",
    "Liquidity USD", "Volume 24h", "FDV", "Transacciones 24h", "Market Cap", "Created At"
]
ws_ca_tracking = get_or_create_worksheet(spreadsheet, "ca_tracking", ca_tracking_headers)

group_to_col_index = {name: i+1 for i, name in enumerate(groups.values())}

# Hojas individuales: {symbol: worksheet}
crypto_sheets = {}
crypto_sheet_headers = [
    "Timestamp", "Price USD", "Profit (%)", "Liquidity USD",
    "Volume 24h", "FDV", "Market Cap", "Pair Created At", "Dex ID", "Token Symbol"
]

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
        """Carga pairs ya registrados desde la hoja ca_tracking."""
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
#  🧮 FUNCIONES AUXILIARES
#############################################

def parse_float(num_str):
    try:
        return float(num_str.replace(",", ".")) if num_str else None
    except:
        return None

def compute_profit_percent(current_price, initial_price):
    try:
        return round(((current_price - initial_price)/initial_price) * 100, 2)
    except:
        return 0.0

#############################################
#  🤖 GEMINI: CLASIFICACIÓN IA CON FALLBACK
#############################################

def gemini_classify(text):
    """
    Clasifica el mensaje como relevante o no.
    Si la API de Gemini falla (ejemplo 429), marcamos por fallback el mensaje como relevante
    para no bloquear el flujo.
    """
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel("gemini-pro")
        prompt = f"Clasifica como RELEVANTE sólo si menciona nuevo token:\n'{text}'"
        resp = model.generate_content(prompt)
        is_relevant = "relevante" in resp.text.strip().lower()
        print(f"🔍 Gemini classify => {is_relevant}")
        return is_relevant
    except Exception as e:
        print(f"⚠️ Error Gemini: {e}. Fallback => True (procesar mensaje).")
        # Retornamos True para no bloquear la detección de nuevos CAs
        return True

#############################################
#  💾 ALMACÉN DE PARES (tracked_pairs)
#############################################

tracked_pairs = {}

def load_tracked_pairs():
    """Carga la info (CA, symbol, initial_price, row_index) desde ca_tracking."""
    try:
        values = ws_ca_tracking.get_all_values()
        if len(values) < 2:
            print("⚠️ ca_tracking sin registros.")
            return
        headers = values[0]
        for i, row in enumerate(values[1:], start=2):
            try:
                pair_addr = row[headers.index("PairAddress")]
                if pair_addr and pair_addr not in tracked_pairs:
                    init_price_str = row[headers.index("Initial Price USD")]
                    init_price = parse_float(init_price_str)
                    if init_price and init_price > 0:
                        ca_str = row[headers.index("CA")]
                        symb_str = row[headers.index("Símbolo")]
                        tracked_pairs[pair_addr] = {
                            "ca": ca_str,
                            "symbol": symb_str,
                            "initial_price": init_price,
                            "row_index": i
                        }
                        ensure_crypto_sheet(symb_str)
            except Exception as e:
                print(f"⚠️ Error fila {i} => {e}")
        print(f"✅ Se cargaron {len(tracked_pairs)} pares.")
    except Exception as e:
        print(f"❌ Error al cargar tracked_pairs: {e}")

#############################################
#  📊 DEXSCREENER: OBTENER DATOS
#############################################

def get_dexscreener_data_for_pairs(chain, pair_addresses):
    """
    Llama a /latest/dex/pairs/{chain}/{addr1},{addr2}...
    Retorna un listado de pares.
    """
    joined = ",".join(pair_addresses)
    url = f"https://api.dexscreener.com/latest/dex/pairs/{chain}/{joined}"
    print(f"==> DexScreener request: {url}")
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        
        # data puede contener 'pairs': [...] o 'pair': ...
        pairs = data.get('pairs')
        if pairs is not None:
            return pairs
        single = data.get('pair')
        if single:
            return [single]
        return []
    except Exception as e:
        print(f"⚠️ Error en get_dexscreener_data_for_pairs: {e}")
        return []

def extract_data_fields(pair_json):
    """Extrae campos relevantes del JSON."""
    try:
        price_str = pair_json.get('priceUsd', "0")
        price = parse_float(price_str) or 0.0
        liq = parse_float(str(pair_json.get('liquidity', {}).get('usd', 0))) or 0.0
        vol = parse_float(str(pair_json.get('volume', {}).get('h24', 0))) or 0.0
        fdv = parse_float(str(pair_json.get('fdv', 0))) or 0.0
        txns_24 = (
            pair_json.get('txns', {}).get('h24', {}).get('buys', 0)
            + pair_json.get('txns', {}).get('h24', {}).get('sells', 0)
        )
        mc = parse_float(str(pair_json.get('marketCap', 0))) or 0.0
        pair_created = pair_json.get('pairCreatedAt', None)
        dex_id = pair_json.get('dexId', "")
        symb = pair_json.get('baseToken', {}).get('symbol', "???")
        return {
            "price": price,
            "liquidity": liq,
            "volume_24h": vol,
            "fdv": fdv,
            "txns_24h": txns_24,
            "market_cap": mc,
            "pair_created_at": pair_created,
            "dex_id": dex_id,
            "symbol": symb
        }
    except Exception as e:
        print(f"⚠️ Error extrayendo campos => {e}")
        return None

#############################################
#  🕓 ACTUALIZACIÓN PERIÓDICA
#############################################

async def update_price_history():
    """
    Actualiza periódicamente (cada 30s) el precio en ca_tracking y
    añade histórico en cada hoja individual.
    """
    while True:
        try:
            if not tracked_pairs:
                print("⚠️ Sin pares en seguimiento.")
                await asyncio.sleep(UPDATE_INTERVAL)
                continue
            
            all_pair_addresses = list(tracked_pairs.keys())
            print(f"🔄 Actualizando {len(all_pair_addresses)} pares en DexScreener...")
            
            chunk_size = 30
            updates_ca_tracking = []
            now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            for i in range(0, len(all_pair_addresses), chunk_size):
                batch = all_pair_addresses[i:i+chunk_size]
                results = get_dexscreener_data_for_pairs("solana", batch)
                
                for pair_data in results:
                    pair_addr = pair_data.get('pairAddress', "")
                    if pair_addr not in tracked_pairs:
                        continue
                    extracted = extract_data_fields(pair_data)
                    if not extracted or extracted["price"] <= 0:
                        print(f"⚠️ Datos inválidos en par => {pair_addr}")
                        continue
                    
                    info_tracked = tracked_pairs[pair_addr]
                    init_price = info_tracked["initial_price"]
                    profit = compute_profit_percent(extracted["price"], init_price)
                    row_idx = info_tracked["row_index"]
                    
                    # Actualizar en ca_tracking
                    updates_ca_tracking.append({
                        "range": f"H{row_idx}:K{row_idx}",
                        "values": [[
                            extracted["price"],
                            profit,
                            extracted["liquidity"],
                            extracted["volume_24h"]
                        ]]
                    })
                    
                    # Registrar histórico
                    ws_symb = ensure_crypto_sheet(info_tracked["symbol"])
                    if ws_symb:
                        hist_row = [
                            now_str,
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
                        await asyncio.to_thread(safe_append_row, ws_symb, hist_row)
            
            if updates_ca_tracking:
                await asyncio.to_thread(ws_ca_tracking.batch_update, updates_ca_tracking)
                print(f"🔄 {len(updates_ca_tracking)} actualizaciones en ca_tracking.")
            else:
                print("ℹ️ No hay updates para ca_tracking esta ronda.")
        except Exception as e:
            print(f"❌ Error en update_price_history => {e}")
        
        await asyncio.sleep(UPDATE_INTERVAL)

#############################################
#  🔍 DETECCIÓN CA/DEX LINKS
#############################################

CA_REGEX = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")
DEX_LINK_REGEX = re.compile(r"https?://dexscreener\.com/solana/([^/\s\?]+)")

def detect_addresses(text):
    """
    Retorna una lista de tuplas (source, address) con:
     - source="CA"  => dirección base
     - source="DEX" => address extraído de link DexScreener
    """
    found = []
    for match in CA_REGEX.findall(text):
        found.append(("CA", match))
    for match in DEX_LINK_REGEX.findall(text):
        found.append(("DEX", match))
    return found

#############################################
#  🏷️ PROCESAMIENTO DE MENSAJES
#############################################

processed_msg_ids = set()

async def process_message(event):
    msg_id = event.message.id
    if msg_id in processed_msg_ids:
        print(f"⚠️ Mensaje duplicado {msg_id}, se omite.")
        return
    processed_msg_ids.add(msg_id)
    
    chat_id = event.chat_id
    group_name = groups.get(chat_id, "Desconocido")
    msg_text = event.message.message
    ts_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Guardar en raw_messages
    row = [ts_str] + [""]*(len(raw_headers)-1)
    if group_name in group_to_col_index:
        row[group_to_col_index[group_name]] = msg_text
    await asyncio.to_thread(safe_append_row, ws_messages, row)
    
    # Clasificación con Gemini (con fallback en error => True)
    relevant = await asyncio.to_thread(gemini_classify, msg_text)
    if not relevant:
        print("⚠️ Mensaje irrelevante IA.")
        return
    
    # Detectar direcciones
    candidates = detect_addresses(msg_text)
    if not candidates:
        print("❌ No se hallaron direcciones CA/DEX en el mensaje.")
        return
    print(f"🔍 Direcciones halladas => {candidates}")
    
    for (src, addr) in candidates:
        if src == "CA":
            # Buscar best pair => /search?q=addr&chain=solana
            try:
                best_pair_data = find_best_pair_for_ca(addr)
                if not best_pair_data:
                    print(f"⚠️ No se encontró par con liquidez > 0 para CA {addr}")
                    continue
                register_new_pair(best_pair_data, addr, group_name, ts_str)
            except Exception as e:
                print(f"❌ Error en CA {addr} => {e}")
        else:
            # Link DexScreener => interpretado como pairAddress
            try:
                pair_data = find_pair_by_address(addr)
                if not pair_data:
                    print(f"⚠️ No se halló info para pair {addr}")
                    continue
                register_new_pair(pair_data, addr, group_name, ts_str)
            except Exception as e:
                print(f"❌ Error en pair {addr} => {e}")

def find_best_pair_for_ca(ca):
    url = f"https://api.dexscreener.com/latest/dex/search?q={ca}&chain=solana"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()
    pairs = data.get("pairs", [])
    valid = [p for p in pairs if p.get('baseToken', {}).get('address', '').lower() == ca.lower()]
    if not valid:
        return None
    return max(valid, key=lambda x: float(x.get('liquidity', {}).get('usd', 0) or 0))

def find_pair_by_address(pair_addr):
    url = f"https://api.dexscreener.com/latest/dex/search?q={pair_addr}&chain=solana"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()
    pairs = data.get("pairs", [])
    for p in pairs:
        if p.get('pairAddress', '').lower() == pair_addr.lower():
            return p
    return None

def register_new_pair(pair_data, raw_addr, group_name, ts_str):
    pair_addr = pair_data.get("pairAddress")
    if not pair_addr:
        print(f"⚠️ pairAddress faltante en {raw_addr}")
        return
    # Checar duplicado
    if duplicate_checker.is_duplicate(pair_addr):
        print(f"ℹ️ Par duplicado => {pair_addr}")
        return
    
    extracted = extract_data_fields(pair_data)
    if not extracted or extracted["price"] <= 0:
        print(f"⚠️ Datos inválidos para {raw_addr}")
        return
    
    # Inserción en ca_tracking
    row = [
        ts_str,
        group_name,
        raw_addr,
        extracted["dex_id"],
        extracted["symbol"],
        pair_addr,
        extracted["price"],
        extracted["price"],
        0.0,
        extracted["liquidity"],
        extracted["volume_24h"],
        extracted["fdv"],
        extracted["txns_24h"],
        extracted["market_cap"],
        extracted["pair_created_at"]
    ]
    row_idx = safe_append_row(ws_ca_tracking, row)
    if not row_idx:
        print("❌ Falló la inserción en ca_tracking.")
        return
    
    tracked_pairs[pair_addr] = {
        "ca": raw_addr,
        "symbol": extracted["symbol"],
        "initial_price": extracted["price"],
        "row_index": row_idx
    }
    duplicate_checker.existing_pairs.add(pair_addr)
    ensure_crypto_sheet(extracted["symbol"])
    print(f"🆕 Nuevo par registrado => {extracted['symbol']} / {pair_addr}")

#############################################
#  🔄 ACTUALIZACIÓN INICIAL
#############################################

async def immediate_update():
    """
    Actualiza los pares ya existentes en tracked_pairs antes de iniciar el bot,
    usando /pairs/solana/<addr1>,<addr2>... en lotes de 30.
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
