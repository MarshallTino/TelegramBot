# bot.py

import asyncio
import re
from telethon import TelegramClient, events
from google.oauth2.service_account import Credentials

from gemini_ai.gemini_classifier import gemini_classify
from dex_screener.dex_api import (
    get_pairs_data,
    search_pairs,
    extract_pair_fields,
    extract_all_data_as_json,
    extract_all_columns
)
from google_sheets.sheets_manager import connect_sheets, get_or_create_worksheet, safe_append_row
from utils.common import (
    parse_float,
    compute_profit_percent,
    current_timestamp_str,
    sheet_name_for_chain_symbol
)

#############################################
#  CONFIG
#############################################

API_ID = 28644650
API_HASH = "e963f9b807bcf9d665b1d20de66f7c69"
GEMINI_API_KEY = "YOUR_DUMMY_KEY"

SHEET_ID = "1K7p3Yeu6k1CzFrfJGUUD3DQocUNdPjgIGQl8YNtjAjQ"
CREDENTIALS_FILE = "credentials.json"

UPDATE_INTERVAL = 30

#############################################
#  GRUPOS
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

raw_headers = ["Timestamp"] + list(groups.values())
group_to_col_index = {
    name: i+1 for i, name in enumerate(groups.values())
}

#############################################
#  SHEETS
#############################################

creds = Credentials.from_service_account_file(
    CREDENTIALS_FILE,
    scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
)
spreadsheet = connect_sheets(creds, SHEET_ID)

ws_messages = get_or_create_worksheet(spreadsheet, "raw_messages", raw_headers)

ca_tracking_headers = [
    "Timestamp",
    "Chain",
    "Grupo",
    "CA",
    "PairAddress",
    "Símbolo",
    "Initial Price USD",
    "Current Price USD",
    "Profit (%)"
]
ws_ca_tracking = get_or_create_worksheet(spreadsheet, "ca_tracking", ca_tracking_headers)

# Hoja individual => incl. raw_api_data
crypto_extended_headers = [
    "Timestamp",
    "chainId",
    "dexId",
    "pairAddress",
    "baseTokenAddress",
    "baseTokenName",
    "baseTokenSymbol",
    "quoteTokenAddress",
    "quoteTokenName",
    "quoteTokenSymbol",
    "priceNative",
    "priceUsd",
    "txns24hBuys",
    "txns24hSells",
    "volume24h",
    "priceChange24h",
    "liquidityUsd",
    "liquidityBase",
    "liquidityQuote",
    "fdv",
    "marketCap",
    "pairCreatedAt",
    "raw_api_data"
]

crypto_sheets = {}

crypto_grouped_titles = [
    "General Info","General Info","General Info",
    "Price Info","Price Info","Liquidity Info",
    "Liquidity Info","Liquidity Info","Liquidity Info",
    "Pair Info","Pair Info","Pair Info","Full JSON"
]

def ensure_crypto_sheet(chain, symbol):
    sname = sheet_name_for_chain_symbol(chain, symbol)
    if sname in crypto_sheets:
        return crypto_sheets[sname]
    try:
        ws = spreadsheet.worksheet(sname)
        print(f"✅ Hoja '{sname}' ya existe.")
    except:
        print(f"📄 Creando/Verificando nueva hoja '{sname}'")
        ws = get_or_create_worksheet(spreadsheet, sname, crypto_extended_headers)
        ws.insert_row(crypto_grouped_titles, 1)
        print(f"✅ Fila de agrupación creada en '{sname}'")
    crypto_sheets[sname] = ws
    return ws

#############################################
#  DUPLICADOS
#############################################

class DuplicateChecker:
    def __init__(self):
        self.existing_pairs = set()
        self.load_existing_pairs()
    def load_existing_pairs(self):
        try:
            recs = ws_ca_tracking.get_all_records()
            self.existing_pairs = {r["PairAddress"] for r in recs if r.get("PairAddress")}
            print(f"✅ DuplicateChecker: {len(self.existing_pairs)} pares cargados.")
        except Exception as e:
            print(f"❌ Error cargando duplicados: {e}")
    def is_duplicate(self, pair):
        self.load_existing_pairs()
        return pair in self.existing_pairs

duplicate_checker = DuplicateChecker()

#############################################
#  tracked_pairs
#############################################

tracked_pairs = {}

def load_tracked_pairs():
    try:
        vals = ws_ca_tracking.get_all_values()
        if len(vals)<2:
            print("⚠️ ca_tracking sin registros.")
            return
        heads = vals[0]
        for i, row in enumerate(vals[1:], start=2):
            pair_addr = row[heads.index("PairAddress")]
            if not pair_addr:
                continue
            chain = row[heads.index("Chain")]
            grupo = row[heads.index("Grupo")]
            symb = row[heads.index("Símbolo")]
            initp = parse_float(row[heads.index("Initial Price USD")])
            tracked_pairs[pair_addr] = {
                "chain": chain,
                "group": grupo,
                "symbol": symb,
                "initial_price": initp,
                "row_index": i
            }
            ensure_crypto_sheet(chain, symb)
        print(f"✅ Se cargaron {len(tracked_pairs)} pares en memoria.")
    except Exception as e:
        print(f"❌ Error load_tracked_pairs => {e}")

#############################################
#  ACTUALIZACIÓN
#############################################

async def update_loop():
    while True:
        try:
            if not tracked_pairs:
                print("⚠️ No hay pares en tracked_pairs.")
                await asyncio.sleep(UPDATE_INTERVAL)
                continue

            chain_map = {}
            for paddr, info in tracked_pairs.items():
                c = info["chain"]
                chain_map.setdefault(c, []).append(paddr)

            updates = []
            now_str = current_timestamp_str()

            for chain, addrs in chain_map.items():
                if not addrs: continue
                chunk=30
                for i in range(0, len(addrs), chunk):
                    batch = addrs[i:i+chunk]
                    pair_data_list = get_pairs_data(chain, batch) # dex_screener
                    for pd in pair_data_list:
                        paddr = pd.get("pairAddress","")
                        if paddr not in tracked_pairs:
                            continue
                        extracted = extract_pair_fields(pd)
                        if not extracted or extracted["price"]<=0:
                            continue
                        initp = tracked_pairs[paddr]["initial_price"]
                        rowi = tracked_pairs[paddr]["row_index"]
                        g = tracked_pairs[paddr]["group"]
                        symb = tracked_pairs[paddr]["symbol"]
                        profit = compute_profit_percent(extracted["price"], initp)

                        # Actualizar en ca_tracking
                        # col 7 => init, 8 => current, 9=>profit
                        updates.append({
                            "range": f"H{rowi}:I{rowi}",
                            "values": [[extracted["price"], profit]]
                        })

                        # Insertar en hoja individual
                        # Con emoticon => ensure_crypto_sheet(chain, symb)
                        ws_sym = ensure_crypto_sheet(chain, symb)
                        if ws_sym:
                            # Extraemos JSON con TODO
                            pd_all = extract_all_columns(pd)
                            raw_data = extract_all_data_as_json(pd)
                            new_row = [
                                now_str,
                                pd_all["chainId"],
                                pd_all["dexId"],
                                pd_all["pairAddress"],
                                pd_all["baseTokenAddress"],
                                pd_all["baseTokenName"],
                                pd_all["baseTokenSymbol"],
                                pd_all["quoteTokenAddress"],
                                pd_all["quoteTokenName"],
                                pd_all["quoteTokenSymbol"],
                                pd_all["priceNative"],
                                pd_all["priceUsd"],
                                pd_all["txns24hBuys"],
                                pd_all["txns24hSells"],
                                pd_all["volume24h"],
                                pd_all["priceChange24h"],
                                pd_all["liquidityUsd"],
                                pd_all["liquidityBase"],
                                pd_all["liquidityQuote"],
                                pd_all["fdv"],
                                pd_all["marketCap"],
                                pd_all["pairCreatedAt"],
                                raw_data  # JSON completo
                            ]
                            await asyncio.to_thread(safe_append_row, ws_sym, new_row)

            if updates:
                await asyncio.to_thread(ws_ca_tracking.batch_update, updates)
                print(f"🔄 {len(updates)} actualizaciones en ca_tracking.")
            else:
                print("ℹ️ Sin updates en esta ronda.")

        except Exception as e:
            print(f"❌ Error en update_loop => {e}")
        await asyncio.sleep(UPDATE_INTERVAL)

#############################################
#  TELEGRAM PROCESAMIENTO
#############################################

client = TelegramClient("session_name", API_ID, API_HASH)

# Regex
RE_CA_BSC_ETH = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
RE_CA_SOL = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")
DEX_LINK_REGEX = re.compile(r"https?://dexscreener\.com/(solana|bsc|ethereum)/([^/\s\?]+)")

processed_msg_ids = set()

@client.on(events.NewMessage(chats=list(groups.keys())))
async def handle_message(event):
    msg_id = event.message.id
    if msg_id in processed_msg_ids:
        return
    processed_msg_ids.add(msg_id)

    chat_id = event.chat_id
    group_name = groups.get(chat_id, "Desconocido")
    text = event.message.message
    now_str = current_timestamp_str()

    # raw_messages
    row = [now_str] + [""]*(len(raw_headers)-1)
    if group_name in group_to_col_index:
        idx = group_to_col_index[group_name]
        row[idx] = text
    await asyncio.to_thread(safe_append_row, ws_messages, row)

    # Gemini
    relevant = await asyncio.to_thread(gemini_classify, text, GEMINI_API_KEY)
    if not relevant:
        print("⚠️ Mensaje irrelevante IA.")
        return

    # 1) Dex links
    found_links = DEX_LINK_REGEX.findall(text)
    for (chain, pair_addr) in found_links:
        register_by_pairaddr(chain, pair_addr, group_name, now_str)

    # 2) EVM addresses
    evms = RE_CA_BSC_ETH.findall(text)
    for evm_ca in evms:
        # Asumimos BSC, o ethereum => tu decides
        register_by_ca("bsc", evm_ca, group_name, now_str)

    # 3) Sol addresses
    sols = RE_CA_SOL.findall(text)
    for sol_ca in sols:
        register_by_ca("solana", sol_ca, group_name, now_str)

def register_by_ca(chain, ca, grupo, ts_str):
    from dex_screener.dex_api import search_pairs
    pairs = search_pairs(chain, ca)
    # filtrar baseToken.address == ca
    valid = [p for p in pairs if p.get('baseToken',{}).get('address','').lower()==ca.lower()]
    if not valid:
        print(f"⚠️ No se hallaron pares para CA={ca} en {chain}")
        return
    best = max(valid, key=lambda x: float(x.get('liquidity',{}).get('usd',0)or 0))
    register_pair(best, chain, ca, grupo, ts_str)

def register_by_pairaddr(chain, pair_addr, grupo, ts_str):
    from dex_screener.dex_api import search_pairs
    pairs = search_pairs(chain, pair_addr)
    for p in pairs:
        if p.get('pairAddress','').lower()==pair_addr.lower():
            register_pair(p, chain, pair_addr, grupo, ts_str)
            return
    print(f"⚠️ No se encontró pair exacto {pair_addr} en {chain}")

def register_pair(pair_data, chain, raw_ca_or_pair, grupo, ts_str):
    from dex_screener.dex_api import extract_pair_fields
    pair_addr = pair_data.get("pairAddress","")
    if not pair_addr:
        return
    if duplicate_checker.is_duplicate(pair_addr):
        print(f"ℹ️ Par duplicado => {pair_addr}")
        return

    extracted = extract_pair_fields(pair_data)
    if not extracted or extracted["price"]<=0:
        print(f"⚠️ Datos inválidos => {raw_ca_or_pair}")
        return

    # Insertar en ca_tracking
    row = [
        ts_str,
        chain,
        grupo,
        raw_ca_or_pair,
        pair_addr,
        extracted["symbol"],
        extracted["price"],  # init
        extracted["price"],  # current
        0.0
    ]
    row_idx = safe_append_row(ws_ca_tracking, row)
    if row_idx:
        tracked_pairs[pair_addr] = {
            "chain": chain,
            "group": grupo,
            "symbol": extracted["symbol"],
            "initial_price": extracted["price"],
            "row_index": row_idx
        }
        duplicate_checker.existing_pairs.add(pair_addr)
        ensure_crypto_sheet(chain, extracted["symbol"])
        print(f"🆕 Nuevo token => chain:{chain} symbol:{extracted['symbol']} pair:{pair_addr}")
    else:
        print("❌ Falló inserción en ca_tracking.")

#############################################
#  MAIN
#############################################

async def main():
    print("🚀 Iniciando Bot DexScreener + IA + Sheets...")
    await asyncio.to_thread(load_tracked_pairs)
    await client.start()
    print("✅ Bot conectado a Telegram.")
    asyncio.create_task(update_loop())
    print(f"🤖 Bot corriendo. Intervalo: {UPDATE_INTERVAL}s")
    await client.run_until_disconnected()

if __name__=="__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"❌ Error fatal => {e}")
    finally:
        print("🛑 Bot detenido.")
