# google_sheets/sheets_manager.py

import gspread
import time

INITIAL_SHEET_ROWS = 5000
INITIAL_SHEET_COLS = 30

def connect_sheets(credentials, sheet_id):
    """Recibe credenciales y el ID del sheet, retorna el objeto spreadsheet."""
    try:
        gs_client = gspread.authorize(credentials)
        spreadsheet = gs_client.open_by_key(sheet_id)
        print("✅ Conectado a Google Sheets")
        return spreadsheet
    except Exception as e:
        print(f"❌ Error conectando a Google Sheets: {e}")
        raise

def safe_append_row(sheet, row_data):
    """Añade una fila nueva a 'sheet', expandiendo si se requiere."""
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
    """Obtiene (o crea) una hoja en 'spreadsheet' con las cabeceras dadas."""
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
            rows=INITIAL_SHEET_ROWS,
            cols=INITIAL_SHEET_COLS
        )
        ws.append_row(headers)
        return ws
    except Exception as e:
        print(f"❌ Error crítico con hoja '{sheet_name}': {e}")
        raise
