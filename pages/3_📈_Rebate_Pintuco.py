# -*- coding: utf-8 -*-
"""
Módulo de Seguimiento de Rebate para PINTUCO COLOMBIA SAS (Versión 4.0 - Gerencial Premium UI).

Este módulo es una herramienta de análisis gerencial diseñada para:
1.  Sincronizar de forma independiente e inteligente todas las facturas de PINTUCO,
    reconociendo múltiples nombres de proveedor (alias).
2.  Analizar la estructura compleja de XML anidados para una extracción de datos precisa.
3.  Cargar y procesar correctamente las Notas de Crédito desde Dropbox.
4.  Almacenar un historial completo en una pestaña dedicada dentro del libro principal.
5.  Calcular y visualizar en tiempo real el progreso del acuerdo de rebate,
    enfocándose en la toma de decisiones con proyecciones claras.

Mejoras en v4.0 (Gerencial Premium UI):
- **Rediseño Completo de la UI Gerencial:** Se ha realizado un rediseño significativo
  de la sección "Panel de Decisión Gerencial por Trimestre" para una estética
  mucho más limpia, moderna y profesional.
- **Uso de Expanders para Organización:** Los análisis trimestrales (3Q y 4Q)
  ahora se presentan dentro de `st.expander` para una vista inicial concisa
  y la posibilidad de desplegar detalles solo cuando sea necesario.
- **Indicadores Clave de Rendimiento (KPIs) Mejorados:** Uso de `st.metric` con
  iconos y colores para resaltar la información más importante de forma visual.
- **Mensajes de Proyección Claros y Atractivos:** Los mensajes sobre el faltante
  para las metas y la Nota de Crédito potencial se han formateado con mayor
  énfasis visual (colores, iconos, tamaños de texto) para guiar la decisión.
- **Barras de Progreso Dinámicas:** Visualización clara del progreso hacia las
  metas de volumen y estacionalidad.
- **Consistencia Visual:** Armonización de colores, fuentes y espaciado para
  una experiencia de usuario superior.
- **Mantener Funcionalidad Base:** Todas las capacidades de sincronización,
  cálculo de rebates (volumen, estacionalidad, profundidad) y generación
  de reportes en Excel se conservan y mejoran indirectamente con la claridad de la UI.
"""

# --- 0. IMPORTACIÓN DE LIBRERÍAS ---
import streamlit as st
import pandas as pd
import numpy as np
import io
import gspread
import dropbox
import imaplib
import email
import xml.etree.ElementTree as ET
import zipfile
import re
from datetime import datetime, date, timedelta
import pytz
from google.oauth2.service_account import Credentials
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows

# ======================================================================================
# --- INICIO DEL BLOQUE DE SEGURIDAD ---
# ======================================================================================
if 'password_correct' not in st.session_state:
    st.session_state['password_correct'] = False

if not st.session_state["password_correct"]:
    st.error("🔒 Debes iniciar sesión para acceder a esta página.")
    st.info("Por favor, ve a la página principal 'Dashboard General' para ingresar la contraseña.")
    st.stop()
# --- FIN DEL BLOQUE DE SEGURIDAD ---


# --- 1. CONFIGURACIÓN INICIAL Y CONSTANTES ---
st.set_page_config(layout="wide", page_title="Seguimiento Rebate | Pintuco", page_icon="🎯")

# --- Constantes Globales ---
PINTUCO_ALIASES = ["PINTUCO", "COMPANIA GLOBAL DE PINTURAS"] # Nombres conocidos para el proveedor
PINTUCO_PROVIDER_NAME_ERP = "PINTUCO COLOMBIA S.A.S"
COLOMBIA_TZ = pytz.timezone('America/Bogota')
INITIAL_START_DATE_SYNC = date(2025, 7, 1)
NON_APPLICABLE_PURCHASE_FACTOR = 0.88 # Factor de compra que SÍ aplica para metas (100% - 5% = 95%)

# --- Constantes de Conexión ---
IMAP_SERVER = "imap.gmail.com"
EMAIL_FOLDER = "TFHKA/Recepcion/Descargados"
DROPBOX_FILE_PATH = "/data/Proveedores.csv"
PINTUCO_WORKSHEET_NAME = "Rebate_Pintuco"

# --- Constantes del Acuerdo de Rebate ---
META_VOLUMEN = {
    "Julio-Agosto": {"Escala 1": 3662382180, "Rebate 1": 0.005, "Escala 2": 3918748933, "Rebate 2": 0.01},
    "Agosto-Sept.": {"Escala 1": 3876058532, "Rebate 1": 0.005, "Escala 2": 4147382629, "Rebate 2": 0.01},
    "3er Trimestre (3Q)": {"Escala 1": 5653504866, "Rebate 1": 0.01, "Escala 2": 6049250207, "Rebate 2": 0.02},
    "Octubre-Nov.": {"Escala 1": 4272097392, "Rebate 1": 0.005, "Escala 2": 4571144209, "Rebate 2": 0.01},
    "Noviembre-Dic.": {"Escala 1": 3970984742, "Rebate 1": 0.005, "Escala 2": 4248953674, "Rebate 2": 0.01},
    "4to Trimestre (4Q)": {"Escala 1": 6119230865, "Rebate 1": 0.01, "Escala 2": 6547577026, "Rebate 2": 0.02},
    "2do Semestre (2Sem)": {"Escala 1": 11772735731, "Rebate 1": 0.0075, "Escala 2": 12596827232, "Rebate 2": 0.015},
}
META_ESTACIONALIDAD = {
    7: {"Nombre": "Julio", "Escala 1": 1777446334, "Rebate 1": 0.007, "Escala 2": 1901867577, "Rebate 2": 0.01},
    8: {"Nombre": "Agosto", "Escala 1": 1884935846, "Rebate 1": 0.007, "Escala 2": 2016881355, "Rebate 2": 0.01},
    9: {"Nombre": "Septiembre", "Escala 1": 1991122686, "Rebate 1": 0.007, "Escala 2": 2130501274, "Rebate 2": 0.01},
    10: {"Nombre": "Octubre", "Escala 1": 2148246123, "Rebate 1": 0.007, "Escala 2": 2298623352, "Rebate 2": 0.01},
    11: {"Nombre": "Noviembre", "Escala 1": 2123851269, "Rebate 1": 0.007, "Escala 2": 2272520858, "Rebate 2": 0.01},
    12: {"Nombre": "Diciembre", "Escala 1": 1847133473, "Rebate 1": 0.007, "Escala 2": 1976432816, "Rebate 2": 0.01},
}
REBATE_PROFUNDIDAD_Q = 0.01

# --- 2. FUNCIONES DE CONEXIÓN Y UTILIDADES ---
@st.cache_resource(show_spinner="Conectando a Google Sheets...")
def connect_to_google_sheets():
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(st.secrets["google_credentials"], scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ Error crítico al autenticar con Google: {e}"); return None

def get_worksheet(client: gspread.Client, sheet_key: str, worksheet_name: str):
    try:
        spreadsheet = client.open_by_key(sheet_key)
    except gspread.exceptions.APIError as e:
        st.error(f"❌ **Error de API de Google:** No se pudo abrir el libro. Verifica que 'google_sheet_id' en tus secrets sea correcto y que hayas compartido el libro con el correo de servicio.")
        st.info(f"Correo de servicio: `{st.secrets.google_credentials['client_email']}`"); st.stop(); return None
    try:
        return spreadsheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        st.warning(f"La pestaña '{worksheet_name}' no fue encontrada. Creando una nueva...")
        return spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols="50")

def update_gsheet_from_df(worksheet: gspread.Worksheet, df: pd.DataFrame):
    try:
        df_to_upload = df.copy()
        for col in df_to_upload.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, America/Bogota]']).columns:
            df_to_upload[col] = df_to_upload[col].dt.strftime('%Y-%m-%d')

        df_to_upload = df_to_upload.astype(str).replace({'nan': '', 'NaT': '', 'None': ''})

        worksheet.clear()
        worksheet.update([df_to_upload.columns.values.tolist()] + df_to_upload.values.tolist(), 'A1')
        return True
    except Exception as e:
        st.error(f"❌ Error al actualizar la hoja '{worksheet.title}': {e}"); return False

def normalize_invoice_number(inv_num: any) -> str:
    if not isinstance(inv_num, str): inv_num = str(inv_num)
    return re.sub(r'[^A-Z0-9]', '', inv_num.upper()).strip()

def clean_and_convert_numeric(value: any) -> float:
    if pd.isna(value) or value is None: return np.nan
    cleaned_str = str(value).strip().replace('$', '').replace(',', '')
    try: return float(cleaned_str)
    except (ValueError, TypeError): return np.nan

# --- 3. FUNCIONES DE EXTRACCIÓN DE DATOS ---
@st.cache_data(ttl=600, show_spinner="Descargando cartera vigente de Dropbox...")
def load_pending_documents_from_dropbox() -> set:
    try:
        dbx = dropbox.Dropbox(oauth2_refresh_token=st.secrets.dropbox["refresh_token"], app_key=st.secrets.dropbox["app_key"], app_secret=st.secrets.dropbox["app_secret"])
        _, response = dbx.files_download(DROPBOX_FILE_PATH)
        df = pd.read_csv(io.StringIO(response.content.decode('latin1')), sep='{', header=None, engine='python', names=['nombre_proveedor_erp', 'serie', 'num_entrada', 'num_factura', 'doc_erp', 'fecha_emision_erp', 'fecha_vencimiento_erp', 'valor_total_erp'])
        pintuco_df = df[df['nombre_proveedor_erp'] == PINTUCO_PROVIDER_NAME_ERP].copy()
        pintuco_df['valor_total_erp'] = pintuco_df['valor_total_erp'].apply(clean_and_convert_numeric)
        credit_note_mask = (pintuco_df['valor_total_erp'] < 0) & (pintuco_df['num_factura'].isna() | (pintuco_df['num_factura'].str.strip() == ''))
        if credit_note_mask.any():
            pintuco_df.loc[credit_note_mask, 'num_factura'] = 'NC-' + pintuco_df.loc[credit_note_mask, 'doc_erp'].astype(str).str.strip() + '-' + pintuco_df.loc[credit_note_mask, 'valor_total_erp'].abs().astype(int).astype(str)
        pintuco_df.dropna(subset=['num_factura'], inplace=True)
        pending_docs = set(pintuco_df['num_factura'].apply(normalize_invoice_number))
        st.info(f"Encontrados {len(pending_docs)} documentos pendientes de Pintuco en Dropbox.")
        return pending_docs
    except Exception as e:
        st.error(f"❌ Error cargando cartera de Dropbox: {e}"); return set()

def parse_invoice_xml(xml_content: str) -> dict or None:
    try:
        ns = {'cac': "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2", 'cbc': "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2"}
        xml_content = re.sub(r'^[^\<]+', '', xml_content.strip())
        root = ET.fromstring(xml_content.encode('utf-8'))
        description_node = root.find('.//cac:Attachment/cac:ExternalReference/cbc:Description', ns)
        if description_node is not None and description_node.text and '<Invoice' in description_node.text:
            inner_xml_text = re.sub(r'^[^\<]+', '', description_node.text.strip())
            invoice_root = ET.fromstring(inner_xml_text.encode('utf-8'))
        else:
            invoice_root = root
        supplier_name_node = invoice_root.find('.//cac:AccountingSupplierParty/cac:Party/cac:PartyLegalEntity/cbc:RegistrationName', ns)
        if supplier_name_node is None: return None
        supplier_name = supplier_name_node.text.strip()
        if not any(alias in supplier_name.upper() for alias in PINTUCO_ALIASES):
            return None
        invoice_number_node = invoice_root.find('./cbc:ID', ns)
        issue_date_node = invoice_root.find('./cbc:IssueDate', ns)
        net_value_node = invoice_root.find('.//cac:LegalMonetaryTotal/cbc:TaxExclusiveAmount', ns)

        if invoice_number_node is None or issue_date_node is None or net_value_node is None:
            return None

        invoice_number = invoice_number_node.text.strip()
        issue_date = issue_date_node.text.strip()

        return {
            "Fecha_Factura": issue_date,
            "Numero_Factura": normalize_invoice_number(invoice_number),
            "Valor_Neto": float(net_value_node.text.strip()),
            "Proveedor_Correo": supplier_name
        }
    except Exception:
        return None

def fetch_pintuco_invoices_from_email(start_date: date) -> pd.DataFrame:
    invoices_data = []
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(st.secrets.email["address"], st.secrets.email["password"])
        mail.select(f'"{EMAIL_FOLDER}"')
        search_query = f'(SINCE "{start_date.strftime("%d-%b-%Y")}")'
        _, messages = mail.search(None, search_query)
        message_ids = messages[0].split()
        if not message_ids: mail.logout(); return pd.DataFrame()
        progress_text = f"Procesando {len(message_ids)} correos encontrados..."
        progress_bar = st.progress(0, text=progress_text)
        for i, num in enumerate(message_ids):
            _, data = mail.fetch(num, "(RFC822)")
            msg = email.message_from_bytes(data[0][1])
            for part in msg.walk():
                if part.get_content_maintype() == "multipart" or part.get("Content-Disposition") is None: continue
                filename = part.get_filename()
                if filename and filename.lower().endswith('.zip'):
                    try:
                        with zipfile.ZipFile(io.BytesIO(part.get_payload(decode=True))) as zf:
                            for name in zf.namelist():
                                if name.lower().endswith('.xml'):
                                    xml_content = zf.read(name).decode('utf-8', 'ignore')
                                    details = parse_invoice_xml(xml_content)
                                    if details: invoices_data.append(details)
                    except: continue
            progress_bar.progress((i + 1) / len(message_ids), text=f"{progress_text} ({i+1}/{len(message_ids)})")
        mail.logout()
        return pd.DataFrame(invoices_data)
    except Exception as e:
        st.error(f"❌ Error procesando correos: {e}"); return pd.DataFrame()

# --- 4. LÓGICA PRINCIPAL DE SINCRONIZACIÓN (ROBUSTA) ---
def run_pintuco_sync():
    with st.spinner('Iniciando sincronización de Pintuco...'):
        st.info("Paso 1/4: Descargando cartera pendiente de Dropbox...")
        pending_docs_set = load_pending_documents_from_dropbox()

        st.info("Paso 2/4: Conectando a Google Sheets para optimizar búsqueda...")
        gs_client = connect_to_google_sheets()
        if not gs_client:
            st.error("Sincronización cancelada. No se pudo conectar a Google.")
            st.stop()
        worksheet = get_worksheet(gs_client, st.secrets["google_sheet_id"], PINTUCO_WORKSHEET_NAME)

        historical_df = pd.DataFrame()
        start_date = INITIAL_START_DATE_SYNC

        try:
            records = worksheet.get_all_records()
            if records:
                historical_df = pd.DataFrame(records)
                historical_df['Fecha_Factura'] = pd.to_datetime(historical_df['Fecha_Factura'])
                last_sync_date = historical_df['Fecha_Factura'].max().date()
                start_date = last_sync_date - timedelta(days=3) # Margen de seguridad
        except Exception as e:
            st.warning(f"No se pudieron cargar datos históricos de Google Sheets. Sincronizando desde el inicio. Error: {e}")

        st.info(f"Paso 3/4: Buscando facturas en el correo desde {start_date.strftime('%Y-%m-%d')}...")
        new_invoices_df = fetch_pintuco_invoices_from_email(start_date)

        combined_df = historical_df.copy()
        if not new_invoices_df.empty:
            new_invoices_df['Fecha_Factura'] = pd.to_datetime(new_invoices_df['Fecha_Factura'])
            new_invoices_df['Valor_Neto'] = pd.to_numeric(new_invoices_df['Valor_Neto'])

            combined_df = pd.concat([historical_df, new_invoices_df], ignore_index=True)
            combined_df.drop_duplicates(subset=['Numero_Factura'], keep='last', inplace=True)

            st.info(f"Se encontraron y consolidaron {len(new_invoices_df)} facturas nuevas.")
        else:
            st.success("No se encontraron **nuevas** facturas de Pintuco en el correo.")

        if not combined_df.empty:
            st.info("Paso 4/4: Actualizando estado de pago y guardando en Google Sheets...")
            combined_df['Numero_Factura_Normalized'] = combined_df['Numero_Factura'].apply(normalize_invoice_number)

            combined_df['Estado_Pago'] = combined_df['Numero_Factura_Normalized'].apply(lambda x: 'Pendiente' if x in pending_docs_set else 'Pagada')
            combined_df.drop(columns=['Numero_Factura_Normalized'], inplace=True)

            final_columns = ["Fecha_Factura", "Numero_Factura", "Valor_Neto", "Proveedor_Correo", "Estado_Pago"]
            combined_df = combined_df.reindex(columns=final_columns)

            if update_gsheet_from_df(worksheet, combined_df.sort_values(by="Fecha_Factura")):
                st.success("✅ ¡Base de datos de Pintuco actualizada exitosamente!")
            else:
                st.error("❌ Falló la actualización en Google Sheets.")
        else:
            st.warning("No hay documentos para subir a la hoja de cálculo. La base de datos está vacía.")

        st.session_state['last_pintuco_sync'] = datetime.now(COLOMBIA_TZ).strftime('%Y-%m-%d %H:%M:%S')
        st.balloons()


# --- 5. LÓGICA DE CÁLCULO Y ANÁLISiS AVANZADO ---
@st.cache_data(ttl=300)
def load_pintuco_data_from_gsheet() -> pd.DataFrame:
    try:
        gs_client = connect_to_google_sheets()
        worksheet = get_worksheet(gs_client, st.secrets["google_sheet_id"], PINTUCO_WORKSHEET_NAME)
        records = worksheet.get_all_records()
        if not records: return pd.DataFrame()
        df = pd.DataFrame(records)
        df['Fecha_Factura'] = pd.to_datetime(df['Fecha_Factura'])
        df['Valor_Neto'] = pd.to_numeric(df['Valor_Neto'].astype(str).str.replace(',', ''), errors='coerce')
        df['Compra_Aplicable_Rebate'] = df['Valor_Neto'] * NON_APPLICABLE_PURCHASE_FACTOR
        return df
    except Exception as e:
        st.error(f"Error al cargar datos desde Google Sheets: {e}"); return pd.DataFrame()

def generate_rebate_analysis(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return pd.DataFrame()
    df = df.copy()
    df['Mes'] = df['Fecha_Factura'].dt.month
    analysis_data = []

    def process_period(period_name, purchase_value_applicable, meta_info, rebate_type):
        meta_e1 = meta_info.get("Escala 1", 0)
        meta_e2 = meta_info.get("Escala 2", 0)
        rebate_e1 = meta_info.get("Rebate 1", 0)
        rebate_e2 = meta_info.get("Rebate 2", 0)

        faltante_e1 = max(0, meta_e1 - purchase_value_applicable)
        faltante_e2 = max(0, meta_e2 - purchase_value_applicable)
        cumplimiento_e1 = (purchase_value_applicable / meta_e1) if meta_e1 > 0 else 0
        cumplimiento_e2 = (purchase_value_applicable / meta_e2) if meta_e2 > 0 else 0

        rebate_ganado = 0
        if purchase_value_applicable >= meta_e2:
            rebate_ganado = purchase_value_applicable * rebate_e2
        elif purchase_value_applicable >= meta_e1:
            rebate_ganado = purchase_value_applicable * rebate_e1

        rebate_potencial_e1 = meta_e1 * rebate_e1 if meta_e1 > 0 else 0
        rebate_potencial_e2 = meta_e2 * rebate_e2 if meta_e2 > 0 else 0

        return {
            "Periodo": period_name, "Tipo": rebate_type, "Compra_Aplicable": purchase_value_applicable,
            "Meta_E1": meta_e1, "Faltante_E1": faltante_e1, "%_Cumplimiento_E1": cumplimiento_e1, "Rebate_Potencial_E1": rebate_potencial_e1,
            "Meta_E2": meta_e2, "Faltante_E2": faltante_e2, "%_Cumplimiento_E2": cumplimiento_e2, "Rebate_Potencial_E2": rebate_potencial_e2,
            "Rebate_Ganado_Actual": rebate_ganado
        }

    # --- Análisis de Estacionalidad ---
    for month, meta in META_ESTACIONALIDAD.items():
        monthly_df = df[df['Mes'] == month]
        total_month_purchase_neto = monthly_df['Valor_Neto'].sum()
        total_month_purchase_aplicable = monthly_df['Compra_Aplicable_Rebate'].sum()
        purchase_before_20th_neto = monthly_df[monthly_df['Fecha_Factura'].dt.day <= 20]['Valor_Neto'].sum()

        if total_month_purchase_neto > 0 and (purchase_before_20th_neto / total_month_purchase_neto) >= 0.9:
            analysis_data.append(process_period(meta["Nombre"], total_month_purchase_aplicable, meta, "Estacionalidad"))
        else:
            result = process_period(meta["Nombre"], total_month_purchase_aplicable, meta, "Estacionalidad")
            result["Rebate_Ganado_Actual"] = -1
            analysis_data.append(result)

    # --- Análisis de Volumen ---
    period_map = {
        "Julio-Agosto": [7, 8], "Agosto-Sept.": [8, 9], "3er Trimestre (3Q)": [7, 8, 9],
        "Octubre-Nov.": [10, 11], "Noviembre-Dic.": [11, 12], "4to Trimestre (4Q)": [10, 11, 12],
        "2do Semestre (2Sem)": list(range(7, 13))
    }
    for period, months in period_map.items():
        period_df = df[df['Mes'].isin(months)]
        total_period_purchase_aplicable = period_df['Compra_Aplicable_Rebate'].sum()
        meta_vol = META_VOLUMEN.get(period, {})
        analysis_data.append(process_period(period, total_period_purchase_aplicable, meta_vol, "Volumen"))

    # --- Análisis de Profundidad ---
    for q_period, months in {"3er Trimestre (3Q)": [7, 8, 9], "4to Trimestre (4Q)": [10, 11, 12]}.items():
        q_df = df[df['Mes'].isin(months)]
        total_q_purchase_aplicable = q_df['Compra_Aplicable_Rebate'].sum()
        rebate_profundidad = total_q_purchase_aplicable * REBATE_PROFUNDIDAD_Q
        analysis_data.append({
            "Periodo": q_period, "Tipo": "Profundidad", "Compra_Aplicable": total_q_purchase_aplicable,
            "Meta_E1": 0, "Faltante_E1": 0, "%_Cumplimiento_E1": 1, "Rebate_Potencial_E1": rebate_profundidad,
            "Meta_E2": 0, "Faltante_E2": 0, "%_Cumplimiento_E2": 1, "Rebate_Potencial_E2": 0,
            "Rebate_Ganado_Actual": rebate_profundidad
        })

    full_analysis_df = pd.DataFrame(analysis_data)
    return full_analysis_df

def generate_excel_report(analysis_df: pd.DataFrame):
    output = io.BytesIO()
    wb = Workbook()
    ws = wb.active
    ws.title = "Análisis Rebate Pintuco"

    analysis_df_report = analysis_df.rename(columns={"Compra_Aplicable": "Compra Aplicable (95%)"})
    rows = dataframe_to_rows(analysis_df_report, index=False, header=True)

    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
    currency_format = '_("$"* #,##0_);_("$"* (#,##0);_("$"* "-"??_);_(@_)'
    percent_format = '0.0%'
    center_align = Alignment(horizontal='center', vertical='center')

    for r_idx, row in enumerate(rows, 1):
        if r_idx == 1:
            for c_idx, value in enumerate(row, 1):
                cell = ws.cell(row=r_idx, column=c_idx, value=value)
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = center_align
        else:
            for c_idx, value in enumerate(row, 1):
                cell = ws.cell(row=r_idx, column=c_idx, value=value)
                col_name = analysis_df_report.columns[c_idx-1]
                if "Compra_" in col_name or "Faltante_" in col_name or "Rebate_" in col_name or "Meta_" in col_name:
                    cell.number_format = currency_format
                if "%_Cumplimiento" in col_name:
                    cell.number_format = percent_format

    for column_cells in ws.columns:
        length = max(len(str(cell.value)) for cell in column_cells)
        ws.column_dimensions[column_cells[0].column_letter].width = length + 2

    wb.save(output)
    output.seek(0)
    return output

# --- 6. APLICACIÓN PRINCIPAL (STREAMLIT UI) ---
st.title("🎯 Módulo de Seguimiento de Rebate: PINTUCO")
st.markdown("Herramienta analítica para la planificación y seguimiento del acuerdo de desempeño comercial con **PINTUCO COLOMBIA S.A.S**.")
st.warning("⚠️ **Regla de negocio importante:** El análisis se basa en el **95%** del valor neto de las compras, ya que hay un 5% que no aplica para el cumplimiento de metas de rebate.", icon="ℹ️")

if st.button("🔄 Sincronizar Facturas de Pintuco", type="primary"):
    run_pintuco_sync()
    st.cache_data.clear()
    st.rerun()

if 'last_pintuco_sync' in st.session_state:
    st.success(f"Última sincronización de Pintuco: {st.session_state.last_pintuco_sync}")

pintuco_df = load_pintuco_data_from_gsheet()
if pintuco_df.empty:
    st.warning("No hay datos de Pintuco para analizar. Realiza la primera sincronización.")
    st.stop()

analysis_df = generate_rebate_analysis(pintuco_df)
total_rebate_ganado = analysis_df[analysis_df['Rebate_Ganado_Actual'] >= 0]['Rebate_Ganado_Actual'].sum()

max_potential_rebate_e2 = analysis_df[analysis_df['Tipo'] != 'Profundidad']['Rebate_Potencial_E2'].sum()
max_potential_rebate_prof = analysis_df[analysis_df['Tipo'] == 'Profundidad']['Rebate_Potencial_E1'].sum()
max_potential_total = max_potential_rebate_e2 + max_potential_rebate_prof

total_comprado_neto_s2 = pintuco_df[pintuco_df['Fecha_Factura'].dt.month.isin(range(7,13))]['Valor_Neto'].sum()
total_comprado_aplicable_s2 = pintuco_df[pintuco_df['Fecha_Factura'].dt.month.isin(range(7,13))]['Compra_Aplicable_Rebate'].sum()

st.divider()
st.header("📊 Resumen Ejecutivo del Rebate (2do Semestre)")
kpi1, kpi2, kpi3 = st.columns(3)
kpi1.metric("✅ Rebate Total Ganado (Acumulado)", f"${int(total_rebate_ganado):,}")
kpi2.metric("💰 Rebate Potencial Máximo (Objetivo)", f"${int(max_potential_total):,}")
kpi3.metric("🛒 Total Comprado Neto (2do Semestre)", f"${int(total_comprado_neto_s2):,}", f"Aplicable a Rebate: ${int(total_comprado_aplicable_s2):,}")

meta_semestral_info = META_VOLUMEN["2do Semestre (2Sem)"]
meta_semestral_e1 = meta_semestral_info["Escala 1"]
progreso_semestral = (total_comprado_aplicable_s2 / meta_semestral_e1) * 100 if meta_semestral_e1 > 0 else 0
st.metric("📈 Progreso Meta Semestral (Escala 1)", f"{progreso_semestral:.1f}%", f"Meta: ${int(meta_semestral_e1):,}")
st.progress(int(progreso_semestral) if progreso_semestral <= 100 else 100)
st.divider()

# --- PANEL DE DECISIÓN GERENCIAL POR TRIMESTRE (MEJORADO UI) ---
st.header("🚀 Panel de Decisión Gerencial por Trimestre")
st.markdown("<p style='font-size: 1.1em; color: gray;'>Planifica tus compras y visualiza el impacto inmediato en tu Nota de Crédito.</p>", unsafe_allow_html=True)


def display_quarterly_analysis_premium(quarter_name: str, months: list, analysis_df: pd.DataFrame, pintuco_df: pd.DataFrame):
    
    # Obtener la información de metas y rebates del diccionario original
    meta_info_q = META_VOLUMEN.get(quarter_name, {})
    rebate_1_percent = meta_info_q.get("Rebate 1", 0)
    rebate_2_percent = meta_info_q.get("Rebate 2", 0)

    # Datos del rebate de Volumen para el Q
    vol_q_data = analysis_df[(analysis_df['Tipo'] == 'Volumen') & (analysis_df['Periodo'] == quarter_name)].iloc[0]

    # Datos de Profundidad para el Q
    prof_q_data = analysis_df[(analysis_df['Tipo'] == 'Profundidad') & (analysis_df['Periodo'] == quarter_name)].iloc[0]
    rebate_ganado_profundidad = prof_q_data['Rebate_Ganado_Actual']

    # Datos de Estacionalidad para los meses del Q
    est_q_df = analysis_df[(analysis_df['Tipo'] == 'Estacionalidad') & (analysis_df['Periodo'].isin([META_ESTACIONALIDAD[m]['Nombre'] for m in months]))]
    rebate_ganado_estacionalidad = est_q_df[est_q_df['Rebate_Ganado_Actual'] >= 0]['Rebate_Ganado_Actual'].sum()

    # Totales de compra del Q
    total_comprado_q_neto = pintuco_df[pintuco_df['Fecha_Factura'].dt.month.isin(months)]['Valor_Neto'].sum()
    compra_aplicable_q = vol_q_data['Compra_Aplicable']
    
    # Mapa de periodos bimensuales para la recomposición
    bimonthly_map = {
        "3er Trimestre (3Q)": ["Julio-Agosto", "Agosto-Sept."],
        "4to Trimestre (4Q)": ["Octubre-Nov.", "Noviembre-Dic."]
    }
    relevant_bimonthly = bimonthly_map.get(quarter_name, [])
    bimonthly_df = analysis_df[(analysis_df['Tipo'] == 'Volumen') & (analysis_df['Periodo'].isin(relevant_bimonthly))]

    with st.expander(f"✨ **Detalle y Proyección del {quarter_name}**", expanded=False):
        st.markdown(f"### Análisis Consolidado del {quarter_name}")
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("🛒 Compra Neta Total", f"${int(total_comprado_q_neto):,}", help="Valor total de facturas Pintuco sin aplicar el 5% de exclusión.")
        with col2:
            st.metric("🎯 Compra Aplicable a Metas", f"${int(compra_aplicable_q):,}", help="Valor de compras que efectivamente cuentan para el cálculo de rebates (95% del valor neto).")

        st.markdown(f"""
            <div style="
                background-color: #e0f2f7;  /* Light blue background */
                padding: 15px;
                border-radius: 10px;
                border-left: 5px solid #007bff; /* Blue left border */
                margin-top: 15px;
                margin-bottom: 20px;
            ">
                <h4 style="margin-top: 0; color: #007bff;">💰 Rebates Ya Ganados en el {quarter_name}:</h4>
                <p style="font-size: 1.1em; margin-bottom: 5px;">
                    <strong>Profundidad:</strong> ${int(rebate_ganado_profundidad):,} <br>
                    <strong>Estacionalidad:</strong> ${int(rebate_ganado_estacionalidad):,}
                </p>
                <h3 style="margin-top: 10px; color: #28a745;">Total Ganado: <strong>${int(rebate_ganado_profundidad + rebate_ganado_estacionalidad):,}</strong></h3>
            </div>
        """, unsafe_allow_html=True)

        st.markdown("---")
        st.markdown("### 📈 Proyecciones de Ganancia por Volumen")

        # --- CÁLCULOS AVANZADOS DE PROYECCIÓN ---

        # Proyección Escala 1
        faltante_e1 = vol_q_data['Faltante_E1']
        meta_e1_compra_total = vol_q_data['Meta_E1']
        
        # 1. Rebate de Volumen del Q
        nc_potencial_volumen_e1 = vol_q_data['Rebate_Potencial_E1']
        # 2. Rebate de Profundidad proyectado
        projected_profundidad_e1 = meta_e1_compra_total * REBATE_PROFUNDIDAD_Q
        # 3. Rebate de Estacionalidad proyectado (se asume que se cumplen todos los meses del Q)
        projected_estacionalidad_e1 = est_q_df['Rebate_Potencial_E1'].sum()
        # 4. Rebate de Volumen bimensual por recomposición
        projected_recomposicion_e1 = 0
        for _, row in bimonthly_df.iterrows():
            if meta_e1_compra_total >= row['Meta_E1']:
                projected_recomposicion_e1 += row['Rebate_Potencial_E1']

        nc_total_proyectada_e1 = nc_potencial_volumen_e1 + projected_profundidad_e1 + projected_estacionalidad_e1 + projected_recomposicion_e1
        
        # Proyección Escala 2
        faltante_e2 = vol_q_data['Faltante_E2']
        meta_e2_compra_total = vol_q_data['Meta_E2']

        # 1. Rebate de Volumen del Q
        nc_potencial_volumen_e2 = vol_q_data['Rebate_Potencial_E2']
        # 2. Rebate de Profundidad proyectado
        projected_profundidad_e2 = meta_e2_compra_total * REBATE_PROFUNDIDAD_Q
        # 3. Rebate de Estacionalidad proyectado (se asume que se cumplen todos los meses del Q al máximo)
        projected_estacionalidad_e2 = est_q_df['Rebate_Potencial_E2'].sum()
        # 4. Rebate de Volumen bimensual por recomposición (al máximo)
        projected_recomposicion_e2 = 0
        for _, row in bimonthly_df.iterrows():
            if meta_e2_compra_total >= row['Meta_E2']:
                 projected_recomposicion_e2 += row['Rebate_Potencial_E2']
            elif meta_e2_compra_total >= row['Meta_E1']:
                 projected_recomposicion_e2 += row['Rebate_Potencial_E1']

        nc_total_proyectada_e2 = nc_potencial_volumen_e2 + projected_profundidad_e2 + projected_estacionalidad_e2 + projected_recomposicion_e2

        # --- FIN DE CÁLCULOS AVANZADOS ---

        progreso_e1 = (compra_aplicable_q / vol_q_data['Meta_E1']) * 100 if vol_q_data['Meta_E1'] > 0 else 0

        st.markdown("#### Meta de Volumen: Escala 1")
        st.info(f"Objetivo: Alcanzar **${int(vol_q_data['Meta_E1']):,}** en compras aplicables para un **{rebate_1_percent*100:.1f}%** de rebate por volumen.")
        st.progress(min(100, int(progreso_e1)), text=f"Progreso: **{progreso_e1:.1f}%**")

        if compra_aplicable_q >= vol_q_data['Meta_E1']:
            st.success(f"🎉 **¡Meta de Volumen (Escala 1) SUPERADA!** "
                       f"Rebate por Volumen asegurado: **${int(vol_q_data['Rebate_Ganado_Actual']):,}**")
        else:
            st.markdown(f"""
                <div style="
                    background-color: #fff3cd; /* Light yellow background */
                    padding: 15px;
                    border-radius: 10px;
                    border-left: 5px solid #ffc107; /* Yellow left border */
                    margin-bottom: 15px;
                ">
                    <p style="font-size: 1.2em; margin-top: 0;">
                        Necesitas comprar <strong><span style="color: #dc3545;">${int(faltante_e1):,}</span></strong> adicionales (valor aplicable)
                        para alcanzar la Escala 1.
                    </p>
                    <p style="font-size: 1.2em;">
                        Si logras esta meta, tu <strong>Nota de Crédito TOTAL</strong> proyectada para el {quarter_name} será de
                        <strong><span style="color: #007bff;">${int(nc_total_proyectada_e1):,}</span></strong>
                        (sumando volumen, estacionalidad y profundidad).
                    </p>
                </div>
            """, unsafe_allow_html=True)

        st.markdown("---")

        progreso_e2 = (compra_aplicable_q / vol_q_data['Meta_E2']) * 100 if vol_q_data['Meta_E2'] > 0 else 0

        st.markdown("#### Meta de Volumen: Escala 2 (Máximo Potencial)")
        st.info(f"Objetivo: Alcanzar **${int(vol_q_data['Meta_E2']):,}** en compras aplicables para un **{rebate_2_percent*100:.1f}%** de rebate por volumen.")
        st.progress(min(100, int(progreso_e2)), text=f"Progreso: **{progreso_e2:.1f}%**")

        if compra_aplicable_q >= vol_q_data['Meta_E2']:
            st.success(f"🚀 **¡Felicidades! Meta de Volumen (Escala 2) ALCANZADA.** "
                       f"Rebate por Volumen asegurado: **${int(vol_q_data['Rebate_Ganado_Actual']):,}**")
        else:
            st.markdown(f"""
                <div style="
                    background-color: #e2fce4; /* Light green background */
                    padding: 15px;
                    border-radius: 10px;
                    border-left: 5px solid #28a745; /* Green left border */
                ">
                    <p style="font-size: 1.2em; margin-top: 0;">
                        Para maximizar tus ganancias, te falta comprar
                        <strong><span style="color: #dc3545;">${int(faltante_e2):,}</span></strong> adicionales (valor aplicable)
                        y así alcanzar la Escala 2.
                    </p>
                    <p style="font-size: 1.2em;">
                        Si logras esta meta, tu <strong>Nota de Crédito TOTAL</strong> proyectada para el {quarter_name} será de
                        <strong><span style="color: #007bff;">${int(nc_total_proyectada_e2):,}</span></strong>.
                        ¡Este es tu máximo potencial de rebate para el trimestre!
                    </p>
                </div>
            """, unsafe_allow_html=True)


# Llamar a la función con el nuevo diseño
display_quarterly_analysis_premium("3er Trimestre (3Q)", [7, 8, 9], analysis_df, pintuco_df)
display_quarterly_analysis_premium("4to Trimestre (4Q)", [10, 11, 12], analysis_df, pintuco_df)

st.divider()

st.subheader("📥 Reporte Profesional en Excel")
st.markdown("Descarga un archivo Excel con el análisis completo y detallado de todas las metas de rebate.")
excel_data = generate_excel_report(analysis_df)
st.download_button(
    label="⬇️ Descargar Análisis Detallado en Excel",
    data=excel_data,
    file_name=f"Analisis_Rebate_Pintuco_{date.today()}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    help="Haz clic para descargar un reporte completo en formato Excel."
)

# --- PESTAÑAS DE ANÁLISIS DETALLADO ---
tab_vol, tab_est, tab_prof, tab_docs = st.tabs(["💧 Análisis por Volumen", "☀️ Análisis por Estacionalidad", "💎 Análisis por Profundidad", "📑 Detalle de Documentos"])

column_format_config = {
    "Compra_Aplicable": st.column_config.NumberColumn("Compra Aplicable (95%)", format="$ %d"),
    "Meta_E1": st.column_config.NumberColumn("Meta E1", format="$ %d"),
    "Faltante_E1": st.column_config.NumberColumn("Faltante E1", format="$ %d"),
    "%_Cumplimiento_E1": st.column_config.ProgressColumn("% Cumpl. E1", format="%.1f%%", min_value=0, max_value=100),
    "Rebate_Potencial_E1": st.column_config.NumberColumn("NC Proyectada E1", format="$ %d"),
    "Meta_E2": st.column_config.NumberColumn("Meta E2", format="$ %d"),
    "Faltante_E2": st.column_config.NumberColumn("Faltante E2", format="$ %d"),
    "%_Cumplimiento_E2": st.column_config.ProgressColumn("% Cumpl. E2", format="%.1f%%", min_value=0, max_value=100),
    "Rebate_Potencial_E2": st.column_config.NumberColumn("NC Proyectada E2", format="$ %d"),
    "Rebate_Ganado_Actual": st.column_config.NumberColumn("NC Ganada Actual", format="$ %d"),
}

with tab_vol:
    st.header("Análisis de Cumplimiento por Volumen de Compra")
    st.markdown("Aquí puedes ver el detalle de tu progreso hacia las metas de volumen para cada período.")
    df_vol = analysis_df[analysis_df['Tipo'] == 'Volumen'].copy()
    df_vol['%_Cumplimiento_E1'] *= 100
    df_vol['%_Cumplimiento_E2'] *= 100
    st.dataframe(df_vol, use_container_width=True, hide_index=True, column_config=column_format_config)

with tab_est:
    st.header("Análisis de Cumplimiento por Estacionalidad")
    st.info("ℹ️ **Condición Clave:** Para aplicar al rebate por estacionalidad, debes haber comprado al menos el **90% del total del mes** antes del día 20. Si esta condición no se cumple, el rebate para ese mes será $0.")
    df_est = analysis_df[analysis_df['Tipo'] == 'Estacionalidad'].copy()

    def format_rebate_ganado_est(val):
        if val == -1: # Usamos -1 como indicador de "condición no cumplida"
            return "❌ Condición No Cumplida"
        return f"${int(val):,}"

    df_est['Rebate_Ganado_Actual_Formatted'] = df_est['Rebate_Ganado_Actual'].apply(format_rebate_ganado_est)
    df_est['%_Cumplimiento_E1'] *= 100
    df_est['%_Cumplimiento_E2'] *= 100
    
    est_column_config = {
        "Periodo": "Periodo",
        "Compra_Aplicable": st.column_config.NumberColumn("Compra Aplicable (95%)", format="$ %d"),
        "Meta_E1": st.column_config.NumberColumn("Meta E1", format="$ %d"),
        "Faltante_E1": st.column_config.NumberColumn("Faltante E1", format="$ %d"),
        "%_Cumplimiento_E1": st.column_config.ProgressColumn("% Cumpl. E1", format="%.1f%%", min_value=0, max_value=100),
        "Rebate_Potencial_E1": st.column_config.NumberColumn("NC Proyectada E1", format="$ %d"),
        "Meta_E2": st.column_config.NumberColumn("Meta E2", format="$ %d"),
        "Faltante_E2": st.column_config.NumberColumn("Faltante E2", format="$ %d"),
        "%_Cumplimiento_E2": st.column_config.ProgressColumn("% Cumpl. E2", format="%.1f%%", min_value=0, max_value=100),
        "Rebate_Potencial_E2": st.column_config.NumberColumn("NC Proyectada E2", format="$ %d"),
        "Rebate_Ganado_Actual_Formatted": "NC Ganada Actual" # Usar la columna formateada
    }
    
    # Seleccionar las columnas en el orden deseado
    display_cols_est = ["Periodo", "Compra_Aplicable", "Meta_E1", "Faltante_E1", "%_Cumplimiento_E1", "Rebate_Potencial_E1",
                        "Meta_E2", "Faltante_E2", "%_Cumplimiento_E2", "Rebate_Potencial_E2", "Rebate_Ganado_Actual_Formatted"]

    st.dataframe(df_est[display_cols_est], use_container_width=True, hide_index=True, column_config=est_column_config)

with tab_prof:
    st.header("Análisis de Rebate por Profundidad")
    st.info("Este rebate corresponde a un **1% adicional** sobre la compra neta aplicable (95%) de cada trimestre (3Q y 4Q).")
    df_prof = analysis_df[analysis_df['Tipo'] == 'Profundidad']
    st.dataframe(df_prof[["Periodo", "Compra_Aplicable", "Rebate_Ganado_Actual"]], use_container_width=True, hide_index=True,
                 column_config={
                     "Compra_Aplicable": st.column_config.NumberColumn("Compra Aplicable Trimestre (95%)", format="$ %d"),
                     "Rebate_Ganado_Actual": st.column_config.NumberColumn("Nota Crédito Ganada (1%)", format="$ %d", help="Calculado como el 1% de la Compra Aplicable del trimestre."),
                 })

with tab_docs:
    st.subheader("Historial Completo de Documentos de Pintuco")
    st.markdown("Aquí puedes revisar el detalle de todas las facturas y su estado de pago. Los valores de 'Compra Aplicable a Rebate' ya reflejan el 95%.")
    st.dataframe(pintuco_df.sort_values(by="Fecha_Factura", ascending=False), use_container_width=True, hide_index=True,
                 column_config={
                     "Fecha_Factura": st.column_config.DateColumn("Fecha", format="YYYY-MM-DD"),
                     "Numero_Factura": "Número de Factura",
                     "Valor_Neto": st.column_config.NumberColumn("Valor Neto (Antes de IVA)", format="$ %d", help="Valor de la factura antes de aplicar el factor del 95%."),
                     "Compra_Aplicable_Rebate": st.column_config.NumberColumn("Valor Aplicable a Rebate (95%)", format="$ %d", help="Valor que suma para las metas de rebate (95% del valor neto)."),
                     "Proveedor_Correo": "Proveedor (Correo)",
                     "Estado_Pago": st.column_config.TextColumn("Estado de Pago", help="Indica si la factura está Pendiente de pago (según Dropbox) o Pagada."),
                 })
