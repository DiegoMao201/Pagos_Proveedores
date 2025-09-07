# -*- coding: utf-8 -*-
"""
Módulo de Seguimiento de Rebate para PINTUCO COLOMBIA SAS (Versión 3.1 - Gerencial Corregida).

Este módulo es una herramienta de análisis gerencial diseñada para:
1.  Sincronizar de forma independiente e inteligente todas las facturas de PINTUCO,
    reconociendo múltiples nombres de proveedor (alias).
2.  Analizar la estructura compleja de XML anidados para una extracción de datos precisa.
3.  Cargar y procesar correctamente las Notas de Crédito desde Dropbox.
4.  Almacenar un historial completo en una pestaña dedicada dentro del libro principal.
5.  Calcular y visualizar en tiempo real el progreso del acuerdo de rebate,
    enfocándose en la toma de decisiones con proyecciones claras.

Mejoras en v3.1 (Gerencial Corregida):
- **Corrección de Error Crítico (KeyError):** Se solucionó un error que impedía la visualización
  del panel de decisión gerencial al no encontrar los porcentajes de rebate. Ahora la lógica
  consulta correctamente la configuración inicial del acuerdo.
- **Análisis Gerencial por Trimestre (Q):** Se mantiene el dashboard de alto nivel
  enfocado en los resultados trimestrales (3Q y 4Q). Este panel presenta de forma
  clara e intuitiva:
    - El total comprado y el monto aplicable al rebate.
    - Mensajes de acción directos: "Te falta X para la meta" y "Si cumples, tu
      Nota de Crédito TOTAL para el trimestre será Y".
    - El cálculo de la Nota de Crédito potencial totaliza TODOS los rebates
      posibles para el trimestre (Volumen, Estacionalidad de los 3 meses y Profundidad),
      ofreciendo una visión 360° del impacto de una decisión de compra.
- **Exclusión de Compras No Aplicables:** Se mantiene la regla de negocio clave donde
  un 5% de las compras netas se excluye del cálculo para el cumplimiento de metas.
- **Interfaz Intuitiva y Reportes:** Se conservan la UI optimizada para la toma de
  decisiones y la funcionalidad de descarga de reportes profesionales en Excel.
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
NON_APPLICABLE_PURCHASE_FACTOR = 0.95 # Factor de compra que SÍ aplica para metas (100% - 5% = 95%)

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
st.warning("**Regla de negocio importante:** El análisis se basa en el **95%** del valor neto de las compras, ya que hay un 5% que no aplica para el cumplimiento de metas de rebate.", icon="ℹ️")

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

# --- PANEL DE DECISIÓN GERENCIAL POR TRIMESTRE (CON CORRECCIÓN) ---
st.header("🚀 Panel de Decisión Gerencial por Trimestre")

def display_quarterly_analysis(quarter_name: str, months: list, analysis_df: pd.DataFrame, pintuco_df: pd.DataFrame):
    with st.container(border=True):
        st.subheader(f"Análisis Consolidado del {quarter_name}")

        vol_q_data = analysis_df[(analysis_df['Tipo'] == 'Volumen') & (analysis_df['Periodo'] == quarter_name)].iloc[0]
        prof_q_data = analysis_df[(analysis_df['Tipo'] == 'Profundidad') & (analysis_df['Periodo'] == quarter_name)].iloc[0]
        est_q_df = analysis_df[(analysis_df['Tipo'] == 'Estacionalidad') & (analysis_df['Periodo'].isin([META_ESTACIONALIDAD[m]['Nombre'] for m in months]))]
        
        # **INICIO DE LA CORRECCIÓN**
        # Obtener la información de metas y rebates del diccionario original
        meta_info_q = META_VOLUMEN.get(quarter_name, {})
        rebate_1_percent = meta_info_q.get("Rebate 1", 0)
        rebate_2_percent = meta_info_q.get("Rebate 2", 0)
        # **FIN DE LA CORRECCIÓN**

        rebate_ganado_profundidad = prof_q_data['Rebate_Ganado_Actual']
        rebate_ganado_estacionalidad = est_q_df[est_q_df['Rebate_Ganado_Actual'] >= 0]['Rebate_Ganado_Actual'].sum()
        total_comprado_q_neto = pintuco_df[pintuco_df['Fecha_Factura'].dt.month.isin(months)]['Valor_Neto'].sum()
        compra_aplicable_q = vol_q_data['Compra_Aplicable']

        c1, c2 = st.columns(2)
        c1.metric(f"🛒 Compra Neta Total ({quarter_name})", f"${int(total_comprado_q_neto):,}")
        c2.metric(f"🎯 Compra Aplicable a Metas ({quarter_name})", f"${int(compra_aplicable_q):,}")

        st.info(f"**Rebates ya ganados este trimestre:** Profundidad (${int(rebate_ganado_profundidad):,}) + Estacionalidad (${int(rebate_ganado_estacionalidad):,}) = **${int(rebate_ganado_profundidad + rebate_ganado_estacionalidad):,}**")

        st.markdown("---")
        st.subheader("Proyecciones de Ganancia (Rebate por Volumen)")

        # Proyección Escala 1
        faltante_e1 = vol_q_data['Faltante_E1']
        nc_potencial_volumen_e1 = vol_q_data['Rebate_Potencial_E1']
        nc_total_proyectada_e1 = nc_potencial_volumen_e1 + rebate_ganado_profundidad + rebate_ganado_estacionalidad

        if compra_aplicable_q >= vol_q_data['Meta_E1']:
            st.success(f"✅ **¡Meta Escala 1 Superada!** | NC por Volumen: **${int(vol_q_data['Rebate_Ganado_Actual']):,}**")
        else:
            st.markdown(f"**Para alcanzar la Escala 1 ({rebate_1_percent*100:.1f}% de rebate):**")
            msg1 = f"Te falta comprar **${int(faltante_e1):,}** (valor aplicable)."
            msg2 = f"Al lograrlo, tu Nota de Crédito **TOTAL** para el {quarter_name} será de **${int(nc_total_proyectada_e1):,}**."
            st.markdown(f"<h3>{msg1}<br>{msg2}</h3>", unsafe_allow_html=True)

        # Proyección Escala 2
        faltante_e2 = vol_q_data['Faltante_E2']
        nc_potencial_volumen_e2 = vol_q_data['Rebate_Potencial_E2']
        nc_total_proyectada_e2 = nc_potencial_volumen_e2 + rebate_ganado_profundidad + rebate_ganado_estacionalidad

        if compra_aplicable_q >= vol_q_data['Meta_E2']:
            st.success(f"🎉 **¡Meta Escala 2 Superada!** | NC por Volumen: **${int(vol_q_data['Rebate_Ganado_Actual']):,}**")
        else:
            st.markdown(f"**Para alcanzar la Escala 2 ({rebate_2_percent*100:.1f}% de rebate):**")
            msg1_e2 = f"Te falta comprar **${int(faltante_e2):,}** (valor aplicable)."
            msg2_e2 = f"Al lograrlo, tu Nota de Crédito **TOTAL** para el {quarter_name} será de **${int(nc_total_proyectada_e2):,}**."
            st.markdown(f"<h3>{msg1_e2}<br>{msg2_e2}</h3>", unsafe_allow_html=True)


display_quarterly_analysis("3er Trimestre (3Q)", [7, 8, 9], analysis_df, pintuco_df)
display_quarterly_analysis("4to Trimestre (4Q)", [10, 11, 12], analysis_df, pintuco_df)

st.divider()

st.subheader("📥 Reporte Profesional en Excel")
excel_data = generate_excel_report(analysis_df)
st.download_button(
    label="Descargar Análisis Detallado en Excel",
    data=excel_data,
    file_name=f"Analisis_Rebate_Pintuco_{date.today()}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

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
    df_vol = analysis_df[analysis_df['Tipo'] == 'Volumen']
    st.dataframe(df_vol, use_container_width=True, hide_index=True, column_config=column_format_config)

with tab_est:
    st.header("Análisis de Cumplimiento por Estacionalidad")
    st.info("Condición: Se debe comprar al menos el 90% del total del mes antes del día 20. Si no se cumple, el rebate para ese mes es $0.")
    df_est = analysis_df[analysis_df['Tipo'] == 'Estacionalidad'].copy()

    def format_rebate_ganado(val):
        if val < 0:
            return "Condición No Cumplida"
        return f"${int(val):,}"

    df_est['Rebate_Ganado_Actual'] = df_est['Rebate_Ganado_Actual'].apply(format_rebate_ganado)

    est_config = column_format_config.copy()
    del est_config['Rebate_Ganado_Actual']

    st.dataframe(df_est, use_container_width=True, hide_index=True, column_config=est_config)

with tab_prof:
    st.header("Análisis de Rebate por Profundidad")
    st.info("Este rebate corresponde al 1% sobre la compra neta aplicable (95%) de cada trimestre (3Q y 4Q).")
    df_prof = analysis_df[analysis_df['Tipo'] == 'Profundidad']
    st.dataframe(df_prof[["Periodo", "Compra_Aplicable", "Rebate_Ganado_Actual"]], use_container_width=True, hide_index=True,
                 column_config={
                     "Compra_Aplicable": st.column_config.NumberColumn("Compra Aplicable Trimestre (95%)", format="$ %d"),
                     "Rebate_Ganado_Actual": st.column_config.NumberColumn("Nota Crédito Ganada (1%)", format="$ %d"),
                 })

with tab_docs:
    st.subheader("Historial Completo de Documentos de Pintuco")
    st.dataframe(pintuco_df.sort_values(by="Fecha_Factura", ascending=False), use_container_width=True, hide_index=True,
                 column_config={
                     "Fecha_Factura": st.column_config.DateColumn("Fecha", format="YYYY-MM-DD"),
                     "Valor_Neto": st.column_config.NumberColumn("Valor Neto (Antes de IVA)", format="$ %d"),
                     "Compra_Aplicable_Rebate": st.column_config.NumberColumn("Valor Aplicable a Rebate (95%)", format="$ %d"),
                 })
