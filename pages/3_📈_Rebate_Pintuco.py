# -*- coding: utf-8 -*-
"""
Módulo de Seguimiento de Rebate para PINTUCO COLOMBIA SAS (Versión 3.0 - Gerencial).

Este módulo es una herramienta de análisis gerencial diseñada para:
1.  Sincronizar de forma independiente e inteligente todas las facturas de PINTUCO,
    reconociendo múltiples nombres de proveedor (alias).
2.  Analizar la estructura compleja de XML anidados para una extracción de datos precisa.
3.  Cargar y procesar correctamente las Notas de Crédito desde Dropbox.
4.  Almacenar un historial completo en una pestaña dedicada dentro del libro principal.
5.  Calcular y visualizar en tiempo real el progreso del acuerdo de rebate,
    enfocándose en la toma de decisiones con proyecciones claras y gráficas.

Mejoras en v3.0 (Gerencial):
- **Exclusión del 5% no aplicable:** Se descuenta un 5% del total de compras que no
  suma para el acuerdo de rebate, asegurando que los cálculos sean precisos.
- **KPIs Trimestrales y Semestrales:** La vista principal ahora incluye métricas clave
  tanto para el semestre como para el trimestre en curso.
- **Visualización Gráfica Avanzada:** Se reemplazan tablas estáticas por gráficos de barras
  interactivos que muestran claramente la compra real vs. las metas de cada escala.
- **Proyecciones Claras y Detalladas:** El cálculo de la nota de crédito proyectada
  ahora considera el impacto total del cumplimiento trimestral ("recomposición").
- **Interfaz Gerencial Mejorada:** Se rediseñaron los comentarios automáticos para ser
  más claros, legibles y estratégicos, proporcionando insights accionables.
- **Reporte Profesional en Excel:** Se mantiene la funcionalidad para descargar un informe
  detallado para su distribución y estudio fuera de la aplicación.
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
import plotly.graph_objects as go
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
NON_APPLICABLE_PURCHASE_PERCENTAGE = 0.05 # 5% de las compras no suman al rebate

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
    except gspread.exceptions.APIError:
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
        if supplier_name_node is None or not any(alias in supplier_name_node.text.upper() for alias in PINTUCO_ALIASES):
            return None
        invoice_number = invoice_root.find('./cbc:ID', ns).text.strip()
        issue_date = invoice_root.find('./cbc:IssueDate', ns).text.strip()
        net_value = float(invoice_root.find('.//cac:LegalMonetaryTotal/cbc:TaxExclusiveAmount', ns).text.strip())
        return {"Fecha_Factura": issue_date, "Numero_Factura": normalize_invoice_number(invoice_number), "Valor_Neto": net_value, "Proveedor_Correo": supplier_name_node.text.strip()}
    except Exception:
        return None

def fetch_pintuco_invoices_from_email(start_date: date) -> pd.DataFrame:
    invoices_data = []
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(st.secrets.email["address"], st.secrets.email["password"])
        mail.select(f'"{EMAIL_FOLDER}"')
        _, messages = mail.search(None, f'(SINCE "{start_date.strftime("%d-%b-%Y")}")')
        message_ids = messages[0].split()
        if not message_ids: mail.logout(); return pd.DataFrame()
        progress_bar = st.progress(0, text=f"Procesando {len(message_ids)} correos...")
        for i, num in enumerate(message_ids):
            _, data = mail.fetch(num, "(RFC822)")
            msg = email.message_from_bytes(data[0][1])
            for part in msg.walk():
                if part.get_filename() and part.get_filename().lower().endswith('.zip'):
                    with zipfile.ZipFile(io.BytesIO(part.get_payload(decode=True))) as zf:
                        for name in zf.namelist():
                            if name.lower().endswith('.xml'):
                                details = parse_invoice_xml(zf.read(name).decode('utf-8', 'ignore'))
                                if details: invoices_data.append(details)
            progress_bar.progress((i + 1) / len(message_ids), text=f"Procesando correos... ({i+1}/{len(message_ids)})")
        mail.logout()
        return pd.DataFrame(invoices_data)
    except Exception as e:
        st.error(f"❌ Error procesando correos: {e}"); return pd.DataFrame()

# --- 4. LÓGICA PRINCIPAL DE SINCRONIZACIÓN (ROBUSTA) ---
def run_pintuco_sync():
    with st.spinner('Iniciando sincronización de Pintuco...'):
        pending_docs_set = load_pending_documents_from_dropbox()
        gs_client = connect_to_google_sheets()
        if not gs_client: st.error("Sincronización cancelada."); st.stop()
        worksheet = get_worksheet(gs_client, st.secrets["google_sheet_id"], PINTUCO_WORKSHEET_NAME)
        
        historical_df = pd.DataFrame()
        start_date = INITIAL_START_DATE_SYNC
        try:
            records = worksheet.get_all_records()
            if records:
                historical_df = pd.DataFrame(records)
                historical_df['Fecha_Factura'] = pd.to_datetime(historical_df['Fecha_Factura'])
                start_date = historical_df['Fecha_Factura'].max().date() - timedelta(days=3)
        except Exception as e:
            st.warning(f"No se cargaron datos históricos. Sincronizando desde el inicio. Error: {e}")

        new_invoices_df = fetch_pintuco_invoices_from_email(start_date)
        
        if not new_invoices_df.empty:
            new_invoices_df['Fecha_Factura'] = pd.to_datetime(new_invoices_df['Fecha_Factura'])
            combined_df = pd.concat([historical_df, new_invoices_df], ignore_index=True).drop_duplicates(subset=['Numero_Factura'], keep='last')
            st.info(f"Se consolidaron {len(new_invoices_df)} facturas nuevas.")
        else:
            combined_df = historical_df
            st.success("No se encontraron **nuevas** facturas de Pintuco.")

        if not combined_df.empty:
            combined_df['Numero_Factura_Normalized'] = combined_df['Numero_Factura'].apply(normalize_invoice_number)
            combined_df['Estado_Pago'] = np.where(combined_df['Numero_Factura_Normalized'].isin(pending_docs_set), 'Pendiente', 'Pagada')
            final_df = combined_df.drop(columns=['Numero_Factura_Normalized']).reindex(columns=["Fecha_Factura", "Numero_Factura", "Valor_Neto", "Proveedor_Correo", "Estado_Pago"])
            if update_gsheet_from_df(worksheet, final_df.sort_values(by="Fecha_Factura")):
                st.success("✅ ¡Base de datos de Pintuco actualizada!")
        st.session_state['last_pintuco_sync'] = datetime.now(COLOMBIA_TZ).strftime('%Y-%m-%d %H:%M:%S')
        st.balloons()

# --- 5. LÓGICA DE CÁLCULO Y ANÁLISIS AVANZADO ---
@st.cache_data(ttl=300)
def load_and_prepare_data() -> pd.DataFrame:
    try:
        gs_client = connect_to_google_sheets()
        worksheet = get_worksheet(gs_client, st.secrets["google_sheet_id"], PINTUCO_WORKSHEET_NAME)
        records = worksheet.get_all_records()
        if not records: return pd.DataFrame()
        df = pd.DataFrame(records)
        df['Fecha_Factura'] = pd.to_datetime(df['Fecha_Factura'])
        df['Valor_Neto'] = pd.to_numeric(df['Valor_Neto'].astype(str).str.replace(',', ''), errors='coerce')
        
        # **NUEVO**: Aplicar la reducción del 5% a las compras que no suman
        df['Valor_Neto_Aplicable'] = df['Valor_Neto'] * (1 - NON_APPLICABLE_PURCHASE_PERCENTAGE)
        
        return df
    except Exception as e:
        st.error(f"Error al cargar datos desde Google Sheets: {e}"); return pd.DataFrame()

def generate_rebate_analysis(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return pd.DataFrame()
    df_copy = df.copy()
    df_copy['Mes'] = df_copy['Fecha_Factura'].dt.month
    analysis_data = []

    def process_period(period_name, purchase_value, meta_info, rebate_type):
        meta_e1, rebate_e1 = meta_info.get("Escala 1", 0), meta_info.get("Rebate 1", 0)
        meta_e2, rebate_e2 = meta_info.get("Escala 2", 0), meta_info.get("Rebate 2", 0)
        
        rebate_ganado = purchase_value * rebate_e2 if purchase_value >= meta_e2 else (purchase_value * rebate_e1 if purchase_value >= meta_e1 else 0)
        
        return {
            "Periodo": period_name, "Tipo": rebate_type, "Compra_Real": purchase_value,
            "Meta_E1": meta_e1, "Faltante_E1": max(0, meta_e1 - purchase_value), "%_Cumplimiento_E1": (purchase_value / meta_e1) if meta_e1 > 0 else 0, "Rebate_Potencial_E1": meta_e1 * rebate_e1,
            "Meta_E2": meta_e2, "Faltante_E2": max(0, meta_e2 - purchase_value), "%_Cumplimiento_E2": (purchase_value / meta_e2) if meta_e2 > 0 else 0, "Rebate_Potencial_E2": meta_e2 * rebate_e2,
            "Rebate_Ganado_Actual": rebate_ganado
        }

    # Análisis de Estacionalidad y Volumen
    period_map_vol = {
        "Julio-Agosto": [7, 8], "Agosto-Sept.": [8, 9], "3er Trimestre (3Q)": [7, 8, 9],
        "Octubre-Nov.": [10, 11], "Noviembre-Dic.": [11, 12], "4to Trimestre (4Q)": [10, 11, 12],
        "2do Semestre (2Sem)": list(range(7, 13))
    }
    for period, months in period_map_vol.items():
        total_period_purchase = df_copy[df_copy['Mes'].isin(months)]['Valor_Neto_Aplicable'].sum()
        analysis_data.append(process_period(period, total_period_purchase, META_VOLUMEN.get(period, {}), "Volumen"))
    
    for month, meta in META_ESTACIONALIDAD.items():
        monthly_df = df_copy[df_copy['Mes'] == month]
        total_month_purchase = monthly_df['Valor_Neto_Aplicable'].sum()
        purchase_before_20th = monthly_df[monthly_df['Fecha_Factura'].dt.day <= 20]['Valor_Neto_Aplicable'].sum()
        
        if total_month_purchase > 0 and (purchase_before_20th / total_month_purchase) >= 0.9:
            analysis_data.append(process_period(meta["Nombre"], total_month_purchase, meta, "Estacionalidad"))
        else:
            entry = process_period(meta["Nombre"], total_month_purchase, {}, "Estacionalidad")
            entry["Rebate_Ganado_Actual"] = -1 # Condición no cumplida
            analysis_data.append(entry)

    # Análisis de Profundidad
    for q_period, months in {"3er Trimestre (3Q)": [7, 8, 9], "4to Trimestre (4Q)": [10, 11, 12]}.items():
        total_q_purchase = df_copy[df_copy['Mes'].isin(months)]['Valor_Neto_Aplicable'].sum()
        analysis_data.append({
            "Periodo": q_period, "Tipo": "Profundidad", "Compra_Real": total_q_purchase,
            "Rebate_Ganado_Actual": total_q_purchase * REBATE_PROFUNDIDAD_Q,
            # Relleno para consistencia del DataFrame
            "Meta_E1": 0, "Faltante_E1": 0, "%_Cumplimiento_E1": 1, "Rebate_Potencial_E1": total_q_purchase * REBATE_PROFUNDIDAD_Q,
            "Meta_E2": 0, "Faltante_E2": 0, "%_Cumplimiento_E2": 1, "Rebate_Potencial_E2": 0,
        })
        
    return pd.DataFrame(analysis_data)

def generate_excel_report(analysis_df: pd.DataFrame):
    output = io.BytesIO()
    wb = Workbook()
    ws = wb.active
    ws.title = "Análisis Rebate Pintuco"
    rows = dataframe_to_rows(analysis_df, index=False, header=True)
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
    center_align = Alignment(horizontal='center', vertical='center')

    for r_idx, row in enumerate(rows, 1):
        for c_idx, value in enumerate(row, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=value)
            if r_idx == 1:
                cell.font = header_font; cell.fill = header_fill; cell.alignment = center_align
            else:
                col_name = analysis_df.columns[c_idx - 1]
                if any(k in col_name for k in ["Compra_", "Faltante_", "Rebate_", "Meta_"]): cell.number_format = '_("$"* #,##0_);_("$"* (#,##0);_("$"* "-"??_);_(@_)'
                if "%_Cumplimiento" in col_name: cell.number_format = '0.0%'
    
    for column_cells in ws.columns:
        length = max(len(str(cell.value)) for cell in column_cells)
        ws.column_dimensions[column_cells[0].column_letter].width = length + 2
        
    wb.save(output); output.seek(0)
    return output

# --- 6. APLICACIÓN PRINCIPAL (STREAMLIT UI) ---
st.title("🎯 Dashboard Gerencial de Rebate: PINTUCO")

col1, col2 = st.columns([3, 1])
with col1:
    st.markdown("Herramienta de **análisis y decisión** para el seguimiento del acuerdo comercial con **PINTUCO COLOMBIA S.A.S**.")
    st.info(f"**Anotación Importante:** Los cálculos descuentan un **{NON_APPLICABLE_PURCHASE_PERCENTAGE:.0%}** del total de compras netas que no aplica para el rebate.")
with col2:
    if st.button("🔄 Sincronizar Facturas", type="primary", use_container_width=True):
        run_pintuco_sync()
        st.cache_data.clear()
        st.rerun()
    if 'last_pintuco_sync' in st.session_state:
        st.caption(f"Última Sincronización: {st.session_state.last_pintuco_sync}")

pintuco_df = load_and_prepare_data()
if pintuco_df.empty:
    st.warning("No hay datos para analizar. Realiza la primera sincronización."); st.stop()

analysis_df = generate_rebate_analysis(pintuco_df)
total_rebate_ganado = analysis_df[analysis_df['Rebate_Ganado_Actual'] >= 0]['Rebate_Ganado_Actual'].sum()

# --- KPIs PRINCIPALES ---
st.divider()
st.header("📊 Resumen Ejecutivo")

# Datos para KPIs
sem_data = analysis_df.loc[(analysis_df['Periodo'] == '2do Semestre (2Sem)')].iloc[0]
current_q_num = (datetime.now().month - 1) // 3 + 1
current_q_str = "3er Trimestre (3Q)" if current_q_num == 3 else "4to Trimestre (4Q)"
q_data = analysis_df.loc[(analysis_df['Periodo'] == current_q_str) & (analysis_df['Tipo'] == 'Volumen')].iloc[0]
prof_data_q = analysis_df.loc[(analysis_df['Periodo'] == current_q_str) & (analysis_df['Tipo'] == 'Profundidad')].iloc[0]

# Potencial Trimestral (Volumen Q + Profundidad Q)
q_potential_rebate = q_data['Rebate_Potencial_E2'] + prof_data_q['Rebate_Potencial_E1']

kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
kpi_col1.metric("✅ Rebate Total Ganado (Acumulado)", f"${int(total_rebate_ganado):,}")
kpi_col2.metric(f"💰 Potencial del Semestre (Escala 2)", f"${int(sem_data['Rebate_Potencial_E2']):,}")
kpi_col3.metric(f"🎯 Potencial del {current_q_str}", f"${int(q_potential_rebate):,}", help="Suma del rebate por Volumen (Escala 2) y Profundidad del trimestre.")
kpi_col4.metric(f"📈 Faltante para Meta E1 ({current_q_str})", f"${int(q_data['Faltante_E1']):,}", delta_color="inverse")

st.divider()

# --- Descarga de Excel ---
st.download_button(
    label="📥 Descargar Análisis Detallado en Excel",
    data=generate_excel_report(analysis_df),
    file_name=f"Analisis_Rebate_Pintuco_{date.today()}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

tab_vol, tab_est, tab_prof, tab_docs = st.tabs(["💧 Análisis por Volumen", "☀️ Análisis por Estacionalidad", "💎 Análisis por Profundidad", "📑 Detalle de Documentos"])

# --- Función para Gráficos y Comentarios ---
def create_progress_chart(period_data):
    period_name = period_data['Periodo']
    compra_real = period_data['Compra_Real']
    meta_e1, meta_e2 = period_data['Meta_E1'], period_data['Meta_E2']
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=['Progreso'], x=[compra_real], name='Compra Real', orientation='h',
        marker=dict(color='rgba(50, 171, 96, 0.7)', line=dict(color='rgba(50, 171, 96, 1.0)', width=1))
    ))
    
    fig.update_layout(
        title=f'<b>Progreso vs. Metas para {period_name}</b>',
        xaxis=dict(title='Valor de Compra (COP)'),
        yaxis=dict(showticklabels=False),
        barmode='stack',
        shapes=[
            dict(type='line', y0=-0.5, y1=0.5, x0=meta_e1, x1=meta_e1, line=dict(color='orange', width=3, dash='dash'), name='Meta E1'),
            dict(type='line', y0=-0.5, y1=0.5, x0=meta_e2, x1=meta_e2, line=dict(color='red', width=3, dash='dash'), name='Meta E2')
        ],
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=20, r=20, t=40, b=20),
        height=250
    )
    # Añadir anotaciones para las metas
    fig.add_annotation(x=meta_e1, y=0.3, text=f"Meta E1: ${int(meta_e1/1e6)}M", showarrow=True, arrowhead=1, ax=0, ay=-40)
    fig.add_annotation(x=meta_e2, y=0.3, text=f"Meta E2: ${int(meta_e2/1e6)}M", showarrow=True, arrowhead=1, ax=0, ay=-60)
    
    return fig

def generate_managerial_insights(period_data):
    st.markdown("---")
    st.subheader("💡 Conclusiones y Estrategia")
    
    faltante_e1, rebate_pot_e1 = period_data['Faltante_E1'], period_data['Rebate_Potencial_E1']
    faltante_e2, rebate_pot_e2 = period_data['Rebate_Potencial_E2']
    
    if period_data['Compra_Real'] == 0:
        st.info(f"No hay compras registradas para el período **{period_data['Periodo']}**.")
        return

    if faltante_e1 > 0:
        st.warning(f"""
        **Objetivo Primario (Escala 1):**
        - **Acción:** Comprar **${int(faltante_e1):,}** adicionales.
        - **Resultado:** Alcanzar la **Escala 1** para el período **{period_data['Periodo']}**, lo que generaría una Nota de Crédito de **${int(rebate_pot_e1):,}**.
        """)
        st.info(f"""
        **Objetivo Óptimo (Escala 2):**
        - **Acción:** Para maximizar el retorno, se necesita una compra total de **${int(faltante_e2):,}** adicionales.
        - **Resultado:** Esto desbloquearía la **Escala 2**, resultando en una Nota de Crédito superior de **${int(rebate_pot_e2):,}**.
        """)
    elif faltante_e2 > 0:
        st.success(f"""
        **¡Meta de Escala 1 Alcanzada! ✅**
        - Ya se ha asegurado una Nota de Crédito de **${int(rebate_pot_e1):,}**.
        """)
        st.info(f"""
        **Siguiente Nivel (Escala 2):**
        - **Acción:** Comprar **${int(faltante_e2):,}** adicionales.
        - **Resultado:** Incrementar la Nota de Crédito final a **${int(rebate_pot_e2):,}**.
        """)
    else:
        st.success(f"""
        **¡Excelente Desempeño! 🏆**
        - Has superado la **Escala 2** para el período **{period_data['Periodo']}**.
        - Se ha asegurado la máxima Nota de Crédito por volumen de **${int(rebate_pot_e2):,}**.
        """)

with tab_vol:
    st.header("Análisis de Cumplimiento por Volumen de Compra")
    df_vol = analysis_df[analysis_df['Tipo'] == 'Volumen']

    st.subheader("3er Trimestre (3Q)")
    with st.container(border=True):
        data_q3 = df_vol[df_vol['Periodo'] == '3er Trimestre (3Q)'].iloc[0]
        st.plotly_chart(create_progress_chart(data_q3), use_container_width=True)
        generate_managerial_insights(data_q3)
        with st.expander("Ver desglose bimestral del 3Q"):
             st.dataframe(df_vol[df_vol['Periodo'].isin(['Julio-Agosto', 'Agosto-Sept.'])], use_container_width=True, hide_index=True)


    st.subheader("4to Trimestre (4Q)")
    with st.container(border=True):
        data_q4 = df_vol[df_vol['Periodo'] == '4to Trimestre (4Q)'].iloc[0]
        st.plotly_chart(create_progress_chart(data_q4), use_container_width=True)
        generate_managerial_insights(data_q4)
        with st.expander("Ver desglose bimestral del 4Q"):
             st.dataframe(df_vol[df_vol['Periodo'].isin(['Octubre-Nov.', 'Noviembre-Dic.'])], use_container_width=True, hide_index=True)


with tab_est:
    st.header("Análisis de Cumplimiento por Estacionalidad")
    st.info("Condición: Se debe comprar al menos el 90% del total aplicable del mes antes del día 20. Si no se cumple, el rebate para ese mes es $0.")
    df_est = analysis_df[analysis_df['Tipo'] == 'Estacionalidad'].copy()
    
    def format_rebate_ganado(val):
        if val < 0: return "⚠️ No Cumplido"
        return f"${int(val):,}"

    df_est['Rebate_Ganado_Actual_Fmt'] = df_est['Rebate_Ganado_Actual'].apply(format_rebate_ganado)
    df_est['%_Cumplimiento_E1'] = (df_est['%_Cumplimiento_E1']*100).round(1)

    st.subheader("3er Trimestre (3Q)")
    with st.container(border=True):
        st.dataframe(df_est.loc[df_est['Periodo'].isin(['Julio', 'Agosto', 'Septiembre']), ["Periodo", "Compra_Real", "%_Cumplimiento_E1", "Rebate_Ganado_Actual_Fmt"]], use_container_width=True, hide_index=True)

    st.subheader("4to Trimestre (4Q)")
    with st.container(border=True):
        st.dataframe(df_est.loc[df_est['Periodo'].isin(['Octubre', 'Noviembre', 'Diciembre']), ["Periodo", "Compra_Real", "%_Cumplimiento_E1", "Rebate_Ganado_Actual_Fmt"]], use_container_width=True, hide_index=True)

with tab_prof:
    st.header("Análisis de Rebate por Profundidad")
    st.info("Este rebate corresponde al 1% sobre la compra neta aplicable de cada trimestre (3Q y 4Q).")
    df_prof = analysis_df[analysis_df['Tipo'] == 'Profundidad']
    
    col1, col2 = st.columns(2)
    with col1:
        with st.container(border=True):
             data_q3 = df_prof[df_prof['Periodo'] == '3er Trimestre (3Q)'].iloc[0]
             st.metric("NC por Profundidad (3Q)", f"${int(data_q3['Rebate_Ganado_Actual']):,}")
             st.caption(f"Basado en una compra de ${int(data_q3['Compra_Real']):,}")
    with col2:
        with st.container(border=True):
             data_q4 = df_prof[df_prof['Periodo'] == '4to Trimestre (4Q)'].iloc[0]
             st.metric("NC por Profundidad (4Q)", f"${int(data_q4['Rebate_Ganado_Actual']):,}")
             st.caption(f"Basado en una compra de ${int(data_q4['Compra_Real']):,}")

with tab_docs:
    st.subheader("Historial Completo de Documentos de Pintuco")
    st.dataframe(pintuco_df.sort_values(by="Fecha_Factura", ascending=False), use_container_width=True, hide_index=True,
                column_config={
                    "Fecha_Factura": st.column_config.DateColumn("Fecha", format="YYYY-MM-DD"),
                    "Valor_Neto": st.column_config.NumberColumn("Valor Neto (Antes de IVA)", format="$ %d"),
                    "Valor_Neto_Aplicable": st.column_config.NumberColumn("Valor Aplicable para Rebate", format="$ %d", help=f"Es el Valor Neto menos el {NON_APPLICABLE_PURCHASE_PERCENTAGE:.0%}"),
                })
