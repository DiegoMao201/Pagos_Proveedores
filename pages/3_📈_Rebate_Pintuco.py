# -*- coding: utf-8 -*-
"""
Módulo de Seguimiento de Rebate para PINTUCO COLOMBIA SAS (Versión 1.9 - Mejorada).

Este módulo es una herramienta de análisis gerencial diseñada para:
1.  Sincronizar de forma independiente e inteligente todas las facturas de PINTUCO,
    reconociendo múltiples nombres de proveedor (alias).
2.  Analizar la estructura compleja de XML anidados para una extracción de datos precisa.
3.  Cargar y procesar correctamente las Notas de Crédito desde Dropbox.
4.  Almacenar un historial completo en una pestaña dedicada dentro del libro principal.
5.  Calcular y visualizar en tiempo real el progreso del acuerdo de rebate.

Mejoras en v1.9:
- **Análisis exhaustivo y proyecciones:** Se ha añadido una lógica completa para proyectar
   las notas de crédito potenciales, detallando lo que falta para cada meta y el
   rebate que se obtendría en las dos escalas.
- **Visualización mejorada:** La interfaz de usuario ha sido rediseñada para ser
   más analítica, mostrando el progreso, lo que falta y el rebate proyectado.
- **Exportación profesional a Excel:** Se ha incorporado una función para generar
   un archivo Excel profesional y detallado, ideal para la toma de decisiones.
- **Manejo de Notas de Crédito:** Se ha ajustado la lógica de procesamiento para
   incluir las Notas de Crédito como parte del cálculo del volumen.
"""

# --- 0. IMPORTACIÓN DE LIBRERÍAS ---
import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import gspread
import dropbox
import imaplib
import email
import xml.etree.ElementTree as ET
import zipfile
import io
import re
from datetime import datetime, date, timedelta
import pytz
from google.oauth2.service_account import Credentials
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font, PatternFill, Alignment

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
    7: {"Escala 1": 1777446334, "Rebate 1": 0.007, "Escala 2": 1901867577, "Rebate 2": 0.01},
    8: {"Escala 1": 1884935846, "Rebate 1": 0.007, "Escala 2": 2016881355, "Rebate 2": 0.01},
    9: {"Escala 1": 1991122686, "Rebate 1": 0.007, "Escala 2": 2130501274, "Rebate 2": 0.01},
    10: {"Escala 1": 2148246123, "Rebate 1": 0.007, "Escala 2": 2298623352, "Rebate 2": 0.01},
    11: {"Escala 1": 2123851269, "Rebate 1": 0.007, "Escala 2": 2272520858, "Rebate 2": 0.01},
    12: {"Escala 1": 1847133473, "Rebate 1": 0.007, "Escala 2": 1976432816, "Rebate 2": 0.01},
}

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
        
        # Asegurarse de que todas las columnas son strings para la carga
        df_to_upload = df_to_upload.astype(str).replace({'nan': '', 'NaT': '', 'None': ''})
        
        # Limpiar y actualizar la hoja
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
    cleaned_str = str(value).strip().replace('$', '').replace(',', '')  # Corregido para quitar la coma
    try: return float(cleaned_str)
    except (ValueError, TypeError): return np.nan

def generate_excel_download(df_summary: pd.DataFrame, df_history: pd.DataFrame):
    output = io.BytesIO()
    wb = Workbook()
    
    # Hoja de Resumen Ejecutivo
    ws_summary = wb.active
    ws_summary.title = "Resumen Ejecutivo Rebate"
    
    # Título
    ws_summary['A1'] = "Análisis Detallado y Proyección de Rebate - Pintuco"
    ws_summary['A1'].font = Font(bold=True, size=16)
    
    # Llenar la hoja de resumen
    summary_headers = ["Período", "Tipo de Rebate", "Volumen Comprado", "Meta Escala 1", "Falta para Escala 1", "Rebate Proyectado (Escala 1)", "Meta Escala 2", "Falta para Escala 2", "Rebate Proyectado (Escala 2)", "Rebate Ganado"]
    ws_summary.append(summary_headers)
    for row in dataframe_to_rows(df_summary, index=False, header=False):
        ws_summary.append(row)
    
    # Formateo de la hoja de resumen
    for col in ws_summary.columns:
        max_length = 0
        column = col[0]
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = (max_length + 2)
        ws_summary.column_dimensions[column].width = adjusted_width
    
    # Estilos
    header_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
    for cell in ws_summary[2]:
        cell.font = Font(bold=True)
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center")
    for row in ws_summary.iter_rows(min_row=3, max_row=ws_summary.max_row, min_col=3, max_col=10):
        for cell in row:
            cell.number_format = '"$ "#,##0'
    
    # Hoja de Historial de Facturas
    ws_history = wb.create_sheet(title="Historial de Facturas")
    ws_history['A1'] = "Historial Completo de Documentos de Pintuco"
    ws_history['A1'].font = Font(bold=True, size=16)
    for row in dataframe_to_rows(df_history, index=False, header=True):
        ws_history.append(row)
    
    # Formateo de la hoja de historial
    for col in ws_history.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = (max_length + 2)
        ws_history.column_dimensions[column].width = adjusted_width
    
    for cell in ws_history[2]:
        cell.font = Font(bold=True)
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center")
    for row in ws_history.iter_rows(min_row=3, max_row=ws_history.max_row, min_col=3, max_col=3):
        for cell in row:
            cell.number_format = '"$ "#,##0'
    
    wb.save(output)
    output.seek(0)
    return output

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
            
            # Combinar datos históricos y nuevos, eliminando duplicados
            combined_df = pd.concat([historical_df, new_invoices_df], ignore_index=True)
            combined_df.drop_duplicates(subset=['Numero_Factura'], keep='last', inplace=True)
            
            st.info(f"Se encontraron y consolidaron {len(new_invoices_df)} facturas nuevas.")
        else:
            st.success("No se encontraron **nuevas** facturas de Pintuco en el correo.")
        
        if not combined_df.empty:
            st.info("Paso 4/4: Actualizando estado de pago y guardando en Google Sheets...")
            # Normalizar números de factura para la comparación
            combined_df['Numero_Factura_Normalized'] = combined_df['Numero_Factura'].apply(normalize_invoice_number)
            
            # Actualizar el estado de pago
            combined_df['Estado_Pago'] = combined_df['Numero_Factura_Normalized'].apply(lambda x: 'Pendiente' if x in pending_docs_set else 'Pagada')
            combined_df.drop(columns=['Numero_Factura_Normalized'], inplace=True) # Eliminar columna auxiliar
            
            # Asegurarse de que el orden de las columnas sea consistente
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

# --- 5. LÓGICA DE CÁLCULO Y VISUALIZACIÓN ---
@st.cache_data(ttl=300)
def load_pintuco_data_from_gsheet() -> pd.DataFrame:
    try:
        gs_client = connect_to_google_sheets()
        worksheet = get_worksheet(gs_client, st.secrets["google_sheet_id"], PINTUCO_WORKSHEET_NAME)
        records = worksheet.get_all_records()
        if not records: return pd.DataFrame()
        df = pd.DataFrame(records)
        df['Fecha_Factura'] = pd.to_datetime(df['Fecha_Factura'])
        df['Valor_Neto'] = pd.to_numeric(df['Valor_Neto'])
        # Asegurarse de que las notas de crédito se resten del total
        df['Valor_Neto'] = df.apply(lambda row: -abs(row['Valor_Neto']) if 'NC' in str(row['Numero_Factura']).upper() else row['Valor_Neto'], axis=1)
        return df
    except Exception as e:
        st.error(f"Error al cargar datos desde Google Sheets: {e}"); return pd.DataFrame()

def calculate_rebate_projections(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return pd.DataFrame()
    df = df.copy(); df['Mes'] = df['Fecha_Factura'].dt.month
    summary_data = []

    # Cálculos de Estacionalidad por Mes
    for month in range(7, 13):
        monthly_df = df[df['Mes'] == month]
        total_month_purchase = monthly_df['Valor_Neto'].sum()
        meta_est = META_ESTACIONALIDAD.get(month, {"Escala 1": 0, "Rebate 1": 0, "Escala 2": 0, "Rebate 2": 0})
        
        purchase_before_20th = monthly_df[monthly_df['Fecha_Factura'].dt.day <= 20]['Valor_Neto'].sum()
        qualifies_for_estacionalidad = (purchase_before_20th / total_month_purchase) >= 0.9 if total_month_purchase > 0 else False
        
        if qualifies_for_estacionalidad:
            # Escala 1
            falta_e1 = max(0, meta_est["Escala 1"] - total_month_purchase)
            rebate_e1_proj = total_month_purchase * meta_est["Rebate 1"] if total_month_purchase >= meta_est["Escala 1"] else 0
            # Escala 2
            falta_e2 = max(0, meta_est["Escala 2"] - total_month_purchase)
            rebate_e2_proj = total_month_purchase * meta_est["Rebate 2"] if total_month_purchase >= meta_est["Escala 2"] else 0
            # Rebate Ganado
            rebate_ganado = 0
            if total_month_purchase >= meta_est["Escala 2"]: rebate_ganado = total_month_purchase * meta_est["Rebate 2"]
            elif total_month_purchase >= meta_est["Escala 1"]: rebate_ganado = total_month_purchase * meta_est["Rebate 1"]
            
            summary_data.append({
                "Período": f"Mes {month} (Estacionalidad)", "Tipo de Rebate": "Estacionalidad",
                "Volumen Comprado": total_month_purchase,
                "Meta Escala 1": meta_est["Escala 1"], "Falta para Escala 1": falta_e1, "Rebate Proyectado (Escala 1)": rebate_e1_proj,
                "Meta Escala 2": meta_est["Escala 2"], "Falta para Escala 2": falta_e2, "Rebate Proyectado (Escala 2)": rebate_e2_proj,
                "Rebate Ganado": rebate_ganado
            })

    # Cálculos de Volumen y Profundidad por Período
    period_map = { "Julio-Agosto": [7, 8], "Agosto-Sept.": [8, 9], "3er Trimestre (3Q)": [7, 8, 9], "Octubre-Nov.": [10, 11], "Noviembre-Dic.": [11, 12], "4to Trimestre (4Q)": [10, 11, 12], "2do Semestre (2Sem)": list(range(7, 13)) }
    for period, months in period_map.items():
        period_df = df[df['Mes'].isin(months)]
        total_period_purchase = period_df['Valor_Neto'].sum()
        meta_vol = META_VOLUMEN.get(period, {"Escala 1": 0, "Rebate 1": 0, "Escala 2": 0, "Rebate 2": 0})
        
        # Rebate por Volumen
        falta_e1_vol = max(0, meta_vol["Escala 1"] - total_period_purchase)
        rebate_e1_proj_vol = (total_period_purchase + falta_e1_vol) * meta_vol["Rebate 1"] if falta_e1_vol == 0 else 0
        falta_e2_vol = max(0, meta_vol["Escala 2"] - total_period_purchase)
        rebate_e2_proj_vol = (total_period_purchase + falta_e2_vol) * meta_vol["Rebate 2"] if falta_e2_vol == 0 else 0
        rebate_ganado_vol = 0
        if total_period_purchase >= meta_vol["Escala 2"]: rebate_ganado_vol = total_period_purchase * meta_vol["Rebate 2"]
        elif total_period_purchase >= meta_vol["Escala 1"]: rebate_ganado_vol = total_period_purchase * meta_vol["Rebate 1"]
        
        summary_data.append({
            "Período": period, "Tipo de Rebate": "Volumen",
            "Volumen Comprado": total_period_purchase,
            "Meta Escala 1": meta_vol["Escala 1"], "Falta para Escala 1": falta_e1_vol, "Rebate Proyectado (Escala 1)": rebate_e1_proj_vol,
            "Meta Escala 2": meta_vol["Escala 2"], "Falta para Escala 2": falta_e2_vol, "Rebate Proyectado (Escala 2)": rebate_e2_proj_vol,
            "Rebate Ganado": rebate_ganado_vol
        })
        
        # Rebate por Profundidad (si aplica)
        if "Q" in period:
            rebate_profundidad = total_period_purchase * 0.01 if total_period_purchase > 0 else 0
            summary_data.append({
                "Período": period, "Tipo de Rebate": "Profundidad",
                "Volumen Comprado": total_period_purchase,
                "Meta Escala 1": 0, "Falta para Escala 1": 0, "Rebate Proyectado (Escala 1)": rebate_profundidad,
                "Meta Escala 2": 0, "Falta para Escala 2": 0, "Rebate Proyectado (Escala 2)": rebate_profundidad,
                "Rebate Ganado": rebate_profundidad
            })
    
    summary_df = pd.DataFrame(summary_data)
    return summary_df

# --- 6. APLICACIÓN PRINCIPAL (STREAMLIT UI) ---
st.title("🎯 Módulo de Seguimiento de Rebate: PINTUCO")
st.markdown("Herramienta para el análisis y seguimiento del acuerdo de desempeño comercial con **PINTUCO COLOMBIA S.A.S**.")
st.info("Este módulo es independiente. La primera sincronización puede tardar. Las siguientes serán mucho más rápidas.")

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

rebate_projections_df = calculate_rebate_projections(pintuco_df)
total_rebate_ganado = rebate_projections_df['Rebate Ganado'].sum()
total_comprado_s2_series = rebate_projections_df[(rebate_projections_df['Período'] == '2do Semestre (2Sem)') & (rebate_projections_df['Tipo de Rebate'] == 'Volumen')]['Volumen Comprado']
total_comprado_s2 = total_comprado_s2_series.iloc[0] if not total_comprado_s2_series.empty else 0
meta_semestral = META_VOLUMEN["2do Semestre (2Sem)"]["Escala 1"]
progreso_semestral = (total_comprado_s2 / meta_semestral) * 100 if meta_semestral > 0 else 0

st.divider()
st.header("📊 Resumen Ejecutivo del Rebate (2do Semestre)")
kpi1, kpi2, kpi3 = st.columns(3)
kpi1.metric("Rebate Total Acumulado", f"${int(total_rebate_ganado):,}")
kpi2.metric("Total Comprado (Neto)", f"${int(total_comprado_s2):,}")
kpi3.metric("Progreso Meta Semestral", f"{progreso_semestral:.1f}%", f"Meta: ${int(meta_semestral):,}")
st.progress(int(progreso_semestral) if progreso_semestral <= 100 else 100)
tab1, tab2 = st.tabs(["📈 Análisis y Proyecciones", "📑 Detalle de Documentos"])

with tab1:
    st.subheader("Análisis de Cumplimiento por Período y Proyecciones")
    st.dataframe(rebate_projections_df, use_container_width=True, hide_index=True,
        column_config={
            "Volumen Comprado": st.column_config.NumberColumn("Volumen Comprado", format="$ %d"),
            "Meta Escala 1": st.column_config.NumberColumn("Meta Escala 1", format="$ %d"),
            "Falta para Escala 1": st.column_config.NumberColumn("Falta para Escala 1", format="$ %d"),
            "Rebate Proyectado (Escala 1)": st.column_config.NumberColumn("Rebate Proy. (E1)", format="$ %d"),
            "Meta Escala 2": st.column_config.NumberColumn("Meta Escala 2", format="$ %d"),
            "Falta para Escala 2": st.column_config.NumberColumn("Falta para Escala 2", format="$ %d"),
            "Rebate Proyectado (Escala 2)": st.column_config.NumberColumn("Rebate Proy. (E2)", format="$ %d"),
            "Rebate Ganado": st.column_config.NumberColumn("Rebate Ganado", format="$ %d")
        })
    
    col1, col2 = st.columns([1,2])
    with col1:
        st.markdown("#### Proyección de Notas de Crédito")
        total_proyectado_e1 = rebate_projections_df['Rebate Proyectado (Escala 1)'].sum()
        total_proyectado_e2 = rebate_projections_df['Rebate Proyectado (Escala 2)'].sum()
        st.info(f"**Nota de Crédito Proyectada (Escala 1):** ${int(total_proyectado_e1):,}")
        st.success(f"**Nota de Crédito Proyectada (Escala 2):** ${int(total_proyectado_e2):,}")
        st.warning(f"**Rebate Total Acumulado hasta la fecha:** ${int(total_rebate_ganado):,}")
    with col2:
        # Gráfico de Barras de progreso de volumen
        df_bar = rebate_projections_df[rebate_projections_df['Tipo de Rebate'] == 'Volumen'].copy()
        df_bar['Meta_1'] = df_bar['Meta Escala 1']
        df_bar['Meta_2'] = df_bar['Meta Escala 2']
        df_bar['Volumen Comprado'] = df_bar['Volumen Comprado'].round(2)
        df_bar['Meta_1'] = df_bar['Meta_1'].round(2)
        df_bar['Meta_2'] = df_bar['Meta_2'].round(2)

        # Crear un DataFrame para la visualización de las metas
        df_chart = df_bar[['Período', 'Volumen Comprado', 'Meta_1', 'Meta_2']].melt(id_vars='Período', var_name='Tipo', value_name='Valor')
        # Definir los colores y el orden para la leyenda
        color_scale = alt.Scale(domain=['Meta_2', 'Meta_1', 'Volumen Comprado'], range=['#5A9C51', '#8BC34A', '#2D4E13'])
        
        chart = alt.Chart(df_chart).mark_bar().encode(
            x=alt.X('Período:N', title='Período', axis=alt.Axis(labels=True, title=None)),
            y=alt.Y('Valor:Q', title='Volumen (COP)'),
            color=alt.Color('Tipo:N', scale=color_scale, legend=alt.Legend(title="Leyenda")),
            tooltip=[
                alt.Tooltip('Período', title='Período'),
                alt.Tooltip('Tipo', title='Tipo de Dato'),
                alt.Tooltip('Valor', title='Valor', format='$,.0f')
            ]
        ).properties(
            title='Progreso de Volumen por Período vs Metas'
        ).interactive()
        
        st.altair_chart(chart, use_container_width=True)

with tab2:
    st.subheader("Historial Completo de Documentos de Pintuco")
    st.dataframe(pintuco_df.sort_values(by="Fecha_Factura", ascending=False), use_container_width=True, hide_index=True,
        column_config={
            "Fecha_Factura": st.column_config.DateColumn("Fecha", format="YYYY-MM-DD"),
            "Valor_Neto": st.column_config.NumberColumn("Valor Neto (Antes de IVA)", format="$ %d"),
            "Estado_Pago": st.column_config.TextColumn("Estado", help="Indica si la factura está pendiente de pago en Dropbox.")
        })
    
    excel_data = generate_excel_download(rebate_projections_df, pintuco_df)
    st.download_button(
        label="📥 Descargar Análisis en Excel",
        data=excel_data,
        file_name="Analisis_Rebate_Pintuco.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
