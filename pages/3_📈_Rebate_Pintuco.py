# -*- coding: utf-8 -*-
"""
Módulo de Seguimiento de Rebate para PINTUCO COLOMBIA SAS (Versión 1.8 - Mejorada).

Este módulo es una herramienta de análisis gerencial diseñada para:
1.  Sincronizar de forma independiente e inteligente todas las facturas de PINTUCO,
    reconociendo múltiples nombres de proveedor (alias).
2.  Analizar la estructura compleja de XML anidados para una extracción de datos precisa.
3.  Cargar y procesar correctamente las Notas de Crédito desde Dropbox.
4.  Almacenar un historial completo en una pestaña dedicada dentro del libro principal.
5.  Calcular y visualizar en tiempo real el progreso del acuerdo de rebate.

Mejoras en v1.8:
- **Flujo de Sincronización Mejorado:** Se garantiza la correcta consolidación de datos
  históricos y nuevos, eliminando redundancias y asegurando que los datos se escriban.
- **Manejo de Errores Más Robusto:** La lógica de manejo de errores ha sido refinada
  para ofrecer mensajes más claros al usuario.
- **Eficiencia en la Sincronización:** Se optimiza la actualización de la hoja de Google
  Sheets para asegurar que todos los datos, incluso si no hay facturas nuevas,
  se procesen y se actualice el estado de pago.
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
    cleaned_str = str(value).strip().replace('$', '').replace(',', '')  # Corregido para quitar la coma
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
        return df
    except Exception as e:
        st.error(f"Error al cargar datos desde Google Sheets: {e}"); return pd.DataFrame()

def calculate_rebate_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return pd.DataFrame()
    df = df.copy(); df['Mes'] = df['Fecha_Factura'].dt.month
    summary_data = []
    
    # Cálculos de Estacionalidad por Mes
    for month in range(7, 13):
        monthly_df = df[df['Mes'] == month]
        total_month_purchase = monthly_df['Valor_Neto'].sum()
        rebate_estacionalidad = 0
        if total_month_purchase > 0:
            purchase_before_20th = monthly_df[monthly_df['Fecha_Factura'].dt.day <= 20]['Valor_Neto'].sum()
            if (purchase_before_20th / total_month_purchase) >= 0.9:
                meta_est = META_ESTACIONALIDAD.get(month, {})
                if total_month_purchase >= meta_est.get("Escala 2", float('inf')): rebate_estacionalidad = total_month_purchase * meta_est.get("Rebate 2", 0)
                elif total_month_purchase >= meta_est.get("Escala 1", float('inf')): rebate_estacionalidad = total_month_purchase * meta_est.get("Rebate 1", 0)
        summary_data.append({"Período": f"Mes {month} (Estacionalidad)", "Meta": 0, "Compra Real": total_month_purchase, "Rebate Calculado": rebate_estacionalidad, "Tipo": "Estacionalidad"})
    
    # Cálculos de Volumen por Período
    period_map = { "Julio-Agosto": [7, 8], "Agosto-Sept.": [8, 9], "3er Trimestre (3Q)": [7, 8, 9], "Octubre-Nov.": [10, 11], "Noviembre-Dic.": [11, 12], "4to Trimestre (4Q)": [10, 11, 12], "2do Semestre (2Sem)": list(range(7, 13)) }
    for period, months in period_map.items():
        period_df = df[df['Mes'].isin(months)]
        total_period_purchase = period_df['Valor_Neto'].sum()
        meta_vol = META_VOLUMEN.get(period, {})
        rebate_volumen = 0
        if total_period_purchase >= meta_vol.get("Escala 2", float('inf')): rebate_volumen = total_period_purchase * meta_vol.get("Rebate 2", 0)
        elif total_period_purchase >= meta_vol.get("Escala 1", float('inf')): rebate_volumen = total_period_purchase * meta_vol.get("Rebate 1", 0)
        summary_data.append({"Período": period, "Meta": meta_vol.get("Escala 1", 0), "Compra Real": total_period_purchase, "Rebate Calculado": rebate_volumen, "Tipo": "Volumen"})
        if "Q" in period:
            rebate_profundidad = total_period_purchase * 0.01
            summary_data.append({"Período": period, "Meta": 0, "Compra Real": total_period_purchase, "Rebate Calculado": rebate_profundidad, "Tipo": "Profundidad"})
    
    return pd.DataFrame(summary_data)

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

rebate_summary_df = calculate_rebate_summary(pintuco_df)
total_rebate_ganado = rebate_summary_df['Rebate Calculado'].sum()
total_comprado_s2_series = rebate_summary_df[rebate_summary_df['Período'] == '2do Semestre (2Sem)']['Compra Real']
total_comprado_s2 = total_comprado_s2_series.iloc[0] if not total_comprado_s2_series.empty else 0

st.divider()
st.header("📊 Resumen Ejecutivo del Rebate (2do Semestre)")
kpi1, kpi2, kpi3 = st.columns(3)
kpi1.metric("Rebate Total Potencial Ganado", f"${int(total_rebate_ganado):,}")
kpi2.metric("Total Comprado (Neto)", f"${int(total_comprado_s2):,}")
meta_semestral = META_VOLUMEN["2do Semestre (2Sem)"]["Escala 1"]
progreso_semestral = (total_comprado_s2 / meta_semestral) * 100 if meta_semestral > 0 else 0
kpi3.metric("Progreso Meta Semestral", f"{progreso_semestral:.1f}%", f"Meta: ${int(meta_semestral):,}")
st.progress(int(progreso_semestral) if progreso_semestral <= 100 else 100)
tab1, tab2 = st.tabs(["📈 Desglose del Rebate", "📑 Detalle de Documentos"])
with tab1:
    st.subheader("Análisis de Cumplimiento por Período")
    st.dataframe(rebate_summary_df, use_container_width=True, hide_index=True,
        column_config={ "Meta": st.column_config.NumberColumn("Meta (Escala 1)", format="$ %d"), "Compra Real": st.column_config.NumberColumn(format="$ %d"), "Rebate Calculado": st.column_config.NumberColumn(format="$ %d"), })
with tab2:
    st.subheader("Historial Completo de Documentos de Pintuco")
    st.dataframe(pintuco_df.sort_values(by="Fecha_Factura", ascending=False), use_container_width=True, hide_index=True,
        column_config={ "Fecha_Factura": st.column_config.DateColumn("Fecha", format="YYYY-MM-DD"), "Valor_Neto": st.column_config.NumberColumn("Valor Neto (Antes de IVA)", format="$ %d"), })
