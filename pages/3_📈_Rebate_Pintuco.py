# -*- coding: utf-8 -*-
"""
Módulo de Seguimiento de Rebate para PINTUCO COLOMBIA SAS (Versión 1.0).

Este módulo es una herramienta de análisis gerencial diseñada para:
1.  **Sincronizar de forma independiente** todas las facturas del proveedor PINTUCO COLOMBIA SAS
    desde el correo electrónico, a partir de una fecha específica.
2.  **Cruza la información de facturación** con el reporte de cartera vigente (Dropbox)
    para determinar el estado de pago de cada factura (Pendiente o Pagada).
3.  **Almacena un historial completo** y persistente en una hoja de cálculo dedicada en Google Sheets.
4.  **Calcula y visualiza en tiempo real** el progreso y el cumplimiento de las metas del
    acuerdo de rebate, incluyendo los pilares de Volumen, Estacionalidad y Profundidad de Portafolio.
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
from datetime import datetime, date
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
st.set_page_config(
    layout="wide",
    page_title="Seguimiento Rebate | Pintuco",
    page_icon="🎯"
)

# --- Constantes Globales ---
PINTUCO_PROVIDER_NAME = "PINTUCO COLOMBIA S.A.S"
COLOMBIA_TZ = pytz.timezone('America/Bogota')
START_DATE_SYNC = date(2025, 7, 1)

# --- Constantes de Conexión ---
IMAP_SERVER = "imap.gmail.com"
EMAIL_FOLDER = "TFHKA/Recepcion/Descargados"
DROPBOX_FILE_PATH = "/data/Proveedores.csv"
GSHEET_REBATE_NAME = "Seguimiento Rebate Pintuco 2025"
GSHEET_REBATE_WORKSHEET = "Facturacion"

# --- Constantes del Acuerdo de Rebate (Extraídas del PDF) ---
# Metas de Compras (Volumen)
META_VOLUMEN = {
    "Julio": {"Escala 1": 1777446334, "Rebate 1": 0.005, "Escala 2": 1901867577, "Rebate 2": 0.01},
    "Julio-Agosto": {"Escala 1": 3662382180, "Rebate 1": 0.005, "Escala 2": 3918748933, "Rebate 2": 0.01},
    "Agosto-Sept.": {"Escala 1": 3876058532, "Rebate 1": 0.005, "Escala 2": 4147382629, "Rebate 2": 0.01},
    "3er Trimestre (3Q)": {"Escala 1": 5653504866, "Rebate 1": 0.01, "Escala 2": 6049250207, "Rebate 2": 0.02},
    "Octubre": {"Escala 1": 2148246123, "Rebate 1": 0.005, "Escala 2": 2298623352, "Rebate 2": 0.01},
    "Octubre-Nov.": {"Escala 1": 4272097392, "Rebate 1": 0.005, "Escala 2": 4571144209, "Rebate 2": 0.01},
    "Noviembre-Dic.": {"Escala 1": 3970984742, "Rebate 1": 0.005, "Escala 2": 4248953674, "Rebate 2": 0.01},
    "4to Trimestre (4Q)": {"Escala 1": 6119230865, "Rebate 1": 0.01, "Escala 2": 6547577026, "Rebate 2": 0.02},
    "2do Semestre (2Sem)": {"Escala 1": 11772735731, "Rebate 1": 0.0075, "Escala 2": 12596827232, "Rebate 2": 0.015},
}

# Metas de Estacionalidad
META_ESTACIONALIDAD = {
    7: {"Escala 1": 1777446334, "Rebate 1": 0.007, "Escala 2": 1901867577, "Rebate 2": 0.01},
    8: {"Escala 1": 1884935846, "Rebate 1": 0.007, "Escala 2": 2016881355, "Rebate 2": 0.01},
    9: {"Escala 1": 1991122686, "Rebate 1": 0.007, "Escala 2": 2130501274, "Rebate 2": 0.01},
    10: {"Escala 1": 2148246123, "Rebate 1": 0.007, "Escala 2": 2298623352, "Rebate 2": 0.01},
    11: {"Escala 1": 2123851269, "Rebate 1": 0.007, "Escala 2": 2272520858, "Rebate 2": 0.01},
    12: {"Escala 1": 1847133473, "Rebate 1": 0.007, "Escala 2": 1976432816, "Rebate 2": 0.01},
}

# --- 2. FUNCIONES DE CONEXIÓN Y UTILIDADES ---

# (Se adaptan funciones del código principal para mantener la consistencia)

@st.cache_resource(show_spinner="Conectando a Google Sheets...")
def connect_to_google_sheets():
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(st.secrets["google_credentials"], scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ Error crítico al autenticar con Google: {e}")
        return None

def get_or_create_worksheet(client: gspread.Client, sheet_name: str, worksheet_name: str):
    try:
        spreadsheet = client.open(sheet_name)
    except gspread.SpreadsheetNotFound:
        st.warning(f"Creando nueva hoja de cálculo '{sheet_name}'...")
        spreadsheet = client.create(sheet_name)
        spreadsheet.share(st.secrets.google_credentials['client_email'], perm_type='user', role='writer')
    
    try:
        return spreadsheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        st.warning(f"Creando nueva pestaña '{worksheet_name}'...")
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
        st.error(f"❌ Error al actualizar la hoja '{worksheet.title}': {e}")
        return False

def normalize_invoice_number(inv_num: any) -> str:
    if not isinstance(inv_num, str):
        inv_num = str(inv_num)
    return re.sub(r'[^A-Z0-9]', '', inv_num.upper()).strip()

# --- 3. FUNCIONES DE EXTRACCIÓN DE DATOS ---

@st.cache_data(ttl=600, show_spinner="Descargando cartera vigente de Dropbox...")
def load_pending_invoices_from_dropbox() -> set:
    """ Carga la cartera de Pintuco desde Dropbox y devuelve un set de N° de factura pendientes."""
    try:
        dbx = dropbox.Dropbox(
            oauth2_refresh_token=st.secrets.dropbox["refresh_token"],
            app_key=st.secrets.dropbox["app_key"],
            app_secret=st.secrets.dropbox["app_secret"]
        )
        _, response = dbx.files_download(DROPBOX_FILE_PATH)
        
        df = pd.read_csv(io.StringIO(response.content.decode('latin1')),
                         sep='{', header=None, engine='python',
                         names=['nombre_proveedor_erp', 'serie', 'num_entrada', 'num_factura', 
                                'doc_erp', 'fecha_emision_erp', 'fecha_vencimiento_erp', 'valor_total_erp'])

        pintuco_df = df[df['nombre_proveedor_erp'] == PINTUCO_PROVIDER_NAME].copy()
        pintuco_df.dropna(subset=['num_factura'], inplace=True)
        pending_invoices = set(pintuco_df['num_factura'].apply(normalize_invoice_number))
        
        st.info(f"Encontradas {len(pending_invoices)} facturas pendientes de Pintuco en Dropbox.")
        return pending_invoices
    except Exception as e:
        st.error(f"❌ Error cargando cartera de Dropbox: {e}")
        return set()

def parse_invoice_xml(xml_content: str) -> dict or None:
    """ Parsea el contenido de un XML de factura para extraer los datos clave. """
    try:
        xml_content = re.sub(r'^[^\<]+', '', xml_content.strip())
        root = ET.fromstring(xml_content.encode('utf-8'))
        ns = {
            'cbc': "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2",
            'cac': "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2",
        }

        supplier_name_node = root.find('.//cac:AccountingSupplierParty/cac:Party/cac:PartyLegalEntity/cbc:RegistrationName', ns)
        if supplier_name_node is None:
            return None # Si no hay nombre de proveedor, no podemos procesar

        supplier_name = supplier_name_node.text.strip()
        
        # Filtro principal: solo procesar si es de Pintuco
        if PINTUCO_PROVIDER_NAME not in supplier_name:
            return None

        invoice_number = root.find('./cbc:ID', ns).text.strip()
        issue_date = root.find('./cbc:IssueDate', ns).text.strip()
        # El valor para el rebate es el valor ANTES de impuestos (TaxExclusiveAmount)
        net_value = root.find('.//cac:TaxTotal/../cac:LegalMonetaryTotal/cbc:TaxExclusiveAmount', ns).text.strip()
        
        return {
            "Fecha_Factura": issue_date,
            "Numero_Factura": normalize_invoice_number(invoice_number),
            "Valor_Neto": float(net_value),
            "Proveedor_Correo": supplier_name
        }
    except:
        return None

@st.cache_data(ttl=600, show_spinner="Buscando nuevas facturas de Pintuco en el correo...")
def fetch_pintuco_invoices_from_email(start_date: date) -> pd.DataFrame:
    """ Busca, descarga y extrae datos de facturas de Pintuco desde Gmail. """
    invoices_data = []
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(st.secrets.email["address"], st.secrets.email["password"])
        mail.select(f'"{EMAIL_FOLDER}"')
        
        search_query = f'(SINCE "{start_date.strftime("%d-%b-%Y")}")'
        _, messages = mail.search(None, search_query)
        message_ids = messages[0].split()

        if not message_ids:
            st.warning("No se encontraron correos nuevos en el período de búsqueda.")
            mail.logout()
            return pd.DataFrame()

        progress_text = f"Procesando {len(message_ids)} correos encontrados..."
        progress_bar = st.progress(0, text=progress_text)
        
        for i, num in enumerate(message_ids):
            _, data = mail.fetch(num, "(RFC822)")
            msg = email.message_from_bytes(data[0][1])

            for part in msg.walk():
                if part.get_content_maintype() == "multipart" or part.get("Content-Disposition") is None:
                    continue
                
                filename = part.get_filename()
                if filename and filename.lower().endswith('.zip'):
                    try:
                        with zipfile.ZipFile(io.BytesIO(part.get_payload(decode=True))) as zf:
                            for name in zf.namelist():
                                if name.lower().endswith('.xml'):
                                    xml_content = zf.read(name).decode('utf-8', 'ignore')
                                    details = parse_invoice_xml(xml_content)
                                    if details:
                                        invoices_data.append(details)
                    except:
                        continue
            progress_bar.progress((i + 1) / len(message_ids), text=f"{progress_text} ({i+1}/{len(message_ids)})")
        
        mail.logout()
        return pd.DataFrame(invoices_data)
    except Exception as e:
        st.error(f"❌ Error procesando correos: {e}")
        return pd.DataFrame()

# --- 4. LÓGICA PRINCIPAL DE SINCRONIZACIÓN ---

def run_pintuco_sync():
    """ Orquesta el proceso completo de sincronización de datos de Pintuco. """
    with st.spinner('Iniciando sincronización de Pintuco...'):
        st.info("Paso 1/4: Descargando cartera pendiente de Dropbox...")
        pending_invoices_set = load_pending_invoices_from_dropbox()

        st.info(f"Paso 2/4: Buscando facturas en el correo desde {START_DATE_SYNC.strftime('%Y-%m-%d')}...")
        new_invoices_df = fetch_pintuco_invoices_from_email(START_DATE_SYNC)
        
        if new_invoices_df.empty:
            st.warning("No se encontraron nuevas facturas de Pintuco en el correo para procesar.")
            st.session_state['last_pintuco_sync'] = datetime.now(COLOMBIA_TZ).strftime('%Y-%m-%d %H:%M:%S')
            return

        st.info("Paso 3/4: Conectando a Google Sheets para consolidar datos...")
        gs_client = connect_to_google_sheets()
        if not gs_client:
            st.error("Sincronización cancelada. No se pudo conectar a Google.")
            st.stop()
        
        worksheet = get_or_create_worksheet(gs_client, GSHEET_REBATE_NAME, GSHEET_REBATE_WORKSHEET)
        
        try:
            existing_records = worksheet.get_all_records()
            historical_df = pd.DataFrame(existing_records)
        except:
            historical_df = pd.DataFrame()

        st.info("Paso 4/4: Consolidando, actualizando estado de pago y guardando...")
        combined_df = pd.concat([historical_df, new_invoices_df]).drop_duplicates(subset=['Numero_Factura'], keep='last')
        
        # Lógica de cruce para definir estado de pago
        combined_df['Estado_Pago'] = combined_df['Numero_Factura'].apply(
            lambda x: 'Pendiente' if x in pending_invoices_set else 'Pagada'
        )

        # Limpieza de tipos de datos antes de guardar
        combined_df['Fecha_Factura'] = pd.to_datetime(combined_df['Fecha_Factura'])
        combined_df['Valor_Neto'] = pd.to_numeric(combined_df['Valor_Neto'])

        if update_gsheet_from_df(worksheet, combined_df):
            st.success("✅ ¡Base de datos de Pintuco actualizada exitosamente en Google Sheets!")
        else:
            st.error("Falló la actualización en Google Sheets.")

        st.session_state['last_pintuco_sync'] = datetime.now(COLOMBIA_TZ).strftime('%Y-%m-%d %H:%M:%S')
        st.balloons()

# --- 5. LÓGICA DE CÁLCULO Y VISUALIZACIÓN DEL REBATE ---

@st.cache_data(ttl=300)
def load_pintuco_data_from_gsheet() -> pd.DataFrame:
    """ Carga los datos de facturación de Pintuco desde la hoja de cálculo dedicada. """
    try:
        gs_client = connect_to_google_sheets()
        worksheet = get_or_create_worksheet(gs_client, GSHEET_REBATE_NAME, GSHEET_REBATE_WORKSHEET)
        df = pd.DataFrame(worksheet.get_all_records())
        if df.empty:
            return pd.DataFrame()
        
        df['Fecha_Factura'] = pd.to_datetime(df['Fecha_Factura'])
        df['Valor_Neto'] = pd.to_numeric(df['Valor_Neto'])
        return df
    except Exception as e:
        return pd.DataFrame()

def calculate_rebate_summary(df: pd.DataFrame) -> pd.DataFrame:
    """ Calcula el desglose del rebate basado en el DataFrame de facturación. """
    if df.empty:
        return pd.DataFrame()
    
    df['Mes'] = df['Fecha_Factura'].dt.month
    df['Trimestre'] = df['Fecha_Factura'].dt.quarter
    
    summary_data = []

    # Pilar 1: Volumen y Pilar 2: Estacionalidad
    for month in range(7, 13):
        # Volumen Mensual
        monthly_df = df[df['Mes'] == month]
        total_month_purchase = monthly_df['Valor_Neto'].sum()

        # Estacionalidad
        purchase_before_20th = monthly_df[monthly_df['Fecha_Factura'].dt.day <= 20]['Valor_Neto'].sum()
        rebate_estacionalidad = 0
        if total_month_purchase > 0 and (purchase_before_20th / total_month_purchase) >= 0.9:
            meta_est = META_ESTACIONALIDAD.get(month, {})
            if total_month_purchase >= meta_est.get("Escala 2", float('inf')):
                rebate_estacionalidad = total_month_purchase * meta_est.get("Rebate 2", 0)
            elif total_month_purchase >= meta_est.get("Escala 1", float('inf')):
                rebate_estacionalidad = total_month_purchase * meta_est.get("Rebate 1", 0)

        summary_data.append({
            "Período": f"Mes {month}", "Meta": 0, "Compra Real": total_month_purchase,
            "Rebate Volumen": 0, "Rebate Estacionalidad": rebate_estacionalidad, "Rebate Profundidad": 0
        })

    # Ciclos Móviles de Volumen y Trimestres
    period_map = {
        "Julio-Agosto": [7, 8], "Agosto-Sept.": [8, 9], "3er Trimestre (3Q)": [7, 8, 9],
        "Octubre-Nov.": [10, 11], "Noviembre-Dic.": [11, 12], "4to Trimestre (4Q)": [10, 11, 12],
        "2do Semestre (2Sem)": [7, 8, 9, 10, 11, 12]
    }
    
    for period, months in period_map.items():
        period_df = df[df['Mes'].isin(months)]
        total_period_purchase = period_df['Valor_Neto'].sum()
        
        meta_vol = META_VOLUMEN.get(period, {})
        rebate_volumen = 0
        if total_period_purchase >= meta_vol.get("Escala 2", float('inf')):
            rebate_volumen = total_period_purchase * meta_vol.get("Rebate 2", 0)
        elif total_period_purchase >= meta_vol.get("Escala 1", float('inf')):
            rebate_volumen = total_period_purchase * meta_vol.get("Rebate 1", 0)
        
        # Pilar 3: Profundidad (1% trimestral)
        rebate_profundidad = 0
        if "Q" in period:
            rebate_profundidad = total_period_purchase * 0.01

        summary_data.append({
            "Período": period, "Meta": meta_vol.get("Escala 1", 0), "Compra Real": total_period_purchase,
            "Rebate Volumen": rebate_volumen, "Rebate Estacionalidad": 0, "Rebate Profundidad": rebate_profundidad
        })
    
    summary_df = pd.DataFrame(summary_data)
    summary_df['Rebate Total Ganado'] = summary_df['Rebate Volumen'] + summary_df['Rebate Estacionalidad'] + summary_df['Rebate Profundidad']
    return summary_df


# --- 6. APLICACIÓN PRINCIPAL (STREAMLIT UI) ---

st.title("🎯 Módulo de Seguimiento de Rebate: PINTUCO")
st.markdown("Herramienta para el análisis y seguimiento del acuerdo de desempeño comercial con **PINTUCO COLOMBIA S.A.S**.")
st.info("Este módulo es independiente. Presiona el botón para sincronizar las facturas de Pintuco desde el correo y cruzarlas con la cartera vigente de Dropbox.")

if st.button("🔄 Sincronizar Facturas de Pintuco", type="primary"):
    run_pintuco_sync()
    # Forzar la limpieza del cache de datos para recargar la información actualizada
    st.cache_data.clear()
    st.rerun()

if 'last_pintuco_sync' in st.session_state:
    st.success(f"Última sincronización de Pintuco: {st.session_state.last_pintuco_sync}")

# --- Cargar y mostrar datos ---
pintuco_df = load_pintuco_data_from_gsheet()

if pintuco_df.empty:
    st.warning("No hay datos de Pintuco para analizar. Realiza la primera sincronización.")
    st.stop()

# --- Cálculos y Visualización ---
rebate_summary_df = calculate_rebate_summary(pintuco_df)
total_comprado_s2 = rebate_summary_df[rebate_summary_df['Período'] == '2do Semestre (2Sem)']['Compra Real'].sum()
total_rebate_ganado = rebate_summary_df['Rebate Total Ganado'].sum()

st.divider()
st.header("📊 Resumen Ejecutivo del Rebate (2do Semestre)")

kpi1, kpi2, kpi3 = st.columns(3)
kpi1.metric(
    "Rebate Total Potencial Ganado",
    f"${int(total_rebate_ganado):,}",
    help="Suma de todos los rebates calculados (Volumen, Estacionalidad y Profundidad)."
)
kpi2.metric(
    "Total Comprado (Neto)",
    f"${int(total_comprado_s2):,}",
    help="Suma neta de todas las facturas de Pintuco desde el 1 de Julio de 2025."
)

meta_semestral = META_VOLUMEN["2do Semestre (2Sem)"]["Escala 1"]
progreso_semestral = (total_comprado_s2 / meta_semestral) * 100 if meta_semestral > 0 else 0
kpi3.metric(
    "Progreso Meta Semestral",
    f"{progreso_semestral:.1f}%",
    f"Meta: ${int(meta_semestral):,}"
)
st.progress(int(progreso_semestral) if progreso_semestral <= 100 else 100)

tab1, tab2 = st.tabs(["📈 Desglose del Rebate", "📑 Detalle de Facturas"])

with tab1:
    st.subheader("Análisis de Cumplimiento por Período")
    st.dataframe(
        rebate_summary_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Meta": st.column_config.NumberColumn("Meta (Escala 1)", format="$ %d"),
            "Compra Real": st.column_config.NumberColumn(format="$ %d"),
            "Rebate Volumen": st.column_config.NumberColumn(format="$ %d"),
            "Rebate Estacionalidad": st.column_config.NumberColumn(format="$ %d"),
            "Rebate Profundidad": st.column_config.NumberColumn(format="$ %d"),
            "Rebate Total Ganado": st.column_config.NumberColumn(format="$ %d"),
        }
    )

with tab2:
    st.subheader("Historial Completo de Facturas de Pintuco")
    st.dataframe(
        pintuco_df.sort_values(by="Fecha_Factura", ascending=False),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Fecha_Factura": st.column_config.DateColumn("Fecha", format="YYYY-MM-DD"),
            "Valor_Neto": st.column_config.NumberColumn("Valor Neto (Antes de IVA)", format="$ %d"),
        }
    )
