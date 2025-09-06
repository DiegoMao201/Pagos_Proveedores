# -*- coding: utf-8 -*-
"""
Plataforma de Gestión Inteligente de Facturas para FERREINOX.

Aplicación web construida con Streamlit para automatizar la conciliación de
facturas de proveedores recibidas por correo electrónico contra los registros
del sistema ERP (extraídos de Dropbox).

Funcionalidades principales:
- Autenticación segura por contraseña.
- Sincronización automática de facturas desde una cuenta de Gmail.
- Carga de datos de cuentas por pagar desde un archivo CSV en Dropbox.
- Proceso de conciliación inteligente para cruzar datos del ERP y correos.
- Dashboard interactivo con métricas, alertas de vencimiento y filtros.
- Visualización de datos y reportes por proveedor.
- Actualización de una base de datos y un reporte consolidado en Google Sheets.
"""

# ======================================================================================
# --- 0. IMPORTACIÓN DE LIBRERÍAS ---
# ======================================================================================
# Librerías estándar de Python
import email
import imaplib
import io
import re
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

# Librerías de terceros (instaladas)
import altair as alt
import dropbox
import gspread
import numpy as np
import pandas as pd
import pytz
import streamlit as st
from google.oauth2.service_account import Credentials
from gspread import Client, Worksheet

# ======================================================================================
# --- 1. CONFIGURACIÓN INICIAL Y CONSTANTES GLOBALES ---
# ======================================================================================

# --- Configuración de la página de Streamlit ---
# Debe ser el primer comando de Streamlit en ejecutarse.
st.set_page_config(
    page_title="Gestión Inteligente de Facturas | FERREINOX",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Constantes Globales ---
# Facilita la gestión y modificación de valores fijos.
COLOMBIA_TZ = pytz.timezone('America/Bogota')

# Credenciales y rutas de servicios externos
IMAP_SERVER = "imap.gmail.com"
EMAIL_FOLDER = "TFHKA/Recepcion/Descargados"
DROPBOX_FILE_PATH = "/data/Proveedores.csv"
GSHEET_DB_NAME = "FacturasCorreo_DB"
GSHEET_REPORT_NAME = "ReporteConsolidado_Activo"

# Parámetros de la aplicación
SEARCH_DAYS_AGO = 30  # Días hacia atrás para buscar correos si no hay historial previo.
APP_PASSWORD = st.secrets.get("password", "DEFAULT_PASSWORD") # Obtiene la contraseña de los secrets

# Nombres de columnas para evitar errores de tipeo (magic strings)
COL_NUM_FACTURA = 'num_factura'
COL_PROVEEDOR_ERP = 'nombre_proveedor_erp'
COL_VALOR_ERP = 'valor_total_erp'
COL_FECHA_EMISION_ERP = 'fecha_emision_erp'
COL_FECHA_VENCIMIENTO_ERP = 'fecha_vencimiento_erp'
COL_PROVEEDOR_CORREO = 'nombre_proveedor_correo'
COL_VALOR_CORREO = 'valor_total_correo'
COL_FECHA_EMISION_CORREO = 'fecha_emision_correo'
COL_FECHA_VENCIMIENTO_CORREO = 'fecha_vencimiento_correo'

# ======================================================================================
# --- 2. ESTADO DE SESIÓN Y ESTILOS CSS ---
# ======================================================================================

def initialize_session_state():
    """Inicializa las variables en el estado de sesión si no existen."""
    defaults = {
        "password_correct": False,
        "data_loaded": False,
        "erp_df": pd.DataFrame(),
        "email_df": pd.DataFrame(),
        "master_df": pd.DataFrame(),
        "filtered_df": pd.DataFrame(),
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

def load_css():
    """ Carga estilos CSS personalizados para mejorar la apariencia del dashboard. """
    st.markdown("""
        <style>
            .main .block-container { padding-top: 2rem; }
            .stMetric {
                background-color: #FFFFFF;
                border: 1px solid #E0E0E0;
                border-radius: 12px;
                padding: 20px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.05);
            }
            .stMetric [data-testid="stMetricLabel"] { /* Selector más robusto para la etiqueta */
                font-size: 1rem;
                color: #4F4F4F;
                font-weight: 600;
            }
            .stMetric [data-testid="stMetricValue"] { /* Selector más robusto para el valor */
                font-size: 2.2rem;
                font-weight: 700;
            }
            .stButton>button { width: 100%; border-radius: 8px; }
            .st-expander { border-radius: 12px !important; border: 1px solid #E0E0E0 !important; }
        </style>
    """, unsafe_allow_html=True)

# ======================================================================================
# --- 3. LÓGICA DE AUTENTICACIÓN Y SEGURIDAD ---
# ======================================================================================

def check_password() -> bool:
    """
    Muestra un formulario de contraseña y verifica el acceso.
    Retorna True si la contraseña es correcta, de lo contrario False.
    """
    if st.session_state.get("password_correct", False):
        return True

    st.header("🔒 Acceso Restringido")
    st.write("Por favor, ingresa la contraseña para acceder al panel de gestión.")
    
    with st.form("password_form"):
        password = st.text_input("Contraseña:", type="password", key="password_input")
        submitted = st.form_submit_button("Ingresar")

        if submitted:
            if password == APP_PASSWORD:
                st.session_state.password_correct = True
                st.rerun() # Recarga la app para mostrar el contenido principal
            else:
                st.error("Contraseña incorrecta. Inténtalo de nuevo.")
                
    return False

# ======================================================================================
# --- 4. CONEXIONES A SERVICIOS EXTERNOS (GOOGLE, DROPBOX, EMAIL) ---
# ======================================================================================

@st.cache_resource(show_spinner="Conectando a Google Sheets...")
def connect_to_google_sheets() -> Optional[Client]:
    """
    Establece conexión con la API de Google Sheets usando las credenciales de Streamlit.
    Retorna un objeto cliente de gspread o None si falla la conexión.
    """
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(st.secrets["google_credentials"], scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ Error crítico al autenticar con Google Sheets: {e}")
        return None

def get_or_create_worksheet(client: Client, sheet_key: str, worksheet_name: str) -> Optional[Worksheet]:
    """
    Obtiene una hoja de cálculo por su nombre, o la crea si no existe.
    Retorna un objeto Worksheet o None si ocurre un error.
    """
    try:
        spreadsheet = client.open_by_key(sheet_key)
        try:
            return spreadsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            st.warning(f"Hoja '{worksheet_name}' no encontrada. Creando una nueva...")
            return spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols="30")
    except gspread.exceptions.APIError as e:
        st.error(f"Error de API de Google al acceder a la hoja de cálculo: {e}")
    except Exception as e:
        st.error(f"Error inesperado accediendo a la hoja de cálculo '{worksheet_name}': {e}")
    return None

def update_gsheet_from_df(worksheet: Worksheet, df: pd.DataFrame) -> bool:
    """
    Actualiza una hoja de Google Sheets con los datos de un DataFrame de forma segura.
    Retorna True si la actualización fue exitosa, de lo contrario False.
    """
    if not isinstance(worksheet, Worksheet):
        st.error("Se intentó actualizar una hoja de cálculo inválida.")
        return False
    try:
        worksheet.clear()
        # Prepara el DataFrame para la subida a Google Sheets
        df_to_upload = df.copy()
        for col in df_to_upload.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]', 'datetime64[ns, America/Bogota]']).columns:
            df_to_upload[col] = df_to_upload[col].dt.strftime('%Y-%m-%d')
        
        # Convierte todo a string para evitar problemas de formato con gspread
        df_to_upload = df_to_upload.astype(str).replace({'nan': '', 'NaT': '', 'None': ''})
        
        # Actualiza la hoja con los encabezados y los datos
        worksheet.update([df_to_upload.columns.values.tolist()] + df_to_upload.values.tolist())
        return True
    except Exception as e:
        st.error(f"❌ Error al actualizar la hoja '{worksheet.title}': {e}")
        return False

# ======================================================================================
# --- 5. LECTURA Y PROCESAMIENTO DE DATOS (ERP & CORREO) ---
# ======================================================================================
# **MEJORA**: Se elimina la función `robust_date_parser` que causaba el error.
# La conversión de fechas se hará de forma vectorizada y más eficiente directamente
# en las funciones de carga y procesamiento de datos.

def clean_and_convert_numeric(value: Any) -> float:
    """
    Limpia una cadena de texto que representa un número (formato colombiano) y la
    convierte a float. Maneja valores nulos o no-string de forma segura.
    """
    if pd.isna(value) or not isinstance(value, str):
        return 0.0
    try:
        # Elimina símbolos de moneda, espacios y puntos de miles
        cleaned_str = re.sub(r'[$\s\.]', '', value)
        # Reemplaza la coma decimal por un punto
        cleaned_str = cleaned_str.replace(',', '.')
        return float(cleaned_str)
    except (ValueError, TypeError):
        return 0.0

@st.cache_data(show_spinner="Descargando datos del ERP (Dropbox)...", ttl=900)
def load_erp_data() -> pd.DataFrame:
    """
    Carga los datos de facturas desde un archivo CSV en Dropbox.
    Realiza la limpieza y conversión de tipos de datos de forma vectorizada.
    """
    try:
        dbx = dropbox.Dropbox(
            oauth2_refresh_token=st.secrets.dropbox["refresh_token"],
            app_key=st.secrets.dropbox["app_key"],
            app_secret=st.secrets.dropbox["app_secret"]
        )
        _, response = dbx.files_download(DROPBOX_FILE_PATH)
        
        column_names = [
            COL_PROVEEDOR_ERP, 'serie', 'num_entrada', COL_NUM_FACTURA, 
            'doc_erp', COL_FECHA_EMISION_ERP, COL_FECHA_VENCIMIENTO_ERP, COL_VALOR_ERP
        ]
        
        # Lee el CSV en memoria
        df = pd.read_csv(io.StringIO(response.content.decode('latin1')), 
                         sep='{', header=None, names=column_names, engine='python')

        # --- Limpieza y transformación de datos ---
        df[COL_NUM_FACTURA] = df[COL_NUM_FACTURA].astype(str).str.strip().str.upper()
        df[COL_VALOR_ERP] = df[COL_VALOR_ERP].apply(clean_and_convert_numeric)
        
        # **MEJORA CLAVE**: Conversión de fechas vectorizada y segura.
        # `errors='coerce'` convierte fechas inválidas a NaT (Not a Time) sin fallar.
        # `.dt.normalize()` elimina la parte de la hora, dejando solo la fecha.
        df[COL_FECHA_EMISION_ERP] = pd.to_datetime(df[COL_FECHA_EMISION_ERP], errors='coerce').dt.normalize()
        df[COL_FECHA_VENCIMIENTO_ERP] = pd.to_datetime(df[COL_FECHA_VENCIMIENTO_ERP], errors='coerce').dt.normalize()
        
        # Elimina filas esenciales que son nulas
        return df.dropna(subset=[COL_NUM_FACTURA, COL_PROVEEDOR_ERP])

    except dropbox.exceptions.ApiError as e:
        st.error(f"❌ Error de API de Dropbox: No se pudo descargar el archivo. Verifica la ruta y permisos. {e}")
    except Exception as e:
        st.error(f"❌ Error crítico cargando datos del ERP: {e}")
        
    return pd.DataFrame()

def parse_invoice_xml(xml_content: str) -> Optional[Dict[str, Any]]:
    """
    Parsea de forma robusta el contenido de un XML de factura electrónica DIAN (UBL 2.1).
    Busca en varias rutas posibles para cada campo clave.
    Retorna un diccionario con los datos o None si faltan campos esenciales.
    """
    try:
        # Limpieza inicial del contenido XML
        xml_content = xml_content.strip()
        if xml_content.startswith('<?xml'):
             xml_content = re.sub(r'^[^\<]+', '', xml_content)
        
        root = ET.fromstring(xml_content.encode('utf-8'))

        # Definición de namespaces para facilitar las búsquedas con XPath
        ns = {
            'cbc': "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2",
            'cac': "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2"
        }

        def find_text_robust(paths: List[str]) -> Optional[str]:
            """Función anidada para buscar un texto en varias rutas XPath."""
            for path in paths:
                node = root.find(path, ns)
                if node is not None and node.text:
                    return node.text.strip()
            return None

        # Búsqueda robusta de cada campo en rutas comunes
        invoice_number = find_text_robust(['./cbc:ID'])
        supplier_name = find_text_robust(['./cac:AccountingSupplierParty/cac:Party/cac:PartyLegalEntity/cbc:RegistrationName',
                                          './cac:AccountingSupplierParty/cac:Party/cac:PartyName/cbc:Name'])
        issue_date = find_text_robust(['./cbc:IssueDate'])
        due_date = find_text_robust(['./cac:PaymentMeans/cbc:PaymentDueDate', 
                                     './cbc:DueDate', 
                                     './cac:PaymentTerms/cbc:SettlementPeriod/cbc:EndDate'])
        total_value = find_text_robust(['./cac:LegalMonetaryTotal/cbc:PayableAmount', 
                                        './cac:RequestedMonetaryTotal/cbc:PayableAmount'])

        # Valida que los campos mínimos requeridos existan
        if not all([invoice_number, supplier_name, total_value]):
            return None

        return {
            COL_NUM_FACTURA: invoice_number.upper(),
            COL_PROVEEDOR_CORREO: supplier_name,
            COL_FECHA_EMISION_CORREO: issue_date,
            COL_FECHA_VENCIMIENTO_CORREO: due_date,
            COL_VALOR_CORREO: total_value
        }
    except (ET.ParseError, Exception):
        # Captura errores de parseo XML o cualquier otro error inesperado
        return None

def fetch_new_invoices_from_email(start_date: datetime) -> pd.DataFrame:
    """
    Busca, descarga y extrae datos de facturas desde archivos adjuntos en una cuenta de Gmail.
    """
    invoices_data = []
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(st.secrets.email["address"], st.secrets.email["password"])
        mail.select(f'"{EMAIL_FOLDER}"')
        
        search_query = f'(SINCE "{start_date.strftime("%d-%b-%Y")}")'
        _, messages = mail.search(None, search_query)
        
        message_ids = messages[0].split()
        if not message_ids:
            st.info(f"✅ No se encontraron correos nuevos desde {start_date.strftime('%Y-%m-%d')}.")
            mail.logout()
            return pd.DataFrame()
            
        progress_bar = st.progress(0, text=f"Procesando {len(message_ids)} correos...")
        
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
                    except (zipfile.BadZipFile, io.UnsupportedOperation):
                        # Ignora archivos zip corruptos o que no son zip
                        continue
            progress_bar.progress((i + 1) / len(message_ids), text=f"Procesando {i+1}/{len(message_ids)} correos...")
        
        mail.logout()
        
    except imaplib.IMAP4.error as e:
        st.warning(f"⚠️ Error de conexión IMAP. Verifica las credenciales o la configuración del servidor: {e}")
    except Exception as e:
        st.warning(f"⚠️ No se pudo procesar el correo: {e}")
        
    return pd.DataFrame(invoices_data)

# ======================================================================================
# --- 6. LÓGICA DE PROCESAMIENTO Y CONCILIACIÓN DE DATOS ---
# ======================================================================================
def process_and_reconcile(erp_df: pd.DataFrame, email_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cruza los datos del ERP y del correo para crear un DataFrame maestro conciliado,
    calculando estados de pago y conciliación.
    """
    if erp_df.empty:
        st.error("El análisis no puede continuar sin datos del ERP.")
        return pd.DataFrame()

    # --- Preparación del DataFrame de Correos ---
    if not email_df.empty:
        # Asegura que las columnas esperadas existan, incluso si no se encontró ninguna factura
        expected_email_cols = {
            COL_NUM_FACTURA: '', COL_PROVEEDOR_CORREO: '',
            COL_FECHA_EMISION_CORREO: pd.NaT, COL_FECHA_VENCIMIENTO_CORREO: pd.NaT,
            COL_VALOR_CORREO: np.nan
        }
        for col, default_val in expected_email_cols.items():
            if col not in email_df.columns:
                email_df[col] = default_val

        # Limpieza y conversión de tipos
        email_df[COL_VALOR_CORREO] = email_df[COL_VALOR_CORREO].apply(clean_and_convert_numeric)
        email_df[COL_NUM_FACTURA] = email_df[COL_NUM_FACTURA].astype(str).str.strip().str.upper()
        
        # **MEJORA CLAVE**: Conversión de fechas vectorizada y segura, igual que en el ERP.
        email_df[COL_FECHA_EMISION_CORREO] = pd.to_datetime(email_df[COL_FECHA_EMISION_CORREO], errors='coerce').dt.normalize()
        email_df[COL_FECHA_VENCIMIENTO_CORREO] = pd.to_datetime(email_df[COL_FECHA_VENCIMIENTO_CORREO], errors='coerce').dt.normalize()
        
        # Elimina duplicados, conservando la última factura recibida por correo
        email_df = email_df.drop_duplicates(subset=[COL_NUM_FACTURA], keep='last')
    
    # --- Fusión de Datos ---
    master_df = pd.merge(erp_df, email_df, on=COL_NUM_FACTURA, how='outer', indicator=True)
    
    # --- Cálculo de Estados de Conciliación ---
    conditions_conciliacion = [
        (master_df['_merge'] == 'right_only'),
        (master_df['_merge'] == 'left_only'),
        (~np.isclose(master_df[COL_VALOR_ERP], master_df[COL_VALOR_CORREO], atol=1.0) & (master_df['_merge'] == 'both')),
        (master_df['_merge'] == 'both')
    ]
    choices_conciliacion = ['📧 Solo en Correo', '📬 Pendiente de Correo', '⚠️ Discrepancia de Valor', '✅ Conciliada']
    master_df['estado_conciliacion'] = np.select(conditions_conciliacion, choices_conciliacion, default='-')

    # --- Cálculo de Estados de Pago ---
    today = pd.to_datetime(datetime.now(COLOMBIA_TZ).date())
    master_df['dias_para_vencer'] = (master_df[COL_FECHA_VENCIMIENTO_ERP] - today).dt.days
    
    conditions_pago = [
        master_df['dias_para_vencer'] < 0,
        (master_df['dias_para_vencer'] >= 0) & (master_df['dias_para_vencer'] <= 7)
    ]
    choices_pago = ["🔴 Vencida", "🟠 Por Vencer (7 días)"]
    master_df['estado_pago'] = np.select(conditions_pago, choices_pago, default="🟢 Vigente")
    master_df['estado_pago'] = np.where(master_df[COL_FECHA_VENCIMIENTO_ERP].isna(), 'Sin Fecha ERP', master_df['estado_pago'])
    
    # --- Limpieza Final ---
    # Unifica el nombre del proveedor, dando prioridad al del ERP
    master_df['nombre_proveedor'] = master_df[COL_PROVEEDOR_ERP].fillna(master_df[COL_PROVEEDOR_CORREO])
    master_df.drop(columns=['_merge'], inplace=True)
    
    return master_df

# ======================================================================================
# --- 7. ORQUESTACIÓN DE SINCRONIZACIÓN ---
# ======================================================================================

def run_full_sync():
    """
    Orquesta el proceso completo de sincronización de datos:
    1. Conecta a Google Sheets.
    2. Lee el historial de correos.
    3. Busca nuevos correos.
    4. Combina y actualiza la base de datos de correos.
    5. Carga datos del ERP.
    6. Procesa y reconcilia los datos.
    7. Actualiza el reporte final en Google Sheets.
    """
    with st.spinner('Iniciando sincronización completa...'):
        st.info("Paso 1/6: Conectando a servicios de Google...")
        gs_client = connect_to_google_sheets()
        if not gs_client:
            st.error("Sincronización cancelada. No se pudo conectar a Google.")
            st.stop()
        
        st.info(f"Paso 2/6: Accediendo a la base de datos de correos '{GSHEET_DB_NAME}'...")
        db_sheet = get_or_create_worksheet(gs_client, st.secrets["google_sheet_id"], GSHEET_DB_NAME)
        historical_email_df = pd.DataFrame(db_sheet.get_all_records()) if db_sheet else pd.DataFrame()

        # Determina la fecha de inicio para la búsqueda de nuevos correos
        if not historical_email_df.empty and 'fecha_lectura' in historical_email_df.columns:
            last_date = pd.to_datetime(historical_email_df['fecha_lectura'], errors='coerce').max()
            # Busca desde 5 días antes de la última lectura para cubrir posibles desfases
            search_start_date = (last_date - timedelta(days=5)) if pd.notna(last_date) else (datetime.now(COLOMBIA_TZ) - timedelta(days=SEARCH_DAYS_AGO))
        else:
            search_start_date = datetime.now(COLOMBIA_TZ) - timedelta(days=SEARCH_DAYS_AGO)
        
        st.info(f"Paso 3/6: Buscando nuevos correos desde {search_start_date.strftime('%Y-%m-%d')}...")
        new_email_df = fetch_new_invoices_from_email(search_start_date)
        
        if not new_email_df.empty:
            st.success(f"¡Se encontraron {len(new_email_df)} facturas nuevas en el correo!")
            new_email_df['fecha_lectura'] = datetime.now(COLOMBIA_TZ).isoformat()
            combined_email_df = pd.concat([historical_email_df, new_email_df]).drop_duplicates(subset=[COL_NUM_FACTURA], keep='last')
            
            st.info(f"Paso 4/6: Actualizando base de datos de correos '{GSHEET_DB_NAME}'...")
            update_gsheet_from_df(db_sheet, combined_email_df)
        else:
            combined_email_df = historical_email_df
            
        st.session_state.email_df = combined_email_df
        
        st.info("Paso 5/6: Cargando datos del ERP y conciliando...")
        st.session_state.erp_df = load_erp_data()
        final_df = process_and_reconcile(st.session_state.erp_df, st.session_state.email_df)
        st.session_state.master_df = final_df
        
        st.info(f"Paso 6/6: Actualizando reporte '{GSHEET_REPORT_NAME}' en Google Sheets...")
        report_sheet = get_or_create_worksheet(gs_client, st.secrets["google_sheet_id"], GSHEET_REPORT_NAME)
        if report_sheet and update_gsheet_from_df(report_sheet, final_df):
            st.success("✅ ¡Sincronización completada con éxito!")
        
        st.session_state.data_loaded = True

# ======================================================================================
# --- 8. COMPONENTES DE LA INTERFAZ DE USUARIO (STREAMLIT) ---
# ======================================================================================
# **MEJORA**: Se divide la interfaz en funciones más pequeñas y manejables
# para mejorar la legibilidad y mantenimiento del código.

def display_sidebar(master_df: pd.DataFrame):
    """Renderiza la barra lateral con el logo, botón de sincronización y filtros."""
    with st.sidebar:
        # Asegúrate de que este archivo de imagen esté en el directorio raíz de tu app
        st.image("LOGO FERREINOX SAS BIC 2024.png", use_container_width=True)
        st.title("Panel de Control")
        
        if st.button("🔄 Sincronizar Todo", type="primary", use_container_width=True):
            run_full_sync()
            st.rerun() # Recarga la app para reflejar los nuevos datos

        if 'master_df' in st.session_state and not st.session_state.master_df.empty:
            st.divider()
            st.header("Filtros Globales 🔎")
            
            proveedores_lista = sorted(master_df['nombre_proveedor'].dropna().unique().tolist())
            selected_suppliers = st.multiselect("Proveedor:", proveedores_lista, default=proveedores_lista)
            
            # Manejo seguro de rangos de fechas
            min_date_val = master_df[COL_FECHA_EMISION_ERP].dropna().min()
            max_date_val = master_df[COL_FECHA_EMISION_ERP].dropna().max()
            
            today = datetime.now().date()
            min_date = min_date_val.date() if pd.notna(min_date_val) else today - timedelta(days=365)
            max_date = max_date_val.date() if pd.notna(max_date_val) else today
            
            if min_date > max_date: min_date = max_date
            
            date_range = st.date_input("Fecha de Emisión (ERP):", 
                                       value=(min_date, max_date), 
                                       min_value=min_date, max_value=max_date)
            
            # Aplica los filtros
            filtered_df = master_df[master_df['nombre_proveedor'].isin(selected_suppliers)]
            if len(date_range) == 2:
                start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
                filtered_df = filtered_df[
                    (filtered_df[COL_FECHA_EMISION_ERP] >= start_date) & 
                    (filtered_df[COL_FECHA_EMISION_ERP] <= end_date)
                ]
            st.session_state.filtered_df = filtered_df

def display_dashboard(df: pd.DataFrame):
    """Renderiza el contenido principal del dashboard: métricas, alertas y tabs."""
    st.header("📊 Resumen Financiero y de Gestión")
    c1, c2, c3, c4 = st.columns(4)
    
    # Cálculos para las métricas
    total_deuda = df.loc[df['estado_conciliacion'] != '📧 Solo en Correo', COL_VALOR_ERP].sum()
    monto_vencido = df.loc[df['estado_pago'] == '🔴 Vencida', COL_VALOR_ERP].sum()
    por_vencer_monto = df.loc[df['estado_pago'] == '🟠 Por Vencer (7 días)', COL_VALOR_ERP].sum()
    
    c1.metric("Deuda Total (en ERP)", f"${total_deuda:,.2f}")
    c2.metric("Monto Vencido", f"${monto_vencido:,.2f}")
    c3.metric("Monto por Vencer (7 días)", f"${por_vencer_monto:,.2f}")
    c4.metric("Total Facturas Gestionadas", f"{len(df)}")
    
    st.divider()

    # --- Centro de Alertas y Búsqueda ---
    vencidas_df = df[df['estado_pago'] == '🔴 Vencida'].sort_values('dias_para_vencer')
    por_vencer_df = df[df['estado_pago'] == '🟠 Por Vencer (7 días)'].sort_values('dias_para_vencer')

    with st.expander(f"🚨 Centro de Alertas: {len(vencidas_df)} Vencidas y {len(por_vencer_df)} por Vencer", expanded=True):
        st.subheader("🔴 Facturas Vencidas (Acción Inmediata)")
        if not vencidas_df.empty:
            st.dataframe(vencidas_df[['nombre_proveedor', COL_NUM_FACTURA, COL_FECHA_VENCIMIENTO_ERP, COL_VALOR_ERP, 'dias_para_vencer']], use_container_width=True)
        else:
            st.info("¡Excelente! No hay facturas vencidas.")
            
        st.subheader("🟠 Facturas por Vencer (Próximos 7 días)")
        if not por_vencer_df.empty:
            st.dataframe(por_vencer_df[['nombre_proveedor', COL_NUM_FACTURA, COL_FECHA_VENCIMIENTO_ERP, COL_VALOR_ERP, 'dias_para_vencer']], use_container_width=True)
        else:
            st.info("No hay facturas con vencimiento en los próximos 7 días.")

    st.divider()

    # --- Pestañas de Análisis ---
    tab1, tab2, tab3 = st.tabs(["📑 Explorador de Datos", "🏢 Análisis de Proveedores", "⚙️ Estado de Conciliación"])

    with tab1:
        st.subheader("Explorador de Datos Consolidados")
        display_cols = ['nombre_proveedor', COL_NUM_FACTURA, COL_FECHA_EMISION_ERP, COL_FECHA_VENCIMIENTO_ERP, COL_VALOR_ERP, 'estado_pago', 'dias_para_vencer', 'estado_conciliacion', COL_VALOR_CORREO]
        st.dataframe(df[display_cols], use_container_width=True, hide_index=True,
            column_config={
                COL_VALOR_ERP: st.column_config.NumberColumn("Valor ERP", format="$ {:,.2f}"),
                COL_VALOR_CORREO: st.column_config.NumberColumn("Valor Correo", format="$ {:,.2f}"),
                COL_FECHA_EMISION_ERP: st.column_config.DateColumn("Emitida", format="YYYY-MM-DD"),
                COL_FECHA_VENCIMIENTO_ERP: st.column_config.DateColumn("Vence", format="YYYY-MM-DD"),
                "dias_para_vencer": st.column_config.ProgressColumn("Días para Vencer", format="%d días", min_value=-90, max_value=90),
            })
    
    with tab2:
        st.subheader("Análisis por Proveedor")
        provider_summary = df.groupby('nombre_proveedor').agg(
            total_facturado=(COL_VALOR_ERP, 'sum'),
            numero_facturas=(COL_NUM_FACTURA, 'count'),
            monto_vencido=(COL_VALOR_ERP, lambda x: x[df.loc[x.index, 'estado_pago'] == '🔴 Vencida'].sum())
        ).reset_index().sort_values('total_facturado', ascending=False)
        st.dataframe(provider_summary, use_container_width=True, hide_index=True, column_config={"total_facturado": st.column_config.NumberColumn("Total Facturado", format="$ {:,.2f}"), "monto_vencido": st.column_config.NumberColumn("Monto Vencido", format="$ {:,.2f}")})
        
        st.markdown("##### Top 15 Proveedores por Monto Facturado")
        chart = alt.Chart(provider_summary.head(15)).mark_bar().encode(
            x=alt.X('total_facturado:Q', title='Total Facturado ($)'),
            y=alt.Y('nombre_proveedor:N', sort='-x', title='Proveedor'),
            tooltip=[
                alt.Tooltip('nombre_proveedor', title='Proveedor'),
                alt.Tooltip('total_facturado:Q', title='Facturado', format='$,.2f'),
                'numero_facturas'
            ]
        ).properties(height=400)
        st.altair_chart(chart, use_container_width=True)

    with tab3:
        st.subheader("Resumen del Estado de Conciliación")
        conc_summary = df.groupby('estado_conciliacion').agg(
            numero_facturas=(COL_NUM_FACTURA, 'count'),
            valor_total=(COL_VALOR_ERP, 'sum')
        ).reset_index()
        c1, c2 = st.columns([1, 2])
        with c1:
            st.dataframe(conc_summary, use_container_width=True, hide_index=True, column_config={"valor_total": st.column_config.NumberColumn("Valor Total", format="$ {:,.2f}")})
        with c2:
            pie_chart = alt.Chart(conc_summary).mark_arc(innerRadius=50).encode(
                theta=alt.Theta(field="numero_facturas", type="quantitative"),
                color=alt.Color(field="estado_conciliacion", type="nominal", title="Estado"),
                tooltip=['estado_conciliacion', 'numero_facturas']
            ).properties(title="Distribución de Facturas por Estado")
            st.altair_chart(pie_chart, use_container_width=True)

# ======================================================================================
# --- 9. APLICACIÓN PRINCIPAL (PUNTO DE ENTRADA) ---
# ======================================================================================

def main_app():
    """
    Función principal que construye y renderiza la interfaz de la aplicación.
    """
    load_css()
    display_sidebar(st.session_state.master_df)
    
    st.title("Plataforma de Gestión Inteligente de Facturas")
    st.markdown("Bienvenido al centro de control de cuentas por pagar. Sincroniza para obtener la información más reciente.")
    
    if not st.session_state.data_loaded:
        st.info("👋 Presiona 'Sincronizar Todo' en la barra lateral para comenzar.")
        st.stop()
    
    filtered_df = st.session_state.get('filtered_df')
    if filtered_df is None or filtered_df.empty:
        st.warning("No hay datos que coincidan con los filtros seleccionados o no hay datos cargados.")
        st.stop()

    display_dashboard(filtered_df)

if __name__ == "__main__":
    initialize_session_state()
    if check_password():
        main_app()
