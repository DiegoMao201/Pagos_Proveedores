# 1_📈_Dashboard_General.py
# -*- coding: utf-8 -*-
"""
Plataforma de Gestión Inteligente de Facturas para FERREINOX.

Aplicación web construida con Streamlit para automatizar la conciliación de
facturas de proveedores recibidas por correo electrónico contra los registros
del sistema ERP (extraídos de Dropbox).

Funcionalidades principales:
- Autenticación segura por contraseña.
- Sincronización de facturas de los últimos 20 días desde una cuenta de Gmail.
- Carga de datos de cuentas por pagar desde un archivo CSV en Dropbox.
- Proceso de conciliación robusto para cruzar datos del ERP y correos.
- Dashboard interactivo con métricas, alertas de vencimiento y filtros.
- Visualización de datos y reportes por proveedor.
- Actualización de una base de datos y un reporte consolidado en Google Sheets.

**NUEVAS FUNCIONALIDADES (Versión 2.0):**
- Módulo de análisis de descuentos financieros por pronto pago.
- Plan de pagos sugerido para maximizar ahorros.
- Panel visual dedicado a la gestión de oportunidades de descuento.

**Dependencias (guardar como requirements.txt):**
altair==5.3.0
dropbox==11.36.2
gspread==6.0.2
numpy==1.26.4
pandas==2.2.2
pytz==2024.1
streamlit==1.35.0
google-oauth2-tool==1.1.0
gspread-pandas==3.3.0
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
# ### INICIO DE LA CORRECCIÓN DEL IMPORT ERROR ###
from gspread.utils import column_letter
# ### FIN DE LA CORRECCIÓN DEL IMPORT ERROR ###

# ======================================================================================
# --- 1. CONFIGURACIÓN INICIAL Y CONSTANTES GLOBALES ---
# ======================================================================================

# --- Configuración de la página de Streamlit ---
st.set_page_config(
    page_title="Gestión Inteligente de Facturas | FERREINOX",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Constantes Globales ---
COLOMBIA_TZ = pytz.timezone('America/Bogota')

# Credenciales y rutas (extraídas de st.secrets para mayor seguridad)
IMAP_SERVER = "imap.gmail.com"
EMAIL_FOLDER = "TFHKA/Recepcion/Descargados"
DROPBOX_FILE_PATH = "/data/Proveedores.csv"
GSHEET_DB_NAME = "FacturasCorreo_DB"
GSHEET_REPORT_NAME = "ReporteConsolidado_Activo"
APP_PASSWORD = st.secrets.get("password", "DEFAULT_PASSWORD")

# Parámetros de la aplicación
SEARCH_DAYS_AGO = 20  # Búsqueda ampliada a los últimos 20 días.

# Nombres de columnas estandarizados para evitar errores de tipeo
COL_NUM_FACTURA = 'num_factura'
COL_PROVEEDOR_ERP = 'nombre_proveedor_erp'
COL_VALOR_ERP = 'valor_total_erp'
COL_FECHA_EMISION_ERP = 'fecha_emision_erp'
COL_FECHA_VENCIMIENTO_ERP = 'fecha_vencimiento_erp'
COL_PROVEEDOR_CORREO = 'nombre_proveedor_correo'
COL_VALOR_CORREO = 'valor_total_correo'
COL_FECHA_EMISION_CORREO = 'fecha_emision_correo'
COL_FECHA_VENCIMIENTO_CORREO = 'fecha_vencimiento_correo'

# --- NUEVA CONSTANTE: Reglas de Descuentos por Proveedor ---
DISCOUNT_PROVIDERS = {
    "ABRASIVOS DE COLOMBIA S.A": [{"days": 10, "rate": 0.05}],
    "ARMETALES S.A.": [{"days": 10, "rate": 0.02}],
    "ASSA ABLOY COLOMBIA S.A.S": [{"days": 12, "rate": 0.025}],
    "INDUMA S.C.A": [{"days": 15, "rate": 0.035}],
    "INDUSTRIAS GOYAINCOL LTDA": [{"days": 30, "rate": 0.03}],
    "PINTUCO COLOMBIA S.A.S": [
        {"days": 15, "rate": 0.03},
        {"days": 30, "rate": 0.02}
    ],
    "SAINT - GOBAIN COLOMBIA S.A.S.": [{"days": 10, "rate": 0.03}]
}


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
    """Carga estilos CSS personalizados para mejorar la apariencia del dashboard."""
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
            .stMetric [data-testid="stMetricLabel"] {
                font-size: 1rem; color: #4F4F4F; font-weight: 600;
            }
            .stMetric [data-testid="stMetricValue"] {
                font-size: 2.2rem; font-weight: 700;
            }
            .stButton>button { width: 100%; border-radius: 8px; }
            .st-expander { border-radius: 12px !important; border: 1px solid #E0E0E0 !important; }
        </style>
    """, unsafe_allow_html=True)

# ======================================================================================
# --- 3. LÓGICA DE AUTENTICACIÓN Y SEGURIDAD ---
# ======================================================================================

def check_password() -> bool:
    """Muestra un formulario de contraseña y verifica el acceso."""
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
                st.rerun()
            else:
                st.error("Contraseña incorrecta. Inténtalo de nuevo.")

    return False

# ======================================================================================
# --- 4. CONEXIONES A SERVICIOS EXTERNOS (GOOGLE, DROPBOX, EMAIL) ---
# ======================================================================================

@st.cache_resource(show_spinner="Conectando a Google Sheets...")
def connect_to_google_sheets() -> Optional[Client]:
    """Establece conexión con la API de Google Sheets."""
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(st.secrets["google_credentials"], scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ Error crítico al autenticar con Google Sheets: {e}")
        return None

def get_or_create_worksheet(client: Client, sheet_key: str, worksheet_name: str) -> Optional[Worksheet]:
    """Obtiene una hoja de cálculo por su nombre, o la crea si no existe."""
    try:
        spreadsheet = client.open_by_key(sheet_key)
        try:
            return spreadsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            st.warning(f"Hoja '{worksheet_name}' no encontrada. Creando una nueva...")
            return spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols="50")
    except gspread.exceptions.APIError as e:
        st.error(f"Error de API de Google al acceder a la hoja de cálculo: {e}")
    except Exception as e:
        st.error(f"Error inesperado accediendo a la hoja '{worksheet_name}': {e}")
    return None

def update_gsheet_from_df(worksheet: Worksheet, df: pd.DataFrame) -> bool:
    """
    Actualiza una hoja de Google Sheets con los datos de un DataFrame,
    preservando las columnas existentes fuera del rango del DataFrame.
    """
    if not isinstance(worksheet, Worksheet):
        st.error("Se intentó actualizar una hoja de cálculo inválida.")
        return False

    try:
        # --- PREPARACIÓN DE DATOS ---
        df_to_upload = df.copy()

        # Bucle robusto para convertir todas las columnas de fecha a texto.
        for col in df_to_upload.columns:
            if pd.api.types.is_datetime64_any_dtype(df_to_upload[col]):
                df_to_upload[col] = df_to_upload[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Convierte todo el DataFrame a string y reemplaza los nulos para una subida limpia.
        df_to_upload = df_to_upload.astype(str).replace({
            'nan': '', 'NaT': '', 'None': ''
        })

        # --- LÓGICA DE ACTUALIZACIÓN MODIFICADA PARA NO BORRAR COLUMNAS ADICIONALES ---
        
        # 1. Preparar los datos para la subida (encabezados + filas).
        data_to_upload = [df_to_upload.columns.values.tolist()] + df_to_upload.values.tolist()
        num_rows_data = len(data_to_upload)
        num_cols_data = len(df_to_upload.columns)
        
        # 2. Obtener la letra de la última columna que el script manejará.
        # ### INICIO DE LA CORRECCIÓN DEL IMPORT ERROR ###
        last_col_letter = column_letter(num_cols_data)
        # ### FIN DE LA CORRECCIÓN DEL IMPORT ERROR ###
        
        # 3. Primero, actualiza el contenido nuevo desde la celda A1.
        worksheet.update(data_to_upload, 'A1')

        # 4. Limpiar las filas sobrantes DEBAJO de los nuevos datos.
        start_row_to_clear = num_rows_data + 1
        total_sheet_rows = worksheet.row_count
        
        if start_row_to_clear <= total_sheet_rows:
            range_to_clear = f'A{start_row_to_clear}:{last_col_letter}{total_sheet_rows}'
            
            rows_to_clear_count = total_sheet_rows - start_row_to_clear + 1
            if rows_to_clear_count > 0:
                empty_data = [[''] * num_cols_data for _ in range(rows_to_clear_count)]
                worksheet.update(range_to_clear, empty_data, raw=False)

        return True

    except Exception as e:
        st.error(f"❌ Error al actualizar la hoja '{worksheet.title}': {e}")
        return False


# ======================================================================================
# --- 5. LECTURA Y PROCESAMIENTO DE DATOS (ERP & CORREO) ---
# ======================================================================================

def clean_and_convert_numeric(value: Any) -> float:
    """Limpia una cadena de texto que representa un número y la convierte a float."""
    if pd.isna(value) or value is None:
        return np.nan
    if isinstance(value, (int, float)):
        return float(value)
    
    cleaned_str = str(value).strip().replace('$', '').replace('COP', '').strip()
    try:
        if '.' in cleaned_str and ',' in cleaned_str:
            if cleaned_str.rfind('.') > cleaned_str.rfind(','):
                cleaned_str = cleaned_str.replace(',', '')
            else:
                cleaned_str = cleaned_str.replace('.', '').replace(',', '.')
        else:
            cleaned_str = cleaned_str.replace(',', '.')
        return float(cleaned_str)
    except (ValueError, TypeError):
        return np.nan

def normalize_invoice_number(inv_num: Any) -> str:
    """Limpia y estandariza el número de factura para un cruce más efectivo."""
    if not isinstance(inv_num, str):
        inv_num = str(inv_num)
    return re.sub(r'[^A-Z0-9]', '', inv_num.upper()).strip()

@st.cache_data(show_spinner="Descargando datos del ERP (Dropbox)...", ttl=600)
def load_erp_data() -> pd.DataFrame:
    """Carga los datos de facturas desde un archivo CSV en Dropbox y los limpia."""
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
        
        try:
            df = pd.read_csv(io.StringIO(response.content.decode('latin1')),
                                     sep='{', header=None, names=column_names, engine='python')
        except Exception as csv_error:
            st.error(f"❌ Error al procesar el archivo CSV de Dropbox: {csv_error}")
            return pd.DataFrame()

        df[COL_VALOR_ERP] = df[COL_VALOR_ERP].apply(clean_and_convert_numeric)
        df.dropna(subset=[COL_PROVEEDOR_ERP, COL_VALOR_ERP], inplace=True)

        credit_note_mask = (df[COL_VALOR_ERP] < 0) & (df[COL_NUM_FACTURA].isna() | df[COL_NUM_FACTURA].str.strip() == '')
        
        if credit_note_mask.any():
            df.loc[credit_note_mask, COL_NUM_FACTURA] = 'NC-' + \
                df.loc[credit_note_mask, 'doc_erp'].astype(str) + '-' + \
                df.loc[credit_note_mask, COL_VALOR_ERP].abs().astype(int).astype(str)

        df.dropna(subset=[COL_NUM_FACTURA], inplace=True)

        df[COL_NUM_FACTURA] = df[COL_NUM_FACTURA].apply(normalize_invoice_number)
        
        df[COL_FECHA_EMISION_ERP] = pd.to_datetime(df[COL_FECHA_EMISION_ERP], errors='coerce').dt.tz_localize(COLOMBIA_TZ, ambiguous='infer')
        df[COL_FECHA_VENCIMIENTO_ERP] = pd.to_datetime(df[COL_FECHA_VENCIMIENTO_ERP], errors='coerce').dt.tz_localize(COLOMBIA_TZ, ambiguous='infer')
        
        st.success(f"✅ Datos del ERP cargados exitosamente ({len(df)} registros).")
        return df

    except dropbox.exceptions.ApiError as e:
        st.error(f"❌ Error de API de Dropbox: No se pudo descargar el archivo. Verifica la ruta y permisos. {e}")
    except Exception as e:
        st.error(f"❌ Error crítico cargando datos del ERP: {e}")

    return pd.DataFrame()


def parse_invoice_xml(xml_content: str) -> Optional[Dict[str, Any]]:
    """Parsea de forma robusta el contenido de un XML de factura electrónica DIAN."""
    try:
        xml_content = re.sub(r'^[^\<]+', '', xml_content.strip())
        root = ET.fromstring(xml_content.encode('utf-8'))
        
        ns = {
            'cbc': "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2",
            'cac': "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2",
        }
        
        description_node = root.find('.//cac:Attachment/cac:ExternalReference/cbc:Description', ns)
        if description_node is not None and description_node.text and '<Invoice' in description_node.text:
            try:
                inner_xml_text = re.sub(r'^[^\<]+', '', description_node.text.strip())
                root = ET.fromstring(inner_xml_text.encode('utf-8'))
            except ET.ParseError:
                pass

        def find_text_robust(element, paths: List[str]) -> Optional[str]:
            for path in paths:
                node = element.find(path, ns)
                if node is not None and node.text:
                    return node.text.strip()
            return None

        invoice_number = find_text_robust(root, ['./cbc:ID'])
        supplier_name = find_text_robust(root, [
            './cac:AccountingSupplierParty/cac:Party/cac:PartyName/cbc:Name',
            './cac:AccountingSupplierParty/cac:Party/cac:PartyLegalEntity/cbc:RegistrationName'
        ])
        issue_date = find_text_robust(root, ['./cbc:IssueDate'])
        due_date = find_text_robust(root, ['./cbc:DueDate', './cac:PaymentMeans/cbc:PaymentDueDate'])
        total_value = find_text_robust(root, ['./cac:LegalMonetaryTotal/cbc:PayableAmount'])

        if not all([invoice_number, supplier_name, total_value]):
            return None

        return {
            COL_NUM_FACTURA: normalize_invoice_number(invoice_number),
            COL_PROVEEDOR_CORREO: supplier_name,
            COL_FECHA_EMISION_CORREO: issue_date,
            COL_FECHA_VENCIMIENTO_CORREO: due_date,
            COL_VALOR_CORREO: total_value
        }
    except (ET.ParseError, Exception):
        return None

def fetch_new_invoices_from_email(start_date: datetime) -> pd.DataFrame:
    """Busca, descarga y extrae datos de facturas desde archivos adjuntos en Gmail."""
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
                    except (zipfile.BadZipFile, io.UnsupportedOperation):
                        continue
            progress_bar.progress((i + 1) / len(message_ids), text=f"Procesando {i+1}/{len(message_ids)} correos...")
        
        mail.logout()

    except imaplib.IMAP4.error as e:
        st.warning(f"⚠️ Error de conexión IMAP. Verifica credenciales o configuración: {e}")
    except Exception as e:
        st.warning(f"⚠️ Error inesperado procesando correos: {e}")

    return pd.DataFrame(invoices_data)


# ======================================================================================
# --- 6. LÓGICA DE PROCESAMIENTO Y CONCILIACIÓN DE DATOS ---
# ======================================================================================

def calculate_financial_discounts(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula los descuentos por pronto pago disponibles para cada factura."""
    df['descuento_pct'] = 0.0
    df['valor_descuento'] = 0.0
    df['valor_con_descuento'] = df[COL_VALOR_ERP]
    df['fecha_limite_descuento'] = pd.Series(dtype='datetime64[ns, UTC]')
    df['estado_descuento'] = 'No Aplica'

    today = datetime.now(COLOMBIA_TZ)

    df_copy = df.copy()

    for provider, tiers in DISCOUNT_PROVIDERS.items():
        provider_mask = (df_copy['nombre_proveedor'] == provider) & (df_copy[COL_FECHA_EMISION_ERP].notna()) & (df_copy['estado_pago'] != '🔴 Vencida')
        
        if provider_mask.any():
            sorted_tiers = sorted(tiers, key=lambda x: x['days'])
            
            for tier in sorted_tiers:
                days_limit = tier['days']
                rate = tier['rate']
                
                fecha_limite = df_copy.loc[provider_mask, COL_FECHA_EMISION_ERP] + timedelta(days=days_limit)
                
                eligible_mask = (fecha_limite >= today) & (df_copy['estado_descuento'] == 'No Aplica') & provider_mask
                
                if eligible_mask.any():
                    df_copy.loc[eligible_mask, 'descuento_pct'] = rate
                    df_copy.loc[eligible_mask, 'valor_descuento'] = df_copy.loc[eligible_mask, COL_VALOR_ERP] * rate
                    df_copy.loc[eligible_mask, 'valor_con_descuento'] = df_copy.loc[eligible_mask, COL_VALOR_ERP] - df_copy.loc[eligible_mask, 'valor_descuento']
                    df_copy.loc[eligible_mask, 'fecha_limite_descuento'] = fecha_limite[eligible_mask]
                    df_copy.loc[eligible_mask, 'estado_descuento'] = f'Disponible ({rate:.1%} en {days_limit} días)'
    
    return df_copy

def process_and_reconcile(erp_df: pd.DataFrame, email_df: pd.DataFrame) -> pd.DataFrame:
    """Cruza los datos del ERP y del correo para crear un DataFrame maestro conciliado."""
    if erp_df.empty:
        st.error("El análisis no puede continuar sin datos del ERP.")
        return pd.DataFrame()

    erp_df_proc = erp_df.copy()
    erp_df_proc[COL_VALOR_ERP] = pd.to_numeric(erp_df_proc[COL_VALOR_ERP], errors='coerce')

    if not email_df.empty:
        email_df_proc = email_df.copy()
        email_df_proc[COL_VALOR_CORREO] = email_df_proc[COL_VALOR_CORREO].apply(clean_and_convert_numeric)
        
        date_cols_correo = [COL_FECHA_EMISION_CORREO, COL_FECHA_VENCIMIENTO_CORREO]
        for col in date_cols_correo:
            if col in email_df_proc.columns:
                date_series = pd.to_datetime(email_df_proc[col], errors='coerce')
                
                if date_series.dt.tz is None:
                    email_df_proc[col] = date_series.dt.tz_localize(COLOMBIA_TZ, ambiguous='infer')
                else:
                    email_df_proc[col] = date_series.dt.tz_convert(COLOMBIA_TZ)

        email_df_proc = email_df_proc.drop_duplicates(subset=[COL_NUM_FACTURA], keep='last')
    else:
        email_df_proc = pd.DataFrame(columns=[COL_NUM_FACTURA, COL_PROVEEDOR_CORREO, COL_VALOR_CORREO, COL_FECHA_EMISION_CORREO, COL_FECHA_VENCIMIENTO_CORREO])

    master_df = pd.merge(erp_df_proc, email_df_proc, on=COL_NUM_FACTURA, how='outer', indicator=True)

    master_df[COL_VALOR_ERP] = pd.to_numeric(master_df[COL_VALOR_ERP], errors='coerce')
    master_df[COL_VALOR_CORREO] = pd.to_numeric(master_df[COL_VALOR_CORREO], errors='coerce')

    is_credit_note_mask = (master_df[COL_VALOR_ERP] < 0)

    erp_vals = master_df[COL_VALOR_ERP].fillna(0)
    email_vals = master_df[COL_VALOR_CORREO].fillna(0)
    conditions_conciliacion = [
        (is_credit_note_mask),
        (master_df['_merge'] == 'right_only'),
        (master_df['_merge'] == 'left_only'),
        (master_df[COL_VALOR_ERP].notna() & master_df[COL_VALOR_CORREO].notna() & ~np.isclose(erp_vals, email_vals, atol=1.0)),
        (master_df['_merge'] == 'both')
    ]
    choices_conciliacion = ['📝 Nota Crédito ERP', '📧 Solo en Correo', '📬 Pendiente de Correo', '⚠️ Discrepancia de Valor', '✅ Conciliada']
    master_df['estado_conciliacion'] = np.select(conditions_conciliacion, choices_conciliacion, default='-')

    today = datetime.now(COLOMBIA_TZ)
    master_df['dias_para_vencer'] = (master_df[COL_FECHA_VENCIMIENTO_ERP] - today).dt.days
    
    conditions_pago = [
        (is_credit_note_mask),
        master_df['dias_para_vencer'] < 0,
        (master_df['dias_para_vencer'] >= 0) & (master_df['dias_para_vencer'] <= 7)
    ]
    choices_pago = ["N/A (Nota Crédito)", "🔴 Vencida", "🟠 Por Vencer (7 días)"]
    master_df['estado_pago'] = np.select(conditions_pago, choices_pago, default="🟢 Vigente")
    master_df['estado_pago'] = np.where(master_df[COL_FECHA_VENCIMIENTO_ERP].isna() & ~is_credit_note_mask, 'Sin Fecha ERP', master_df['estado_pago'])

    master_df['nombre_proveedor'] = master_df[COL_PROVEEDOR_ERP].fillna(master_df[COL_PROVEEDOR_CORREO])
    master_df.drop(columns=['_merge'], inplace=True)

    if not master_df.empty:
        master_df = calculate_financial_discounts(master_df)

    return master_df


# ======================================================================================
# --- 7. ORQUESTACIÓN DE SINCRONIZACIÓN ---
# ======================================================================================

def run_full_sync():
    """Orquesta el proceso completo de sincronización de datos."""
    with st.spinner('Iniciando sincronización completa...'):
        st.info("Paso 1/5: Conectando a servicios de Google...")
        gs_client = connect_to_google_sheets()
        if not gs_client:
            st.error("Sincronización cancelada. No se pudo conectar a Google.")
            st.stop()

        search_start_date = datetime.now(COLOMBIA_TZ) - timedelta(days=SEARCH_DAYS_AGO)
        st.info(f"Paso 2/5: Buscando nuevos correos desde {search_start_date.strftime('%Y-%m-%d')}...")
        email_df = fetch_new_invoices_from_email(search_start_date)

        db_sheet = get_or_create_worksheet(gs_client, st.secrets["google_sheet_id"], GSHEET_DB_NAME)
        historical_df = pd.DataFrame()
        if db_sheet:
            try:
                historical_df = pd.DataFrame(db_sheet.get_all_records())
                if not historical_df.empty:
                    date_cols_to_convert = [COL_FECHA_EMISION_CORREO, COL_FECHA_VENCIMIENTO_CORREO, 'fecha_lectura']
                    for col in date_cols_to_convert:
                        if col in historical_df.columns:
                            date_series = pd.to_datetime(historical_df[col], errors='coerce')
                            
                            if date_series.dt.tz is None:
                                historical_df[col] = date_series.dt.tz_localize(COLOMBIA_TZ, ambiguous='infer')
                            else:
                                historical_df[col] = date_series.dt.tz_convert(COLOMBIA_TZ)

            except gspread.exceptions.GSpreadException as e:
                st.warning(f"No se pudo leer la base de datos histórica de correos: {e}")

        if not email_df.empty:
            st.success(f"¡Se encontraron y procesaron {len(email_df)} facturas nuevas en el correo!")
            email_df['fecha_lectura'] = datetime.now(COLOMBIA_TZ)
            st.info(f"Paso 3/5: Actualizando base de datos de correos '{GSHEET_DB_NAME}'...")
            
            combined_df = pd.concat([historical_df, email_df]).drop_duplicates(subset=[COL_NUM_FACTURA], keep='last')
            if db_sheet:
                update_gsheet_from_df(db_sheet, combined_df)
            st.session_state.email_df = combined_df
        else:
            st.info("No se encontraron facturas nuevas para añadir a la base de datos.")
            st.session_state.email_df = historical_df

        st.info("Paso 4/5: Cargando datos del ERP y conciliando...")
        st.session_state.erp_df = load_erp_data()
        final_df = process_and_reconcile(st.session_state.erp_df, st.session_state.email_df)
        st.session_state.master_df = final_df

        st.info(f"Paso 5/5: Actualizando reporte '{GSHEET_REPORT_NAME}' en Google Sheets...")
        report_sheet = get_or_create_worksheet(gs_client, st.secrets["google_sheet_id"], GSHEET_REPORT_NAME)
        if report_sheet and not final_df.empty:
            if update_gsheet_from_df(report_sheet, final_df):
                st.success(f"✅ ¡Reporte '{GSHEET_REPORT_NAME}' actualizado en Google Sheets!")
        else:
            st.warning("No se actualizó el reporte en Google Sheets (sin datos finales o sin acceso a la hoja).")

        st.session_state['last_sync_time'] = datetime.now(COLOMBIA_TZ).strftime('%Y-%m-%d %H:%M:%S')
        st.session_state.data_loaded = True
        st.balloons()

# ======================================================================================
# --- 8. COMPONENTES DE LA INTERFAZ DE USUARIO (STREAMLIT) ---
# ======================================================================================

def display_sidebar(master_df: pd.DataFrame):
    """Renderiza la barra lateral con el logo, botón de sincronización y filtros."""
    with st.sidebar:
        st.image("LOGO FERREINOX SAS BIC 2024.png")
        st.title("Panel de Control")

        if st.button("🔄 Sincronizar Todo", type="primary", help=f"Busca correos de los últimos {SEARCH_DAYS_AGO} días, recarga el ERP y actualiza reportes."):
            run_full_sync()
            st.rerun()

        if 'master_df' in st.session_state and not st.session_state.master_df.empty:
            st.divider()
            st.header("Filtros Globales 🔎")

            proveedores_lista = sorted(master_df['nombre_proveedor'].dropna().unique().tolist())
            selected_suppliers = st.multiselect("Proveedor:", proveedores_lista, default=proveedores_lista)
            
            min_date_val = master_df[COL_FECHA_EMISION_ERP].dropna().min()
            max_date_val = master_df[COL_FECHA_EMISION_ERP].dropna().max()
            
            today = datetime.now().date()
            min_date = min_date_val.date() if pd.notna(min_date_val) else today - timedelta(days=365)
            max_date = max_date_val.date() if pd.notna(max_date_val) else today
            
            date_range = (min_date, max_date)
            if min_date <= max_date:
                date_range = st.date_input(
                    "Fecha de Emisión (ERP):",
                    value=(min_date, max_date),
                    min_value=min_date, max_value=max_date
                )
            
            filtered_df = master_df[master_df['nombre_proveedor'].isin(selected_suppliers)]
            if len(date_range) == 2:
                start_date = pd.to_datetime(date_range[0]).tz_localize(COLOMBIA_TZ)
                end_date = pd.to_datetime(date_range[1]).tz_localize(COLOMBIA_TZ) + timedelta(days=1)
                erp_dates_mask = filtered_df[COL_FECHA_EMISION_ERP].notna()
                filtered_df = filtered_df[erp_dates_mask & (filtered_df[COL_FECHA_EMISION_ERP] >= start_date) & (filtered_df[COL_FECHA_EMISION_ERP] < end_date)]

            st.session_state.filtered_df = filtered_df

def display_dashboard(df: pd.DataFrame):
    """Renderiza el contenido principal del dashboard."""
    st.header("📊 Resumen Financiero y de Gestión")
    c1, c2, c3, c4 = st.columns(4)

    total_deuda = df.loc[df['estado_conciliacion'] != '📧 Solo en Correo', COL_VALOR_ERP].sum()
    monto_vencido = df.loc[df['estado_pago'] == '🔴 Vencida', COL_VALOR_ERP].sum()
    por_vencer_monto = df.loc[df['estado_pago'] == '🟠 Por Vencer (7 días)', COL_VALOR_ERP].sum()

    c1.metric("Deuda Total (en ERP)", f"{int(total_deuda):,}")
    c2.metric("Monto Vencido", f"{int(monto_vencido):,}")
    c3.metric("Monto por Vencer (7 días)", f"{int(por_vencer_monto):,}")
    c4.metric("Total Documentos Gestionados", f"{len(df)}")

    st.divider()
    
    vencidas_df = df[df['estado_pago'] == '🔴 Vencida'].sort_values('dias_para_vencer')
    por_vencer_df = df[df['estado_pago'] == '🟠 Por Vencer (7 días)'].sort_values('dias_para_vencer')

    with st.expander(f"🚨 Centro de Alertas: {len(vencidas_df)} Vencidas y {len(por_vencer_df)} por Vencer", expanded=True):
        st.subheader("🔴 Facturas Vencidas (Acción Inmediata)")
        if not vencidas_df.empty:
            st.dataframe(vencidas_df[['nombre_proveedor', COL_NUM_FACTURA, COL_FECHA_VENCIMIENTO_ERP, COL_VALOR_ERP, 'dias_para_vencer']], use_container_width=True)
        else:
            st.success("¡Excelente! No hay facturas vencidas en la selección actual.")
        
        st.subheader("🟠 Facturas por Vencer (Próximos 7 días)")
        if not por_vencer_df.empty:
            st.dataframe(por_vencer_df[['nombre_proveedor', COL_NUM_FACTURA, COL_FECHA_VENCIMIENTO_ERP, COL_VALOR_ERP, 'dias_para_vencer']], use_container_width=True)
        else:
            st.info("No hay facturas con vencimiento en los próximos 7 días.")

    st.divider()

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📑 Explorador de Datos", 
        "🏢 Análisis de Proveedores", 
        "⚙️ Estado de Conciliación",
        "💡 Descuentos y Pagos",
        "📝 Notas Crédito Pendientes"
    ])

    with tab1:
        st.subheader("Explorador de Datos Consolidados")
        display_cols = ['nombre_proveedor', COL_NUM_FACTURA, COL_FECHA_EMISION_ERP, COL_FECHA_VENCIMIENTO_ERP, COL_VALOR_ERP, 'estado_pago', 'dias_para_vencer', 'estado_conciliacion', COL_VALOR_CORREO]
        st.dataframe(df[display_cols], use_container_width=True, hide_index=True,
          column_config={
              COL_VALOR_ERP: st.column_config.NumberColumn("Valor ERP", format="%d"),
              COL_VALOR_CORREO: st.column_config.NumberColumn("Valor Correo", format="%d"),
              COL_FECHA_EMISION_ERP: st.column_config.DateColumn("Emitida", format="YYYY-MM-DD"),
              COL_FECHA_VENCIMIENTO_ERP: st.column_config.DateColumn("Vence", format="YYYY-MM-DD"),
              "dias_para_vencer": st.column_config.ProgressColumn("Días para Vencer", format="%d días", min_value=-90, max_value=90),
          })
    
    with tab2:
        st.subheader("Análisis por Proveedor")
        provider_summary = df.groupby('nombre_proveedor').agg(
            total_facturado=(COL_VALOR_ERP, 'sum'),
            numero_documentos=(COL_NUM_FACTURA, 'count'),
            monto_vencido=(COL_VALOR_ERP, lambda x: x[df.loc[x.index, 'estado_pago'] == '🔴 Vencida'].sum())
        ).reset_index().sort_values('total_facturado', ascending=False)
        st.dataframe(provider_summary, use_container_width=True, hide_index=True,
            column_config={
                "total_facturado": st.column_config.NumberColumn("Saldo Neto (Facturas - NC)", format="%d"),
                "monto_vencido": st.column_config.NumberColumn("Monto Vencido", format="%d")
            })
        
        st.markdown("##### Top 15 Proveedores por Saldo Neto")
        chart = alt.Chart(provider_summary.head(15)).mark_bar().encode(
            x=alt.X('total_facturado:Q', title='Saldo Neto'),
            y=alt.Y('nombre_proveedor:N', sort='-x', title='Proveedor'),
            tooltip=[alt.Tooltip('nombre_proveedor', title='Proveedor'), alt.Tooltip('total_facturado:Q', title='Saldo', format=',.0f'), 'numero_documentos']
        ).properties(height=400)
        st.altair_chart(chart, use_container_width=True)

    with tab3:
        st.subheader("Resumen del Estado de Conciliación")
        conc_summary = df.groupby('estado_conciliacion').agg(
            numero_documentos=(COL_NUM_FACTURA, 'count'),
            valor_total=(COL_VALOR_ERP, 'sum')
        ).reset_index()
        c1, c2 = st.columns([1, 2])
        with c1:
            st.dataframe(conc_summary, use_container_width=True, hide_index=True,
                column_config={"valor_total": st.column_config.NumberColumn("Valor Total", format="%d")})
        with c2:
            pie_chart = alt.Chart(conc_summary).mark_arc(innerRadius=50).encode(
                theta=alt.Theta(field="numero_documentos", type="quantitative"),
                color=alt.Color(field="estado_conciliacion", type="nominal", title="Estado"),
                tooltip=['estado_conciliacion', 'numero_documentos', alt.Tooltip('valor_total:Q', title='Valor', format=',.0f')]
            ).properties(title="Distribución de Documentos por Estado")
            st.altair_chart(pie_chart, use_container_width=True)
            
    with tab4:
        st.subheader("💰 Oportunidades de Descuento por Pronto Pago")
        
        descuentos_df = df[df['estado_descuento'] != 'No Aplica'].copy()
        
        descuentos_df['fecha_limite_descuento'] = pd.to_datetime(
            descuentos_df['fecha_limite_descuento'], errors='coerce'
        )
        
        if descuentos_df.empty or descuentos_df['fecha_limite_descuento'].isna().all():
            st.info("Actualmente no hay facturas con oportunidades de descuento por pronto pago en la selección.")
        else:
            total_ahorro_potencial = descuentos_df['valor_descuento'].sum()
            st.metric("Ahorro Potencial Total", f"{int(total_ahorro_potencial):,}", help="Suma de todos los descuentos disponibles en la selección actual.")

            st.markdown("---")
            st.markdown("### 🧠 Plan de Pagos Sugerido (Próximos 15 días)")
            
            today = datetime.now(COLOMBIA_TZ).date()
            limite_sugerencia = today + timedelta(days=15)
            
            plan_pagos_df = descuentos_df[
                (descuentos_df['fecha_limite_descuento'].notna()) &
                (descuentos_df['fecha_limite_descuento'].dt.date <= limite_sugerencia)
            ].sort_values('fecha_limite_descuento', ascending=True)

            if plan_pagos_df.empty:
                st.success("¡Todo en orden! No hay pagos con descuento que venzan en los próximos 15 días.")
            else:
                st.warning("¡Atención! Estas son las facturas que deberías pagar pronto para maximizar tu ahorro.")
                display_cols_plan = [
                    'nombre_proveedor', COL_NUM_FACTURA, COL_VALOR_ERP, 
                    'estado_descuento', 'valor_descuento', 'valor_con_descuento', 'fecha_limite_descuento'
                ]
                st.dataframe(plan_pagos_df[display_cols_plan], use_container_width=True, hide_index=True,
                    column_config={
                        COL_VALOR_ERP: st.column_config.NumberColumn("Valor Original", format="%d"),
                        'valor_descuento': st.column_config.NumberColumn("Ahorro", format="%d"),
                        'valor_con_descuento': st.column_config.NumberColumn("Valor a Pagar", format="%d"),
                        'fecha_limite_descuento': st.column_config.DateColumn("Pagar Antes de", format="YYYY-MM-DD"),
                    }
                )

            st.markdown("---")
            st.markdown("### 📋 Todas las Oportunidades de Descuento Activas")
            
            with st.expander("Ver todas las facturas con descuentos disponibles"):
                display_cols_all = [
                    'nombre_proveedor', COL_NUM_FACTURA, COL_FECHA_EMISION_ERP, COL_VALOR_ERP, 
                    'estado_descuento', 'valor_descuento', 'valor_con_descuento', 'fecha_limite_descuento'
                ]
                st.dataframe(descuentos_df[descuentos_df['fecha_limite_descuento'].notna()].sort_values('fecha_limite_descuento'), use_container_width=True, hide_index=True,
                    column_config={
                        COL_FECHA_EMISION_ERP: st.column_config.DateColumn("Emitida", format="YYYY-MM-DD"),
                        COL_VALOR_ERP: st.column_config.NumberColumn("Valor Original", format="%d"),
                        'valor_descuento': st.column_config.NumberColumn("Ahorro", format="%d"),
                        'valor_con_descuento': st.column_config.NumberColumn("Valor a Pagar", format="%d"),
                        'fecha_limite_descuento': st.column_config.DateColumn("Pagar Antes de", format="YYYY-MM-DD"),
                    }
                )

    with tab5:
        st.subheader("Notas Crédito Pendientes de Aplicar")
        st.info("Aquí se listan todas las notas crédito registradas en el ERP que están pendientes por cruzar. Estos valores ya se descuentan de la 'Deuda Total'.")
        
        credit_notes_df = df[df['estado_conciliacion'] == '📝 Nota Crédito ERP'].copy()
        
        if credit_notes_df.empty:
            st.success("No hay notas crédito pendientes en la selección de datos actual.")
        else:
            display_cols_nc = [
                'nombre_proveedor',
                COL_FECHA_EMISION_ERP,
                'doc_erp',
                COL_VALOR_ERP,
                'serie'
            ]
            st.dataframe(
                credit_notes_df[display_cols_nc].sort_values(COL_FECHA_EMISION_ERP, ascending=False),
                use_container_width=True,
                hide_index=True,
                column_config={
                    COL_VALOR_ERP: st.column_config.NumberColumn("Valor Nota Crédito", format="%d"),
                    COL_FECHA_EMISION_ERP: st.column_config.DateColumn("Fecha Emisión", format="YYYY-MM-DD"),
                    "doc_erp": "Documento ERP",
                    "serie": "Serie"
                }
            )

# ======================================================================================
# --- 9. APLICACIÓN PRINCIPAL (PUNTO DE ENTRADA) ---
# ======================================================================================

def main_app():
    """Función principal que construye y renderiza la interfaz de la aplicación."""
    load_css()
    master_df = st.session_state.get("master_df", pd.DataFrame())
    display_sidebar(master_df)

    st.title("Plataforma de Gestión Inteligente de Facturas")
    st.markdown("Bienvenido al centro de control de cuentas por pagar. **Esta es la página principal para actualizar los datos desde el correo y Dropbox.**")

    if 'last_sync_time' in st.session_state:
        st.success(f"Última sincronización completada a las: {st.session_state.last_sync_time}")

    if not st.session_state.data_loaded:
        st.info("👋 Presiona 'Sincronizar Todo' en la barra lateral para cargar y procesar los datos más recientes.")
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
