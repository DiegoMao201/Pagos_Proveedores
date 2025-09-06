# ======================================================================================
# --- 0. IMPORTACIÓN DE LIBRERÍAS ---
# ======================================================================================
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import dropbox
import imaplib
import email
import zipfile
import io
import re
import altair as alt
import gspread
from google.oauth2.service_account import Credentials
import xml.etree.ElementTree as ET
import pytz
from typing import List, Dict, Any, Optional

# ======================================================================================
# --- 1. CONFIGURACIÓN INICIAL Y CONSTANTES GLOBALES ---
# ======================================================================================
st.set_page_config(
    page_title="Gestión Inteligente de Facturas | FERREINOX",
    page_icon="🤖",
    layout="wide"
)

# --- Constantes Globales ---
COLOMBIA_TZ = pytz.timezone('America/Bogota')
IMAP_SERVER = "imap.gmail.com"
EMAIL_FOLDER = "TFHKA/Recepcion/Descargados"
DROPBOX_FILE_PATH = "/data/Proveedores.csv"
GSHEET_DB_NAME = "FacturasCorreo_DB"
GSHEET_REPORT_NAME = "ReporteConsolidado_Activo"
SEARCH_DAYS_AGO = 30 # Días hacia atrás para buscar correos si no hay historial

# ======================================================================================
# --- 2. ESTILOS CSS Y COMPONENTES VISUALES ---
# ======================================================================================
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
            .stMetric .css-1g8m2r4 { /* Metric label */
                font-size: 1rem;
                color: #4F4F4F;
                font-weight: 600;
            }
            .stMetric .css-1r6slb0 { /* Metric value */
                font-size: 2.2rem;
                font-weight: 700;
            }
            .stButton>button { width: 100%; }
            .st-expander { border-radius: 12px !important; border: 1px solid #E0E0E0 !important; }
        </style>
    """, unsafe_allow_html=True)

# ======================================================================================
# --- 3. LÓGICA DE AUTENTICACIÓN Y SEGURIDAD ---
# ======================================================================================
def check_password():
    """ Muestra un formulario de contraseña y verifica el acceso. """
    if st.session_state.get("password_correct", False):
        return True

    st.header("🔒 Acceso Restringido")
    st.write("Por favor, ingresa la contraseña para acceder al panel de gestión.")
    
    with st.form("password_form"):
        password = st.text_input("Contraseña:", type="password")
        submitted = st.form_submit_button("Ingresar")

        if submitted:
            if password == st.secrets.get("password"):
                st.session_state.password_correct = True
                st.rerun()
            else:
                st.error("Contraseña incorrecta. Inténtalo de nuevo.")
    return False

# ======================================================================================
# --- 4. CONEXIONES A SERVICIOS EXTERNOS (GOOGLE SHEETS, DROPBOX, EMAIL) ---
# ======================================================================================
@st.cache_resource(show_spinner="Conectando a Google Sheets...")
def connect_to_google_sheets():
    """ Establece conexión con la API de Google Sheets usando las credenciales. """
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_info(st.secrets["google_credentials"], scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ Error crítico al autenticar con Google Sheets: {e}")
        return None

def get_or_create_worksheet(client: gspread.Client, sheet_key: str, worksheet_name: str) -> Optional[gspread.Worksheet]:
    """ Obtiene una hoja de cálculo por su nombre, o la crea si no existe. """
    try:
        spreadsheet = client.open_by_key(sheet_key)
        try:
            return spreadsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            st.warning(f"Hoja '{worksheet_name}' no encontrada. Creando una nueva...")
            return spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols="30")
    except Exception as e:
        st.error(f"Error accediendo a la hoja de cálculo: {e}")
        return None

def update_gsheet_from_df(worksheet: gspread.Worksheet, df: pd.DataFrame) -> bool:
    """ Actualiza una hoja de Google Sheets con los datos de un DataFrame. """
    if worksheet is None: return False
    try:
        worksheet.clear()
        df_to_upload = df.copy()
        for col in df_to_upload.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
            df_to_upload[col] = df_to_upload[col].dt.strftime('%Y-%m-%d')
        df_to_upload = df_to_upload.astype(str).replace({'nan': '', 'NaT': ''})
        worksheet.update([df_to_upload.columns.values.tolist()] + df_to_upload.values.tolist())
        return True
    except Exception as e:
        st.error(f"❌ Error al actualizar la hoja '{worksheet.title}': {e}")
        return False

# ======================================================================================
# --- 5. LECTURA Y PARSEO DE DATOS (ERP & CORREO) ---
# ======================================================================================
def robust_date_parser(date_str: Any) -> pd.Timestamp:
    """ Convierte una cadena de texto a fecha, probando múltiples formatos comunes. """
    if pd.isna(date_str) or date_str is None or date_str == '': return pd.NaT
    formats = ['%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%Y%m%d', '%d%m%Y', '%Y-%m-%d %H:%M:%S']
    for fmt in formats:
        try:
            return pd.to_datetime(str(date_str), format=fmt, errors='raise').normalize()
        except (ValueError, TypeError):
            continue
    return pd.to_datetime(str(date_str), errors='coerce').normalize()

def clean_and_convert_numeric(value_str: Optional[str]) -> float:
    """ Limpia una cadena numérica (formato colombiano) y la convierte a float. """
    if value_str is None or not isinstance(value_str, str):
        return 0.0
    try:
        cleaned_str = re.sub(r'[$\s\.]', '', value_str)
        cleaned_str = cleaned_str.replace(',', '.')
        return float(cleaned_str)
    except (ValueError, TypeError):
        return 0.0

@st.cache_data(show_spinner="Descargando datos del ERP (Dropbox)...", ttl=900)
def load_erp_data() -> pd.DataFrame:
    """ Carga los datos de facturas desde un archivo CSV en Dropbox. """
    try:
        dbx = dropbox.Dropbox(
            oauth2_refresh_token=st.secrets.dropbox["refresh_token"],
            app_key=st.secrets.dropbox["app_key"],
            app_secret=st.secrets.dropbox["app_secret"]
        )
        _, res = dbx.files_download(DROPBOX_FILE_PATH)
        names = ['nombre_proveedor_erp', 'serie', 'num_entrada', 'num_factura', 'doc_erp', 'fecha_emision_erp', 'fecha_vencimiento_erp', 'valor_total_erp']
        df = pd.read_csv(io.StringIO(res.content.decode('latin1')), sep='{', header=None, names=names, engine='python')
        
        df['num_factura'] = df['num_factura'].astype(str).str.strip().str.upper()
        df['valor_total_erp'] = df['valor_total_erp'].apply(clean_and_convert_numeric)
        df['fecha_emision_erp'] = df['fecha_emision_erp'].apply(robust_date_parser)
        df['fecha_vencimiento_erp'] = df['fecha_vencimiento_erp'].apply(robust_date_parser)
        
        return df.dropna(subset=['num_factura', 'nombre_proveedor_erp'])
    except Exception as e:
        st.error(f"❌ Error crítico cargando datos del ERP: {e}")
        return pd.DataFrame()

def parse_invoice_xml(xml_content: str) -> Optional[Dict[str, str]]:
    """
    Parsea de forma robusta el contenido de un XML de factura electrónica, manejando
    múltiples namespaces y rutas de datos comunes en el estándar DIAN de Colombia.
    """
    try:
        xml_content = xml_content.strip()
        if xml_content.startswith('<?xml'):
             xml_content = re.sub(r'^[^\<]+', '', xml_content)
        
        root = ET.fromstring(xml_content.encode('utf-8'))

        ns = {
            'cbc': "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2",
            'cac': "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2",
            'sts': "dian:gov:co:facturaelectronica:Structures-2-1",
            'inv': "urn:oasis:names:specification:ubl:schema:xsd:Invoice-2",
            'ext': "urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2"
        }

        def find_text_robust(paths: List[str]) -> Optional[str]:
            for path in paths:
                node = root.find(path, ns)
                if node is not None and node.text is not None:
                    return node.text.strip()
            return None

        invoice_number = find_text_robust(['./cbc:ID'])
        
        supplier_name = find_text_robust([
            './cac:AccountingSupplierParty/cac:Party/cac:PartyLegalEntity/cbc:RegistrationName',
            './cac:AccountingSupplierParty/cac:Party/cac:PartyName/cbc:Name',
        ])
        
        issue_date = find_text_robust(['./cbc:IssueDate'])

        due_date = find_text_robust([
            './cac:PaymentMeans/cbc:PaymentDueDate',
            './cbc:DueDate',
            './cac:PaymentTerms/cbc:SettlementPeriod/cbc:EndDate'
        ])
        
        total_value = find_text_robust([
            './cac:LegalMonetaryTotal/cbc:PayableAmount',
            './cac:TaxTotal/cbc:TaxAmount',
            './cac:RequestedMonetaryTotal/cbc:PayableAmount'
        ])

        if not invoice_number or not supplier_name or not total_value:
            return None

        return {
            "num_factura": invoice_number.upper(),
            "nombre_proveedor_correo": supplier_name,
            "fecha_emision_correo": issue_date,
            "fecha_vencimiento_correo": due_date,
            "valor_total_correo": total_value
        }
    except ET.ParseError:
        return None
    except Exception:
        return None

def fetch_new_invoices_from_email(start_date: datetime) -> pd.DataFrame:
    """ Busca y extrae datos de facturas desde archivos adjuntos en una cuenta de email. """
    invoices = []
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(st.secrets.email["address"], st.secrets.email["password"])
        mail.select(f'"{EMAIL_FOLDER}"')
        
        search_query = f'(SINCE "{start_date.strftime("%d-%b-%Y")}")'
        _, messages = mail.search(None, search_query)
        
        if not messages[0]:
            st.info(f"✅ No se encontraron correos nuevos desde {start_date.strftime('%Y-%m-%d')}.")
            mail.logout()
            return pd.DataFrame()
            
        message_ids = messages[0].split()
        progress_bar = st.progress(0, text=f"Procesando {len(message_ids)} correos...")
        
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
                                    if details: invoices.append(details)
                    except zipfile.BadZipFile: continue
            progress_bar.progress((i + 1) / len(message_ids), text=f"Procesando {i+1}/{len(message_ids)} correos...")
        
        mail.logout()
        return pd.DataFrame(invoices)
    except Exception as e:
        st.warning(f"⚠️ No se pudo conectar o procesar el correo: {e}")
        return pd.DataFrame()

# ======================================================================================
# --- 6. LÓGICA DE PROCESAMIENTO Y CONCILIACIÓN DE DATOS ---
# ======================================================================================
def process_and_reconcile(erp_df: pd.DataFrame, email_df: pd.DataFrame) -> pd.DataFrame:
    """ Cruza los datos del ERP y del correo para crear un DataFrame maestro conciliado. """
    if erp_df.empty:
        st.error("El análisis no puede continuar sin datos del ERP.")
        return pd.DataFrame()

    if not email_df.empty:
        email_df['valor_total_correo'] = email_df['valor_total_correo'].apply(clean_and_convert_numeric)
        email_df['fecha_emision_correo'] = email_df['fecha_emision_correo'].apply(robust_date_parser)
        email_df['fecha_vencimiento_correo'] = email_df['fecha_vencimiento_correo'].apply(robust_date_parser)
        email_df['num_factura'] = email_df['num_factura'].astype(str).str.strip().str.upper()
        email_df = email_df.drop_duplicates(subset=['num_factura'], keep='last')
    else:
        email_df = pd.DataFrame(columns=['num_factura', 'nombre_proveedor_correo', 'fecha_emision_correo', 'fecha_vencimiento_correo', 'valor_total_correo'])

    master_df = pd.merge(erp_df, email_df, on='num_factura', how='outer', indicator=True)
    
    conditions_conciliacion = [
        (master_df['_merge'] == 'right_only'),
        (master_df['_merge'] == 'left_only'),
        (~np.isclose(master_df['valor_total_erp'], master_df['valor_total_correo'], atol=1.0)),
        (master_df['_merge'] == 'both')
    ]
    choices_conciliacion = ['📧 Solo en Correo', '📬 Pendiente de Correo', '⚠️ Discrepancia de Valor', '✅ Conciliada']
    master_df['estado_conciliacion'] = np.select(conditions_conciliacion, choices_conciliacion, default='-')

    today = pd.to_datetime(datetime.now(COLOMBIA_TZ).date())
    master_df['dias_para_vencer'] = (master_df['fecha_vencimiento_erp'] - today).dt.days
    
    conditions_pago = [
        master_df['dias_para_vencer'] < 0,
        (master_df['dias_para_vencer'] >= 0) & (master_df['dias_para_vencer'] <= 7)
    ]
    choices_pago = ["🔴 Vencida", "🟠 Por Vencer (7 días)"]
    master_df['estado_pago'] = np.select(conditions_pago, choices_pago, default="🟢 Vigente")
    master_df['estado_pago'] = np.where(master_df['fecha_vencimiento_erp'].isna(), 'Sin Fecha ERP', master_df['estado_pago'])

    master_df['nombre_proveedor'] = master_df['nombre_proveedor_erp'].fillna(master_df['nombre_proveedor_correo'])
    master_df.drop(columns=['_merge'], inplace=True)
    
    return master_df

# ======================================================================================
# --- 7. APLICACIÓN PRINCIPAL Y DISEÑO DEL DASHBOARD (STREAMLIT) ---
# ======================================================================================
def main_app():
    """ Construye y renderiza la interfaz de usuario del dashboard. """
    load_css()
    
    with st.sidebar:
        st.image("LOGO FERREINOX SAS BIC 2024.png", use_container_width=True)
        st.title("Panel de Control")
        
        if st.button("🔄 Sincronizar Todo", type="primary", use_container_width=True):
            run_full_sync()
            st.rerun()

        if 'master_df' in st.session_state and not st.session_state.master_df.empty:
            st.divider()
            st.header("Filtros Globales 🔎")
            master_df = st.session_state.master_df
            
            proveedores_lista = sorted(master_df['nombre_proveedor'].dropna().unique().tolist())
            selected_suppliers = st.multiselect("Proveedor:", proveedores_lista, default=proveedores_lista)
            
            min_date_val = master_df['fecha_emision_erp'].dropna().min()
            max_date_val = master_df['fecha_emision_erp'].dropna().max()
            
            min_date = min_date_val.date() if pd.notna(min_date_val) else datetime.now().date() - timedelta(days=365)
            max_date = max_date_val.date() if pd.notna(max_date_val) else datetime.now().date()
            
            if min_date > max_date: min_date = max_date
            
            date_range = st.date_input("Fecha de Emisión (ERP):", value=(min_date, max_date), min_value=min_date, max_value=max_date)
            
            start_date, end_date = (pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])) if len(date_range) == 2 else (None, None)
            
            filtered_df = master_df[master_df['nombre_proveedor'].isin(selected_suppliers)]
            if start_date and end_date:
                filtered_df = filtered_df[(filtered_df['fecha_emision_erp'] >= start_date) & (filtered_df['fecha_emision_erp'] <= end_date)]
            st.session_state.filtered_df = filtered_df

    st.title("Plataforma de Gestión Inteligente de Facturas")
    st.markdown("Bienvenido al centro de control de cuentas por pagar. Sincroniza para obtener la información más reciente.")
    
    if 'data_loaded' not in st.session_state or not st.session_state.data_loaded:
        st.info("👋 Presiona 'Sincronizar Todo' en la barra lateral para comenzar.")
        st.stop()
    
    if 'filtered_df' not in st.session_state or st.session_state.filtered_df.empty:
        st.warning("No hay datos que coincidan con los filtros seleccionados o no hay datos cargados.")
        st.stop()

    df = st.session_state.filtered_df
    
    st.header("📊 Resumen Financiero y de Gestión")
    c1, c2, c3, c4 = st.columns(4)
    total_deuda = df[df['estado_conciliacion'] != '📧 Solo en Correo']['valor_total_erp'].sum()
    monto_vencido = df[df['estado_pago'] == '🔴 Vencida']['valor_total_erp'].sum()
    por_vencer_monto = df[df['estado_pago'] == '🟠 Por Vencer (7 días)']['valor_total_erp'].sum()
    
    c1.metric("Deuda Total (en ERP)", f"${total_deuda:,.2f}")
    c2.metric("Monto Vencido", f"${monto_vencido:,.2f}")
    c3.metric("Monto por Vencer (7 días)", f"${por_vencer_monto:,.2f}")
    c4.metric("Total Facturas Gestionadas", f"{len(df)}")
    
    st.divider()

    st.header("🔍 Buscar Factura Específica")
    search_term = st.text_input("Ingresa el número de factura exacto para buscar:", placeholder="Ej: FVE-12345")
    if search_term:
        search_result = df[df['num_factura'].str.contains(search_term.strip().upper(), case=False, na=False)]
        if not search_result.empty:
            st.success(f"¡Factura '{search_term}' encontrada!")
            st.dataframe(search_result[['num_factura', 'nombre_proveedor', 'estado_pago', 'estado_conciliacion', 'valor_total_erp', 'valor_total_correo']], use_container_width=True)
        else:
            st.warning(f"No se encontró ninguna factura con el número '{search_term}'.")

    st.divider()
    
    vencidas_df = df[df['estado_pago'] == '🔴 Vencida'].sort_values('dias_para_vencer')
    por_vencer_df = df[df['estado_pago'] == '🟠 Por Vencer (7 días)'].sort_values('dias_para_vencer')
    
    with st.expander(f"🚨 Centro de Alertas: {len(vencidas_df)} Vencidas y {len(por_vencer_df)} por Vencer", expanded=True):
        st.subheader("🔴 Facturas Vencidas (Acción Inmediata)")
        if not vencidas_df.empty:
            st.dataframe(vencidas_df[['nombre_proveedor', 'num_factura', 'fecha_vencimiento_erp', 'valor_total_erp', 'dias_para_vencer']], use_container_width=True)
        else:
            st.info("¡Excelente! No hay facturas vencidas.")
            
        st.subheader("🟠 Facturas por Vencer (Próximos 7 días)")
        if not por_vencer_df.empty:
            st.dataframe(por_vencer_df[['nombre_proveedor', 'num_factura', 'fecha_vencimiento_erp', 'valor_total_erp', 'dias_para_vencer']], use_container_width=True)
        else:
            st.info("No hay facturas con vencimiento en los próximos 7 días.")

    st.divider()

    tab1, tab2, tab3 = st.tabs(["📑 Explorador de Datos", "🏢 Análisis de Proveedores", "⚙️ Estado de Conciliación"])

    with tab1:
        st.subheader("Explorador de Datos Consolidados")
        display_cols = ['nombre_proveedor', 'num_factura', 'fecha_emision_erp', 'fecha_vencimiento_erp', 'valor_total_erp', 'estado_pago', 'dias_para_vencer', 'estado_conciliacion', 'valor_total_correo']
        st.dataframe(df[display_cols], use_container_width=True, hide_index=True,
            column_config={
                "valor_total_erp": st.column_config.NumberColumn("Valor ERP", format="$ {:,.2f}"),
                "valor_total_correo": st.column_config.NumberColumn("Valor Correo", format="$ {:,.2f}"),
                "fecha_emision_erp": st.column_config.DateColumn("Emitida", format="YYYY-MM-DD"),
                "fecha_vencimiento_erp": st.column_config.DateColumn("Vence", format="YYYY-MM-DD"),
                "dias_para_vencer": st.column_config.ProgressColumn("Días para Vencer", format="%d días", min_value=-90, max_value=90),
            })
    
    with tab2:
        st.subheader("Análisis por Proveedor")
        provider_summary = df.groupby('nombre_proveedor').agg(
            total_facturado=('valor_total_erp', 'sum'),
            numero_facturas=('num_factura', 'count'),
            monto_vencido=('valor_total_erp', lambda x: x[df.loc[x.index, 'estado_pago'] == '🔴 Vencida'].sum())
        ).reset_index().sort_values('total_facturado', ascending=False)
        
        st.dataframe(provider_summary, use_container_width=True, hide_index=True, 
            column_config={
                "total_facturado": st.column_config.NumberColumn("Total Facturado", format="$ {:,.2f}"),
                "monto_vencido": st.column_config.NumberColumn("Monto Vencido", format="$ {:,.2f}")
            })
        
        st.markdown("##### Top 15 Proveedores por Monto Facturado")
        chart = alt.Chart(provider_summary.head(15)).mark_bar().encode(
            x=alt.X('total_facturado:Q', title='Total Facturado ($)'),
            y=alt.Y('nombre_proveedor:N', sort='-x', title='Proveedor'),
            tooltip=[alt.Tooltip('nombre_proveedor', title='Proveedor'), alt.Tooltip('total_facturado:Q', title='Facturado', format='$,.2f'), 'numero_facturas']
        ).properties(height=400)
        st.altair_chart(chart, use_container_width=True)

    with tab3:
        st.subheader("Resumen del Estado de Conciliación")
        conc_summary = df.groupby('estado_conciliacion').agg(numero_facturas=('num_factura', 'count'), valor_total=('valor_total_erp', 'sum')).reset_index()
        
        c1, c2 = st.columns([1,2])
        with c1:
            st.dataframe(conc_summary, use_container_width=True, hide_index=True,
                column_config={"valor_total": st.column_config.NumberColumn("Valor Total", format="$ {:,.2f}")})
        with c2:
            pie_chart = alt.Chart(conc_summary).mark_arc(innerRadius=50).encode(
                theta=alt.Theta(field="numero_facturas", type="quantitative"),
                color=alt.Color(field="estado_conciliacion", type="nominal", title="Estado"),
                tooltip=['estado_conciliacion', 'numero_facturas']
            ).properties(title="Distribución de Facturas por Estado de Conciliación")
            st.altair_chart(pie_chart, use_container_width=True)

def run_full_sync():
    """ Orquesta el proceso completo de sincronización de datos. """
    with st.spinner('Iniciando sincronización completa...'):
        st.info("Paso 1/6: Conectando a servicios de Google...")
        gs_client = connect_to_google_sheets()
        if not gs_client: st.stop()
        
        st.info(f"Paso 2/6: Accediendo a la base de datos de correos '{GSHEET_DB_NAME}'...")
        db_sheet = get_or_create_worksheet(gs_client, st.secrets["google_sheet_id"], GSHEET_DB_NAME)
        historical_email_df = pd.DataFrame(db_sheet.get_all_records()) if db_sheet else pd.DataFrame()

        if not historical_email_df.empty and 'fecha_lectura' in historical_email_df.columns:
            historical_email_df['fecha_lectura'] = pd.to_datetime(historical_email_df['fecha_lectura'], errors='coerce')
            last_date = historical_email_df['fecha_lectura'].max()
            search_start_date = (last_date - timedelta(days=5)) if pd.notna(last_date) else (datetime.now(COLOMBIA_TZ) - timedelta(days=SEARCH_DAYS_AGO))
        else:
            search_start_date = datetime.now(COLOMBIA_TZ) - timedelta(days=SEARCH_DAYS_AGO)
        
        st.info(f"Paso 3/6: Buscando nuevos correos desde {search_start_date.strftime('%Y-%m-%d')}...")
        new_email_df = fetch_new_invoices_from_email(search_start_date)
        
        if not new_email_df.empty:
            st.success(f"¡Se encontraron {len(new_email_df)} facturas nuevas en el correo!")
            new_email_df['fecha_lectura'] = datetime.now(COLOMBIA_TZ).isoformat()
            combined_email_df = pd.concat([historical_email_df, new_email_df]).drop_duplicates(subset=['num_factura'], keep='last')
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
# --- 8. PUNTO DE ENTRADA DE LA APLICACIÓN ---
# ======================================================================================
if __name__ == "__main__":
    if check_password():
        main_app()
