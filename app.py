# app.py
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
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

# --- CONFIGURACIÓN Y CONSTANTES ---
st.set_page_config(
    page_title="Control de Facturación IA",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Constantes de configuración
COLOMBIA_TZ = pytz.timezone('America/Bogota')
IMAP_SERVER = "imap.gmail.com"
EMAIL_FOLDER = "TFHKA/Recepcion/Descargados"
DROPBOX_FILE_PATH = "/data/Proveedores.csv"
GSHEET_NAME = "FacturasCorreo" # Opcional si se quiere seguir usando

# --- ESTILOS VISUALES (CSS) ---
def load_css():
    """Inyecta CSS personalizado para una interfaz más moderna y atractiva."""
    st.markdown("""
        <style>
            .metric-card {
                background-color: #FFFFFF;
                border: 1px solid #E0E0E0;
                border-radius: 12px;
                padding: 25px;
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.05);
                transition: transform 0.2s ease-in-out;
                text-align: center;
            }
            .metric-card:hover {
                transform: translateY(-5px);
                box-shadow: 0 8px 24px rgba(0, 0, 0, 0.1);
            }
            .metric-card h3 { font-size: 1.1rem; color: #4F4F4F; margin-bottom: 10px; font-weight: 500; }
            .metric-card p { font-size: 2.2rem; font-weight: 700; color: #1a1a1a; }
        </style>
    """, unsafe_allow_html=True)

# --- LÓGICA DE AUTENTICACIÓN ---
def check_password():
    """Verifica si la contraseña ingresada por el usuario es correcta."""
    if not st.session_state.get("password_correct", False):
        st.header("🔒 Acceso Restringido al Centro de Control")
        password = st.text_input("Ingresa la contraseña para acceder:", type="password")
        if st.button("Ingresar"):
            if password == st.secrets.get("password"):
                st.session_state.password_correct = True
                st.rerun()
            else:
                st.error("Contraseña incorrecta. Por favor, intenta de nuevo.")
        return False
    return True

# --- FUNCIONES AUXILIARES PARA LIMPIEZA DE DATOS ---
def clean_monetary_value(value):
    if isinstance(value, (int, float)): return float(value)
    if isinstance(value, str):
        value = re.sub(r'[$\s]', '', value)
        if '.' in value and ',' in value: value = value.replace('.', '').replace(',', '.')
        else: value = value.replace(',', '')
        try: return float(value)
        except (ValueError, TypeError): return 0.0
    return 0.0

def parse_date(date_str):
    """Convierte de forma inteligente una cadena de texto a fecha."""
    if pd.isna(date_str) or date_str is None: return pd.NaT
    
    # ✅ INTELIGENCIA DE FECHAS MEJORADA: Acepta múltiples formatos comunes.
    formats_to_try = [
        '%Y%m%d', '%d%m%Y', '%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y',
        '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'
    ]
    for fmt in formats_to_try:
        try:
            return pd.to_datetime(str(date_str), format=fmt, errors='raise').normalize()
        except (ValueError, TypeError):
            continue
    # Intento final genérico
    return pd.to_datetime(str(date_str), errors='coerce').normalize()

# --- LÓGICA DE CARGA DE DATOS ---
@st.cache_data(show_spinner="Descargando datos del ERP desde Dropbox...", ttl=1800)
def load_erp_data_from_dropbox():
    """Carga, limpia y procesa los datos del ERP. Esta es la fuente principal."""
    try:
        dbx = dropbox.Dropbox(
            oauth2_refresh_token=st.secrets.dropbox["refresh_token"],
            app_key=st.secrets.dropbox["app_key"],
            app_secret=st.secrets.dropbox["app_secret"]
        )
        _, res = dbx.files_download(DROPBOX_FILE_PATH)
        column_names = [
            'nombre_proveedor', 'serie_almacen', 'num_entrada',
            'num_factura', 'tipo_documento_erp', 'fecha_emision',
            'fecha_vencimiento', 'valor_total'
        ]
        df = pd.read_csv(io.StringIO(res.content.decode('latin1')), sep='{', header=None, names=column_names, engine='python')
        
        st.success(f"✅ ERP: Se leyeron {len(df)} registros desde Dropbox.")
        df['valor_total'] = df['valor_total'].apply(clean_monetary_value)
        df['fecha_emision'] = df['fecha_emision'].apply(parse_date)
        df['fecha_vencimiento'] = df['fecha_vencimiento'].apply(parse_date)
        df['num_factura'] = df['num_factura'].astype(str).str.strip()
        df.dropna(subset=['num_factura', 'nombre_proveedor'], inplace=True)
        return df
    except Exception as e:
        st.error(f"❌ Error crítico al cargar datos del ERP: {e}")
    return pd.DataFrame()

def parse_invoice_xml(xml_content):
    """Extrae detalles clave de un archivo XML de factura."""
    try:
        xml_content = re.sub(r'\sxmlns="[^"]+"', '', xml_content, count=1)
        root = ET.fromstring(xml_content.encode('utf-8'))
        def find_text(paths):
            for path in paths:
                node = root.find(path)
                if node is not None and node.text: return node.text.strip()
            return None
        invoice_number = find_text(['.//{urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2}ID'])
        total_value = find_text(['.//{urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2}LegalMonetaryTotal/{urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2}PayableAmount'])
        due_date = find_text(['.//{urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2}PaymentMeans/{urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2}PaymentDueDate'])
        if not invoice_number: return None
        return {
            "num_factura_correo": invoice_number,
            "valor_total_correo": total_value or "0",
            "fecha_vencimiento_correo": due_date,
        }
    except Exception: return None

@st.cache_data(show_spinner="Buscando facturas en el correo...", ttl=600)
def fetch_invoices_from_email():
    """Busca y extrae datos de facturas desde los correos del día."""
    invoices = []
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(st.secrets.email["address"], st.secrets.email["password"])
        mail.select(f'"{EMAIL_FOLDER}"')
        today_colombia = datetime.now(COLOMBIA_TZ).strftime("%d-%b-%Y")
        _, messages = mail.search(None, f'(SINCE "{today_colombia}")')
        if not messages[0]: return pd.DataFrame()

        message_ids = messages[0].split()
        for num in message_ids:
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
                                    details = parse_invoice_xml(zf.read(name).decode('utf-8', 'ignore'))
                                    if details: invoices.append(details)
                    except zipfile.BadZipFile: continue
        mail.logout()
        st.success(f"📨 Correo: Se encontraron {len(invoices)} facturas en los correos de hoy.")
        return pd.DataFrame(invoices)
    except Exception as e:
        st.warning(f"⚠️ No se pudo conectar al correo o procesar facturas: {e}")
    return pd.DataFrame()

# --- LÓGICA DE PROCESAMIENTO Y CONCILIACIÓN ---
def process_and_merge_data(erp_df, email_df):
    """
    Función principal que toma los datos del ERP como base, los concilia
    con los del correo y calcula todos los estados y métricas.
    """
    if erp_df.empty:
        st.error("No se pudieron cargar los datos del ERP. El análisis no puede continuar.")
        return pd.DataFrame()

    # Pre-procesamiento de datos del correo
    if not email_df.empty:
        email_df.rename(columns={'num_factura_correo': 'num_factura'}, inplace=True)
        email_df['valor_total_correo'] = email_df['valor_total_correo'].apply(clean_monetary_value)
        email_df['fecha_vencimiento_correo'] = email_df['fecha_vencimiento_correo'].apply(parse_date)
        email_df.drop_duplicates(subset=['num_factura'], keep='last', inplace=True)
        # ✅ LÓGICA CENTRAL: El ERP es la base (LEFT MERGE)
        master_df = pd.merge(erp_df, email_df, on='num_factura', how='left')
    else:
        master_df = erp_df.copy()
        master_df['valor_total_correo'] = np.nan
        master_df['fecha_vencimiento_correo'] = pd.NaT

    # --- Definición del Estado de Conciliación ---
    conditions = [
        master_df['valor_total_correo'].isnull(),
        ~np.isclose(master_df['valor_total'], master_df['valor_total_correo'])
    ]
    choices = ['📬 Pendiente de Correo', '⚠️ Discrepancia']
    master_df['estado_conciliacion'] = np.select(conditions, choices, default='✅ Conciliada')

    # --- Cálculo del Estado de Pago ---
    today = pd.to_datetime(datetime.now(COLOMBIA_TZ).date())
    master_df['dias_para_vencer'] = (master_df['fecha_vencimiento'] - today).dt.days
    
    def get_status(dias):
        if pd.isna(dias): return "⚪ Desconocido"
        if dias < 0: return "🔴 Vencida"
        if 0 <= dias <= 7: return "🟠 Por Vencer"
        return "🟢 Vigente"
    master_df['estado_pago'] = master_df['dias_para_vencer'].apply(get_status)

    return master_df

# --- INTERFAZ PRINCIPAL DE LA APLICACIÓN ---
def main_app():
    load_css()
    st.image("LOGO FERREINOX SAS BIC 2024.png", width=250)
    st.title("Centro de Control y Conciliación de Facturas")
    st.divider()

    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False

    if st.sidebar.button("🔄 Sincronizar y Analizar Datos", type="primary", use_container_width=True):
        with st.spinner('Realizando sincronización completa...'):
            erp_df = load_erp_data_from_dropbox()
            email_df = fetch_invoices_from_email()
            st.session_state.master_df = process_and_merge_data(erp_df, email_df)
            st.session_state.data_loaded = True
            st.rerun()

    if not st.session_state.data_loaded:
        st.info("👋 ¡Bienvenido! Presiona 'Sincronizar' en la barra lateral para comenzar.")
        st.stop()

    master_df = st.session_state.master_df
    if master_df.empty: st.stop()
        
    # --- FILTROS GLOBALES (APLICAN A TODAS LAS PESTAÑAS) ---
    st.sidebar.header("Filtros Globales 🔎")
    proveedores_lista = sorted(master_df['nombre_proveedor'].dropna().unique().tolist())
    selected_suppliers = st.sidebar.multiselect("Filtrar por Proveedor:", proveedores_lista, default=proveedores_lista)
    
    min_date, max_date = master_df['fecha_emision'].min().date(), master_df['fecha_emision'].max().date()
    date_range = st.sidebar.date_input("Filtrar por Fecha de Emisión:", value=(min_date, max_date), min_value=min_date, max_value=max_date)

    # Aplicar filtros
    if len(date_range) == 2:
        start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
        filtered_df = master_df[
            (master_df['nombre_proveedor'].isin(selected_suppliers)) &
            (master_df['fecha_emision'] >= start_date) &
            (master_df['fecha_emision'] <= end_date)
        ]
    else:
        filtered_df = master_df[master_df['nombre_proveedor'].isin(selected_suppliers)]
    
    st.success(f"Mostrando {len(filtered_df)} de {len(master_df)} facturas según los filtros.")

    # --- PESTAÑAS DE LA INTERFAZ ---
    tab1, tab2, tab3 = st.tabs(["📊 Dashboard Principal", "🚨 Alertas de Pago", "🔍 Análisis y Conciliación"])

    with tab1:
        st.subheader("Indicadores Clave de Rendimiento (KPIs)")
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        kpi1.markdown(f'<div class="metric-card"><h3>Total Facturado (ERP)</h3><p>${filtered_df["valor_total"].sum():,.2f}</p></div>', unsafe_allow_html=True)
        kpi2.markdown(f'<div class="metric-card"><h3>Monto Vencido</h3><p>${filtered_df[filtered_df["estado_pago"] == "🔴 Vencida"]["valor_total"].sum():,.2f}</p></div>', unsafe_allow_html=True)
        kpi3.markdown(f'<div class="metric-card"><h3>Facturas Vencidas</h3><p>{len(filtered_df[filtered_df["estado_pago"] == "🔴 Vencida"])}</p></div>', unsafe_allow_html=True)
        kpi4.markdown(f'<div class="metric-card"><h3>Facturas por Vencer</h3><p>{len(filtered_df[filtered_df["estado_pago"] == "🟠 Por Vencer"])}</p></div>', unsafe_allow_html=True)

        st.divider()
        col_a, col_b = st.columns(2)
        with col_a:
            st.subheader("Distribución por Estado de Pago")
            status_counts = filtered_df['estado_pago'].value_counts().reset_index()
            chart = alt.Chart(status_counts).mark_arc(innerRadius=60).encode(
                theta="count:Q",
                color=alt.Color("estado_pago:N", title="Estado", scale=alt.Scale(
                    domain=['🔴 Vencida', '🟠 Por Vencer', '🟢 Vigente', '⚪ Desconocido'],
                    range=['#e74c3c', '#f39c12', '#2ecc71', '#bdc3c7'])),
                tooltip=['estado_pago', 'count']
            ).properties(height=350)
            st.altair_chart(chart, use_container_width=True)

        with col_b:
            st.subheader("Distribución por Estado de Conciliación")
            conc_counts = filtered_df['estado_conciliacion'].value_counts().reset_index()
            chart = alt.Chart(conc_counts).mark_arc(innerRadius=60).encode(
                theta="count:Q",
                color=alt.Color("estado_conciliacion:N", title="Conciliación", scale=alt.Scale(
                    domain=['✅ Conciliada', '📬 Pendiente de Correo', '⚠️ Discrepancia'],
                    range=['#27ae60', '#3498db', '#e67e22'])),
                tooltip=['estado_conciliacion', 'count']
            ).properties(height=350)
            st.altair_chart(chart, use_container_width=True)

    with tab2:
        st.subheader("Centro de Gestión de Pagos")
        st.markdown("##### 🔴 Facturas Vencidas (Acción Inmediata)")
        vencidas_df = filtered_df[filtered_df['estado_pago'] == '🔴 Vencida'].sort_values('dias_para_vencer')
        if not vencidas_df.empty:
            st.dataframe(vencidas_df[['nombre_proveedor', 'num_factura', 'fecha_vencimiento', 'valor_total', 'dias_para_vencer']], use_container_width=True)
        else:
            st.success("✅ ¡Excelente! No hay facturas vencidas.")
        
        st.markdown("##### 🟠 Facturas por Vencer (Próximos 7 días)")
        por_vencer_df = filtered_df[filtered_df['estado_pago'] == '🟠 Por Vencer'].sort_values('dias_para_vencer')
        if not por_vencer_df.empty:
            st.dataframe(por_vencer_df[['nombre_proveedor', 'num_factura', 'fecha_vencimiento', 'valor_total', 'dias_para_vencer']], use_container_width=True)
        else:
            st.info("ℹ️ No hay facturas próximas a vencer.")

    with tab3:
        st.subheader("Explorador y Analizador de Conciliación")
        st.markdown("Utiliza esta tabla interactiva para explorar, ordenar y filtrar todas las facturas de tu ERP.")
        
        # Filtros específicos para esta pestaña
        st.write("Filtros de Conciliación:")
        status_filter = st.multiselect(
            "Filtrar por estado de conciliación:",
            options=filtered_df['estado_conciliacion'].unique(),
            default=filtered_df['estado_conciliacion'].unique()
        )
        
        analysis_df = filtered_df[filtered_df['estado_conciliacion'].isin(status_filter)]
        
        # Columnas a mostrar en el explorador de datos
        display_cols = [
            'nombre_proveedor', 'num_factura', 'fecha_emision', 'fecha_vencimiento', 
            'valor_total', 'estado_pago', 'dias_para_vencer', 'estado_conciliacion', 
            'valor_total_correo', 'fecha_vencimiento_correo'
        ]
        
        st.info(f"Mostrando {len(analysis_df)} facturas en el explorador.")
        st.data_editor(
            analysis_df[display_cols],
            use_container_width=True,
            hide_index=True,
            column_config={
                "valor_total": st.column_config.NumberColumn(format="$ {:,.2f}"),
                "valor_total_correo": st.column_config.NumberColumn(format="$ {:,.2f}"),
                "fecha_emision": st.column_config.DateColumn(format="YYYY-MM-DD"),
                "fecha_vencimiento": st.column_config.DateColumn(format="YYYY-MM-DD"),
                "fecha_vencimiento_correo": st.column_config.DateColumn(format="YYYY-MM-DD"),
            }
        )

# --- EJECUCIÓN DE LA APLICACIÓN ---
if __name__ == "__main__":
    if check_password():
        main_app()
