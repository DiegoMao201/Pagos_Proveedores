# app.py
import streamlit as st
import pandas as pd
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
import pytz # <-- NUEVA LIBRERÍA

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
GSHEET_NAME = "FacturasCorreo"

# --- ESTILOS VISUALES (CSS) ---
def load_css():
    """Inyecta CSS personalizado para una interfaz más moderna y atractiva."""
    st.markdown("""
        <style>
            /* Mejora la tipografía general */
            html, body, [class*="st-"] {
                font-family: 'Inter', 'Source Sans Pro', sans-serif;
            }
            /* Estilo para las tarjetas de KPIs */
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
            .metric-card h3 {
                font-size: 1.1rem;
                color: #4F4F4F;
                margin-bottom: 10px;
                font-weight: 500;
            }
            .metric-card p {
                font-size: 2.2rem;
                font-weight: 700;
                color: #1a1a1a;
            }
            /* Estilo para el botón de sincronización */
            .stButton>button {
                border-radius: 10px;
                font-weight: bold;
            }
        </style>
    """, unsafe_allow_html=True)

# --- LÓGICA DE AUTENTICACIÓN ---
def check_password():
    """Verifica si la contraseña ingresada por el usuario es correcta."""
    if "password_correct" not in st.session_state:
        st.session_state.password_correct = False

    if not st.session_state.password_correct:
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
    """Limpia y convierte un valor monetario a tipo float de forma segura."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        # Elimina símbolos de moneda, espacios y puntos de miles. Reemplaza coma decimal.
        value = re.sub(r'[$\s]', '', value)
        if '.' in value and ',' in value: # Formato 1.234,56
             value = value.replace('.', '').replace(',', '.')
        else: # Formato 1,234.56
             value = value.replace(',', '')
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    return 0.0

def parse_date(date_str):
    """Convierte una cadena de texto a un objeto de fecha, manejando varios formatos."""
    if pd.isna(date_str) or date_str is None:
        return pd.NaT
    # Intenta parsear con múltiples formatos comunes
    for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y'):
        try:
            return pd.to_datetime(str(date_str), format=fmt, errors='coerce').normalize()
        except (ValueError, TypeError):
            continue
    # Intento final genérico
    return pd.to_datetime(str(date_str), errors='coerce').normalize()

# --- CONEXIÓN A GOOGLE SHEETS ---
@st.cache_resource(show_spinner="Conectando a Google Sheets...")
def connect_to_google_sheets():
    try:
        scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(st.secrets["google_credentials"], scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ Error crítico al conectar con Google Sheets: {e}", icon="🔥")
        return None

# --- LÓGICA DE CARGA Y PROCESAMIENTO DE DATOS ---
@st.cache_data(show_spinner="Descargando datos del ERP desde Dropbox...", ttl=3600)
def load_erp_data_from_dropbox():
    """Carga, limpia y procesa los datos del ERP desde un CSV en Dropbox."""
    try:
        dbx = dropbox.Dropbox(
            oauth2_refresh_token=st.secrets.dropbox["refresh_token"],
            app_key=st.secrets.dropbox["app_key"],
            app_secret=st.secrets.dropbox["app_secret"]
        )
        st.info(f"📥 Descargando archivo `{DROPBOX_FILE_PATH}` desde Dropbox...")
        _, res = dbx.files_download(DROPBOX_FILE_PATH)

        # ✅ CORRECCIÓN: Se define la estructura de 8 columnas basada en el ejemplo del usuario.
        column_names = [
            'nombre_proveedor_erp', 'serie_almacen', 'num_entrada',
            'num_factura', 'tipo_documento_erp', 'fecha_emision_erp',
            'fecha_vencimiento_erp', 'valor_total_erp'
        ]

        with io.StringIO(res.content.decode('latin1')) as csv_file:
            df = pd.read_csv(csv_file, sep='{', header=None, names=column_names, engine='python', on_bad_lines='warn')

        if df.shape[1] != len(column_names):
            st.error(f"🔥 Error Crítico de formato en `Proveedores.csv`.", icon="❌")
            st.error(f"Se esperaban {len(column_names)} columnas separadas por '{{', pero se encontraron {df.shape[1]}. Por favor, verifica el archivo en Dropbox.")
            return pd.DataFrame()
        
        st.success("✅ Archivo CSV del ERP leído correctamente desde Dropbox.")

        # Limpieza y conversión de tipos
        df['valor_total_erp'] = df['valor_total_erp'].apply(clean_monetary_value)
        df['fecha_emision_erp'] = df['fecha_emision_erp'].apply(parse_date)
        df['fecha_vencimiento_erp'] = df['fecha_vencimiento_erp'].apply(parse_date)
        df['num_factura'] = df['num_factura'].astype(str).str.strip()
        df.dropna(subset=['num_factura', 'nombre_proveedor_erp'], inplace=True)
        return df

    except dropbox.exceptions.ApiError as e:
        st.error(f"❌ Error de API de Dropbox: No se pudo encontrar o acceder al archivo `{DROPBOX_FILE_PATH}`. Detalles: {e}", icon="📦")
    except Exception as e:
        st.error(f"❌ Error crítico al cargar datos desde Dropbox: {e}", icon="🔥")
    return pd.DataFrame()

def parse_invoice_xml(xml_content):
    """Extrae detalles de la factura del contenido XML de forma robusta."""
    try:
        # Limpieza de namespaces para simplificar la búsqueda
        xml_content = re.sub(r'\sxmlns="[^"]+"', '', xml_content, count=1)
        root = ET.fromstring(xml_content.encode('utf-8'))
        ns = {
            'cac': "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2",
            'cbc': "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2",
        }

        def find_text(paths):
            for path in paths:
                node = root.find(path, ns)
                if node is not None and node.text:
                    return node.text.strip()
            return None

        invoice_number = find_text(['.//cbc:ID', './cbc:ID'])
        supplier_name = find_text([
            './/cac:AccountingSupplierParty/cac:Party/cac:PartyLegalEntity/cbc:RegistrationName',
            './/cac:AccountingSupplierParty/cac:Party/cac:PartyName/cbc:Name'
        ])
        issue_date = find_text(['.//cbc:IssueDate', './cbc:IssueDate'])
        due_date = find_text(['.//cac:PaymentMeans/cbc:PaymentDueDate', './/cbc:DueDate'])
        total_value = find_text(['.//cac:LegalMonetaryTotal/cbc:PayableAmount'])

        if not invoice_number:
            return None # Si no hay número de factura, es inválido

        return {
            "num_factura": invoice_number,
            "nombre_proveedor_correo": supplier_name or "No identificado en XML",
            "fecha_emision_correo": issue_date,
            "fecha_vencimiento_correo": due_date or issue_date, # Usa fecha de emisión si no hay vencimiento
            "valor_total_correo": total_value or "0",
        }
    except ET.ParseError:
        return None # Falla silenciosa si no es un XML válido
    except Exception:
        return None

@st.cache_data(show_spinner="Buscando nuevas facturas en el correo...", ttl=600)
def fetch_invoices_from_email():
    """Busca y procesa nuevas facturas de los adjuntos de correo del día actual."""
    invoices = []
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(st.secrets.email["address"], st.secrets.email["password"])
        st.info("✅ Conexión exitosa al servidor de correo.")

        status, _ = mail.select(f'"{EMAIL_FOLDER}"')
        if status != 'OK':
            st.error(f"❌ No se pudo seleccionar la carpeta: '{EMAIL_FOLDER}'.", icon="📁")
            mail.logout()
            return pd.DataFrame()

        # ✅ CORRECCIÓN: Usa la fecha actual de Colombia para la búsqueda.
        today_colombia = datetime.now(COLOMBIA_TZ).strftime("%d-%b-%Y")
        st.info(f"🔎 Buscando correos en `{EMAIL_FOLDER}` desde el {today_colombia} (hora Colombia).")
        
        status, messages = mail.search(None, f'(SINCE "{today_colombia}")')
        if status != 'OK' or not messages[0]:
            st.info(f"ℹ️ No se encontraron correos nuevos hoy.")
            mail.logout()
            return pd.DataFrame()

        message_ids = messages[0].split()
        progress_text = f"Procesando {len(message_ids)} correo(s) nuevo(s)..."
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
                                        invoices.append(details)
                    except zipfile.BadZipFile:
                        continue # Ignora ZIPs corruptos
            progress_bar.progress((i + 1) / len(message_ids), text=progress_text)
        
        progress_bar.empty()
        mail.logout()
        return pd.DataFrame(invoices)

    except imaplib.IMAP4.error as e:
        st.error(f"❌ Error de conexión de correo (IMAP): {e}", icon="🔑")
    except Exception as e:
        st.error(f"❌ Error crítico al procesar correos: {e}", icon="🔥")
    return pd.DataFrame()

def process_and_merge_data(erp_df, email_df):
    """Combina, limpia y enriquece los datos de ambas fuentes."""
    if erp_df.empty and email_df.empty:
        st.warning("No hay datos disponibles de ninguna fuente para procesar.")
        return pd.DataFrame()

    # Pre-procesamiento de datos del correo
    if not email_df.empty:
        email_df['valor_total_correo'] = email_df['valor_total_correo'].apply(clean_monetary_value)
        email_df['fecha_emision_correo'] = email_df['fecha_emision_correo'].apply(parse_date)
        email_df['fecha_vencimiento_correo'] = email_df['fecha_vencimiento_correo'].apply(parse_date)
        email_df['num_factura'] = email_df['num_factura'].astype(str).str.strip()

    # Combinar datos del ERP y del correo
    if not erp_df.empty and not email_df.empty:
        merged_df = pd.merge(erp_df, email_df, on='num_factura', how='outer')
    elif not erp_df.empty:
        merged_df = erp_df.copy()
    else:
        merged_df = email_df.copy()

    # Consolidar columnas
    merged_df['nombre_proveedor'] = merged_df['nombre_proveedor_erp'].fillna(merged_df.get('nombre_proveedor_correo', pd.Series(dtype='str')))
    merged_df['fecha_emision'] = merged_df['fecha_emision_erp'].fillna(merged_df.get('fecha_emision_correo', pd.Series(dtype='datetime64[ns]')))
    merged_df['fecha_vencimiento'] = merged_df['fecha_vencimiento_erp'].fillna(merged_df.get('fecha_vencimiento_correo', pd.Series(dtype='datetime64[ns]')))
    merged_df['valor_total'] = merged_df['valor_total_erp'].fillna(merged_df.get('valor_total_correo', pd.Series(dtype='float')))
    
    merged_df.dropna(subset=['num_factura', 'nombre_proveedor', 'fecha_vencimiento'], inplace=True)
    merged_df = merged_df[merged_df['num_factura'].str.strip() != '']

    # Calcular estado de la factura
    today = pd.to_datetime(datetime.now(COLOMBIA_TZ).date())
    merged_df['dias_para_vencer'] = (merged_df['fecha_vencimiento'] - today).dt.days
    
    def get_status(dias):
        if pd.isna(dias): return "⚪ Desconocido"
        if dias < 0: return "🔴 Vencida"
        if 0 <= dias <= 7: return "🟠 Por Vencer"
        return "🟢 Vigente"
    merged_df['estado'] = merged_df['dias_para_vencer'].apply(get_status)

    return merged_df

# --- INTERFAZ PRINCIPAL DE LA APLICACIÓN ---
def main_app():
    load_css()
    
    st.image("LOGO FERREINOX SAS BIC 2024.png", width=250)
    st.title("Centro de Control de Facturación Inteligente")
    st.markdown("Sistema proactivo para la conciliación, análisis y gestión de pagos a proveedores.")
    st.divider()

    # --- LÓGICA DE SINCRONIZACIÓN ---
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False

    if st.sidebar.button("🔄 Sincronizar Datos", type="primary", use_container_width=True):
        with st.spinner('Realizando sincronización completa...'):
            erp_df = load_erp_data_from_dropbox()
            email_df = fetch_invoices_from_email() # Usamos una función que no guarda en Google Sheets directamente
            
            # Aquí podrías añadir la lógica para actualizar Google Sheets si lo necesitas
            
            st.session_state.master_df = process_and_merge_data(erp_df, email_df)
            st.session_state.data_loaded = True
            st.success(f"¡Sincronización completa! Se procesaron {len(st.session_state.master_df)} facturas.")
            st.rerun()

    if not st.session_state.data_loaded:
        st.info("👋 ¡Bienvenido! Presiona 'Sincronizar Datos' en la barra lateral para comenzar.")
        st.stop()

    master_df = st.session_state.master_df
    if master_df.empty:
        st.warning("No se encontraron datos de facturas para mostrar después de la sincronización.")
        st.stop()
        
    # --- FILTROS EN LA BARRA LATERAL ---
    st.sidebar.header("Filtros Globales 🔎")
    proveedores_lista = sorted(master_df['nombre_proveedor'].dropna().unique().tolist())
    selected_suppliers = st.sidebar.multiselect("Filtrar por Proveedor:", proveedores_lista, default=proveedores_lista)
    
    min_date = master_df['fecha_emision'].min().date()
    max_date = master_df['fecha_emision'].max().date()
    date_range = st.sidebar.date_input("Filtrar por Fecha de Emisión:", value=(min_date, max_date), min_value=min_date, max_value=max_date)

    if len(date_range) == 2:
        start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
        filtered_df = master_df[
            (master_df['nombre_proveedor'].isin(selected_suppliers)) &
            (master_df['fecha_emision'] >= start_date) &
            (master_df['fecha_emision'] <= end_date)
        ]
    else:
        filtered_df = master_df.copy() # Si el rango no es válido, no filtra por fecha

    # --- PESTAÑAS DE LA INTERFAZ ---
    tab1, tab2, tab3 = st.tabs(["📊 Dashboard Principal", "🚨 Alertas y Acciones", "🔍 Análisis de Datos"])

    with tab1:
        st.subheader("Indicadores Clave de Rendimiento (KPIs)")
        total_facturado = filtered_df['valor_total'].sum()
        total_vencido = filtered_df[filtered_df['estado'] == '🔴 Vencida']['valor_total'].sum()
        n_vencidas = len(filtered_df[filtered_df['estado'] == '🔴 Vencida'])
        n_por_vencer = len(filtered_df[filtered_df['estado'] == '🟠 Por Vencer'])

        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        with kpi1:
            st.markdown(f'<div class="metric-card"><h3>Total Facturado (Filtrado)</h3><p>${total_facturado:,.2f}</p></div>', unsafe_allow_html=True)
        with kpi2:
            st.markdown(f'<div class="metric-card"><h3>Monto Total Vencido</h3><p>${total_vencido:,.2f}</p></div>', unsafe_allow_html=True)
        with kpi3:
            st.markdown(f'<div class="metric-card"><h3>Nº Facturas Vencidas</h3><p>{n_vencidas}</p></div>', unsafe_allow_html=True)
        with kpi4:
            st.markdown(f'<div class="metric-card"><h3>Nº Facturas por Vencer (7 días)</h3><p>{n_por_vencer}</p></div>', unsafe_allow_html=True)

        st.divider()
        col_a, col_b = st.columns(2)
        with col_a:
            st.subheader("Distribución por Estado de Facturas")
            if not filtered_df.empty:
                status_counts = filtered_df['estado'].value_counts().reset_index()
                chart_status = alt.Chart(status_counts).mark_arc(innerRadius=60).encode(
                    theta=alt.Theta(field="count", type="quantitative"),
                    color=alt.Color(field="estado", type="nominal", title="Estado", scale=alt.Scale(
                        domain=['🔴 Vencida', '🟠 Por Vencer', '🟢 Vigente', '⚪ Desconocido'],
                        range=['#e74c3c', '#f39c12', '#2ecc71', '#bdc3c7']
                    )), tooltip=['estado', 'count']
                ).properties(height=350)
                st.altair_chart(chart_status, use_container_width=True)

        with col_b:
            st.subheader("Facturación Mensual")
            if not filtered_df.empty:
                monthly_total = filtered_df.set_index('fecha_emision').resample('M')['valor_total'].sum().reset_index()
                monthly_total['mes'] = monthly_total['fecha_emision'].dt.strftime('%Y-%b')
                chart_monthly = alt.Chart(monthly_total).mark_bar(color='#3498db').encode(
                    x=alt.X('mes:N', sort=None, title='Mes'),
                    y=alt.Y('valor_total:Q', title='Monto Total Facturado'),
                    tooltip=['mes', alt.Tooltip('valor_total:Q', format='$,.2f')]
                ).properties(height=350)
                st.altair_chart(chart_monthly, use_container_width=True)

    with tab2:
        st.subheader("Centro de Gestión de Pagos")
        st.markdown("##### 🔴 Facturas Vencidas (Acción Inmediata)")
        vencidas_df = filtered_df[filtered_df['estado'] == '🔴 Vencida'].sort_values('dias_para_vencer')
        if not vencidas_df.empty:
            st.dataframe(vencidas_df[['nombre_proveedor', 'num_factura', 'fecha_vencimiento', 'valor_total', 'dias_para_vencer']].style.format({
                'valor_total': '${:,.2f}', 'fecha_vencimiento': '{:%Y-%m-%d}'
            }).background_gradient(cmap='Reds_r', subset=['dias_para_vencer']), use_container_width=True)
        else:
            st.success("✅ ¡Excelente! No hay facturas vencidas.")
        
        st.markdown("##### 🟠 Facturas por Vencer (Próximos 7 días)")
        por_vencer_df = filtered_df[filtered_df['estado'] == '🟠 Por Vencer'].sort_values('dias_para_vencer')
        if not por_vencer_df.empty:
            st.dataframe(por_vencer_df[['nombre_proveedor', 'num_factura', 'fecha_vencimiento', 'valor_total', 'dias_para_vencer']].style.format({
                'valor_total': '${:,.2f}', 'fecha_vencimiento': '{:%Y-%m-%d}'
            }).background_gradient(cmap='Oranges_r', subset=['dias_para_vencer']), use_container_width=True)
        else:
            st.info("ℹ️ No hay facturas próximas a vencer en los siguientes 7 días.")

    with tab3:
        st.subheader("Análisis de Conciliación y Datos Completos")
        
        st.markdown("##### ❗ Análisis de Discrepancias")
        unmatched_erp = master_df[master_df['nombre_proveedor_correo'].isnull() & master_df['nombre_proveedor_erp'].notnull()]
        unmatched_email = master_df[master_df['nombre_proveedor_erp'].isnull() & master_df['nombre_proveedor_correo'].notnull()]
        
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Facturas en ERP, no en Correo:**")
            st.dataframe(unmatched_erp[['num_factura', 'nombre_proveedor_erp', 'valor_total_erp']], use_container_width=True)
        with col2:
            st.write("**Facturas en Correo, no en ERP:**")
            st.dataframe(unmatched_email[['num_factura', 'nombre_proveedor_correo', 'valor_total_correo']], use_container_width=True)

        st.divider()
        st.markdown("##### 🔍 Explorador de Datos Consolidados")
        st.dataframe(filtered_df, use_container_width=True)
        
        csv = filtered_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Descargar Datos Filtrados como CSV",
            data=csv,
            file_name=f'reporte_facturacion_{datetime.now(COLOMBIA_TZ).strftime("%Y%m%d")}.csv',
            mime='text/csv',
        )

# --- EJECUCIÓN DE LA APLICACIÓN ---
if __name__ == "__main__":
    if check_password():
        main_app()
