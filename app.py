# app.py
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

# --- 1. CONFIGURACIÓN Y CONSTANTES ---
st.set_page_config(page_title="Gestión Inteligente de Facturas", page_icon="💡", layout="wide")

# --- Constantes Globales ---
COLOMBIA_TZ = pytz.timezone('America/Bogota')
IMAP_SERVER = "imap.gmail.com"
EMAIL_FOLDER = "TFHKA/Recepcion/Descargados"
DROPBOX_FILE_PATH = "/data/Proveedores.csv"
GSHEET_DB_NAME = "FacturasCorreo_DB"
GSHEET_REPORT_NAME = "ReporteConsolidado_Activo"

# --- 2. ESTILOS Y COMPONENTES VISUALES ---
def load_css():
    st.markdown("""
        <style>
            .metric-card { background-color: #FFFFFF; border: 1px solid #E0E0E0; border-radius: 12px; padding: 25px; box-shadow: 0 4px 12px rgba(0,0,0,0.05); text-align: center; }
            .metric-card h3 { font-size: 1.1rem; color: #4F4F4F; margin-bottom: 10px; font-weight: 600; }
            .metric-card p { font-size: 2.1rem; font-weight: 700; color: #1a1a1a; }
        </style>
    """, unsafe_allow_html=True)

# --- 3. LÓGICA DE AUTENTICACIÓN ---
def check_password():
    if not st.session_state.get("password_correct", False):
        st.header("🔒 Acceso Restringido")
        password = st.text_input("Ingresa la contraseña para acceder:", type="password")
        if st.button("Ingresar"):
            if password == st.secrets.get("password"):
                st.session_state.password_correct = True
                st.rerun()
            else: st.error("Contraseña incorrecta.")
        return False
    return True

# --- 4. FUNCIONES DE CONEXIÓN Y GOOGLE SHEETS ---
@st.cache_resource(show_spinner="Conectando a Google Sheets...")
def connect_to_google_sheets():
    try:
        creds = Credentials.from_service_account_info(st.secrets["google_credentials"])
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ Error crítico al conectar con Google Sheets: {e}")
        return None

def get_or_create_worksheet(client, sheet_key, worksheet_name):
    try:
        spreadsheet = client.open_by_key(sheet_key)
        try:
            return spreadsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            return spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols="20")
    except Exception as e:
        st.error(f"Error accediendo a la hoja de cálculo: {e}")
        return None

def update_gsheet_from_df(worksheet, df):
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

# --- 5. FUNCIONES DE LECTURA Y PARSEO DE DATOS ---
def parse_date(date_str):
    if pd.isna(date_str) or date_str is None: return pd.NaT
    formats = ['%Y%m%d', '%d%m%Y', '%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']
    for fmt in formats:
        try: return pd.to_datetime(str(date_str), format=fmt, errors='raise').normalize()
        except (ValueError, TypeError): continue
    return pd.to_datetime(str(date_str), errors='coerce').normalize()

@st.cache_data(show_spinner="Descargando datos del ERP (Dropbox)...", ttl=1800)
def load_erp_data():
    try:
        dbx = dropbox.Dropbox(oauth2_refresh_token=st.secrets.dropbox["refresh_token"], app_key=st.secrets.dropbox["app_key"], app_secret=st.secrets.dropbox["app_secret"])
        _, res = dbx.files_download(DROPBOX_FILE_PATH)
        names = ['nombre_proveedor', 'serie', 'num_entrada', 'num_factura', 'doc_erp', 'fecha_emision', 'fecha_vencimiento', 'valor_total']
        df = pd.read_csv(io.StringIO(res.content.decode('latin1')), sep='{', header=None, names=names, engine='python')
        df['valor_total'] = df['valor_total'].apply(lambda x: pd.to_numeric(x, errors='coerce'))
        df['fecha_emision'] = df['fecha_emision'].apply(parse_date)
        df['fecha_vencimiento'] = df['fecha_vencimiento'].apply(parse_date)
        df['num_factura'] = df['num_factura'].astype(str).str.strip()
        return df.dropna(subset=['num_factura', 'nombre_proveedor'])
    except Exception as e:
        st.error(f"❌ Error crítico cargando datos del ERP: {e}")
        return pd.DataFrame()

def parse_invoice_xml_robust(xml_content):
    """Lector de XML mejorado para encontrar datos clave en múltiples rutas comunes."""
    try:
        xml_content = re.sub(r'xmlns="[^"]+"', '', xml_content, count=1)
        root = ET.fromstring(xml_content.encode('utf-8'))
        ns = { 'cbc': "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2", 'cac': "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2" }

        def find_text_robust(paths):
            for path in paths:
                node = root.find(path, ns)
                if node is not None and node.text: return node.text.strip()
            return None

        # ✅ BÚSQUEDA MEJORADA: Se prueban múltiples rutas para cada dato
        invoice_number = find_text_robust(['.//cbc:ID'])
        supplier_name = find_text_robust(['.//cac:AccountingSupplierParty/cac:Party/cac:PartyLegalEntity/cbc:RegistrationName', './/cac:AccountingSupplierParty/cac:Party/cac:PartyName/cbc:Name'])
        due_date = find_text_robust(['.//cac:PaymentMeans/cbc:PaymentDueDate', './/cbc:DueDate'])
        total_value = find_text_robust(['.//cac:LegalMonetaryTotal/cbc:PayableAmount', './/cac:TaxInclusiveAmount', './/cac:LineExtensionAmount'])

        if not invoice_number: return None
        return { "num_factura": invoice_number, "nombre_proveedor_correo": supplier_name, "fecha_vencimiento_correo": due_date, "valor_total_correo": total_value or "0" }
    except Exception: return None

def fetch_new_invoices_from_email(start_date):
    """Busca y extrae datos de facturas desde los correos a partir de una fecha dada."""
    invoices = []
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(st.secrets.email["address"], st.secrets.email["password"])
        mail.select(f'"{EMAIL_FOLDER}"')
        search_query = f'(SINCE "{start_date.strftime("%d-%b-%Y")}")'
        _, messages = mail.search(None, search_query)
        if not messages[0]:
            st.info(f"📨 No se encontraron correos nuevos desde {start_date.strftime('%Y-%m-%d')}.")
            mail.logout()
            return pd.DataFrame()

        message_ids = messages[0].split()
        st.info(f"📨 Procesando {len(message_ids)} correo(s) nuevo(s)...")
        for num in message_ids:
            _, data = mail.fetch(num, "(RFC822)")
            msg = email.message_from_bytes(data[0][1])
            for part in msg.walk():
                if part.get_content_maintype() == "multipart" or part.get("Content-Disposition") is None: continue
                if part.get_filename() and part.get_filename().lower().endswith('.zip'):
                    try:
                        with zipfile.ZipFile(io.BytesIO(part.get_payload(decode=True))) as zf:
                            for name in zf.namelist():
                                if name.lower().endswith('.xml'):
                                    details = parse_invoice_xml_robust(zf.read(name).decode('utf-8', 'ignore'))
                                    if details: invoices.append(details)
                    except zipfile.BadZipFile: continue
        mail.logout()
        return pd.DataFrame(invoices)
    except Exception as e:
        st.warning(f"⚠️ No se pudo conectar al correo: {e}")
        return pd.DataFrame()

# --- 6. LÓGICA DE PROCESAMIENTO Y CONCILIACIÓN ---
def process_and_reconcile(erp_df, email_df):
    if erp_df.empty:
        st.error("El análisis no puede continuar sin datos del ERP.")
        return pd.DataFrame()
    
    if not email_df.empty:
        email_df['valor_total_correo'] = email_df['valor_total_correo'].apply(lambda x: pd.to_numeric(x, errors='coerce'))
        email_df['fecha_vencimiento_correo'] = email_df['fecha_vencimiento_correo'].apply(parse_date)
        email_df.drop_duplicates(subset=['num_factura'], keep='last', inplace=True)
        master_df = pd.merge(erp_df, email_df, on='num_factura', how='left')
    else:
        master_df = erp_df.copy()
        for col in ['nombre_proveedor_correo', 'fecha_vencimiento_correo', 'valor_total_correo']: master_df[col] = np.nan
    
    # --- Lógica de Conciliación ---
    conditions = [
        master_df['valor_total_correo'].isnull(),
        ~np.isclose(master_df['valor_total'], master_df['valor_total_correo'], atol=1.0) # Tolerancia de $1
    ]
    choices = ['📬 Pendiente de Correo', '⚠️ Discrepancia de Valor']
    master_df['estado_conciliacion'] = np.select(conditions, choices, default='✅ Conciliada')
    
    # --- Lógica de Estado de Pago ---
    today = pd.to_datetime(datetime.now(COLOMBIA_TZ).date())
    master_df['dias_para_vencer'] = (master_df['fecha_vencimiento'] - today).dt.days
    conditions_pago = [master_df['dias_para_vencer'] < 0, master_df['dias_para_vencer'] <= 7]
    choices_pago = ["🔴 Vencida", "🟠 Por Vencer"]
    master_df['estado_pago'] = np.select(conditions_pago, choices_pago, default="🟢 Vigente")
    
    return master_df

# --- 7. APLICACIÓN PRINCIPAL DE STREAMLIT ---
def main_app():
    load_css()
    st.image("LOGO FERREINOX SAS BIC 2024.png", width=250)
    st.title("Plataforma de Gestión Inteligente de Facturas")
    st.divider()

    if 'data_loaded' not in st.session_state: st.session_state.data_loaded = False

    # --- Botón de Sincronización ---
    if st.sidebar.button("🔄 Sincronizar Todo", type="primary", use_container_width=True):
        with st.spinner('Iniciando sincronización completa...'):
            gs_client = connect_to_google_sheets()
            if gs_client:
                # 1. Cargar base de datos de correos histórica desde GSheets
                db_sheet = get_or_create_worksheet(gs_client, st.secrets["google_sheet_id"], GSHEET_DB_NAME)
                historical_email_df = pd.DataFrame(db_sheet.get_all_records())
                
                # 2. Determinar desde qué fecha buscar nuevos correos
                if not historical_email_df.empty:
                    historical_email_df['fecha_lectura'] = pd.to_datetime(historical_email_df['fecha_lectura'])
                    last_date = historical_email_df['fecha_lectura'].max()
                    # ✅ BÚSQUEDA AMPLIADA: 10 días hacia atrás desde la última lectura
                    search_start_date = last_date - timedelta(days=10)
                else:
                    search_start_date = datetime.now(COLOMBIA_TZ) - timedelta(days=10)

                # 3. Buscar solo correos nuevos
                new_email_df = fetch_new_invoices_from_email(search_start_date)
                
                # 4. Combinar y actualizar la base de datos de correos
                if not new_email_df.empty:
                    new_email_df['fecha_lectura'] = datetime.now(COLOMBIA_TZ).strftime('%Y-%m-%d %H:%M:%S')
                    combined_email_df = pd.concat([historical_email_df, new_email_df]).drop_duplicates(subset=['num_factura'], keep='last')
                    update_gsheet_from_df(db_sheet, combined_email_df)
                    st.session_state.email_df = combined_email_df
                else:
                    st.session_state.email_df = historical_email_df
            
                # 5. Cargar ERP y procesar
                st.session_state.erp_df = load_erp_data()
                final_df = process_and_reconcile(st.session_state.erp_df, st.session_state.email_df)
                st.session_state.master_df = final_df
                
                # 6. Actualizar el reporte consolidado en GSheets
                report_sheet = get_or_create_worksheet(gs_client, st.secrets["google_sheet_id"], GSHEET_REPORT_NAME)
                if update_gsheet_from_df(report_sheet, final_df):
                    st.success(f"✅ Reporte consolidado actualizado en Google Sheets.")
                
                st.session_state.data_loaded = True
                st.rerun()

    if not st.session_state.get('data_loaded'):
        st.info("👋 Bienvenido. Presiona 'Sincronizar Todo' para comenzar.")
        st.stop()
    
    master_df = st.session_state.master_df
    if master_df.empty: st.stop()

    # --- Filtros Globales ---
    st.sidebar.header("Filtros Globales 🔎")
    # ... (código de filtros sin cambios)
    proveedores_lista = sorted(master_df['nombre_proveedor'].dropna().unique().tolist())
    selected_suppliers = st.sidebar.multiselect("Proveedor:", proveedores_lista, default=proveedores_lista)
    min_date, max_date = master_df['fecha_emision'].min().date(), master_df['fecha_emision'].max().date()
    date_range = st.sidebar.date_input("Fecha de Emisión:", value=(min_date, max_date), min_value=min_date, max_value=max_date)
    start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    filtered_df = master_df[(master_df['nombre_proveedor'].isin(selected_suppliers)) & (master_df['fecha_emision'] >= start_date) & (master_df['fecha_emision'] <= end_date)]
    st.success(f"Mostrando {len(filtered_df)} de {len(master_df)} facturas.")
    
    # --- Pestañas de la Interfaz ---
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Dashboard Principal", "🚨 Alertas de Pago", "🔍 Explorador de Datos", "🏢 Análisis de Proveedores"])

    with tab1:
        st.subheader("Indicadores de Pago")
        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(f'<div class="metric-card"><h3>Total Facturado (ERP)</h3><p>${filtered_df["valor_total"].sum():,.2f}</p></div>', unsafe_allow_html=True)
        vencido_df = filtered_df[filtered_df['estado_pago'] == "🔴 Vencida"]
        c2.markdown(f'<div class="metric-card"><h3>Monto Vencido</h3><p>${vencido_df["valor_total"].sum():,.2f}</p></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-card"><h3>Facturas Vencidas</h3><p>{len(vencido_df)}</p></div>', unsafe_allow_html=True)
        c4.markdown(f'<div class="metric-card"><h3>Por Vencer (7 días)</h3><p>{len(filtered_df[filtered_df["estado_pago"] == "🟠 Por Vencer"])}</p></div>', unsafe_allow_html=True)
        
        st.subheader("Indicadores de Conciliación")
        c1, c2, c3 = st.columns(3)
        conc_df = filtered_df[filtered_df['estado_conciliacion'] == '✅ Conciliada']
        pend_df = filtered_df[filtered_df['estado_conciliacion'] == '📬 Pendiente de Correo']
        disc_df = filtered_df[filtered_df['estado_conciliacion'] == '⚠️ Discrepancia de Valor']
        c1.markdown(f'<div class="metric-card"><h3>Valor Conciliado</h3><p>${conc_df["valor_total"].sum():,.2f}</p></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-card"><h3>Valor Pendiente</h3><p>${pend_df["valor_total"].sum():,.2f}</p></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-card"><h3>Valor con Discrepancia</h3><p>${disc_df["valor_total"].sum():,.2f}</p></div>', unsafe_allow_html=True)

    with tab2:
        st.subheader("Gestión de Pagos Prioritarios")
        st.markdown("##### 🔴 Facturas Vencidas (Acción Inmediata)")
        st.dataframe(vencido_df[['nombre_proveedor', 'num_factura', 'fecha_vencimiento', 'valor_total', 'dias_para_vencer']], use_container_width=True)
        st.markdown("##### 🟠 Facturas por Vencer (Próximos 7 días)")
        st.dataframe(filtered_df[filtered_df['estado_pago'] == '🟠 Por Vencer'][['nombre_proveedor', 'num_factura', 'fecha_vencimiento', 'valor_total', 'dias_para_vencer']], use_container_width=True)

    with tab3:
        st.subheader("Explorador de Datos Consolidados")
        display_cols = ['nombre_proveedor', 'num_factura', 'fecha_emision', 'fecha_vencimiento', 'valor_total', 'estado_pago', 'dias_para_vencer', 'estado_conciliacion', 'valor_total_correo']
        # ✅ FORMATO CORREGIDO Y MEJORAS VISUALES
        st.data_editor(filtered_df[display_cols], use_container_width=True, hide_index=True,
            column_config={
                "valor_total": st.column_config.NumberColumn("Valor ERP", format="$ {:,.2f}"),
                "valor_total_correo": st.column_config.NumberColumn("Valor Correo", format="$ {:,.2f}"),
                "fecha_emision": st.column_config.DateColumn("Emitida", format="YYYY-MM-DD"),
                "fecha_vencimiento": st.column_config.DateColumn("Vence", format="YYYY-MM-DD"),
                "dias_para_vencer": st.column_config.ProgressColumn("Días para Vencer", format="%d días", min_value=-90, max_value=90),
            })

    with tab4:
        st.subheader("Análisis por Proveedor")
        provider_summary = filtered_df.groupby('nombre_proveedor').agg(
            total_facturado=('valor_total', 'sum'),
            numero_facturas=('num_factura', 'count'),
            numero_discrepancias=('estado_conciliacion', lambda x: (x == '⚠️ Discrepancia de Valor').sum())
        ).reset_index().sort_values('total_facturado', ascending=False)
        
        st.markdown("##### Resumen de Facturación por Proveedor")
        st.dataframe(provider_summary, use_container_width=True, hide_index=True,
            column_config={"total_facturado": st.column_config.NumberColumn("Total Facturado", format="$ {:,.2f}")})
        
        st.markdown("##### Distribución de la Facturación")
        chart = alt.Chart(provider_summary).mark_bar().encode(
            x=alt.X('total_facturado:Q', title='Total Facturado ($)'),
            y=alt.Y('nombre_proveedor:N', sort='-x', title='Proveedor'),
            tooltip=['nombre_proveedor', alt.Tooltip('total_facturado:Q', format='$,.2f'), 'numero_facturas']
        ).properties(height=400)
        st.altair_chart(chart, use_container_width=True)

# --- 8. PUNTO DE ENTRADA ---
if __name__ == "__main__":
    if check_password():
        main_app()
