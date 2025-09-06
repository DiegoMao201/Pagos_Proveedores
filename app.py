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
import pytz # Importante para manejar zonas horarias

# --- Configuración de la página de Streamlit ---
st.set_page_config(
    page_title="Control de Facturación IA",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Lógica de Autenticación ---
def check_password():
    """Verifica si la contraseña ingresada por el usuario es correcta."""
    def password_correct():
        return st.session_state.get("password") == st.secrets.get("password")

    if not st.session_state.get("password_correct", False):
        st.header("🔒 Acceso Restringido al Centro de Control")
        with st.form("login_form"):
            st.markdown("Por favor, ingresa la contraseña para acceder al sistema.")
            password = st.text_input("Contraseña", type="password", key="password")
            st.form_submit_button("Ingresar", on_click=lambda: st.session_state.update({"password_correct": password_correct()}))
        
        if "password" in st.session_state and st.session_state["password"] and not st.session_state["password_correct"]:
            st.error("Contraseña incorrecta. Por favor, intenta de nuevo.")
        return False
    return True

# --- Funciones Auxiliares para Limpieza de Datos ---
def clean_monetary_value(value):
    """Limpia y convierte un valor monetario a tipo float de forma segura."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        value = re.sub(r'[$\s.]', '', value)
        value = value.replace(',', '.')
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    return 0.0

def parse_date(date_str):
    """Convierte una cadena de texto a un objeto de fecha, manejando varios formatos."""
    if pd.isna(date_str) or date_str is None:
        return pd.NaT
    # Intenta convertir la fecha usando varios formatos conocidos
    for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f'):
        try:
            # Normaliza para quitar la parte de la hora
            return pd.to_datetime(str(date_str), format=fmt).normalize()
        except (ValueError, TypeError):
            continue
    # Si ningún formato funciona, intenta el conversor genérico de pandas
    try:
        return pd.to_datetime(str(date_str)).normalize()
    except (ValueError, TypeError):
        return pd.NaT

# --- Funciones de Conexión a Servicios Externos ---
@st.cache_resource(show_spinner="Conectando a Google Sheets...")
def connect_to_google_sheets():
    try:
        scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(st.secrets["google_credentials"], scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ Error crítico al conectar con Google Sheets: {e}", icon="🔥")
        return None

def load_data_from_gsheet(client, sheet_name):
    try:
        spreadsheet = client.open_by_key(st.secrets["google_sheet_id"])
        worksheet = spreadsheet.worksheet(sheet_name)
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        required_cols = ["num_factura", "nombre_proveedor_correo", "fecha_emision_correo", "fecha_vencimiento_correo", "valor_total_correo"]
        for col in required_cols:
            if col not in df.columns:
                df[col] = pd.Series(dtype='object')
        return df
    except gspread.exceptions.WorksheetNotFound:
        st.warning(f"⚠️ La hoja '{sheet_name}' no existe. Se usará una tabla vacía.", icon="📝")
        return pd.DataFrame(columns=required_cols)
    except Exception as e:
        st.error(f"❌ Error al leer datos desde Google Sheets: {e}", icon="🔥")
        return pd.DataFrame()

def update_gsheet_from_df(client, sheet_name, df):
    try:
        spreadsheet = client.open_by_key(st.secrets["google_sheet_id"])
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
        df_to_upload = df.copy()
        for col in df_to_upload.select_dtypes(include=['datetime64[ns]']).columns:
            df_to_upload[col] = df_to_upload[col].dt.strftime('%Y-%m-%d').replace({pd.NaT: ''})
        df_to_upload = df_to_upload.astype(str).replace({'nan': '', 'NaT': ''})
        worksheet.update([df_to_upload.columns.values.tolist()] + df_to_upload.values.tolist())
        return True
    except Exception as e:
        st.error(f"❌ Error al actualizar la hoja de Google Sheets: {e}", icon="🔥")
        return False

# --- Lógica de Carga y Procesamiento de Datos ---
@st.cache_data(show_spinner="Cargando datos del ERP desde Dropbox...", ttl=3600)
def load_erp_data_from_dropbox():
    """Carga y procesa los datos del ERP desde un CSV en Dropbox, ajustado a la estructura correcta."""
    try:
        dbx = dropbox.Dropbox(
            oauth2_refresh_token=st.secrets["dropbox"]["refresh_token"],
            app_key=st.secrets["dropbox"]["app_key"],
            app_secret=st.secrets["dropbox"]["app_secret"]
        )
        dbx.users_get_current_account()
        
        file_path = "/data/Proveedores.csv"
        st.info(f"📥 Descargando `{file_path}` desde Dropbox...")
        _, res = dbx.files_download(file_path)
        
        separator = '{'
        
        with io.StringIO(res.content.decode('latin1')) as csv_file:
            df = pd.read_csv(csv_file, sep=separator, header=None, engine='python', on_bad_lines='skip')

        if df.empty:
            st.warning("⚠️ El archivo CSV de Dropbox se leyó pero está vacío.", icon="🧐")
            return pd.DataFrame()
        
        # **CORRECCIÓN CLAVE**: El archivo tiene 8 columnas. Mapeamos las correctas.
        # VILLADA SALAZAR LUIS CARLOS{J57C{10115{8493{P{2024-01-02{...{55992.0
        # Col 0: Nombre, Col 3: Factura, Col 5: Emisión, Col 6: Vencimiento, Col 7: Valor
        
        if df.shape[1] < 8:
            st.error(f"🔥 Error de formato en CSV. Se esperaban 8 columnas, pero se encontraron {df.shape[1]}.", icon="❌")
            st.info("Por favor, verifica la estructura del archivo `Proveedores.csv` en Dropbox.")
            return None

        # Seleccionamos y renombramos solo las columnas que nos interesan
        column_selection = {
            0: 'nombre_proveedor_erp',
            3: 'num_factura',
            5: 'fecha_emision_erp',
            6: 'fecha_vencimiento_erp',
            7: 'valor_total_erp'
        }
        
        df_processed = df[list(column_selection.keys())].copy()
        df_processed.rename(columns=column_selection, inplace=True)

        st.success("✅ Datos del ERP cargados y procesados correctamente.")

        # Limpieza de datos
        df_processed['valor_total_erp'] = df_processed['valor_total_erp'].apply(clean_monetary_value)
        df_processed['fecha_emision_erp'] = df_processed['fecha_emision_erp'].apply(parse_date)
        df_processed['fecha_vencimiento_erp'] = df_processed['fecha_vencimiento_erp'].apply(parse_date)
        df_processed['num_factura'] = df_processed['num_factura'].astype(str).str.strip()
        df_processed.dropna(subset=['num_factura', 'nombre_proveedor_erp'], inplace=True)

        return df_processed

    except dropbox.exceptions.ApiError as e:
        st.error(f"❌ Error de API de Dropbox: {e}. ¿El archivo `{file_path}` existe?", icon="📦")
        return None
    except Exception as e:
        st.error(f"❌ Error crítico al cargar datos desde Dropbox: {e}", icon="🔥")
        return None

def parse_invoice_xml(xml_content):
    """Extrae detalles de la factura del XML de forma robusta, priorizando datos legibles."""
    try:
        xml_content = re.sub(r' xmlns="[^"]+"', '', xml_content, count=1)
        root = ET.fromstring(xml_content)
        ns = {
            'cac': 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
            'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
        }

        def find_text_with_fallbacks(element, paths, namespaces):
            for path in paths:
                node = element.find(path, namespaces)
                if node is not None and node.text is not None:
                    return node.text.strip()
            return None

        # **LÓGICA MEJORADA**: Se buscan múltiples rutas para cada dato.
        # Para el número de factura, se priorizan los que no parecen un hash largo.
        all_invoice_ids = [node.text.strip() for node in root.findall('.//cbc:ID', ns) if node.text is not None]
        invoice_number = next((id_ for id_ in all_invoice_ids if len(id_) < 20 and not re.match(r'^[a-f0-9]{32}$', id_)), None)
        if not invoice_number and all_invoice_ids:
            invoice_number = all_invoice_ids[0] # Si solo hay hashes, toma el primero

        supplier_name_paths = [
            './/cac:AccountingSupplierParty/cac:Party/cac:PartyName/cbc:Name',
            './/cac:AccountingSupplierParty/cac:Party/cac:PartyLegalEntity/cbc:RegistrationName',
        ]
        total_value_paths = ['.//cac:LegalMonetaryTotal/cbc:PayableAmount']
        issue_date_paths = ['.//cbc:IssueDate']
        due_date_paths = ['.//cbc:DueDate', './/cac:PaymentMeans/cbc:PaymentDueDate']
        
        supplier_name = find_text_with_fallbacks(root, supplier_name_paths, ns)
        total_value = find_text_with_fallbacks(root, total_value_paths, ns)
        issue_date = find_text_with_fallbacks(root, issue_date_paths, ns)
        due_date = find_text_with_fallbacks(root, due_date_paths, ns)

        if not invoice_number:
            return None

        return {
            "num_factura": invoice_number,
            "nombre_proveedor_correo": supplier_name or "N/A",
            "fecha_emision_correo": issue_date,
            "fecha_vencimiento_correo": due_date or issue_date,
            "valor_total_correo": total_value or "0",
        }
    except Exception:
        return None

@st.cache_data(show_spinner="Buscando nuevas facturas en el correo...", ttl=600)
def fetch_todays_invoices_from_email():
    """Busca y procesa facturas de correos del día actual en la zona horaria de Colombia."""
    invoices = []
    try:
        # **CORRECCIÓN CLAVE**: Usar la zona horaria de Colombia.
        colombia_tz = pytz.timezone('America/Bogota')
        today_date_str = datetime.now(colombia_tz).strftime("%d-%b-%Y")
        st.info(f"🇨🇴 Buscando correos con fecha: {today_date_str} (Hora de Colombia)")
        
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(st.secrets["email"]["address"], st.secrets["email"]["password"])
        
        folder = "TFHKA/Recepcion/Descargados"
        mail.select(f'"{folder}"')
        
        status, messages = mail.search(None, f'(SINCE "{today_date_str}")')

        if status != 'OK' or not messages[0]:
            st.info(f"ℹ️ No se encontraron correos nuevos en `{folder}` para hoy.")
            mail.logout()
            return pd.DataFrame(), 0

        message_ids = messages[0].split()
        st.info(f"📬 {len(message_ids)} correo(s) nuevo(s) encontrado(s). Procesando...")

        for num in message_ids:
            _, data = mail.fetch(num, "(RFC822)")
            msg = email.message_from_bytes(data[0][1])
            for part in msg.walk():
                if part.get_content_maintype() == "multipart" or part.get("Content-Disposition") is None: continue
                filename = part.get_filename()
                if filename and filename.lower().endswith('.zip'):
                    try:
                        with zipfile.ZipFile(io.BytesIO(part.get_payload(decode=True))) as zip_file:
                            for name in zip_file.namelist():
                                if name.lower().endswith('.xml'):
                                    xml_content = zip_file.read(name).decode('utf-8', 'ignore')
                                    invoice_details = parse_invoice_xml(xml_content)
                                    if invoice_details: invoices.append(invoice_details)
                    except zipfile.BadZipFile: continue
        mail.logout()
        
        if invoices: st.success(f"📧 Se extrajeron {len(invoices)} facturas nuevas del correo.")
        else: st.info("ℹ️ Se procesaron correos, pero no se extrajeron facturas válidas.")
        
        return pd.DataFrame(invoices), len(invoices)

    except Exception as e:
        st.error(f"❌ Error crítico al procesar correos: {e}", icon="🔥")
        return pd.DataFrame(), 0

# --- Interfaz Principal de la Aplicación ---
def main_app():
    
    st.image("LOGO FERREINOX SAS BIC 2024.png", width=250)
    st.title("Centro de Control de Facturación Inteligente")
    st.markdown("Sistema proactivo para la conciliación, análisis y predicción de pagos a proveedores.")
    st.markdown("---")

    # --- BARRA LATERAL (SIDEBAR) ---
    with st.sidebar:
        st.header("Panel de Control ⚙️")
        if st.button("🔌 Sincronizar Todos los Datos", type="primary", use_container_width=True):
            with st.spinner('Realizando sincronización completa...'):
                st.session_state.clear()
                
                gs_client = connect_to_google_sheets()
                if not gs_client: return

                erp_df = load_erp_data_from_dropbox()
                historical_email_df = load_data_from_gsheet(gs_client, "FacturasCorreo")
                todays_email_df, new_invoices_count = fetch_todays_invoices_from_email()

                if not todays_email_df.empty:
                    combined_df = pd.concat([historical_email_df, todays_email_df], ignore_index=True)
                    combined_df.drop_duplicates(subset=['num_factura'], keep='last', inplace=True)
                    if update_gsheet_from_df(gs_client, "FacturasCorreo", combined_df):
                        st.success(f"✅ Base de datos de correos actualizada.")
                    email_df = combined_df.copy()
                else:
                    email_df = historical_email_df.copy()

                st.session_state['erp_df'] = erp_df
                st.session_state['email_df'] = email_df
                
                # Guardar estado para la sidebar
                st.session_state['last_sync_time'] = datetime.now(pytz.timezone('America/Bogota')).strftime("%I:%M %p, %d %b")
                st.session_state['erp_rows_loaded'] = len(erp_df) if erp_df is not None else 0
                st.session_state['email_invoices_found'] = new_invoices_count
                st.session_state['data_loaded'] = True
        
        st.markdown("---")
        st.header("Estado de Sincronización 📊")
        if 'data_loaded' in st.session_state:
            st.info(f"**Última Sincronización:**\n{st.session_state.get('last_sync_time', 'N/A')}")
            st.metric("Registros Cargados del ERP", st.session_state.get('erp_rows_loaded', 0))
            st.metric("Nuevas Facturas del Correo", st.session_state.get('email_invoices_found', 0))
        else:
            st.info("Presiona 'Sincronizar' para cargar los datos más recientes.")
        
        st.markdown("---")
        st.header("Filtros Globales 🔎")
        
    # --- LÓGICA DE PROCESAMIENTO Y VISUALIZACIÓN ---
    if st.session_state.get('data_loaded', False):
        erp_df = st.session_state.get('erp_df', pd.DataFrame())
        email_df = st.session_state.get('email_df', pd.DataFrame())

        # Limpieza y estandarización de datos de correo
        required_email_cols = ["num_factura", "nombre_proveedor_correo", "fecha_emision_correo", "fecha_vencimiento_correo", "valor_total_correo"]
        for col in required_email_cols:
            if col not in email_df.columns: email_df[col] = pd.NA
        email_df['valor_total_correo'] = email_df['valor_total_correo'].apply(clean_monetary_value)
        email_df['fecha_emision_correo'] = email_df['fecha_emision_correo'].apply(parse_date)
        email_df['fecha_vencimiento_correo'] = email_df['fecha_vencimiento_correo'].apply(parse_date)
        email_df['num_factura'] = email_df['num_factura'].astype(str).str.strip()

        # Fusión de datos
        merged_df = pd.merge(erp_df, email_df, on='num_factura', how='outer') if erp_df is not None and not erp_df.empty else email_df.copy()
        
        # Consolidación de columnas
        merged_df['fecha_emision'] = merged_df['fecha_emision_erp'].fillna(merged_df['fecha_emision_correo'])
        merged_df['fecha_vencimiento'] = merged_df['fecha_vencimiento_erp'].fillna(merged_df['fecha_vencimiento_correo'])
        merged_df['valor_total'] = merged_df['valor_total_erp'].fillna(merged_df['valor_total_correo'])
        merged_df['nombre_proveedor'] = merged_df['nombre_proveedor_erp'].fillna(merged_df['nombre_proveedor_correo'])
        
        merged_df.dropna(subset=['num_factura', 'fecha_emision', 'fecha_vencimiento', 'nombre_proveedor'], inplace=True)
        merged_df = merged_df[merged_df['nombre_proveedor'] != 'N/A']

        # Cálculo de estado
        today = pd.to_datetime(datetime.now(pytz.timezone('America/Bogota')).date())
        merged_df['dias_para_vencer'] = (merged_df['fecha_vencimiento'] - today).dt.days
        
        def get_status(dias):
            if pd.isna(dias): return "⚪ Desconocido"
            if dias < 0: return "🔴 Vencida"
            elif 0 <= dias <= 7: return "🟠 Por Vencer (7 días)"
            else: return "🟢 Vigente"
        merged_df['estado'] = merged_df['dias_para_vencer'].apply(get_status)

        # Filtros en la sidebar
        proveedores_lista = sorted(merged_df['nombre_proveedor'].dropna().unique().tolist())
        selected_suppliers = st.sidebar.multiselect("Filtrar por Proveedor:", proveedores_lista, default=proveedores_lista)
        
        min_date, max_date = merged_df['fecha_emision'].min().date(), merged_df['fecha_emision'].max().date()
        date_range = st.sidebar.date_input("Filtrar por Fecha de Emisión:", value=(min_date, max_date), min_value=min_date, max_value=max_date)
        
        start_date, end_date = (pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])) if len(date_range) == 2 else (None, None)
        filtered_df = merged_df[merged_df['nombre_proveedor'].isin(selected_suppliers) & (merged_df['fecha_emision'] >= start_date) & (merged_df['fecha_emision'] <= end_date)]
        
        # --- PESTAÑAS DE LA INTERFAZ ---
        tab1, tab2, tab3 = st.tabs(["📊 Dashboard Principal", "🚨 Alertas y Acciones", "🔍 Análisis de Datos"])

        with tab1:
            st.subheader("Indicadores Clave de Rendimiento (KPIs)")
            total_facturado = filtered_df['valor_total'].sum()
            total_vencido = filtered_df[filtered_df['estado'] == '🔴 Vencida']['valor_total'].sum()
            
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Facturado (Filtrado)", f"${total_facturado:,.2f}")
            col2.metric("Monto Total Vencido", f"${total_vencido:,.2f}")
            col3.metric("Nº Facturas Vencidas", f"{len(filtered_df[filtered_df['estado'] == '🔴 Vencida'])}")
            col4.metric("Nº Facturas por Vencer", f"{len(filtered_df[filtered_df['estado'] == '🟠 Por Vencer (7 días)'])}")
            
            st.markdown("---")
            
            col_a, col_b = st.columns([0.4, 0.6])
            with col_a:
                st.subheader("Distribución por Estado")
                status_counts = filtered_df['estado'].value_counts().reset_index()
                st.altair_chart(alt.Chart(status_counts).mark_arc(innerRadius=50).encode(
                    theta=alt.Theta(field="count", type="quantitative"),
                    color=alt.Color(field="estado", type="nominal", title="Estado", scale=alt.Scale(domain=['🔴 Vencida', '🟠 Por Vencer (7 días)', '🟢 Vigente'], range=['#d62728', '#ff7f0e', '#2ca02c'])),
                    tooltip=['estado', 'count']
                ), use_container_width=True)

            with col_b:
                st.subheader("Top 5 Proveedores por Deuda Vencida")
                deuda_vencida = filtered_df[filtered_df['estado'] == '🔴 Vencida'].groupby('nombre_proveedor')['valor_total'].sum().nlargest(5).reset_index()
                if not deuda_vencida.empty:
                    chart = alt.Chart(deuda_vencida).mark_bar().encode(
                        x=alt.X('valor_total:Q', title='Monto Vencido ($)'),
                        y=alt.Y('nombre_proveedor:N', sort='-x', title='Proveedor'),
                        tooltip=[alt.Tooltip('nombre_proveedor', title='Proveedor'), alt.Tooltip('valor_total', title='Monto Vencido', format='$,.2f')]
                    ).properties(height=250)
                    st.altair_chart(chart, use_container_width=True)
                else:
                    st.info("¡No hay deuda vencida para mostrar en el top!")

        with tab2:
            st.subheader("Centro de Gestión de Pagos")
            st.caption("Facturas que requieren tu atención inmediata o próxima.")
            
            st.markdown("##### 🔴 Facturas Vencidas (Acción Inmediata)")
            vencidas_df = filtered_df[filtered_df['estado'] == '🔴 Vencida'].sort_values('dias_para_vencer')
            if not vencidas_df.empty:
                df_display = vencidas_df.copy(); df_display['fecha_vencimiento'] = df_display['fecha_vencimiento'].dt.strftime('%Y-%m-%d')
                st.dataframe(df_display[['nombre_proveedor', 'num_factura', 'fecha_vencimiento', 'valor_total', 'dias_para_vencer']].style.format({'valor_total': '${:,.2f}'}).background_gradient(cmap='Reds_r', subset=['dias_para_vencer']), use_container_width=True)
            else: st.info("¡Excelente! No hay facturas vencidas.")

            st.markdown("##### 🟠 Facturas por Vencer (Próximos 7 días)")
            por_vencer_df = filtered_df[filtered_df['estado'] == '🟠 Por Vencer (7 días)'].sort_values('dias_para_vencer')
            if not por_vencer_df.empty:
                df_display = por_vencer_df.copy(); df_display['fecha_vencimiento'] = df_display['fecha_vencimiento'].dt.strftime('%Y-%m-%d')
                st.dataframe(df_display[['nombre_proveedor', 'num_factura', 'fecha_vencimiento', 'valor_total', 'dias_para_vencer']].style.format({'valor_total': '${:,.2f}'}).background_gradient(cmap='Oranges_r', subset=['dias_para_vencer']), use_container_width=True)
            else: st.info("No hay facturas por vencer en los próximos 7 días.")
        
        with tab3:
            st.subheader("Análisis de Conciliación y Datos Completos")
            
            st.markdown("##### ❗ Análisis de Discrepancias")
            unmatched_erp = merged_df[merged_df['valor_total_correo'].isnull() & merged_df['valor_total_erp'].notnull()]
            unmatched_email = merged_df[merged_df['valor_total_erp'].isnull() & merged_df['valor_total_correo'].notnull()]
            
            col1, col2 = st.columns(2)
            with col1:
                st.write("**Facturas en ERP, no en Correo:**")
                st.dataframe(unmatched_erp[['num_factura', 'nombre_proveedor', 'valor_total']], use_container_width=True)
            with col2:
                st.write("**Facturas en Correo, no en ERP:**")
                st.dataframe(unmatched_email[['num_factura', 'nombre_proveedor', 'valor_total']], use_container_width=True)
            
            st.markdown("---")
            st.markdown("##### 🔍 Explorador de Datos Consolidados")
            st.dataframe(filtered_df, use_container_width=True)
            
            @st.cache_data
            def convert_df_to_csv(df): return df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Descargar Datos Filtrados (CSV)", convert_df_to_csv(filtered_df), f'reporte_facturacion_{today.strftime("%Y%m%d")}.csv", "text/csv')

# --- Ejecución de la Aplicación ---
if __name__ == "__main__":
    if check_password():
        main_app()
