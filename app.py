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
import xml.etree.ElementTree as ET # Importa para el análisis de XML

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
    """Limpia y convierte un valor monetario a tipo float."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        # Elimina $, espacios, puntos de miles y reemplaza la coma decimal por un punto.
        value = re.sub(r'[$\s.]', '', value)
        value = value.replace(',', '.')
        try:
            float_val = float(value)
            return float_val
        except (ValueError, TypeError):
            return 0.0
    return 0.0

def parse_date(date_str):
    """Convierte una cadena de texto a un objeto de fecha, manejando varios formatos."""
    if pd.isna(date_str) or date_str is None:
        return pd.NaT
    # Maneja varios formatos comunes
    for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f'):
        try:
            return pd.to_datetime(str(date_str), format=fmt).normalize()
        except (ValueError, TypeError):
            continue
    return pd.NaT

# --- Funciones de Conexión a Google Sheets ---
@st.cache_resource(show_spinner="Conectando a Google Sheets...")
def connect_to_google_sheets():
    """Establece la conexión con Google Sheets usando las credenciales de servicio."""
    try:
        scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(
            st.secrets["google_credentials"], scopes=scopes
        )
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"❌ Error crítico al conectar con Google Sheets: {e}")
        return None

def load_data_from_gsheet(client, sheet_name):
    """Carga los datos de una hoja específica de Google Sheets en un DataFrame."""
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
        st.warning(f"⚠️ La hoja '{sheet_name}' no fue encontrada. Se creará una tabla vacía.")
        return pd.DataFrame(columns=required_cols)
    except Exception as e:
        st.error(f"❌ Error al leer datos desde Google Sheets: {e}")
        return pd.DataFrame()

def update_gsheet_from_df(client, sheet_name, df):
    """Actualiza una hoja de Google Sheets con los datos de un DataFrame."""
    try:
        spreadsheet = client.open_by_key(st.secrets["google_sheet_id"])
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
        df_to_upload = df.copy()
        for col in df_to_upload.select_dtypes(include=['datetime64[ns]']).columns:
            df_to_upload[col] = df_to_upload[col].dt.strftime('%Y-%m-%d').replace({pd.NaT: ''})
        worksheet.update([df_to_upload.columns.values.tolist()] + df_to_upload.values.tolist())
        return True
    except Exception as e:
        st.error(f"❌ Error al actualizar la hoja de Google Sheets: {e}")
        return False

# --- Lógica de Datos ---
@st.cache_data(show_spinner="Conectando a Dropbox y cargando datos del ERP...", ttl=3600)
def load_erp_data_from_dropbox():
    """Carga, limpia y renombra los datos del ERP desde un CSV en Dropbox."""
    try:
        dropbox_secrets = st.secrets.get("dropbox", {})
        dbx = dropbox.Dropbox(
            oauth2_refresh_token=dropbox_secrets.get("refresh_token"),
            app_key=dropbox_secrets.get("app_key"),
            app_secret=dropbox_secrets.get("app_secret")
        )
        dbx.users_get_current_account() # Prueba la conexión
        file_path = "/data/Proveedores.csv"
        
        _, res = dbx.files_download(file_path)
        
        with io.StringIO(res.content.decode('latin1')) as csv_file:
            df = pd.read_csv(csv_file, sep='{', on_bad_lines='skip', header=None)

        column_mapping = {
            0: 'nombre_proveedor_erp',
            1: 'serie_almacen',
            2: 'num_entrada',
            3: 'num_factura',
            4: 'fecha_emision_erp',
            5: 'fecha_vencimiento_erp',
            6: 'valor_total_erp'
        }
        
        df = df[list(column_mapping.keys())].copy()
        df.rename(columns=column_mapping, inplace=True)

        df['valor_total_erp'] = df['valor_total_erp'].apply(clean_monetary_value)
        df['fecha_emision_erp'] = df['fecha_emision_erp'].apply(parse_date)
        df['fecha_vencimiento_erp'] = df['fecha_vencimiento_erp'].apply(parse_date)
        df['num_factura'] = df['num_factura'].astype(str).str.strip()
        df.dropna(subset=['num_factura'], inplace=True)

        return df

    except Exception as e:
        st.error(f"❌ Error crítico al cargar datos desde Dropbox: {e}")
        return None

def parse_invoice_xml(xml_content):
    """Extrae los detalles de la factura del contenido XML interno de un adjunto."""
    try:
        ns = {
            'cac': 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
            'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
        }
        root = ET.fromstring(xml_content)
        
        invoice_number = root.find('cbc:ID', ns).text if root.find('cbc:ID', ns) is not None else "N/A"
        issue_date = root.find('cbc:IssueDate', ns).text if root.find('cbc:IssueDate', ns) is not None else "N/A"
        due_date = root.find('cbc:DueDate', ns).text if root.find('cbc:DueDate', ns) is not None else "N/A"
        
        supplier_name_element = root.find('cac:AccountingSupplierParty/cac:Party/cac:PartyName/cbc:Name', ns)
        supplier_name = supplier_name_element.text if supplier_name_element is not None else "N/A"
        
        total_value_element = root.find('cac:LegalMonetaryTotal/cbc:PayableAmount', ns)
        total_value = total_value_element.text if total_value_element is not None else "0"

        return {
            "num_factura": invoice_number,
            "nombre_proveedor_correo": supplier_name,
            "fecha_emision_correo": issue_date,
            "fecha_vencimiento_correo": due_date,
            "valor_total_correo": total_value,
        }
    except ET.ParseError as e:
        st.warning(f"No se pudo procesar un archivo XML adjunto: {e}")
        return None

@st.cache_data(show_spinner="Buscando nuevas facturas en el correo...", ttl=600)
def fetch_todays_invoices_from_email():
    """Busca y procesa nuevas facturas de los adjuntos de correo del día actual."""
    invoices = []
    try:
        email_secrets = st.secrets.get("email", {})
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(email_secrets.get("address"), email_secrets.get("password"))
        mail.select("TFHKA/Recepcion/Descargados")

        today_date = datetime.now().strftime("%d-%b-%Y")
        status, messages = mail.search(None, f'(SINCE "{today_date}")')

        if status != 'OK' or not messages[0]:
            st.info("ℹ️ No se encontraron nuevas facturas por correo en el día de hoy.")
            mail.logout()
            return pd.DataFrame()

        message_ids = messages[0].split()
        progress_text = f"Procesando {len(message_ids)} correo(s) nuevo(s) de hoy..."
        progress_bar = st.progress(0, text=progress_text)

        for i, num in enumerate(message_ids):
            _, data = mail.fetch(num, "(RFC822)")
            msg = email.message_from_bytes(data[0][1])

            for part in msg.walk():
                if part.get_content_maintype() == "multipart" or part.get("Content-Disposition") is None:
                    continue
                
                filename = part.get_filename()
                if filename and filename.endswith('.zip'):
                    zip_bytes = part.get_payload(decode=True)
                    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zip_file:
                        for name in zip_file.namelist():
                            if name.endswith('.xml'):
                                content_bytes = zip_file.read(name)
                                cdata_match = re.search(r'<!\[CDATA\[\s*(<Invoice.*?</Invoice>)\s*\]\]>', content_bytes.decode('utf-8', 'ignore'), re.DOTALL)
                                if cdata_match:
                                    xml_data = cdata_match.group(1)
                                    invoice_details = parse_invoice_xml(xml_data)
                                    if invoice_details:
                                        invoices.append(invoice_details)

            progress_bar.progress((i + 1) / len(message_ids), text=progress_text)
        
        progress_bar.empty()
        mail.logout()

        if not invoices:
            return pd.DataFrame()
        return pd.DataFrame(invoices)

    except imaplib.IMAP4.error as imap_err:
        st.error(f"❌ Error de conexión de correo: {imap_err}. Revisa tu contraseña de aplicación y que IMAP esté activado.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Error crítico al procesar los correos: {e}")
        return pd.DataFrame()

# --- Interfaz Principal de la Aplicación ---
def main_app():
    
    st.image("LOGO FERREINOX SAS BIC 2024.png", width=250)
    st.title("Centro de Control de Facturación Inteligente")
    st.markdown("Sistema proactivo para la conciliación, análisis y predicción de pagos a proveedores.")
    st.markdown("---")

    # --- Sidebar para Filtros y Sincronización ---
    st.sidebar.header("Filtros Globales 🔎")
    
    if st.button("🔌 Sincronizar Datos", type="primary", use_container_width=True):
        st.session_state['data_loaded'] = False # Reinicia el estado de carga
        
        gs_client = connect_to_google_sheets()
        if not gs_client:
            st.session_state['data_loaded'] = False
            return

        historical_email_df = load_data_from_gsheet(gs_client, "FacturasCorreo")
        
        todays_email_df = fetch_todays_invoices_from_email()

        if not todays_email_df.empty:
            combined_df = pd.concat([historical_email_df, todays_email_df], ignore_index=True)
            combined_df.drop_duplicates(subset=['num_factura'], keep='last', inplace=True)
            if update_gsheet_from_df(gs_client, "FacturasCorreo", combined_df):
                st.success(f"✅ Base de datos actualizada con {len(todays_email_df)} factura(s) nueva(s).")
            email_df = combined_df.copy()
        else:
            email_df = historical_email_df.copy()
            if email_df.empty:
                st.info("ℹ️ No hay facturas en Google Sheets y tampoco se encontraron nuevas hoy.")

        required_email_cols = ["num_factura", "nombre_proveedor_correo", "fecha_emision_correo", "fecha_vencimiento_correo", "valor_total_correo"]
        for col in required_email_cols:
            if col not in email_df.columns:
                email_df[col] = pd.Series(dtype='object')

        email_df['valor_total_correo'] = email_df['valor_total_correo'].apply(clean_monetary_value)
        email_df['fecha_emision_correo'] = email_df['fecha_emision_correo'].apply(parse_date)
        email_df['fecha_vencimiento_correo'] = email_df['fecha_vencimiento_correo'].apply(parse_date)
        email_df['num_factura'] = email_df['num_factura'].astype(str).str.strip()

        erp_df = load_erp_data_from_dropbox()

        required_erp_cols = ["num_factura", "nombre_proveedor_erp", "fecha_emision_erp", "fecha_vencimiento_erp", "valor_total_erp"]
        if erp_df is not None:
            for col in required_erp_cols:
                if col not in erp_df.columns:
                    erp_df[col] = pd.Series(dtype='object')
        
        st.session_state['erp_df'] = erp_df
        st.session_state['email_df'] = email_df
        
        if erp_df is not None or not email_df.empty:
            st.session_state['data_loaded'] = True
        else:
            st.error("No se pudieron cargar los datos de ninguna de las fuentes. Revisa los mensajes de error de Dropbox y Google Sheets.")

    if st.session_state.get('data_loaded', False):
        erp_df = st.session_state['erp_df']
        email_df = st.session_state['email_df']

        if erp_df is not None and not erp_df.empty and not email_df.empty:
            merged_df = pd.merge(erp_df, email_df, on='num_factura', how='outer', suffixes=('_erp', '_correo'))
        elif erp_df is not None and not erp_df.empty:
            # Aquí se creaba el problema, ahora lo manejamos explícitamente.
            merged_df = erp_df.copy()
            merged_df['nombre_proveedor_correo'] = pd.NA
            merged_df['fecha_emision_correo'] = pd.NaT
            merged_df['fecha_vencimiento_correo'] = pd.NaT
            merged_df['valor_total_correo'] = 0.0
        elif not email_df.empty:
            merged_df = email_df.copy()
            merged_df['nombre_proveedor_erp'] = pd.NA
            merged_df['fecha_emision_erp'] = pd.NaT
            merged_df['fecha_vencimiento_erp'] = pd.NaT
            merged_df['valor_total_erp'] = 0.0
        else:
            st.warning("No hay datos para mostrar el dashboard.")
            return

        merged_df['fecha_emision'] = merged_df['fecha_emision_erp'].fillna(merged_df['fecha_emision_correo'])
        merged_df['fecha_vencimiento'] = merged_df['fecha_vencimiento_erp'].fillna(merged_df['fecha_vencimiento_correo'])
        merged_df['valor_total'] = merged_df['valor_total_erp'].fillna(merged_df['valor_total_correo'])
        merged_df['nombre_proveedor'] = merged_df['nombre_proveedor_erp'].fillna(merged_df['nombre_proveedor_correo'])
        
        merged_df.dropna(subset=['fecha_emision'], inplace=True)
        today = pd.to_datetime(datetime.now().date())
        merged_df.dropna(subset=['fecha_vencimiento'], inplace=True)
        merged_df['dias_para_vencer'] = (merged_df['fecha_vencimiento'] - today).dt.days
        
        def get_status(dias):
            if dias < 0: return "🔴 Vencida"
            elif 0 <= dias <= 7: return "🟠 Por Vencer (Próximos 7 días)"
            else: return "🟢 Vigente"
        merged_df['estado'] = merged_df['dias_para_vencer'].apply(get_status)

        proveedores_lista = sorted(merged_df['nombre_proveedor'].dropna().unique().tolist())
        selected_suppliers = st.sidebar.multiselect("Filtrar por Proveedor:", proveedores_lista, default=proveedores_lista)
        
        if not merged_df.empty:
            min_date = merged_df['fecha_emision'].min().date()
            max_date = merged_df['fecha_emision'].max().date()
            date_range = st.sidebar.date_input("Filtrar por Fecha de Emisión:", value=(min_date, max_date), min_value=min_date, max_value=max_date)
            
            if len(date_range) == 2:
                start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
                filtered_df = merged_df[(merged_df['nombre_proveedor'].isin(selected_suppliers)) & (merged_df['fecha_emision'] >= start_date) & (merged_df['fecha_emision'] <= end_date)]
            else:
                filtered_df = merged_df[merged_df['nombre_proveedor'].isin(selected_suppliers)]
        else:
            filtered_df = pd.DataFrame()
            st.warning("No hay datos disponibles para filtrar.")
            return
        
        st.success(f"✔ ¡Datos sincronizados! Mostrando {len(filtered_df)} de {len(merged_df)} facturas según los filtros.")

        st.header("📊 Dashboard Principal")

        total_facturado = filtered_df['valor_total'].sum()
        total_vencido = filtered_df[filtered_df['estado'] == '🔴 Vencida']['valor_total'].sum()
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Facturado (Filtrado)", f"${total_facturado:,.2f}")
        col2.metric("Monto Total Vencido", f"${total_vencido:,.2f}")
        col3.metric("Facturas Vencidas", f"{len(filtered_df[filtered_df['estado'] == '🔴 Vencida'])}")
        col4.metric("Facturas por Vencer (7 días)", f"{len(filtered_df[filtered_df['estado'] == '🟠 Por Vencer (Próximos 7 días)'])}")
        
        st.markdown("---")

        col_a, col_b = st.columns(2)
        with col_a:
            st.subheader("Estado General de Facturas")
            if not filtered_df.empty:
                status_counts = filtered_df['estado'].value_counts().reset_index()
                chart_status = alt.Chart(status_counts).mark_arc(innerRadius=50).encode(
                    theta=alt.Theta(field="count", type="quantitative"),
                    color=alt.Color(field="estado", type="nominal", title="Estado", scale=alt.Scale(
                        domain=['🔴 Vencida', '🟠 Por Vencer (Próximos 7 días)', '🟢 Vigente'],
                        range=['#d62728', '#ff7f0e', '#2ca02c']
                    )), tooltip=['estado', 'count']
                ).properties(height=300)
                st.altair_chart(chart_status, use_container_width=True)
            else:
                st.info("No hay datos filtrados para mostrar el gráfico de estado.")

        with col_b:
            st.subheader("Facturación Mensual")
            if not filtered_df.empty:
                monthly_total = filtered_df.set_index('fecha_emision').resample('M')['valor_total'].sum().reset_index()
                monthly_total['mes'] = monthly_total['fecha_emision'].dt.strftime('%Y-%b')
                chart_monthly = alt.Chart(monthly_total).mark_line(point=True, strokeWidth=3).encode(
                    x=alt.X('mes:N', sort=None, title='Mes'),
                    y=alt.Y('valor_total:Q', title='Suma Facturada'),
                    tooltip=['mes', alt.Tooltip('valor_total:Q', format='$,.2f')]
                ).properties(height=300)
                st.altair_chart(chart_monthly, use_container_width=True)
            else:
                st.info("No hay datos filtrados para mostrar el gráfico mensual.")

        st.markdown("---")
        st.header("🚨 Centro de Alertas y Discrepancias")

        with st.expander("🔴 **Facturas Vencidas (Acción Inmediata)**", expanded=True):
            vencidas_df = filtered_df[filtered_df['estado'] == '🔴 Vencida'].sort_values('dias_para_vencer')
            if not vencidas_df.empty:
                st.dataframe(vencidas_df[['nombre_proveedor', 'num_factura', 'fecha_vencimiento', 'valor_total', 'dias_para_vencer']].style.background_gradient(cmap='Reds_r', subset=['dias_para_vencer']), use_container_width=True)
            else:
                st.info("¡No hay facturas vencidas en este momento!")

        with st.expander("🟠 **Facturas por Vencer (Próximos 7 días)**", expanded=True):
            por_vencer_df = filtered_df[filtered_df['estado'] == '🟠 Por Vencer (Próximos 7 días)'].sort_values('dias_para_vencer')
            if not por_vencer_df.empty:
                st.dataframe(por_vencer_df[['nombre_proveedor', 'num_factura', 'fecha_vencimiento', 'valor_total', 'dias_para_vencer']].style.background_gradient(cmap='Oranges_r', subset=['dias_para_vencer']), use_container_width=True)
            else:
                st.info("¡No hay facturas por vencer en los próximos 7 días!")
        
        with st.expander("❗ **Análisis de Discrepancias**"):
            unmatched_erp = merged_df[merged_df['nombre_proveedor_correo'].isnull() & merged_df['nombre_proveedor_erp'].notnull()]
            unmatched_email = merged_df[merged_df['nombre_proveedor_erp'].isnull() & merged_df['nombre_proveedor_correo'].notnull()]
            st.write("**Facturas en ERP pero no en Correo:**")
            if not unmatched_erp.empty:
                st.dataframe(unmatched_erp[['num_factura', 'nombre_proveedor_erp', 'valor_total_erp']], use_container_width=True)
            else:
                st.info("No se encontraron discrepancias de facturas del ERP sin correo.")
            st.write("**Facturas en Correo pero no en ERP:**")
            if not unmatched_email.empty:
                st.dataframe(unmatched_email[['num_factura', 'nombre_proveedor_correo', 'valor_total_correo']], use_container_width=True)
            else:
                st.info("No se encontraron discrepancias de facturas de correo sin ERP.")

        st.markdown("---")
        st.header("🔍 Explorador de Datos Consolidados")
        with st.expander("Ver/Ocultar Tabla de Datos Completa"):
            st.dataframe(filtered_df, use_container_width=True)
            @st.cache_data
            def convert_df_to_csv(df):
                return df.to_csv(index=False).encode('utf-8')
            csv = convert_df_to_csv(filtered_df)
            st.download_button(
                label="📥 Descargar Datos Filtrados como CSV",
                data=csv,
                file_name=f'reporte_facturacion_{today.strftime("%Y%m%d")}.csv',
                mime='text/csv',
            )

# --- Ejecución de la Aplicación ---
if __name__ == "__main__":
    if check_password():
        main_app()
