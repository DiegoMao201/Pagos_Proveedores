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
        # Compara la contraseña ingresada con el secreto almacenado
        return st.session_state.get("password") == st.secrets.get("password")

    if not st.session_state.get("password_correct", False):
        st.header("🔒 Acceso Restringido al Centro de Control")
        with st.form("login_form"):
            st.markdown("Por favor, ingresa la contraseña para acceder al sistema.")
            password = st.text_input("Contraseña", type="password", key="password")
            # El botón de envío llama a la función de verificación
            st.form_submit_button("Ingresar", on_click=lambda: st.session_state.update({"password_correct": password_correct()}))
        
        # Muestra un error si se intentó ingresar una contraseña y fue incorrecta
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
        # Elimina símbolos de moneda, espacios, puntos de miles y estandariza el decimal
        value = re.sub(r'[$\s.]', '', value)
        value = value.replace(',', '.')
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0  # Devuelve 0.0 si la conversión falla
    return 0.0

def parse_date(date_str):
    """Convierte una cadena de texto a un objeto de fecha, manejando varios formatos comunes."""
    if pd.isna(date_str) or date_str is None:
        return pd.NaT
    # Intenta convertir la fecha usando varios formatos conocidos
    for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f'):
        try:
            return pd.to_datetime(str(date_str), format=fmt).normalize()
        except (ValueError, TypeError):
            continue
    # Si ningún formato funciona, intenta el conversor genérico de pandas
    try:
        return pd.to_datetime(str(date_str)).normalize()
    except (ValueError, TypeError):
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
        st.error(f"❌ Error crítico al conectar con Google Sheets: {e}", icon="🔥")
        st.warning("Asegúrate de que tus secretos de `google_credentials` estén bien configurados en Streamlit.")
        return None

def load_data_from_gsheet(client, sheet_name):
    """Carga los datos de una hoja específica de Google Sheets en un DataFrame."""
    try:
        spreadsheet = client.open_by_key(st.secrets["google_sheet_id"])
        worksheet = spreadsheet.worksheet(sheet_name)
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        # Asegura que las columnas esenciales existan, incluso si la hoja está vacía
        required_cols = ["num_factura", "nombre_proveedor_correo", "fecha_emision_correo", "fecha_vencimiento_correo", "valor_total_correo"]
        for col in required_cols:
            if col not in df.columns:
                df[col] = pd.Series(dtype='object')
        return df
    except gspread.exceptions.WorksheetNotFound:
        st.warning(f"⚠️ La hoja '{sheet_name}' no fue encontrada. Se creará una tabla vacía.", icon="📝")
        return pd.DataFrame(columns=required_cols)
    except Exception as e:
        st.error(f"❌ Error al leer datos desde Google Sheets: {e}", icon="🔥")
        return pd.DataFrame()

def update_gsheet_from_df(client, sheet_name, df):
    """Actualiza una hoja de Google Sheets con los datos de un DataFrame."""
    try:
        spreadsheet = client.open_by_key(st.secrets["google_sheet_id"])
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
        # Prepara el DataFrame para la subida: convierte fechas a string y maneja NaT
        df_to_upload = df.copy()
        for col in df_to_upload.select_dtypes(include=['datetime64[ns]']).columns:
            df_to_upload[col] = df_to_upload[col].dt.strftime('%Y-%m-%d').replace({pd.NaT: ''})
        
        # Convierte todos los valores a string para evitar problemas de formato de gspread
        df_to_upload = df_to_upload.astype(str).replace({'nan': '', 'NaT': ''})

        worksheet.update([df_to_upload.columns.values.tolist()] + df_to_upload.values.tolist())
        return True
    except Exception as e:
        st.error(f"❌ Error al actualizar la hoja de Google Sheets: {e}", icon="🔥")
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
        st.info(f"📥 Intentando descargar archivo `{file_path}` desde Dropbox...")
        _, res = dbx.files_download(file_path)
        
        # IMPORTANTE: El separador '{' es muy raro. El más común en archivos de Excel en español es ';'.
        # Si la carga falla o el dataframe está vacío, cambia el valor de `sep` aquí.
        separator = ';' 
        
        with io.StringIO(res.content.decode('latin1')) as csv_file:
            df = pd.read_csv(csv_file, sep=separator, on_bad_lines='warn', header=None, engine='python')

        if df.empty:
            st.warning(f"⚠️ El archivo CSV de Dropbox se leyó pero está vacío. Revisa que el separador sea el correcto (actualmente es '{separator}').", icon="🧐")
            return pd.DataFrame()
        st.info("✅ Archivo CSV de Dropbox descargado y leído correctamente.")

        column_mapping = {
            0: 'nombre_proveedor_erp', 1: 'serie_almacen', 2: 'num_entrada',
            3: 'num_factura', 4: 'fecha_emision_erp', 5: 'fecha_vencimiento_erp',
            6: 'valor_total_erp'
        }
        
        # Mantén solo las columnas que necesitas
        df = df[list(column_mapping.keys())].copy()
        df.rename(columns=column_mapping, inplace=True)

        # Limpieza de datos
        df['valor_total_erp'] = df['valor_total_erp'].apply(clean_monetary_value)
        df['fecha_emision_erp'] = df['fecha_emision_erp'].apply(parse_date)
        df['fecha_vencimiento_erp'] = df['fecha_vencimiento_erp'].apply(parse_date)
        df['num_factura'] = df['num_factura'].astype(str).str.strip()
        df.dropna(subset=['num_factura'], inplace=True)

        return df

    except dropbox.exceptions.ApiError as e:
        st.error(f"❌ Error de API de Dropbox: {e}. ¿El archivo `{file_path}` existe?", icon="📦")
        return None
    except Exception as e:
        st.error(f"❌ Error crítico al cargar datos desde Dropbox: {e}", icon="🔥")
        return None

def parse_invoice_xml(xml_content):
    """Extrae detalles de la factura del contenido XML. Intenta parsear directamente y luego busca en CDATA."""
    try:
        ns = {
            'cac': 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
            'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
            'inv': 'urn:oasis:names:specification:ubl:schema:xsd:Invoice-2' # Namespace raíz común
        }
        
        # Función auxiliar para encontrar texto en elementos XML de forma segura
        def find_text(element, path, namespaces):
            node = element.find(path, namespaces)
            return node.text if node is not None else None

        # Intento 1: Parsear el XML directamente
        root = ET.fromstring(xml_content)
        
        # A veces el namespace raíz se aplica a todo, así que lo buscamos
        if root.tag.startswith('{urn:oasis:names:specification:ubl:schema:xsd:Invoice-2}'):
            # Reconstruimos las rutas con el namespace raíz
            invoice_number = find_text(root, 'cbc:ID', ns)
            issue_date = find_text(root, 'cbc:IssueDate', ns)
            due_date = find_text(root, 'cbc:DueDate', ns)
            supplier_name = find_text(root, 'cac:AccountingSupplierParty/cac:Party/cac:PartyName/cbc:Name', ns)
            total_value = find_text(root, 'cac:LegalMonetaryTotal/cbc:PayableAmount', ns)
        else:
            # Búsqueda normal sin asumir un namespace raíz en la etiqueta principal
            invoice_number = find_text(root, './/cbc:ID', ns)
            issue_date = find_text(root, './/cbc:IssueDate', ns)
            due_date = find_text(root, './/cac:PaymentMeans/cbc:PaymentDueDate', ns) # Ruta alternativa para fecha de vencimiento
            if not due_date: # Si no se encuentra, buscar en la ruta original
                due_date = find_text(root, './/cbc:DueDate', ns)
            supplier_name = find_text(root, './/cac:AccountingSupplierParty/cac:Party/cac:PartyName/cbc:Name', ns)
            total_value = find_text(root, './/cac:LegalMonetaryTotal/cbc:PayableAmount', ns)

        # Intento 2: Si los campos principales no se encuentran, buscar dentro de un bloque CDATA
        if not invoice_number:
            cdata_match = re.search(r'<!\[CDATA\[\s*(<.*?>)\s*\]\]>', xml_content, re.DOTALL)
            if cdata_match:
                st.info("XML encontrado dentro de un bloque CDATA, re-procesando...")
                return parse_invoice_xml(cdata_match.group(1)) # Llamada recursiva con el contenido del CDATA
            else:
                 return None # No se encontró XML válido

        return {
            "num_factura": invoice_number or "N/A",
            "nombre_proveedor_correo": supplier_name or "N/A",
            "fecha_emision_correo": issue_date or "N/A",
            "fecha_vencimiento_correo": due_date or issue_date or "N/A", # Usar fecha de emisión si no hay de vencimiento
            "valor_total_correo": total_value or "0",
        }
    except ET.ParseError as e:
        # No mostramos error, ya que es común que algunos adjuntos no sean XML de factura
        return None
    except Exception as e:
        st.warning(f"⚠️ Ocurrió un error inesperado al procesar un archivo XML: {e}")
        return None


@st.cache_data(show_spinner="Buscando nuevas facturas en el correo...", ttl=600)
def fetch_todays_invoices_from_email():
    """Busca y procesa nuevas facturas de los adjuntos de correo del día actual."""
    invoices = []
    try:
        email_secrets = st.secrets.get("email", {})
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(email_secrets.get("address"), email_secrets.get("password"))
        st.info("✅ Conexión exitosa al servidor de correo (IMAP).")

        folder = "TFHKA/Recepcion/Descargados"
        status, _ = mail.select(f'"{folder}"')
        if status != 'OK':
            st.error(f"❌ No se pudo seleccionar la carpeta de correo: '{folder}'. ¿Estás seguro de que el nombre es correcto?", icon="📁")
            mail.logout()
            return pd.DataFrame()
        st.info(f"📁 Carpeta seleccionada: `{folder}`.")

        today_date = datetime.now().strftime("%d-%b-%Y")
        status, messages = mail.search(None, f'(SINCE "{today_date}")')

        if status != 'OK' or not messages[0]:
            st.info(f"ℹ️ No se encontraron nuevos correos con facturas en la carpeta `{folder}` para la fecha de hoy ({today_date}).")
            mail.logout()
            return pd.DataFrame()

        message_ids = messages[0].split()
        st.info(f"📬 Se encontraron {len(message_ids)} correo(s) nuevo(s) para hoy. Empezando procesamiento...")
        progress_text = f"Procesando {len(message_ids)} correo(s)..."
        progress_bar = st.progress(0, text=progress_text)

        for i, num in enumerate(message_ids):
            _, data = mail.fetch(num, "(RFC822)")
            msg = email.message_from_bytes(data[0][1])

            for part in msg.walk():
                if part.get_content_maintype() == "multipart" or part.get("Content-Disposition") is None:
                    continue
                
                filename = part.get_filename()
                if filename and filename.lower().endswith('.zip'):
                    st.info(f"📄 Encontrado archivo ZIP: `{filename}`. Extrayendo contenido...")
                    zip_bytes = part.get_payload(decode=True)
                    try:
                        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zip_file:
                            for name in zip_file.namelist():
                                if name.lower().endswith('.xml'):
                                    st.info(f"   - Encontrado archivo XML dentro del ZIP: `{name}`.")
                                    xml_content = zip_file.read(name).decode('utf-8', 'ignore')
                                    invoice_details = parse_invoice_xml(xml_content)
                                    if invoice_details:
                                        invoices.append(invoice_details)
                                        st.success(f"     ✓ Factura `{invoice_details['num_factura']}` procesada exitosamente.")
                    except zipfile.BadZipFile:
                        st.warning(f"   - ⚠️ El archivo `{filename}` parece ser un ZIP corrupto y no se pudo procesar.")

            progress_bar.progress((i + 1) / len(message_ids), text=progress_text)
        
        progress_bar.empty()
        mail.logout()

        if not invoices:
            st.info("ℹ️ Se procesaron los correos, pero no se encontraron facturas válidas en los archivos XML adjuntos.")
            return pd.DataFrame()
        return pd.DataFrame(invoices)

    except imaplib.IMAP4.error as imap_err:
        st.error(f"❌ Error de conexión de correo (IMAP): {imap_err}", icon="🔑")
        st.warning("Verifica que tu contraseña de aplicación sea correcta y que el acceso IMAP esté habilitado en la configuración de tu cuenta de Gmail.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Error crítico al procesar los correos: {e}", icon="🔥")
        return pd.DataFrame()

# --- Interfaz Principal de la Aplicación ---
def main_app():
    
    st.image("LOGO FERREINOX SAS BIC 2024.png", width=250)
    st.title("Centro de Control de Facturación Inteligente")
    st.markdown("Sistema proactivo para la conciliación, análisis y predicción de pagos a proveedores.")
    st.markdown("---")

    # --- Sidebar para Filtros y Sincronización ---
    st.sidebar.header("Filtros Globales 🔎")
    
    if st.sidebar.button("🔌 Sincronizar Datos", type="primary", use_container_width=True):
        with st.spinner('Realizando sincronización completa... Por favor, espera.'):
            st.session_state['data_loaded'] = False # Reinicia el estado de carga
            
            gs_client = connect_to_google_sheets()
            if not gs_client:
                st.session_state['data_loaded'] = False
                st.error("La sincronización no puede continuar sin conexión a Google Sheets.")
                return

            # Carga de datos históricos y de hoy, combinando y actualizando la fuente
            historical_email_df = load_data_from_gsheet(gs_client, "FacturasCorreo")
            todays_email_df = fetch_todays_invoices_from_email()

            if not todays_email_df.empty:
                combined_df = pd.concat([historical_email_df, todays_email_df], ignore_index=True)
                # Elimina duplicados por número de factura, manteniendo el más reciente
                combined_df.drop_duplicates(subset=['num_factura'], keep='last', inplace=True)
                if update_gsheet_from_df(gs_client, "FacturasCorreo", combined_df):
                    st.success(f"✅ Base de datos de correos actualizada con {len(todays_email_df)} factura(s) nueva(s).")
                email_df = combined_df.copy()
            else:
                email_df = historical_email_df.copy()

            if email_df.empty:
                st.info("ℹ️ No hay facturas en el histórico de Google Sheets y no se encontraron nuevas hoy.")

            # Limpieza y estandarización del DataFrame de correos
            required_email_cols = ["num_factura", "nombre_proveedor_correo", "fecha_emision_correo", "fecha_vencimiento_correo", "valor_total_correo"]
            for col in required_email_cols:
                if col not in email_df.columns:
                    email_df[col] = pd.Series(dtype='object')

            email_df['valor_total_correo'] = email_df['valor_total_correo'].apply(clean_monetary_value)
            email_df['fecha_emision_correo'] = email_df['fecha_emision_correo'].apply(parse_date)
            email_df['fecha_vencimiento_correo'] = email_df['fecha_vencimiento_correo'].apply(parse_date)
            email_df['num_factura'] = email_df['num_factura'].astype(str).str.strip()

            # Carga de datos del ERP
            erp_df = load_erp_data_from_dropbox()

            st.session_state['erp_df'] = erp_df
            st.session_state['email_df'] = email_df
            
            if (erp_df is not None and not erp_df.empty) or not email_df.empty:
                st.session_state['data_loaded'] = True
            else:
                st.error("No se pudieron cargar datos de ninguna fuente. Revisa los mensajes de diagnóstico de Dropbox, Correo y Google Sheets.")

    if st.session_state.get('data_loaded', False):
        erp_df = st.session_state.get('erp_df', pd.DataFrame())
        email_df = st.session_state.get('email_df', pd.DataFrame())

        # Proceso de fusión de datos robusto
        if erp_df is not None and not erp_df.empty and not email_df.empty:
            merged_df = pd.merge(erp_df, email_df, on='num_factura', how='outer')
        elif erp_df is not None and not erp_df.empty:
            merged_df = erp_df.copy()
            for col in ['nombre_proveedor_correo', 'fecha_emision_correo', 'fecha_vencimiento_correo', 'valor_total_correo']:
                if col not in merged_df: merged_df[col] = pd.NA
        elif not email_df.empty:
            merged_df = email_df.copy()
            for col in ['nombre_proveedor_erp', 'fecha_emision_erp', 'fecha_vencimiento_erp', 'valor_total_erp']:
                if col not in merged_df: merged_df[col] = pd.NA
        else:
            st.warning("No hay datos disponibles para mostrar el dashboard.")
            return

        # Creación de columnas unificadas
        merged_df['fecha_emision'] = merged_df['fecha_emision_erp'].fillna(merged_df['fecha_emision_correo'])
        merged_df['fecha_vencimiento'] = merged_df['fecha_vencimiento_erp'].fillna(merged_df['fecha_vencimiento_correo'])
        merged_df['valor_total'] = merged_df['valor_total_erp'].fillna(merged_df['valor_total_correo'])
        merged_df['nombre_proveedor'] = merged_df['nombre_proveedor_erp'].fillna(merged_df['nombre_proveedor_correo'])
        
        merged_df.dropna(subset=['num_factura', 'fecha_emision', 'fecha_vencimiento'], inplace=True)
        merged_df = merged_df[merged_df['num_factura'] != 'N/A']

        # Cálculo de estado de la factura
        today = pd.to_datetime(datetime.now().date())
        merged_df['dias_para_vencer'] = (merged_df['fecha_vencimiento'] - today).dt.days
        
        def get_status(dias):
            if pd.isna(dias): return "⚪ Estado Desconocido"
            if dias < 0: return "🔴 Vencida"
            elif 0 <= dias <= 7: return "🟠 Por Vencer (Próximos 7 días)"
            else: return "🟢 Vigente"
        merged_df['estado'] = merged_df['dias_para_vencer'].apply(get_status)

        # -- Interfaz de filtros en la Sidebar --
        proveedores_lista = sorted(merged_df['nombre_proveedor'].dropna().unique().tolist())
        selected_suppliers = st.sidebar.multiselect("Filtrar por Proveedor:", proveedores_lista, default=proveedores_lista)
        
        min_date = merged_df['fecha_emision'].min().date() if not merged_df.empty else datetime.now().date()
        max_date = merged_df['fecha_emision'].max().date() if not merged_df.empty else datetime.now().date()
        date_range = st.sidebar.date_input("Filtrar por Fecha de Emisión:", value=(min_date, max_date), min_value=min_date, max_value=max_date)
        
        # Aplicación de filtros
        if len(date_range) == 2:
            start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
            filtered_df = merged_df[
                (merged_df['nombre_proveedor'].isin(selected_suppliers)) & 
                (merged_df['fecha_emision'] >= start_date) & 
                (merged_df['fecha_emision'] <= end_date)
            ]
        else: # Maneja el caso de que el input de fecha no esté completo
            filtered_df = merged_df[merged_df['nombre_proveedor'].isin(selected_suppliers)]
        
        st.success(f"✔ ¡Datos sincronizados! Mostrando {len(filtered_df)} de {len(merged_df)} facturas según los filtros.")

        # --- Dashboard Principal ---
        st.header("📊 Dashboard Principal")

        total_facturado = filtered_df['valor_total'].sum()
        total_vencido = filtered_df[filtered_df['estado'] == '🔴 Vencida']['valor_total'].sum()
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Facturado (Filtrado)", f"${total_facturado:,.2f}")
        col2.metric("Monto Total Vencido", f"${total_vencido:,.2f}")
        col3.metric("Facturas Vencidas", f"{len(filtered_df[filtered_df['estado'] == '🔴 Vencida'])}")
        col4.metric("Facturas por Vencer (7 días)", f"{len(filtered_df[filtered_df['estado'] == '🟠 Por Vencer (Próximos 7 días)'])}")
        
        st.markdown("---")

        # Gráficos
        col_a, col_b = st.columns(2)
        with col_a:
            st.subheader("Estado General de Facturas")
            if not filtered_df.empty:
                status_counts = filtered_df['estado'].value_counts().reset_index()
                chart_status = alt.Chart(status_counts).mark_arc(innerRadius=50).encode(
                    theta=alt.Theta(field="count", type="quantitative"),
                    color=alt.Color(field="estado", type="nominal", title="Estado", scale=alt.Scale(
                        domain=['🔴 Vencida', '🟠 Por Vencer (Próximos 7 días)', '🟢 Vigente', '⚪ Estado Desconocido'],
                        range=['#d62728', '#ff7f0e', '#2ca02c', '#cccccc']
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
                st.info("¡Excelente! No hay facturas vencidas según los filtros actuales.")

        with st.expander("🟠 **Facturas por Vencer (Próximos 7 días)**", expanded=True):
            por_vencer_df = filtered_df[filtered_df['estado'] == '🟠 Por Vencer (Próximos 7 días)'].sort_values('dias_para_vencer')
            if not por_vencer_df.empty:
                st.dataframe(por_vencer_df[['nombre_proveedor', 'num_factura', 'fecha_vencimiento', 'valor_total', 'dias_para_vencer']].style.background_gradient(cmap='Oranges_r', subset=['dias_para_vencer']), use_container_width=True)
            else:
                st.info("No hay facturas por vencer en los próximos 7 días.")
        
        with st.expander("❗ **Análisis de Discrepancias**"):
            unmatched_erp = merged_df[merged_df['valor_total_correo'].isnull() & merged_df['valor_total_erp'].notnull()]
            unmatched_email = merged_df[merged_df['valor_total_erp'].isnull() & merged_df['valor_total_correo'].notnull()]
            st.write("**Facturas en ERP pero no encontradas en Correo:**")
            if not unmatched_erp.empty:
                st.dataframe(unmatched_erp[['num_factura', 'nombre_proveedor_erp', 'valor_total_erp']], use_container_width=True)
            else:
                st.info("Todas las facturas del ERP tienen su contraparte en el correo.")
            st.write("**Facturas en Correo pero no encontradas en ERP:**")
            if not unmatched_email.empty:
                st.dataframe(unmatched_email[['num_factura', 'nombre_proveedor_correo', 'valor_total_correo']], use_container_width=True)
            else:
                st.info("Todas las facturas del correo han sido encontradas en el ERP.")

        st.markdown("---")
        st.header("🔍 Explorador de Datos Consolidados")
        with st.expander("Ver/Ocultar Tabla de Datos Completa"):
            st.dataframe(filtered_df, use_container_width=True)
            @st.cache_data
            def convert_df_to_csv(df_to_convert):
                return df_to_convert.to_csv(index=False).encode('utf-8')
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
