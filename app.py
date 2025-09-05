import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import dropbox
import imaplib
import email
import zipfile
import io
from bs4 import BeautifulSoup
import re
import altair as alt
import gspread
from google.oauth2.service_account import Credentials

# --- Configuración de la página de Streamlit ---
st.set_page_config(
    page_title="Centro de Control de Facturación IA",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Lógica de Autenticación (Sin cambios) ---
def check_password():
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

# --- Funciones Auxiliares para Limpieza de Datos (Sin cambios) ---
def clean_monetary_value(value):
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        value = re.sub(r'[$\s]', '', value)
        value = value.replace('.', '').replace(',', '.')
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    return 0.0

def parse_date(date_str):
    if pd.isna(date_str) or date_str is None:
        return None
    for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y', '%Y-%m-%d'):
        try:
            return datetime.strptime(str(date_str), fmt)
        except ValueError:
            continue
    return pd.NaT

# --- NUEVAS Funciones de Conexión y Lógica con Google Sheets (MODIFICADAS) ---
@st.cache_resource(show_spinner="Conectando a Google Sheets...")
def connect_to_google_sheets():
    """Establece conexión con Google Sheets usando las credenciales del servicio."""
    try:
        scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        ### CAMBIO REALIZADO: Usamos "google_credentials" en lugar de "gcp_service_account"
        creds = Credentials.from_service_account_info(
            st.secrets["google_credentials"], scopes=scopes
        )
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"❌ Error crítico al conectar con Google Sheets: {e}")
        return None

def load_data_from_gsheet(client, sheet_name):
    """Carga los datos históricos desde la hoja de Google Sheets especificada."""
    try:
        ### CAMBIO REALIZADO: Abrimos la hoja por su ID en lugar de su nombre
        spreadsheet = client.open_by_key(st.secrets["google_sheet_id"])
        worksheet = spreadsheet.worksheet(sheet_name)
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        # Asegurar que las columnas clave existen aunque la hoja esté vacía
        required_cols = ["num_factura", "nombre_proveedor_correo", "fecha_emision_correo", "fecha_vencimiento_correo", "valor_total_correo"]
        for col in required_cols:
            if col not in df.columns:
                df[col] = pd.Series(dtype='object')
        return df
    except gspread.exceptions.WorksheetNotFound:
        st.warning(f"⚠️ La hoja '{sheet_name}' no fue encontrada. Se creará una tabla vacía.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Error al leer datos desde Google Sheets: {e}")
        return pd.DataFrame()

def update_gsheet_from_df(client, sheet_name, df):
    """Actualiza una hoja de Google Sheets con los datos de un DataFrame."""
    try:
        ### CAMBIO REALIZADO: Abrimos la hoja por su ID en lugar de su nombre
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

# --- Lógica de Datos (Original y Modificada) ---
@st.cache_data(show_spinner="Conectando a Dropbox y cargando datos del ERP...")
def load_erp_data_from_dropbox():
    # Esta función permanece igual
    try:
        dropbox_secrets = st.secrets.get("dropbox", {})
        dbx = dropbox.Dropbox(
            oauth2_refresh_token=dropbox_secrets.get("refresh_token"),
            app_key=dropbox_secrets.get("app_key"),
            app_secret=dropbox_secrets.get("app_secret")
        )
        dbx.users_get_current_account()
        file_path = "/data/Proveedores.csv"
        _, res = dbx.files_download(file_path)
        csv_bytes = res.content
        csv_file = io.StringIO(csv_bytes.decode('latin1'))
        df = pd.read_csv(csv_file, sep='{', on_bad_lines='skip', header=None)
        column_mapping = {
            df.columns[1]: 'nombre_proveedor_erp',
            df.columns[4]: 'num_factura',
            df.columns[5]: 'fecha_emision_erp',
            df.columns[6]: 'fecha_vencimiento_erp',
            df.columns[7]: 'valor_total_erp'
        }
        df.rename(columns=column_mapping, inplace=True)
        df = df[list(column_mapping.values())]
        df['valor_total_erp'] = df['valor_total_erp'].apply(clean_monetary_value)
        df['fecha_emision_erp'] = pd.to_datetime(df['fecha_emision_erp'], errors='coerce')
        df['fecha_vencimiento_erp'] = pd.to_datetime(df['fecha_vencimiento_erp'], errors='coerce')
        df['num_factura'] = df['num_factura'].astype(str).str.strip()
        return df
    except Exception as e:
        st.error(f"❌ Error crítico al cargar datos desde Dropbox: {e}")
        return None

def fetch_todays_invoices_from_email():
    # Esta función permanece igual
    invoices = []
    try:
        email_secrets = st.secrets.get("email", {})
        email_user = email_secrets.get("address")
        email_password = email_secrets.get("password")
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(email_user, email_password)
        mail.select("TFHKA/Recepcion/Descargados")
        today_date = datetime.now().strftime("%d-%b-%Y")
        search_criteria = f'(SINCE "{today_date}")'
        status, messages = mail.search(None, search_criteria)
        if status != 'OK' or not messages[0]:
            st.info("ℹ️ No se encontraron nuevas facturas por correo en el día de hoy.")
            mail.logout()
            return pd.DataFrame()
        message_ids = messages[0].split()
        st.write(f"Procesando {len(message_ids)} correo(s) nuevo(s) de hoy...")
        progress_bar = st.progress(0)
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
                            if name.endswith('.html'):
                                html_content = zip_file.read(name).decode('utf-8')
                                soup = BeautifulSoup(html_content, 'html.parser')
                                def get_field(label):
                                    tag = soup.find('td', string=re.compile(label))
                                    return tag.find_next_sibling('td').text.strip() if tag else "N/A"
                                invoices.append({
                                    "num_factura": get_field("Num. Factura"), "nombre_proveedor_correo": get_field("Proveedor"),
                                    "fecha_emision_correo": get_field("Fecha Factura"), "fecha_vencimiento_correo": get_field("Fecha Vencimiento"),
                                    "valor_total_correo": get_field("Valor Total"),
                                })
            progress_bar.progress((i + 1) / len(message_ids))
        mail.logout()
        if not invoices:
            return pd.DataFrame()
        df = pd.DataFrame(invoices)
        return df
    except Exception as e:
        st.error(f"❌ Error crítico al procesar los correos: {e}")
        return pd.DataFrame()

# --- Interfaz Principal de la Aplicación (Sin cambios en su lógica interna) ---
def main_app():
    st.image("LOGO FERREINOX SAS BIC 2024.png", width=350)
    st.title("Centro de Control de Facturación Inteligente")
    st.markdown("Bienvenido al sistema proactivo para la conciliación, análisis y predicción de pagos a proveedores.")
    st.markdown("---")

    st.header("Paso 1: Carga y Sincronización de Datos")
    if st.button("🔌 Sincronizar Datos de Correo, ERP y Base de Datos", type="primary", use_container_width=True):
        st.session_state['data_loaded'] = False
        gs_client = connect_to_google_sheets()
        if gs_client:
            with st.spinner("Cargando historial de facturas desde la base de datos..."):
                historical_email_df = load_data_from_gsheet(gs_client, "FacturasCorreo")
            with st.spinner("Buscando nuevas facturas en el correo de hoy..."):
                todays_email_df = fetch_todays_invoices_from_email()
            if not todays_email_df.empty:
                combined_df = pd.concat([historical_email_df, todays_email_df], ignore_index=True)
                combined_df.drop_duplicates(subset=['num_factura'], keep='last', inplace=True)
                with st.spinner("Actualizando la base de datos con las nuevas facturas..."):
                    if update_gsheet_from_df(gs_client, "FacturasCorreo", combined_df):
                        st.success(f"✅ Base de datos actualizada con {len(todays_email_df)} factura(s) nueva(s).")
                email_df = combined_df.copy()
            else:
                email_df = historical_email_df.copy()

            email_df['valor_total_correo'] = email_df['valor_total_correo'].apply(clean_monetary_value)
            email_df['fecha_emision_correo'] = email_df['fecha_emision_correo'].apply(parse_date)
            email_df['fecha_vencimiento_correo'] = email_df['fecha_vencimiento_correo'].apply(parse_date)
            email_df['num_factura'] = email_df['num_factura'].astype(str).str.strip()

            erp_df = load_erp_data_from_dropbox()
            
            st.session_state['erp_df'] = erp_df
            st.session_state['email_df'] = email_df
            
            if erp_df is not None and email_df is not None:
                st.session_state['data_loaded'] = True
            else:
                st.error("No se pudieron cargar todos los datos. Revisa los mensajes de error y vuelve a intentarlo.")

    if st.session_state.get('data_loaded', False):
        st.success("✔ ¡Datos sincronizados! Ya puedes explorar el análisis completo.")
        st.header("Paso 2: Análisis Inteligente y Dashboard")
        
        erp_df = st.session_state['erp_df']
        email_df = st.session_state['email_df']

        with st.expander("Verificar Tabla de Datos del ERP (Dropbox)"):
            st.dataframe(erp_df, use_container_width=True)
            st.write(f"Se encontraron **{len(erp_df)}** registros en el ERP.")

        with st.expander("Verificar Tabla de Datos Extraídos del Correo (Historial + Hoy)"):
            st.dataframe(email_df, use_container_width=True)
            st.write(f"Se encontraron **{len(email_df)}** facturas en total (Base de datos + Nuevas).")
        
        merged_df = pd.merge(erp_df, email_df, on='num_factura', how='outer', suffixes=('_erp', '_correo'))
        today = pd.to_datetime(datetime.now().date())
        merged_df['fecha_vencimiento'] = merged_df['fecha_vencimiento_erp'].fillna(merged_df['fecha_vencimiento_correo'])
        merged_df.dropna(subset=['fecha_vencimiento'], inplace=True)
        merged_df['dias_para_vencer'] = (merged_df['fecha_vencimiento'] - today).dt.days
        def get_status(dias):
            if dias < 0: return "🔴 Vencida"
            elif 0 <= dias <= 7: return "🟠 Por Vencer (Próximos 7 días)"
            else: return "🟢 Vigente"
        merged_df['estado'] = merged_df['dias_para_vencer'].apply(get_status)
        
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Dashboard Principal", "🚨 Centro de Alertas y Discrepancias", "💡 Análisis Predictivo y Proveedores", "🔍 Explorador de Datos"])

        with tab1:
            st.subheader("Indicadores Clave de Rendimiento (KPIs)")
            total_vencido = merged_df[merged_df['estado'] == '🔴 Vencida']['valor_total_erp'].sum()
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Facturado (ERP)", f"${erp_df['valor_total_erp'].sum():,.2f}")
            col2.metric("Monto Total Vencido", f"${total_vencido:,.2f}", delta_color="inverse")
            col3.metric("Facturas Vencidas", f"{len(merged_df[merged_df['estado'] == '🔴 Vencida'])}")
            col4.metric("Facturas por Vencer (7 días)", f"{len(merged_df[merged_df['estado'] == '🟠 Por Vencer (Próximos 7 días)'])}")
            st.markdown("---")
            col_a, col_b = st.columns(2)
            with col_a:
                st.subheader("Estado General de las Facturas")
                status_counts = merged_df['estado'].value_counts().reset_index()
                status_counts.columns = ['estado', 'cantidad']
                chart_status = alt.Chart(status_counts).mark_arc(innerRadius=50).encode(
                    theta=alt.Theta(field="cantidad", type="quantitative"),
                    color=alt.Color(field="estado", type="nominal", title="Estado"),
                    tooltip=['estado', 'cantidad']
                ).properties(title="Distribución por Estado")
                st.altair_chart(chart_status, use_container_width=True)
            with col_b:
                st.subheader("Evolución Mensual de Facturación")
                monthly_total = erp_df.set_index('fecha_emision_erp').resample('M')['valor_total_erp'].sum().reset_index()
                monthly_total['mes'] = monthly_total['fecha_emision_erp'].dt.strftime('%Y-%b')
                chart_monthly = alt.Chart(monthly_total).mark_line(point=True, strokeWidth=3).encode(
                    x=alt.X('mes:N', sort=None, title='Mes'),
                    y=alt.Y('valor_total_erp:Q', title='Suma Facturada'),
                    tooltip=['mes', 'valor_total_erp']
                ).properties(title="Facturación Mensual (ERP)")
                st.altair_chart(chart_monthly, use_container_width=True)

        with tab2:
            st.subheader("Alertas Priorizadas y Gestión de Discrepancias")
            st.error("🔴 **Facturas Vencidas** (Requieren Acción Inmediata)")
            vencidas_df = merged_df[merged_df['estado'] == '🔴 Vencida'].sort_values('dias_para_vencer')
            st.dataframe(vencidas_df[['nombre_proveedor_erp', 'num_factura', 'fecha_vencimiento', 'valor_total_erp', 'dias_para_vencer']].style.background_gradient(cmap='Reds_r', subset=['dias_para_vencer']), use_container_width=True)
            st.warning("🟠 **Facturas por Vencer** (Próximos 7 días)")
            por_vencer_df = merged_df[merged_df['estado'] == '🟠 Por Vencer (Próximos 7 días)'].sort_values('dias_para_vencer')
            st.dataframe(por_vencer_df[['nombre_proveedor_erp', 'num_factura', 'fecha_vencimiento', 'valor_total_erp', 'dias_para_vencer']].style.background_gradient(cmap='Oranges_r', subset=['dias_para_vencer']), use_container_width=True)
            st.info("❗ **Análisis de Discrepancias**")
            unmatched_erp = merged_df[merged_df['nombre_proveedor_correo'].isnull() & merged_df['nombre_proveedor_erp'].notnull()]
            unmatched_email = merged_df[merged_df['nombre_proveedor_erp'].isnull() & merged_df['nombre_proveedor_correo'].notnull()]
            mismatched_values = merged_df.dropna(subset=['valor_total_erp', 'valor_total_correo'])
            mismatched_values = mismatched_values[abs(mismatched_values['valor_total_erp'] - mismatched_values['valor_total_correo']) > 0.01]
            if not mismatched_values.empty:
                st.write("**Inconsistencias en Valor Total:**")
                st.dataframe(mismatched_values[['num_factura', 'nombre_proveedor_erp', 'valor_total_erp', 'valor_total_correo']], use_container_width=True)
            if not unmatched_erp.empty:
                st.write("**Facturas en ERP pero no recibidas por Correo:**")
                st.dataframe(unmatched_erp[['num_factura', 'nombre_proveedor_erp', 'valor_total_erp']], use_container_width=True)
            if not unmatched_email.empty:
                st.write("**Facturas en Correo pero no registradas en ERP:**")
                st.dataframe(unmatched_email[['num_factura', 'nombre_proveedor_correo', 'valor_total_correo']], use_container_width=True)

        with tab3:
            st.subheader("Análisis por Proveedor y Proyección de Pagos")
            st.markdown("#### 👤 Ficha de Desempeño por Proveedor")
            proveedores_lista = ['Todos'] + sorted(erp_df['nombre_proveedor_erp'].dropna().unique().tolist())
            proveedor_seleccionado = st.selectbox("Selecciona un proveedor para analizar en detalle:", proveedores_lista)
            df_filtrado = merged_df if proveedor_seleccionado == 'Todos' else merged_df[merged_df['nombre_proveedor_erp'] == proveedor_seleccionado]
            total_facturado = df_filtrado['valor_total_erp'].sum()
            num_facturas = len(df_filtrado)
            avg_factura = total_facturado / num_facturas if num_facturas > 0 else 0
            facturas_vencidas = len(df_filtrado[df_filtrado['estado'] == '🔴 Vencida'])
            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            kpi1.metric("Total Facturado", f"${total_facturado:,.2f}")
            kpi2.metric("Nº Facturas", f"{num_facturas}")
            kpi3.metric("Valor Promedio", f"${avg_factura:,.2f}")
            kpi4.metric("Facturas Vencidas", f"{facturas_vencidas}")
            if proveedor_seleccionado != 'Todos':
                st.dataframe(df_filtrado[['num_factura', 'fecha_emision_erp', 'fecha_vencimiento', 'valor_total_erp', 'estado']], use_container_width=True)

            st.markdown("#### 📈 Proyección de Pagos a 30 Días")
            df_futuro = merged_df[(merged_df['dias_para_vencer'] >= 0) & (merged_df['dias_para_vencer'] <= 30)]
            if not df_futuro.empty:
                df_futuro['semana'] = pd.cut(df_futuro['dias_para_vencer'], bins=[-1, 7, 14, 21, 31], labels=['Próximos 7 días', 'Semana 2', 'Semana 3', 'Semana 4'])
                proyeccion = df_futuro.groupby('semana', observed=False)['valor_total_erp'].sum().reset_index()
                chart_proyeccion = alt.Chart(proyeccion).mark_bar().encode(
                    x=alt.X('semana:N', sort=None, title="Periodo de Vencimiento"),
                    y=alt.Y('valor_total_erp:Q', title="Monto a Pagar"),
                    tooltip=['semana', 'valor_total_erp']
                ).properties(title="Necesidad de Flujo de Caja para Próximos Pagos")
                st.altair_chart(chart_proyeccion, use_container_width=True)
            else:
                st.info("No hay pagos proyectados en los próximos 30 días.")

        with tab4:
            st.subheader("Explorador de Datos Consolidados")
            st.markdown("Aquí puedes ver, filtrar y descargar la tabla completa con todos los datos cruzados.")
            st.dataframe(merged_df, use_container_width=True)
            
            @st.cache_data
            def convert_df_to_csv(df):
                return df.to_csv(index=False).encode('utf-8')
            csv = convert_df_to_csv(merged_df)
            st.download_button(
                label="📥 Descargar Tabla Completa como CSV",
                data=csv,
                file_name=f'reporte_facturacion_consolidado_{today.strftime("%Y%m%d")}.csv',
                mime='text/csv',
            )

if __name__ == "__main__":
    if check_password():
        main_app()
