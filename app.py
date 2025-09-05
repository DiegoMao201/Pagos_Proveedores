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
import altair as alt # Biblioteca para gráficos más avanzados

# --- Configuración de la página de Streamlit ---
st.set_page_config(
    page_title="Centro de Control de Facturación",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Lógica de Autenticación (Sin cambios) ---
def check_password():
    """
    Returns `True` if the user entered the correct password, `False` otherwise.
    The password is read in plain text from the 'Secrets' of Streamlit Cloud.
    """
    def password_correct():
        """Checks if the entered password matches the one stored in the secrets."""
        return st.session_state.get("password") == st.secrets.get("password")

    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False

    if not st.session_state["password_correct"]:
        st.header("🔒 Acceso Restringido al Centro de Control")
        with st.form("login_form"):
            st.markdown("Por favor, ingresa la contraseña para acceder al sistema.")
            password = st.text_input("Contraseña", type="password", key="password")
            st.form_submit_button("Ingresar", on_click=lambda: st.session_state.update({"password_correct": password_correct()}))
        if "password" in st.session_state and st.session_state["password"] and not st.session_state["password_correct"]:
            st.error("Contraseña incorrecta. Por favor, intenta de nuevo.")
        return False
    else:
        return True

# --- Funciones Auxiliares para Limpieza de Datos ---
def clean_monetary_value(value):
    """Limpia y convierte un string monetario a un valor numérico (float)."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        # Elimina símbolos de moneda, puntos de miles y reemplaza la coma decimal por un punto
        value = re.sub(r'[$\s]', '', value)
        value = value.replace('.', '').replace(',', '.')
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    return 0.0

def parse_date(date_str):
    """Convierte un string de fecha a un objeto datetime, probando varios formatos comunes."""
    if pd.isna(date_str) or date_str is None:
        return None
    for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y', '%Y-%m-%d'):
        try:
            return datetime.strptime(str(date_str), fmt)
        except ValueError:
            continue
    return pd.NaT # Retorna 'Not a Time' si no puede parsear

# --- Funciones de Conexión y Lógica de Datos ---
@st.cache_data(show_spinner="Conectando a Dropbox y cargando datos del ERP...")
def load_erp_data_from_dropbox():
    """
    Lee, limpia y estandariza el archivo CSV del ERP desde Dropbox.
    """
    try:
        dropbox_secrets = st.secrets.get("dropbox", {})
        dbx = dropbox.Dropbox(
            oauth2_refresh_token=dropbox_secrets.get("refresh_token"),
            app_key=dropbox_secrets.get("app_key"),
            app_secret=dropbox_secrets.get("app_secret")
        )
        dbx.users_get_current_account() # Verifica la conexión
        
        file_path = "/data/Proveedores.csv"
        _, res = dbx.files_download(file_path)
        
        csv_bytes = res.content
        csv_file = io.StringIO(csv_bytes.decode('latin1'))
        
        df = pd.read_csv(csv_file, sep='{', on_bad_lines='skip')
        
        # **MEJORA: Renombrar columnas para mayor claridad y presentación**
        # (Ajusta los nombres originales 'col1', 'col2', etc., según tu archivo CSV)
        # Basado en la imagen, inferimos los nombres de columna.
        # Es posible que necesites ajustar 'Original_Col_Name' al nombre real en tu CSV.
        column_mapping = {
            df.columns[1]: 'nombre_proveedor_erp',
            df.columns[4]: 'num_factura', # Asumiendo que esta es la columna de la factura
            df.columns[6]: 'fecha_emision_erp',
            df.columns[7]: 'fecha_vencimiento_erp',
            df.columns[8]: 'valor_total_erp'
        }
        df.rename(columns=column_mapping, inplace=True)
        
        # Seleccionar solo las columnas de interés
        df = df[list(column_mapping.values())]

        # **MEJORA: Limpieza y conversión de tipos de datos**
        df['valor_total_erp'] = df['valor_total_erp'].apply(clean_monetary_value)
        df['fecha_emision_erp'] = df['fecha_emision_erp'].apply(parse_date)
        df['fecha_vencimiento_erp'] = df['fecha_vencimiento_erp'].apply(parse_date)
        df['num_factura'] = df['num_factura'].astype(str).str.strip()

        st.success("✔ Datos del ERP cargados y procesados exitosamente desde Dropbox.")
        return df

    except Exception as e:
        st.error(f"❌ Error crítico al cargar datos desde Dropbox: {e}")
        return None

@st.cache_data(show_spinner="Buscando y extrayendo facturas del correo electrónico...")
def fetch_invoices_from_email(_year_to_fetch):
    """
    Busca correos del año en curso, extrae, limpia y consolida los datos de las facturas HTML adjuntas.
    """
    invoices = []
    try:
        email_secrets = st.secrets.get("email", {})
        email_user = email_secrets.get("address")
        email_password = email_secrets.get("password")
        
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(email_user, email_password)
        mail.select("TFHKA/Recepcion/Descargados")
        
        # **MEJORA: Filtrar correos solo del año en curso**
        search_criteria = f'(SINCE "01-Jan-{_year_to_fetch}")'
        status, messages = mail.search(None, search_criteria)
        
        if status != 'OK' or not messages[0]:
            st.warning("No se encontraron correos con facturas para el año en curso.")
            mail.logout()
            return pd.DataFrame()

        message_ids = messages[0].split()
        progress_bar = st.progress(0, text=f"Procesando {len(message_ids)} correos...")

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
                                
                                # Extraer campos del HTML
                                def get_field(label):
                                    tag = soup.find('td', string=re.compile(label))
                                    return tag.find_next_sibling('td').text.strip() if tag else "N/A"

                                invoices.append({
                                    "num_factura": get_field("Num. Factura"),
                                    "nombre_proveedor_correo": get_field("Proveedor"),
                                    "fecha_emision_correo": get_field("Fecha Factura"),
                                    "fecha_vencimiento_correo": get_field("Fecha Vencimiento"),
                                    "valor_total_correo": get_field("Valor Total"),
                                })
            progress_bar.progress((i + 1) / len(message_ids), text=f"Procesando {len(message_ids)} correos...")

        mail.logout()

        if not invoices:
            st.warning("No se encontraron archivos de factura válidos en los correos del año en curso.")
            return pd.DataFrame()
        
        df = pd.DataFrame(invoices)
        
        # **MEJORA: Limpieza y conversión de tipos de datos**
        df['valor_total_correo'] = df['valor_total_correo'].apply(clean_monetary_value)
        df['fecha_emision_correo'] = df['fecha_emision_correo'].apply(parse_date)
        df['fecha_vencimiento_correo'] = df['fecha_vencimiento_correo'].apply(parse_date)
        df['num_factura'] = df['num_factura'].astype(str).str.strip()
        
        st.success(f"✔ Se procesaron {len(df)} facturas desde el correo electrónico.")
        return df

    except Exception as e:
        st.error(f"❌ Error crítico al procesar los correos: {e}")
        return None

# --- Interfaz Principal de la Aplicación ---
def main_app():
    """Función principal que renderiza la interfaz y la lógica de análisis."""
    st.image("https://i.imgur.com/u4AXs0S.png", width=400) # Un logo genérico para dar un toque profesional
    st.title("Centro de Control y Gestión de Facturación")
    st.markdown("Bienvenido al sistema inteligente para la conciliación y análisis de facturas de proveedores.")
    st.markdown("---")

    if st.button("🚀 Iniciar Análisis de Facturación", type="primary", use_container_width=True):
        
        current_year = datetime.now().year
        erp_df = load_erp_data_from_dropbox()
        email_df = fetch_invoices_from_email(current_year)

        if erp_df is None or email_df is None:
            st.error("El análisis no puede continuar debido a errores en la carga de datos. Por favor, revisa los mensajes anteriores.")
            return

        # --- FASE 1: Conciliación de Datos ---
        merged_df = pd.merge(erp_df, email_df, on='num_factura', how='outer', suffixes=('_erp', '_correo'))
        
        # --- FASE 2: Enriquecimiento de Datos y KPIs ---
        today = datetime.now()
        
        # Crear columnas de estado y días para vencimiento
        merged_df['fecha_vencimiento'] = merged_df['fecha_vencimiento_erp'].fillna(merged_df['fecha_vencimiento_correo'])
        merged_df.dropna(subset=['fecha_vencimiento'], inplace=True) # Analizar solo facturas con fecha
        
        merged_df['dias_para_vencer'] = (merged_df['fecha_vencimiento'] - today).dt.days
        
        def get_status(dias):
            if dias < 0:
                return "🔴 Vencida"
            elif 0 <= dias <= 7:
                return "🟠 Por Vencer (Próximos 7 días)"
            else:
                return "🟢 Vigente"
        
        merged_df['estado'] = merged_df['dias_para_vencer'].apply(get_status)

        # Identificar discrepancias
        unmatched_erp = merged_df[merged_df['nombre_proveedor_correo'].isnull()]
        unmatched_email = merged_df[merged_df['nombre_proveedor_erp'].isnull()]
        
        # Comparar valores solo donde ambas fuentes existen
        matched_df = merged_df.dropna(subset=['valor_total_erp', 'valor_total_correo'])
        mismatched_values = matched_df[abs(matched_df['valor_total_erp'] - matched_df['valor_total_correo']) > 0.01] # Tolerancia pequeña

        # --- FASE 3: Visualización y Dashboard ---
        st.markdown("## 📊 Dashboard General de Facturación")
        st.markdown(f"Análisis para el año **{current_year}**.")

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Facturado (ERP)", f"${erp_df['valor_total_erp'].sum():,.2f}")
        with col2:
            st.metric("Facturas Vencidas", f"{len(merged_df[merged_df['estado'] == '🔴 Vencida'])}", f"Suma: ${merged_df[merged_df['dias_para_vencer'] < 0]['valor_total_erp'].sum():,.2f}")
        with col3:
            st.metric("Facturas por Vencer (7 días)", f"{len(merged_df[merged_df['estado'] == '🟠 Por Vencer (Próximos 7 días)'])}")
        with col4:
            st.metric("Discrepancias de Monto", f"{len(mismatched_values)}")
        
        st.markdown("---")
        
        # Pestañas para una navegación limpia
        tab1, tab2, tab3 = st.tabs(["🚨 Centro de Alertas y Pagos", "📈 Análisis Visual", "🔍 Explorador de Datos Completo"])

        with tab1:
            st.subheader("🚨 Centro de Alertas y Pagos")
            st.markdown("Aquí se listan las facturas que requieren atención inmediata.")
            
            st.error("🔴 Facturas Vencidas")
            vencidas_df = merged_df[merged_df['estado'] == '🔴 Vencida'].sort_values('dias_para_vencer')
            st.dataframe(vencidas_df[['nombre_proveedor_erp', 'num_factura', 'fecha_vencimiento', 'valor_total_erp', 'dias_para_vencer']], use_container_width=True)
            
            st.warning("🟠 Facturas por Vencer (Próximos 7 días)")
            por_vencer_df = merged_df[merged_df['estado'] == '🟠 Por Vencer (Próximos 7 días)'].sort_values('dias_para_vencer')
            st.dataframe(por_vencer_df[['nombre_proveedor_erp', 'num_factura', 'fecha_vencimiento', 'valor_total_erp', 'dias_para_vencer']], use_container_width=True)
            
            st.info("❗ Discrepancias Encontradas")
            if not mismatched_values.empty:
                st.write("**Inconsistencias en Valor Total:**")
                st.dataframe(mismatched_values[['num_factura', 'nombre_proveedor_erp', 'valor_total_erp', 'valor_total_correo']], use_container_width=True)
            if not unmatched_erp.empty:
                st.write("**Facturas en ERP pero no en Correo:**")
                st.dataframe(unmatched_erp[['num_factura', 'nombre_proveedor_erp', 'valor_total_erp']], use_container_width=True)
            if not unmatched_email.empty:
                st.write("**Facturas en Correo pero no en ERP:**")
                st.dataframe(unmatched_email[['num_factura', 'nombre_proveedor_correo', 'valor_total_correo']], use_container_width=True)

        with tab2:
            st.subheader("📈 Análisis Visual de la Facturación")
            
            # Gráfico 1: Total facturado por proveedor
            provider_total = erp_df.groupby('nombre_proveedor_erp')['valor_total_erp'].sum().reset_index().sort_values('valor_total_erp', ascending=False)
            chart1 = alt.Chart(provider_total.head(10)).mark_bar().encode(
                x=alt.X('valor_total_erp:Q', title='Valor Total Facturado'),
                y=alt.Y('nombre_proveedor_erp:N', sort='-x', title='Proveedor'),
                tooltip=['nombre_proveedor_erp', 'valor_total_erp']
            ).properties(
                title='Top 10 Proveedores por Monto Facturado'
            )
            st.altair_chart(chart1, use_container_width=True)

            # Gráfico 2: Evolución de la facturación mensual
            monthly_total = erp_df.set_index('fecha_emision_erp').resample('M')['valor_total_erp'].sum().reset_index()
            monthly_total['mes'] = monthly_total['fecha_emision_erp'].dt.strftime('%Y-%b')
            chart2 = alt.Chart(monthly_total).mark_line(point=True).encode(
                x=alt.X('mes:N', sort=None, title='Mes de Emisión'),
                y=alt.Y('valor_total_erp:Q', title='Suma Total Facturada'),
                tooltip=['mes', 'valor_total_erp']
            ).properties(
                title='Evolución Mensual de la Facturación'
            )
            st.altair_chart(chart2, use_container_width=True)

        with tab3:
            st.subheader("🔍 Explorador de Datos Completo")
            st.markdown("Utiliza los filtros para explorar la tabla de datos consolidados.")
            st.dataframe(merged_df, use_container_width=True)
            
            # Opción para descargar los datos
            @st.cache_data
            def convert_df_to_csv(df):
                return df.to_csv(index=False).encode('utf-8')

            csv = convert_df_to_csv(merged_df)
            st.download_button(
                label="📥 Descargar Tabla Completa como CSV",
                data=csv,
                file_name=f'reporte_facturacion_{today.strftime("%Y%m%d")}.csv',
                mime='text/csv',
            )

if __name__ == "__main__":
    if check_password():
        main_app()
