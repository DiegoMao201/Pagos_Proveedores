import streamlit as st
import os
import pandas as pd
from datetime import datetime
import dropbox
import imaplib
import email
from email.header import decode_header
import zipfile
import io
from bs4 import BeautifulSoup
import re

# --- Variables de entorno desde Streamlit Cloud Secrets ---
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_HOST = "imap.gmail.com" # Host para Gmail

# --- Configuración de la página de Streamlit ---
st.set_page_config(
    page_title="Sistema de Gestión de Facturas",
    page_icon="🧾",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Autenticación y Lógica de Protección ---
def check_password():
    """
    Devuelve `True` si el usuario ingresó la contraseña correcta, `False` de lo contrario.
    La contraseña se lee en texto plano desde los 'Secrets' de Streamlit Cloud.
    """
    def password_correct():
        """Comprueba si la contraseña ingresada coincide con la almacenada en los secretos."""
        return st.session_state.get("password") == st.secrets.get("password")

    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False

    if not st.session_state["password_correct"]:
        st.header("🔒 Acceso Restringido")
        with st.form("login_form"):
            st.markdown("Por favor, introduce la contraseña para acceder.")
            password = st.text_input("Contraseña", type="password", key="password")
            st.form_submit_button("Entrar", on_click=lambda: st.session_state.update({"password_correct": password_correct()}))
        if "password" in st.session_state and st.session_state["password"] and not st.session_state["password_correct"]:
            st.error("Contraseña incorrecta. Por favor, inténtalo de nuevo.")
        return False
    else:
        return True

# --- Funciones de Conexión y Lógica ---
@st.cache_data(show_spinner=False)
def load_dropbox_data(token, file_path, app_key, app_secret):
    """
    Lee el archivo CSV desde Dropbox usando un token de actualización.
    """
    try:
        st.info(f"Intentando conectar a Dropbox y leer el archivo: {file_path}")
        dbx = dropbox.Dropbox(
            oauth2_refresh_token=token,
            app_key=app_key,
            app_secret=app_secret
        )
        dbx.users_get_current_account()
        metadata, res = dbx.files_download(file_path)
        csv_bytes = res.content
        csv_file = io.StringIO(csv_bytes.decode('utf-8'))
        df = pd.read_csv(csv_file, sep='{', on_bad_lines='skip')
        st.success("✔ Conexión a Dropbox y lectura del archivo exitosas.")
        return df
    except dropbox.exceptions.AuthError as auth_err:
        st.error(f"❌ Error de autenticación en Dropbox. Revisa tu token y credenciales: {auth_err}")
        return None
    except Exception as e:
        st.error(f"❌ Error al cargar los datos de Dropbox: {e}")
        return None

def fetch_email_invoices(email_user, email_password, email_host):
    """
    Busca correos con archivos adjuntos de facturas en una carpeta comprimida.
    """
    invoices = []
    try:
        st.info(f"Intentando conectar al correo: {email_user} en el host: {email_host}")
        mail = imaplib.IMAP4_SSL(email_host)
        mail.login(email_user, email_password)
        mail.select("inbox")
        st.success("✔ Conexión al correo exitosa.")
        status, messages = mail.search(None, '(HAS_ATTACHMENT)')
        message_ids = messages[0].split()
        
        if not message_ids:
            st.warning("No se encontraron correos con archivos adjuntos.")
            return pd.DataFrame()

        st.info(f"Se encontraron {len(message_ids)} correo(s) con adjuntos.")
        
        for num in message_ids:
            status, data = mail.fetch(num, "(RFC822)")
            msg = email.message_from_bytes(data[0][1])
            for part in msg.walk():
                if part.get_content_maintype() == "multipart" or part.get("Content-Disposition") is None:
                    continue
                filename = part.get_filename()
                if filename and filename.endswith('.zip'):
                    st.info(f"Se encontró un archivo ZIP: {filename}")
                    zip_bytes = part.get_payload(decode=True)
                    zip_file = zipfile.ZipFile(io.BytesIO(zip_bytes))
                    
                    for name in zip_file.namelist():
                        if name.endswith('.html'):
                            html_content = zip_file.read(name).decode('utf-8')
                            soup = BeautifulSoup(html_content, 'html.parser')
                            invoice_number_tag = soup.find('td', string=re.compile("Num. Factura"))
                            invoice_number = invoice_number_tag.find_next_sibling('td').text.strip() if invoice_number_tag else "N/A"
                            monto_tag = soup.find('td', string=re.compile("Total"))
                            monto = monto_tag.find_next_sibling('td').text.strip() if monto_tag else "N/A"
                            proveedor_tag = soup.find('td', string=re.compile("Proveedor"))
                            proveedor = proveedor_tag.find_next_sibling('td').text.strip() if proveedor_tag else "N/A"
                            invoices.append({
                                "num_factura_correo": invoice_number,
                                "proveedor_correo": proveedor,
                                "monto_correo": monto
                            })
        mail.logout()
        if invoices:
            st.success("✔ Facturas del correo electrónico procesadas exitosamente.")
            return pd.DataFrame(invoices)
        else:
            st.warning("No se encontraron facturas en los correos con adjuntos.")
            return pd.DataFrame()
    except imaplib.IMAP4.error as imap_err:
        st.error(f"❌ Error de autenticación o conexión IMAP. Revisa tu usuario, contraseña de aplicación y host: {imap_err}")
        return None
    except Exception as e:
        st.error(f"❌ Error inesperado al procesar los correos: {e}")
        return None

# --- UI de la aplicación ---
def main_app():
    """Función principal que ejecuta la aplicación Streamlit si el login es correcto."""
    st.title("🧾 Dashboard de Gestión de Facturas")
    st.markdown("---")
    
    with st.sidebar:
        st.header("Configuración")
        st.info("Credenciales leídas desde los 'Secrets' de Streamlit Cloud.")

        if st.button("Analizar Facturas"):
            required_secrets = ["DROPBOX_REFRESH_TOKEN", "DROPBOX_APP_KEY", "DROPBOX_APP_SECRET", "EMAIL_USER", "EMAIL_PASSWORD", "EMAIL_HOST", "password"]
            
            missing_secrets = [secret for secret in required_secrets if not st.secrets.get(secret)]
            
            if missing_secrets:
                st.error(f"❌ Faltan los siguientes secretos: {', '.join(missing_secrets)}. Por favor, asegúrate de que todas las credenciales estén configuradas en 'Secrets' de Streamlit Cloud.")
                return

            with st.spinner("Procesando... Esto podría tardar unos segundos."):
                
                # Paso 1: Cargar datos del ERP desde Dropbox
                st.subheader("Paso 1: Carga de datos del ERP")
                erp_data = load_dropbox_data(DROPBOX_REFRESH_TOKEN, "/data/Proveedores.csv", DROPBOX_APP_KEY, DROPBOX_APP_SECRET)
                
                if erp_data is not None:
                    st.dataframe(erp_data)
                    
                # Paso 2: Extraer datos de facturas del correo
                st.subheader("Paso 2: Extracción de facturas del correo")
                email_data = fetch_email_invoices(EMAIL_USER, EMAIL_PASSWORD, EMAIL_HOST)
                
                if email_data is not None:
                    st.dataframe(email_data)
                    
                # Paso 3: Realizar el análisis y cruce de datos
                if erp_data is not None and email_data is not None:
                    st.subheader("Paso 3: Análisis y cruce de datos")
                    st.warning("El análisis y las alertas se implementarán en los próximos pasos.")
                    st.write("Análisis de datos simulado completado.")
                    
                    st.info("¡Análisis finalizado! Revisa las secciones de datos.")

if __name__ == "__main__":
    if check_password():
        main_app()
