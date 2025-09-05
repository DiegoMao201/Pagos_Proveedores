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

# --- Configuración de la página de Streamlit ---
st.set_page_config(
    page_title="Sistema de Gestión de Facturas",
    page_icon="🧾",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🧾 Dashboard de Gestión de Facturas")
st.markdown("---")

# --- Funciones de Conexión y Lógica ---
@st.cache_data(show_spinner=False)
def load_dropbox_data(token, file_path):
    """
    Lee el archivo CSV desde Dropbox usando un token de actualización.
    El archivo debe estar en la carpeta 'data/' en tu Dropbox.
    El CSV debe usar el separador '{'.
    """
    try:
        dbx = dropbox.Dropbox(
            oauth2_refresh_token=token,
            app_key=os.getenv("DROPBOX_APP_KEY"),
            app_secret=os.getenv("DROPBOX_APP_SECRET")
        )
        metadata, res = dbx.files_download(file_path)
        
        # El contenido se descarga como bytes, lo convertimos a un objeto similar a un archivo
        csv_bytes = res.content
        csv_file = io.StringIO(csv_bytes.decode('utf-8'))
        
        # Lee el CSV usando el separador especial '{'
        df = pd.read_csv(csv_file, sep='{', on_bad_lines='skip')
        st.success("✔ Archivo de Dropbox cargado exitosamente.")
        return df
    except Exception as e:
        st.error(f"❌ Error al cargar los datos de Dropbox: {e}")
        return None

def fetch_email_invoices(email_user, email_password, email_host):
    """
    Busca correos con archivos adjuntos de facturas en una carpeta comprimida.
    Lee el contenido del HTML y extrae los datos de la factura.
    """
    invoices = []
    try:
        mail = imaplib.IMAP4_SSL(email_host)
        mail.login(email_user, email_password)
        mail.select("inbox")
        
        # Busca correos con un archivo adjunto
        status, messages = mail.search(None, '(HAS_ATTACHMENT)')
        message_ids = messages[0].split()
        
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
                            
                            # --- Lógica de Extracción de Datos del HTML ---
                            # Aquí debes personalizar la lógica de parsing de HTML
                            soup = BeautifulSoup(html_content, 'html.parser')
                            
                            # Ejemplo de extracción usando BeautifulSoup:
                            # (Necesitas adaptar esta parte a la estructura real de tus facturas HTML)
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
        st.success("✔ Facturas del correo electrónico leídas exitosamente.")
        return pd.DataFrame(invoices)
        
    except Exception as e:
        st.error(f"❌ Error al procesar los correos: {e}")
        return None

# --- UI de la aplicación ---
def main():
    """Función principal que ejecuta la aplicación Streamlit."""
    
    with st.sidebar:
        st.header("Configuración")
        st.info("Credenciales leídas desde los 'Secrets' de Streamlit Cloud.")
        
        dropbox_token = os.getenv("DROPBOX_REFRESH_TOKEN")
        email_user = os.getenv("EMAIL_USER")
        email_password = os.getenv("EMAIL_PASSWORD")
        email_host = os.getenv("EMAIL_HOST")

        if st.button("Analizar Facturas"):
            if not dropbox_token or not email_user or not email_password or not email_host:
                st.error("Por favor, asegúrate de que todas las credenciales están configuradas como 'Secrets' en Streamlit Cloud.")
                return

            with st.spinner("Procesando... Esto podría tardar unos segundos."):
                
                # Paso 1: Cargar datos del ERP desde Dropbox
                st.subheader("Paso 1: Carga de datos del ERP")
                erp_data = load_dropbox_data(dropbox_token, "data/Proveedores.csv")
                
                if erp_data is not None:
                    st.dataframe(erp_data)
                    
                # Paso 2: Extraer datos de facturas del correo
                st.subheader("Paso 2: Extracción de facturas del correo")
                email_data = fetch_email_invoices(email_user, email_password, email_host)
                
                if email_data is not None:
                    st.dataframe(email_data)
                    
                # Paso 3: Realizar el análisis y cruce de datos
                if erp_data is not None and email_data is not None:
                    st.subheader("Paso 3: Análisis y cruce de datos")
                    st.warning("El análisis y las alertas se implementarán en los próximos pasos.")
                    st.write("Análisis de datos simulado completado.")
                    
                    st.info("¡Análisis finalizado! Revisa las secciones de datos.")

if __name__ == "__main__":
    main()
