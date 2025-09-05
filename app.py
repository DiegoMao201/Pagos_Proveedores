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
# Ahora se leen con la estructura de secciones.
# Los valores se obtienen dentro de las funciones.

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
    Returns `True` if the user entered the correct password, `False` otherwise.
    The password is read in plain text from the 'Secrets' of Streamlit Cloud.
    """
    def password_correct():
        """Checks if the entered password matches the one stored in the secrets."""
        return st.session_state.get("password") == st.secrets.get("password")

    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False

    if not st.session_state["password_correct"]:
        st.header("🔒 Restricted Access")
        with st.form("login_form"):
            st.markdown("Please enter the password to access.")
            password = st.text_input("Password", type="password", key="password")
            st.form_submit_button("Enter", on_click=lambda: st.session_state.update({"password_correct": password_correct()}))
        if "password" in st.session_state and st.session_state["password"] and not st.session_state["password_correct"]:
            st.error("Incorrect password. Please try again.")
        return False
    else:
        return True

# --- Connection and Logic Functions ---
@st.cache_data(show_spinner=False)
def load_dropbox_data():
    """
    Reads the CSV file from Dropbox using a refresh token.
    """
    try:
        dropbox_secrets = st.secrets.get("dropbox", {})
        refresh_token = dropbox_secrets.get("refresh_token")
        app_key = dropbox_secrets.get("app_key")
        app_secret = dropbox_secrets.get("app_secret")
        file_path = "/data/Proveedores.csv"

        if not all([refresh_token, app_key, app_secret]):
            st.error("❌ Missing Dropbox credentials in secrets.")
            return None

        st.info(f"Attempting to connect to Dropbox and read the file: {file_path}")
        dbx = dropbox.Dropbox(
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret
        )
        dbx.users_get_current_account()
        metadata, res = dbx.files_download(file_path)
        csv_bytes = res.content
        csv_file = io.StringIO(csv_bytes.decode('utf-8'))
        df = pd.read_csv(csv_file, sep='{', on_bad_lines='skip')
        st.success("✔ Successful connection to Dropbox and file read.")
        return df
    except dropbox.exceptions.AuthError as auth_err:
        st.error(f"❌ Dropbox authentication error. Check your token and credentials: {auth_err}")
        return None
    except dropbox.exceptions.ApiError as api_err:
        st.error(f"❌ Error loading data from Dropbox: {api_err}. Please verify that the file path is correct.")
        return None
    except Exception as e:
        st.error(f"❌ Error loading data from Dropbox: {e}")
        return None

def fetch_email_invoices():
    """
    Searches for emails with invoice attachments in a zipped folder.
    """
    invoices = []
    try:
        email_secrets = st.secrets.get("email", {})
        email_user = email_secrets.get("address")
        email_password = email_secrets.get("password")
        email_host = "imap.gmail.com"
        
        if not all([email_user, email_password]):
            st.error("❌ Missing email credentials in secrets.")
            return None

        st.info(f"Attempting to connect to email: {email_user} on host: {email_host}")
        mail = imaplib.IMAP4_SSL(email_host)
        mail.login(email_user, email_password)
        
        # Select the "Descargados" directory
        mail.select("Descargados")
        
        st.success("✔ Successful email connection.")
        
        # The search command has been changed to be more compatible.
        status, messages = mail.search(None, "ALL")
        message_ids = messages[0].split()
        
        if not message_ids:
            st.warning("No emails with attachments were found.")
            return pd.DataFrame()

        st.info(f"Found {len(message_ids)} email(s) to review.")
        
        for num in message_ids:
            status, data = mail.fetch(num, "(RFC822)")
            msg = email.message_from_bytes(data[0][1])
            for part in msg.walk():
                if part.get_content_maintype() == "multipart" or part.get("Content-Disposition") is None:
                    continue
                filename = part.get_filename()
                if filename and filename.endswith('.zip'):
                    st.info(f"Found a ZIP file: {filename}")
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
            st.success("✔ Email invoices successfully processed.")
            return pd.DataFrame(invoices)
        else:
            st.warning("No invoices were found in emails with attachments.")
            return pd.DataFrame()
    except imaplib.IMAP4.error as imap_err:
        st.error(f"❌ IMAP authentication or connection error. Check your user, application password, and host: {imap_err}")
        return None
    except Exception as e:
        st.error(f"❌ Unexpected error when processing emails: {e}")
        return None

# --- App UI ---
def main_app():
    """Main function that runs the Streamlit app if login is correct."""
    st.title("🧾 Invoice Management Dashboard")
    st.markdown("---")
    
    with st.sidebar:
        st.header("Configuration")
        st.info("Credentials read from 'Secrets' of Streamlit Cloud.")

        if st.button("Analyze Invoices"):
            with st.spinner("Processing... This might take a few seconds."):
                
                # Step 1: Load ERP data from Dropbox
                st.subheader("Step 1: Load ERP Data")
                erp_data = load_dropbox_data()
                
                if erp_data is not None:
                    st.dataframe(erp_data)
                    
                # Step 2: Extract invoice data from email
                st.subheader("Step 2: Invoice Extraction from Email")
                email_data = fetch_email_invoices()
                
                if email_data is not None:
                    st.dataframe(email_data)
                    
                # Step 3: Perform data analysis and matching
                if erp_data is not None and email_data is not None:
                    st.subheader("Step 3: Data Analysis and Matching")
                    st.warning("Analysis and alerts will be implemented in the next steps.")
                    st.write("Simulated data analysis completed.")
                    
                    st.info("¡Analysis finished! Review the data sections.")

if __name__ == "__main__":
    if check_password():
        main_app()
