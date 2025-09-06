# common/utils.py
import pandas as pd
import streamlit as st
import gspread
import pytz
from google.oauth2.service_account import Credentials
from typing import List

# --- Constantes ---
COLOMBIA_TZ = pytz.timezone('America/Bogota')
GSHEET_REPORT_NAME = "ReporteConsolidado_Activo"

# Columnas que deben ser tratadas como fechas
DATE_COLS = [
    'fecha_emision_erp', 'fecha_vencimiento_erp', 'fecha_emision_correo',
    'fecha_vencimiento_correo', 'fecha_limite_descuento'
]
# Columnas que deben ser tratadas como números
NUMERIC_COLS = [
    'valor_total_erp', 'valor_total_correo', 'dias_para_vencer',
    'descuento_pct', 'valor_descuento', 'valor_con_descuento'
]

@st.cache_resource(show_spinner="Conectando a Google Sheets...")
def connect_to_google_sheets() -> gspread.Client:
    """Establece conexión con la API de Google Sheets."""
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(st.secrets["google_credentials"], scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ Error crítico al autenticar con Google Sheets: {e}")
        return None

@st.cache_data(ttl=300, show_spinner="Cargando datos desde Google Sheets...")
def load_data_from_gsheet(_gs_client: gspread.Client) -> pd.DataFrame:
    """
    Carga los datos del reporte consolidado desde Google Sheets y los procesa.
    El argumento _gs_client se usa para invalidar el caché si la conexión cambia.
    """
    if not _gs_client:
        return pd.DataFrame()

    try:
        spreadsheet = _gs_client.open_by_key(st.secrets["google_sheet_id"])
        worksheet = spreadsheet.worksheet(GSHEET_REPORT_NAME)
        df = pd.DataFrame(worksheet.get_all_records())

        if df.empty:
            st.warning("El reporte en Google Sheets está vacío.")
            return pd.DataFrame()

        # --- Limpieza y Conversión de Tipos de Datos ---
        for col in NUMERIC_COLS:
            if col in df.columns:
                # ### INICIO DE LA CORRECCIÓN (FutureWarning) ###
                # Se elimina la línea df[col].replace('', 0) que causaba la advertencia.
                # La siguiente función es la forma correcta y segura de hacer la conversión,
                # ya que maneja los textos vacíos ('') y otros valores no numéricos.
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                # ### FIN DE LA CORRECCIÓN ###

        for col in DATE_COLS:
            if col in df.columns:
                # Convierte la columna a tipo fecha, los errores se convierten en NaT (Not a Time)
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # Se verifica si la columna es de tipo datetime antes de manipular zonas horarias.
                # Esto evita errores si la conversión anterior falló y la columna no es de tipo fecha.
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    # Si la columna es datetime pero no tiene zona horaria (naive), se le asigna.
                    if df[col].dt.tz is None:
                        df[col] = df[col].dt.tz_localize(COLOMBIA_TZ, ambiguous='infer')
                    # Si ya tiene una zona horaria, se convierte a la de Colombia para estandarizar.
                    else:
                        df[col] = df[col].dt.tz_convert(COLOMBIA_TZ)

        return df

    except gspread.exceptions.WorksheetNotFound:
        st.error(f"No se encontró la hoja '{GSHEET_REPORT_NAME}' en Google Sheets.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Ocurrió un error al cargar los datos de Google Sheets: {e}")
        return pd.DataFrame()
