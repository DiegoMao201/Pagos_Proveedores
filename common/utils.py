# common/utils.py
import pandas as pd
import streamlit as st
import gspread
import pytz
from google.oauth2.service_account import Credentials
from typing import List

# --- Constantes Globales ---
# Centralizar constantes aquí es una excelente práctica para mantener la consistencia en toda la app.
COLOMBIA_TZ = pytz.timezone('America/Bogota')
GSHEET_REPORT_NAME = "ReporteConsolidado_Activo"

# Listas de columnas para asegurar una conversión de tipos de datos consistente y sin errores.
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
    """
    Establece y gestiona la conexión con la API de Google Sheets.
    Usa @st.cache_resource para asegurar que la conexión se establezca una sola vez
    y se reutilice en toda la sesión del usuario, mejorando el rendimiento.
    """
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
    Carga, procesa y cachea los datos desde Google Sheets.
    Usa @st.cache_data para guardar el DataFrame procesado en memoria por un tiempo (ttl=300s),
    evitando recargas innecesarias desde la red y acelerando la respuesta de la app.
    
    El argumento _gs_client, aunque no se usa directamente en la lógica,
    sirve para que Streamlit invalide el caché si el objeto de conexión cambia.
    """
    if not _gs_client:
        st.warning("La conexión a Google Sheets no está disponible.")
        return pd.DataFrame()

    try:
        spreadsheet = _gs_client.open_by_key(st.secrets["google_sheet_id"])
        worksheet = spreadsheet.worksheet(GSHEET_REPORT_NAME)
        # get_all_records() es conveniente porque convierte la hoja en una lista de diccionarios.
        df = pd.DataFrame(worksheet.get_all_records())

        if df.empty:
            st.warning("El reporte en Google Sheets está vacío o no se pudo leer.")
            return pd.DataFrame()

        # --- Limpieza y Conversión de Tipos de Datos ---
        # Este bloque es crucial para evitar errores en cálculos y visualizaciones posteriores.
        
        # Procesa columnas numéricas
        for col in NUMERIC_COLS:
            if col in df.columns:
                # gspread puede leer celdas vacías como ''. Reemplazarlas asegura una conversión numérica limpia.
                df[col] = df[col].replace('', 0)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # Procesa columnas de fecha
        for col in DATE_COLS:
            if col in df.columns:
                # errors='coerce' convierte cualquier formato de fecha inválido en NaT (Not a Time).
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # Este bloque maneja correctamente las zonas horarias, lo cual es vital para cálculos de vencimiento.
                # Si la fecha no tiene zona horaria (naive), se le asigna la de Colombia.
                if pd.api.types.is_datetime64_any_dtype(df[col]) and df[col].dt.tz is None:
                    df[col] = df[col].dt.tz_localize(COLOMBIA_TZ, ambiguous='infer')
                # Si ya tiene una zona horaria, se convierte a la de Colombia para estandarizar.
                else:
                    df[col] = df[col].dt.tz_convert(COLOMBIA_TZ)

        return df

    except gspread.exceptions.WorksheetNotFound:
        st.error(f"Error Crítico: No se encontró la hoja de cálculo requerida ('{GSHEET_REPORT_NAME}') en el archivo de Google Sheets.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Ocurrió un error inesperado al cargar los datos de Google Sheets: {e}")
        return pd.DataFrame()
