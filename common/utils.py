# common/utils.py
# -*- coding: utf-8 -*-
"""
Utilidades compartidas para la conexión y carga de datos desde Google Sheets.
Versión 3.1: Se añade una lógica de limpieza y normalización robusta para
evitar errores por inconsistencias en los nombres de columnas o datos en la hoja.
"""

import pandas as pd
import streamlit as st
import gspread
import pytz
from google.oauth2.service_account import Credentials

# --- Constantes ---
COLOMBIA_TZ = pytz.timezone('America/Bogota')
GSHEET_REPORT_NAME = "ReporteConsolidado_Activo"

# --- Conexión a Google Sheets (sin cambios) ---
@st.cache_resource(show_spinner="Conectando a Google Sheets...")
def connect_to_google_sheets() -> gspread.Client:
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(st.secrets["google_credentials"], scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ Error crítico al autenticar con Google Sheets: {e}")
        return None

# --- Carga de Datos Mejorada ---
@st.cache_data(ttl=300, show_spinner="Cargando y limpiando datos desde Google Sheets...")
def load_data_from_gsheet(_gs_client: gspread.Client) -> pd.DataFrame:
    """
    Carga datos desde Google Sheets y aplica una limpieza y normalización robusta.
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

        # <-- INICIO DE LA MEJORA: NORMALIZACIÓN DE COLUMNAS -->
        # Normaliza los nombres de las columnas: minúsculas, sin espacios extra, reemplaza espacios con guion bajo.
        # Esto hace que 'Estado Factura' o ' estado_factura ' se conviertan en 'estado_factura'.
        original_cols = df.columns.tolist()
        df.columns = [str(col).strip().lower().replace(' ', '_') for col in original_cols]
        
        # Renombramos explícitamente para asegurar compatibilidad con el resto del código
        # Si tus columnas se llaman diferente, ajústalas aquí.
        rename_map = {
            'nombre_proveedor_erp': 'nombre_proveedor',
            'valor_total_erp': 'valor_total_erp', # Ejemplo, si ya está bien, no cambia
            'num_factura': 'num_factura'
        }
        # Filtramos el mapa de renombre para solo incluir columnas que existen
        valid_rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
        df.rename(columns=valid_rename_map, inplace=True)
        # <-- FIN DE LA MEJORA -->

        # <-- INICIO DE LA MEJORA: LIMPIEZA DE DATOS DE ESTADO -->
        # Asegura que la columna 'estado_factura' exista
        if 'estado_factura' in df.columns:
            # Limpia la columna de estado: quita espacios, capitaliza y llena vacíos.
            # ' pendiente ', 'Pendiente', '' (vacío) se convierten todos en 'Pendiente'.
            df['estado_factura'] = df['estado_factura'].astype(str).str.strip().str.capitalize()
            df['estado_factura'].replace('', 'Pendiente', inplace=True)
        else:
            # Si la columna no existe, la crea y asume que todo está pendiente.
            st.warning("La columna 'estado_factura' no fue encontrada. Se asumirá que todas las facturas están 'Pendiente'.")
            df['estado_factura'] = 'Pendiente'
        # <-- FIN DE LA MEJORA -->

        # --- Conversión de Tipos de Datos (como antes) ---
        numeric_cols = ['valor_total_erp', 'valor_total_correo', 'dias_para_vencer', 'valor_descuento', 'valor_con_descuento']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        date_cols = ['fecha_emision_erp', 'fecha_vencimiento_erp', 'fecha_emision_correo', 'fecha_vencimiento_correo', 'fecha_limite_descuento']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    if df[col].dt.tz is None:
                        df[col] = df[col].dt.tz_localize(COLOMBIA_TZ, ambiguous='infer')
                    else:
                        df[col] = df[col].dt.tz_convert(COLOMBIA_TZ)
        
        return df

    except gspread.exceptions.WorksheetNotFound:
        st.error(f"No se encontró la hoja '{GSHEET_REPORT_NAME}' en Google Sheets.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Ocurrió un error al cargar los datos de Google Sheets: {e}")
        return pd.DataFrame()
