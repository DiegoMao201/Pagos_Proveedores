# common/utils.py
# -*- coding: utf-8 -*-
"""
Utilidades compartidas para la conexión y carga de datos desde Google Sheets.
Versión 3.2: Lógica de mapeo de columnas robusta. Busca la columna correcta
a partir de una lista de posibles nombres (alias) y valida que todas las
columnas críticas existan, dando errores claros al usuario si faltan.
"""

import pandas as pd
import streamlit as st
import gspread
import pytz
from google.oauth2.service_account import Credentials

# --- Constantes ---
COLOMBIA_TZ = pytz.timezone('America/Bogota')
GSHEET_REPORT_NAME = "ReporteConsolidado_Activo"

# <-- CAMBIO CLAVE: MAPA DE ALIAS PARA NOMBRES DE COLUMNAS -->
# El código buscará estos nombres en tu hoja y los estandarizará al nombre principal.
# Puedes añadir más alias si usas otros nombres en tu hoja.
COLUMN_ALIASES = {
    'nombre_proveedor': ['proveedor', 'nombre del proveedor', 'nombre proveedor', 'nombre_proveedor_erp'],
    'num_factura': ['factura', 'nro factura', 'nº factura', 'num_factura'],
    'valor_total_erp': ['valor', 'total', 'valor total', 'valor_total_erp'],
    'fecha_emision_erp': ['fecha emision', 'fecha de emision', 'fecha_emision_erp'],
    'fecha_vencimiento_erp': ['fecha vencimiento', 'vencimiento', 'fecha_vencimiento_erp'],
    'estado_factura': ['estado', 'estado factura', 'estado_factura'],
    'estado_pago': ['estado pago', 'estado_pago'],
    'dias_para_vencer': ['dias para vencer', 'días para vencer', 'dias_para_vencer'],
    'valor_descuento': ['descuento', 'valor descuento', 'valor_descuento'],
    'valor_con_descuento': ['valor con descuento', 'valor_con_descuento'],
    'id_lote_pago': ['id lote', 'id_lote_pago'],
}

# Columnas que son críticas para que la app funcione.
CRITICAL_COLUMNS = ['nombre_proveedor', 'num_factura', 'valor_total_erp']

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

# --- Carga de Datos Súper Robusta ---
@st.cache_data(ttl=300, show_spinner="Cargando y validando datos desde Google Sheets...")
def load_data_from_gsheet(_gs_client: gspread.Client) -> pd.DataFrame:
    """
    Carga datos, los normaliza usando un mapa de alias y valida que las
    columnas críticas existan, proporcionando errores claros.
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

        # Normaliza todos los encabezados leídos de la hoja para una comparación fácil
        df.columns = [str(col).strip().lower() for col in df.columns]

        # <-- CAMBIO CLAVE: RENOMBRADO INTELIGENTE USANDO ALIAS -->
        rename_map = {}
        for standard_name, aliases in COLUMN_ALIASES.items():
            for alias in aliases:
                if alias in df.columns:
                    rename_map[alias] = standard_name
                    break # Pasa al siguiente nombre estándar una vez que encuentra una coincidencia
        df.rename(columns=rename_map, inplace=True)

        # <-- CAMBIO CLAVE: VALIDACIÓN DE COLUMNAS CRÍTICAS -->
        missing_cols = [col for col in CRITICAL_COLUMNS if col not in df.columns]
        if missing_cols:
            st.error(
                f"🚨 ¡Faltan Columnas Críticas! No se encontraron las siguientes columnas en tu Google Sheet: **{', '.join(missing_cols)}**."
                f"\n\nPor favor, asegúrate de que tu hoja tenga columnas con nombres como: `{', '.join(COLUMN_ALIASES[missing_cols[0]])}`."
            )
            return pd.DataFrame() # Detiene la ejecución si faltan columnas

        # --- Limpieza de Datos de Estado (como antes) ---
        if 'estado_factura' in df.columns:
            df['estado_factura'] = df['estado_factura'].astype(str).str.strip().str.capitalize().replace('', 'Pendiente')
        else:
            df['estado_factura'] = 'Pendiente'

        # --- Conversión de Tipos de Datos (como antes) ---
        numeric_cols = ['valor_total_erp', 'valor_total_correo', 'dias_para_vencer', 'valor_descuento', 'valor_con_descuento']
        for col in numeric_cols:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        date_cols = ['fecha_emision_erp', 'fecha_vencimiento_erp', 'fecha_emision_correo', 'fecha_vencimiento_correo', 'fecha_limite_descuento']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.tz_localize(None) # Quita TZ antes de asignar la correcta
                    df[col] = df[col].dt.tz_localize(COLOMBIA_TZ, ambiguous='infer')

        return df

    except gspread.exceptions.WorksheetNotFound:
        st.error(f"No se encontró la hoja '{GSHEET_REPORT_NAME}' en Google Sheets.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Ocurrió un error al cargar los datos de Google Sheets: {e}")
        return pd.DataFrame()
