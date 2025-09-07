# common/utils.py
# -*- coding: utf-8 -*-
"""
Utilidades compartidas para la conexión y carga de datos desde Google Sheets.
Versión 3.2: Se añade una validación para asegurar la existencia de columnas críticas
incluso si vienen vacías desde la fuente de datos.
"""

import pandas as pd
import streamlit as st
import gspread
import pytz
from google.oauth2.service_account import Credentials

# --- Constantes ---
COLOMBIA_TZ = pytz.timezone('America/Bogota')
GSHEET_REPORT_NAME = "ReporteConsolidado_Activo"

# --- Conexión a Google Sheets ---
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
        # Usamos get_all_values para tener más control sobre los datos vacíos
        records = worksheet.get_all_values()
        if len(records) < 2:
            st.warning("El reporte en Google Sheets está vacío o solo tiene encabezados.")
            return pd.DataFrame()

        df = pd.DataFrame(records[1:], columns=records[0])

        # Normaliza los nombres de las columnas
        original_cols = df.columns.tolist()
        df.columns = [str(col).strip().lower().replace(' ', '_') for col in original_cols]
        
        rename_map = {
            'nombre_proveedor_erp': 'nombre_proveedor',
            'valor_total_erp': 'valor_total_erp',
            'num_factura': 'num_factura'
        }
        valid_rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
        df.rename(columns=valid_rename_map, inplace=True)
        
        # <-- INICIO DE LA CORRECCIÓN CRÍTICA -->
        # Se asegura que las columnas fundamentales existan para evitar errores posteriores.
        # Si la columna viene vacía de GSheets, pandas puede no crearla.
        if 'nombre_proveedor' not in df.columns:
            st.warning("⚠️ La columna 'nombre_proveedor' no fue encontrada o está vacía. Se creará una columna con valores por defecto.")
            df['nombre_proveedor'] = 'Proveedor No Especificado'
        
        if 'valor_total_erp' not in df.columns:
            st.warning("⚠️ La columna 'valor_total_erp' no fue encontrada. Se creará y llenará con ceros.")
            df['valor_total_erp'] = 0
        # <-- FIN DE LA CORRECCIÓN CRÍTICA -->

        # Limpieza de datos de estado
        if 'estado_factura' in df.columns:
            df['estado_factura'] = df['estado_factura'].astype(str).str.strip().str.capitalize()
            df['estado_factura'].replace('', 'Pendiente', inplace=True)
        else:
            st.warning("La columna 'estado_factura' no fue encontrada. Se asumirá que todas las facturas están 'Pendiente'.")
            df['estado_factura'] = 'Pendiente'

        # Conversión de Tipos de Datos
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
