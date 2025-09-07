# common/utils.py
# -*- coding: utf-8 -*-
"""
Utilidades compartidas para la conexión y carga de datos desde Google Sheets.
Versión 3.4: Se añade la eliminación automática de columnas duplicadas para
prevenir errores cuando el origen de datos es inconsistente.
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
    """Establece la conexión con la API de Google Sheets de forma segura."""
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(st.secrets["google_credentials"], scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ Error crítico al autenticar con Google Sheets: {e}")
        return None

# --- Carga de Datos Mejorada y Robusta ---
@st.cache_data(ttl=300, show_spinner="Cargando y validando datos desde Google Sheets...")
def load_data_from_gsheet(_gs_client: gspread.Client) -> pd.DataFrame:
    """
    Carga datos, normaliza, elimina duplicados y garantiza la existencia
    de columnas críticas para el funcionamiento de la aplicación.
    """
    if not _gs_client:
        return pd.DataFrame()

    try:
        spreadsheet = _gs_client.open_by_key(st.secrets["google_sheet_id"])
        worksheet = spreadsheet.worksheet(GSHEET_REPORT_NAME)
        
        records = worksheet.get_all_values()
        if len(records) < 2:
            st.warning("El reporte en Google Sheets está vacío o solo tiene encabezados.")
            return pd.DataFrame()

        df = pd.DataFrame(records[1:], columns=records[0])

        # 1. Normalización de Nombres de Columnas
        original_cols = df.columns.tolist()
        df.columns = [str(col).strip().lower().replace(' ', '_') for col in original_cols]
        
        # 2. Renombrado
        rename_map = {
            'nombre_proveedor_erp': 'nombre_proveedor'
        }
        valid_rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
        df.rename(columns=valid_rename_map, inplace=True)

        # 3. <-- INICIO DE LA CORRECCIÓN CRÍTICA -->
        # Esta línea elimina las columnas duplicadas, conservando la primera que encuentra.
        df = df.loc[:, ~df.columns.duplicated(keep='first')]
        # <-- FIN DE LA CORRECCIÓN CRÍTICA -->

        # 4. Garantizar la existencia de columnas críticas
        if 'nombre_proveedor' not in df.columns:
            df['nombre_proveedor'] = 'Proveedor No Especificado'
        
        if 'valor_total_erp' not in df.columns:
            df['valor_total_erp'] = 0

        # 5. Limpieza y Conversión de Tipos
        if 'estado_factura' in df.columns:
            df['estado_factura'] = df['estado_factura'].astype(str).str.strip().str.capitalize().replace('', 'Pendiente')
        else:
            df['estado_factura'] = 'Pendiente'

        numeric_cols = ['valor_total_erp', 'valor_total_correo', 'dias_para_vencer', 'valor_descuento', 'valor_con_descuento']
        for col in numeric_cols:
            if col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        date_cols = ['fecha_emision_erp', 'fecha_vencimiento_erp', 'fecha_emision_correo', 'fecha_vencimiento_correo', 'fecha_limite_descuento']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.tz_localize(None).dt.tz_localize(COLOMBIA_TZ, ambiguous='infer')
        
        return df

    except gspread.exceptions.WorksheetNotFound:
        st.error(f"❌ Error fatal: No se encontró la hoja '{GSHEET_REPORT_NAME}'.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Ocurrió un error inesperado al cargar los datos: {e}")
        return pd.DataFrame()
