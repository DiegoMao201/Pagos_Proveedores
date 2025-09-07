# common/utils.py
# -*- coding: utf-8 -*-
"""
Utilidades compartidas para la conexión y carga de datos desde Google Sheets.
Versión 3.3: Se robustece la carga de datos para garantizar la existencia de
columnas críticas ('nombre_proveedor', 'valor_total_erp') aunque estén vacías
en el origen, previniendo errores en las páginas de la aplicación.
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
    Carga datos desde Google Sheets, normaliza columnas y garantiza la existencia
    de columnas críticas para el funcionamiento de la aplicación.
    """
    if not _gs_client:
        return pd.DataFrame()

    try:
        spreadsheet = _gs_client.open_by_key(st.secrets["google_sheet_id"])
        worksheet = spreadsheet.worksheet(GSHEET_REPORT_NAME)
        
        # Se leen todos los valores para tener control total, incluso sobre filas vacías.
        records = worksheet.get_all_values()
        if len(records) < 2:
            st.warning("El reporte en Google Sheets está vacío o solo tiene encabezados.")
            return pd.DataFrame()

        # Se crea el DataFrame a partir de la segunda fila (datos), usando la primera como encabezados.
        df = pd.DataFrame(records[1:], columns=records[0])

        # 1. Normalización de Nombres de Columnas
        # Convierte a minúsculas, quita espacios y reemplaza por guiones bajos.
        original_cols = df.columns.tolist()
        df.columns = [str(col).strip().lower().replace(' ', '_') for col in original_cols]
        
        # Mapeo para estandarizar nombres clave.
        rename_map = {
            'nombre_proveedor_erp': 'nombre_proveedor',
            'valor_total_erp': 'valor_total_erp',
            'num_factura': 'num_factura'
        }
        valid_rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
        df.rename(columns=valid_rename_map, inplace=True)
        
        # 2. <-- INICIO DE LA CORRECCIÓN CRÍTICA -->
        # Este bloque es la solución definitiva. Garantiza que las columnas que
        # causan errores en otras páginas existan siempre.
        if 'nombre_proveedor' not in df.columns:
            st.warning("⚠️ La columna 'nombre_proveedor' no fue encontrada o está vacía en Google Sheets. Se creará una columna con valores por defecto para evitar errores.")
            df['nombre_proveedor'] = 'Proveedor No Especificado'
        
        if 'valor_total_erp' not in df.columns:
            st.warning("⚠️ La columna 'valor_total_erp' no fue encontrada. Se creará y llenará con ceros.")
            df['valor_total_erp'] = 0
        # <-- FIN DE LA CORRECCIÓN CRÍTICA -->

        # 3. Limpieza de Datos de Estado de Factura
        if 'estado_factura' in df.columns:
            df['estado_factura'] = df['estado_factura'].astype(str).str.strip().str.capitalize()
            df['estado_factura'].replace('', 'Pendiente', inplace=True)
        else:
            st.warning("La columna 'estado_factura' no fue encontrada. Se asumirá que todas las facturas están 'Pendiente'.")
            df['estado_factura'] = 'Pendiente'

        # 4. Conversión de Tipos de Datos (Numéricos y Fechas)
        numeric_cols = ['valor_total_erp', 'valor_total_correo', 'dias_para_vencer', 'valor_descuento', 'valor_con_descuento']
        for col in numeric_cols:
            if col in df.columns:
                # Se reemplazan comas por puntos para el formato decimal y se convierte a numérico.
                if df[col].dtype == 'object':
                    df[col] = df[col].str.replace(',', '.', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        date_cols = ['fecha_emision_erp', 'fecha_vencimiento_erp', 'fecha_emision_correo', 'fecha_vencimiento_correo', 'fecha_limite_descuento']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                # Se localiza la zona horaria de Colombia para un manejo correcto de fechas.
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    if df[col].dt.tz is None:
                        df[col] = df[col].dt.tz_localize(COLOMBIA_TZ, ambiguous='infer')
                    else:
                        df[col] = df[col].dt.tz_convert(COLOMBIA_TZ)
        
        return df

    except gspread.exceptions.WorksheetNotFound:
        st.error(f"❌ Error fatal: No se encontró la hoja '{GSHEET_REPORT_NAME}' en el archivo de Google Sheets.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Ocurrió un error inesperado al cargar los datos de Google Sheets: {e}")
        return pd.DataFrame()
