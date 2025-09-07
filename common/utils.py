# common/utils.py
# -*- coding: utf-8 -*-
"""
Utilidades compartidas para la conexión y carga de datos desde Google Sheets.
VERSIÓN DE DEPURACIÓN: Imprime el estado de las columnas en cada paso.
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

# --- Carga de Datos con Depuración ---
@st.cache_data(ttl=300, show_spinner="Cargando y validando datos desde Google Sheets...")
def load_data_from_gsheet(_gs_client: gspread.Client) -> pd.DataFrame:
    st.info("--- INICIO DE DEPURACIÓN DE CARGA DE DATOS ---")
    if not _gs_client:
        st.error("Error de depuración: El cliente de Google Sheets es inválido.")
        return pd.DataFrame()

    try:
        spreadsheet = _gs_client.open_by_key(st.secrets["google_sheet_id"])
        worksheet = spreadsheet.worksheet(GSHEET_REPORT_NAME)
        
        records = worksheet.get_all_values()
        if len(records) < 2:
            st.warning("El reporte en Google Sheets está vacío o solo tiene encabezados.")
            return pd.DataFrame()

        df = pd.DataFrame(records[1:], columns=records[0])
        st.write("1. **Columnas originales leídas de Google Sheets:**", df.columns.tolist())

        # 1. Normalización
        original_cols = df.columns.tolist()
        df.columns = [str(col).strip().lower().replace(' ', '_') for col in original_cols]
        st.write("2. **Columnas después de normalizar (minúsculas, guion bajo):**", df.columns.tolist())
        
        # 2. Renombrado
        rename_map = {
            'nombre_proveedor_erp': 'nombre_proveedor',
            'valor_total_erp': 'valor_total_erp',
            'num_factura': 'num_factura'
        }
        valid_rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
        df.rename(columns=valid_rename_map, inplace=True)
        st.write("3. **Columnas después de intentar renombrar 'nombre_proveedor_erp':**", df.columns.tolist())
        
        # 3. Garantizar existencia
        if 'nombre_proveedor' not in df.columns:
            st.error("4. **¡La columna 'nombre_proveedor' NO existe en este punto!** Se procederá a crearla.")
            df['nombre_proveedor'] = 'Proveedor No Especificado'
        else:
            st.success("4. **La columna 'nombre_proveedor' SÍ existe en este punto.**")
        
        if 'valor_total_erp' not in df.columns:
            st.error("La columna 'valor_total_erp' NO existe. Se creará con ceros.")
            df['valor_total_erp'] = 0
        
        st.write("5. **Columnas finales justo antes de retornar el DataFrame:**", df.columns.tolist())
        st.info("--- FIN DE DEPURACIÓN ---")

        # El resto del procesamiento no se incluye para aislar el problema de las columnas.
        # Conversión de tipos numéricos y de fecha
        numeric_cols = ['valor_total_erp'] # Simplificado para la prueba
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        return df

    except Exception as e:
        st.error(f"❌ Ocurrió un error durante la carga y depuración: {e}")
        return pd.DataFrame()
