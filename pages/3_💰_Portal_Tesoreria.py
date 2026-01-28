# -*- coding: utf-8 -*-
"""
Módulo de Portal de Tesorería (Versión 4.0 - Mejorado).

Esta página es para el uso exclusivo del equipo de Tesorería. Permite
visualizar los lotes de pago generados por Gerencia, inspeccionar el detalle,
descargar un soporte en Excel y, finalmente, marcar los lotes como 'Pagado'.

Esta versión corrige la lógica de carga de datos para ser consistente
con el módulo de Gerencia, asegurando que las facturas de cada lote se
muestren correctamente.
"""

# --- 0. IMPORTACIÓN DE LIBRERÍAS ---
import streamlit as st
import pandas as pd
import gspread
import io
import pytz
from google.oauth2.service_account import Credentials
import os

# ======================================================================================
# --- INICIO DEL BLOQUE DE SEGURIDAD ---
# Este es el código que debes añadir al principio de cada página protegida.
# ======================================================================================

# 1. Se asegura de que la variable de sesión exista para evitar errores.
if 'password_correct' not in st.session_state:
    st.session_state['password_correct'] = False

# 2. Verifica si la contraseña es correcta (si el usuario ya inició sesión en la página principal).
#    Si no es correcta, muestra un mensaje de error y detiene la carga de la página.
if not st.session_state["password_correct"]:
    st.error("🔒 Debes iniciar sesión para acceder a esta página.")
    st.info("Por favor, ve a la página principal 'Dashboard General' para ingresar la contraseña.")
    st.stop() # ¡Este comando es clave! Detiene la ejecución del resto del script.

# --- FIN DEL BLOQUE DE SEGURIDAD ---


# --- INICIO: Lógica de common/utils.py integrada ---

# --- Constantes ---
COLOMBIA_TZ = pytz.timezone('America/Bogota')
GSHEET_REPORT_NAME = "ReporteConsolidado_Activo"

# --- Conexión a Google Sheets ---
@st.cache_resource(show_spinner="Conectando a Google Sheets...")
def connect_to_google_sheets() -> gspread.Client:
    """Establece la conexión con la API de Google Sheets de forma segura."""
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds_dict = st.secrets["google_credentials"]
        # Asegurarse de que private_key_id no sea None, lo que puede ocurrir con la serialización de secrets.
        if creds_dict.get("private_key_id") is None:
            creds_dict.pop("private_key_id", None)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ Error crítico al autenticar con Google Sheets: {e}")
        return None

# --- FIN: Lógica de common/utils.py integrada ---


# --- 1. CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(
    layout="wide",
    page_title="Portal de Tesorería",
    page_icon="💰"
)

# --- 2. CONSTANTES ---
# Nombres de las hojas de Google Sheets para evitar errores de tipeo
GSHEET_LOTES_NAME = "Historial_Lotes_Pago"
# Estados que se consideran como pendientes de pago para Tesorería
PENDING_STATUSES = ["Pendiente de Pago", "Pendiente de Pago URGENTE"]


# --- 3. FUNCIONES AUXILIARES ---

def to_excel(df: pd.DataFrame) -> bytes:
    """Convierte un DataFrame a un archivo Excel en memoria."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Detalle_Pago')
        # Auto-ajustar el ancho de las columnas para mejor visualización
        for column in df:
            column_width = max(df[column].astype(str).map(len).max(), len(column))
            col_idx = df.columns.get_loc(column)
            writer.sheets['Detalle_Pago'].set_column(col_idx, col_idx, column_width)
    processed_data = output.getvalue()
    return processed_data

@st.cache_data(ttl=300, show_spinner="Cargando Lotes de Pago desde Google Sheets...")
def load_data(_gs_client: gspread.Client):
    """Carga los lotes y las facturas desde Google Sheets de forma robusta."""
    try:
        spreadsheet = _gs_client.open_by_key(st.secrets["google_sheet_id"])
        
        # Cargar historial de lotes
        historial_ws = spreadsheet.worksheet(GSHEET_LOTES_NAME)
        df_lotes = pd.DataFrame(historial_ws.get_all_records())
        
        # --- INICIO DE LA CORRECCIÓN ---
        # Cargar reporte consolidado con la misma lógica robusta del planificador
        reporte_ws = spreadsheet.worksheet(GSHEET_REPORT_NAME)
        reporte_data = reporte_ws.get_all_values()
        if len(reporte_data) < 2:
            st.warning("El reporte consolidado está vacío.")
            return df_lotes, pd.DataFrame()

        # Normalizar encabezados para asegurar consistencia
        reporte_headers = [str(h).strip().lower().replace(' ', '_') for h in reporte_data[0]]
        df_reporte = pd.DataFrame(reporte_data[1:], columns=reporte_headers)

        if 'nombre_proveedor_erp' in df_reporte.columns:
            df_reporte.rename(columns={'nombre_proveedor_erp': 'nombre_proveedor'}, inplace=True)

        # Limpiar columnas duplicadas si existen
        df_reporte = df_reporte.loc[:, ~df_reporte.columns.duplicated(keep='first')]
        # --- FIN DE LA CORRECCIÓN ---

        return df_lotes, df_reporte
        
    except gspread.exceptions.WorksheetNotFound as e:
        st.error(f"Error Crítico: No se encontró la hoja de cálculo '{e.args[0]}'. Verifica los nombres en Google Sheets.")
    except Exception as e:
        st.error(f"No se pudieron cargar los datos desde Google Sheets: {e}")
    return pd.DataFrame(), pd.DataFrame()

def procesar_pago_lote(gs_client: gspread.Client, lote_id: str, df_reporte: pd.DataFrame):
    """
    Actualiza el estado del lote a 'Pagado' y el estado de todas las
    facturas asociadas a 'Pagada'.
    """
    try:
        spreadsheet = gs_client.open_by_key(st.secrets["google_sheet_id"])
        
        # --- 1. Actualizar el estado del LOTE en 'Historial_Lotes_Pago' ---
        historial_ws = spreadsheet.worksheet(GSHEET_LOTES_NAME)
        cell = historial_ws.find(lote_id)
        if not cell:
            st.error(f"No se pudo encontrar el lote con ID {lote_id} en el historial.")
            return False
            
        headers = historial_ws.row_values(1)
        # Usar un bloque try-except para mayor seguridad si la columna no existe
        try:
            estado_lote_col = headers.index('estado_lote') + 1
            historial_ws.update_cell(cell.row, estado_lote_col, 'Pagado')
        except ValueError:
            st.error("No se encontró la columna 'estado_lote' en la hoja de historial.")
            return False

        # --- 2. Actualizar el estado de las FACTURAS en el reporte principal ---
        facturas_del_lote = df_reporte[df_reporte['id_lote_pago'] == lote_id]
        if facturas_del_lote.empty:
            st.warning("El lote se marcó como pagado, pero no se encontraron facturas asociadas en el reporte para actualizar.")
            return True

        reporte_ws = spreadsheet.worksheet(GSHEET_REPORT_NAME)
        # Volver a leer los encabezados para asegurar la posición correcta de las columnas
        reporte_headers_raw = reporte_ws.row_values(1)
        
        try:
            estado_factura_col_idx = reporte_headers_raw.index('estado_factura') + 1
        except ValueError:
            st.error(f"No se encontró la columna 'estado_factura' en la hoja '{GSHEET_REPORT_NAME}'.")
            return False

        updates = []
        # Para encontrar la fila correcta, necesitamos el índice original del DataFrame
        # que corresponde a la fila en la hoja de cálculo
        for index, _ in facturas_del_lote.iterrows():
            row_to_update = int(index) + 2 # +2 por el encabezado y el índice base 0
            updates.append({
                'range': gspread.utils.rowcol_to_a1(row_to_update, estado_factura_col_idx),
                'values': [['Pagada']]
            })

        if updates:
            reporte_ws.batch_update(updates)
            
        return True
        
    except Exception as e:
        st.error(f"Ocurrió un error al procesar el pago: {e}")
        return False

# --- 4. INICIO DE LA APLICACIÓN ---
st.title("💰 Portal de Tesorería")

# --- Leer archivo de proveedores desde la raíz ---
archivo_proveedores = "PROVEDORES_CORREO.xlsx"
if not os.path.exists(archivo_proveedores):
    st.error(f"No se encontró el archivo '{archivo_proveedores}' en la raíz del proyecto.")
    st.stop()

proveedores_correo = pd.read_excel(archivo_proveedores)

# Normalizar nombres de columnas
def normaliza_col(col):
    return (
        col.strip()
        .lower()
        .replace('á', 'a')
        .replace('é', 'e')
        .replace('í', 'i')
        .replace('ó', 'o')
        .replace('ú', 'u')
        .replace('ñ', 'n')
        .replace(' ', '_')
    )

proveedores_correo.columns = [normaliza_col(c) for c in proveedores_correo.columns]

# Buscar columnas equivalentes
col_codigo = next((c for c in proveedores_correo.columns if 'codigo' in c), None)
col_nit = next((c for c in proveedores_correo.columns if 'nit' in c), None)
col_proveedor = next((c for c in proveedores_correo.columns if 'proveedor' in c), None)

if not col_codigo or not col_nit or not col_proveedor:
    st.error("El archivo debe tener columnas identificables como 'Codigo', 'Nit' y 'Proveedor'.")
    st.stop()

# --- Cargar datos de facturas del correo y ERP desde sesión ---
email_df = st.session_state.get("email_df", pd.DataFrame())
erp_df = st.session_state.get("erp_df", pd.DataFrame())
if email_df.empty or erp_df.empty:
    st.warning("No hay datos de correo o ERP cargados. Realiza la sincronización desde el Dashboard General.")
    st.stop()

# Normalizar nombres de proveedor para el cruce
proveedores_lista = proveedores_correo[col_proveedor].astype(str).str.strip().str.upper().unique().tolist()
email_df['nombre_proveedor_correo'] = email_df['nombre_proveedor_correo'].astype(str).str.strip().str.upper()
erp_df['nombre_proveedor_erp'] = erp_df['nombre_proveedor_erp'].astype(str).str.strip().str.upper()

# Filtrar facturas del correo solo de esos proveedores
facturas_correo = email_df[email_df['nombre_proveedor_correo'].isin(proveedores_lista)].copy()

# Cruce: facturas del correo que NO están en el ERP (por número de factura)
facturas_erp = erp_df[['num_factura', 'nombre_proveedor_erp']].copy()
facturas_erp['num_factura'] = facturas_erp['num_factura'].astype(str).str.strip()
facturas_correo['num_factura'] = facturas_correo['num_factura'].astype(str).str.strip()

facturas_faltantes = facturas_correo[~facturas_correo['num_factura'].isin(facturas_erp['num_factura'])]

st.header("Facturas en el correo que faltan en el ERP para los proveedores seleccionados")
if facturas_faltantes.empty:
    st.success("¡No faltan facturas en el ERP para estos proveedores!")
else:
    st.dataframe(
        facturas_faltantes[
            ['nombre_proveedor_correo', 'num_factura', 'valor_total_correo', 'fecha_emision_correo', 'fecha_vencimiento_correo']
        ],
        use_container_width=True
    )
    st.info(f"Total facturas faltantes: {len(facturas_faltantes)}")

# --- INICIO DE LA APLICACIÓN ---
st.title("💰 Portal de Tesorería")
st.markdown("Selecciona un lote de pago pendiente para ver su detalle y confirmar su pago.")

gs_client = connect_to_google_sheets()
if gs_client:
    df_lotes, df_reporte = load_data(gs_client)

    if df_lotes.empty:
        st.info("No se encontraron lotes de pago en el sistema.")
        st.stop()
    
    # FIX: Filtrar por todos los estados que se consideran pendientes.
    df_lotes_pendientes = df_lotes[df_lotes['estado_lote'].isin(PENDING_STATUSES)].copy()
    
    # Convertir columnas a numérico para poder ordenar y formatear
    for col in ['total_pagado_lote', 'ahorro_total_lote', 'num_facturas']:
        if col in df_lotes_pendientes.columns:
            df_lotes_pendientes[col] = pd.to_numeric(df_lotes_pendientes[col], errors='coerce').fillna(0)

    if df_lotes_pendientes.empty:
        st.success("🎉 ¡Excelente! No hay lotes de pago pendientes de procesar.")
        st.stop()

    # --- Sección de Visualización y Selección ---
    st.header("1. Lotes Pendientes de Pago")
    st.info("A continuación se muestran los lotes generados por Gerencia que requieren ser pagados.")
    
    # Ordenar lotes, poniendo los urgentes primero
    df_lotes_pendientes['es_urgente'] = df_lotes_pendientes['estado_lote'].apply(lambda x: 'URGENTE' in str(x))
    df_lotes_pendientes.sort_values(by=['es_urgente', 'fecha_creacion'], ascending=[False, False], inplace=True)
    
    st.dataframe(
        df_lotes_pendientes[['id_lote', 'estado_lote', 'fecha_creacion', 'num_facturas', 'total_pagado_lote', 'ahorro_total_lote']],
        use_container_width=True, hide_index=True,
        column_config={
            "total_pagado_lote": st.column_config.NumberColumn("Total a Pagar (COP)", format="COP %d"),
            "ahorro_total_lote": st.column_config.NumberColumn("Ahorro del Lote (COP)", format="COP %d")
        }
    )
    
    lista_lotes = df_lotes_pendientes['id_lote'].tolist()
    lote_seleccionado_id = st.selectbox(
        "Selecciona el ID del lote que vas a procesar:",
        options=lista_lotes,
        index=None,
        placeholder="Elige un lote..."
    )

    st.divider()

    # --- Sección de Detalle y Acción de Pago ---
    if lote_seleccionado_id:
        st.header(f"2. Detalle y Acciones del Lote: {lote_seleccionado_id}")
        
        lote_detalle = df_lotes[df_lotes['id_lote'] == lote_seleccionado_id].iloc[0]
        # La columna 'id_lote_pago' ahora existe gracias a la carga de datos corregida
        facturas_del_lote = df_reporte[df_reporte['id_lote_pago'] == lote_seleccionado_id].copy()
        
        # Convertir columnas a numérico para el formato
        for col in ['valor_total_erp', 'valor_con_descuento', 'valor_descuento']:
             if col in facturas_del_lote.columns:
                 facturas_del_lote[col] = pd.to_numeric(facturas_del_lote[col], errors='coerce').fillna(0)

        col1, col2, col3 = st.columns(3)
        col1.metric("Total a Pagar (COP)", f"{float(lote_detalle.get('total_pagado_lote', 0)):,.0f}")
        col2.metric("Número de Facturas", lote_detalle.get('num_facturas', 0))
        col3.metric("Ahorro Financiero (COP)", f"{float(lote_detalle.get('ahorro_total_lote', 0)):,.0f}")

        st.markdown("#### Facturas incluidas en este lote:")
        if facturas_del_lote.empty:
            st.warning("No se encontraron las facturas para este lote en el reporte consolidado. El lote podría estar vacío o haber un problema de sincronización.")
        else:
            st.dataframe(facturas_del_lote, use_container_width=True, hide_index=True)

            # --- Funcionalidad de Descarga a Excel ---
            st.download_button(
                label="📄 Descargar Plan de Pago a Excel",
                data=to_excel(facturas_del_lote),
                file_name=f"Plan_de_Pago_{lote_seleccionado_id}.xlsx",
                mime="application/vnd.ms-excel",
                use_container_width=True,
                type="secondary"
            )
        
        st.divider()
        
        # --- Confirmación de Pago ---
        with st.expander("✅ Confirmar Pago del Lote", expanded=True):
            st.warning(f"⚠️ **Acción Irreversible:** Al confirmar, el lote **{lote_seleccionado_id}** y todas sus facturas se marcarán como **'Pagadas'**.")
            
            if st.button(f"Confirmar Pago del Lote {lote_seleccionado_id}", type="primary", use_container_width=True, disabled=facturas_del_lote.empty):
                with st.spinner("Procesando pago y actualizando estados en Google Sheets..."):
                    success = procesar_pago_lote(gs_client, lote_seleccionado_id, df_reporte)
                    
                    if success:
                        st.success("¡Lote procesado exitosamente! Los estados han sido actualizados.")
                        st.balloons()
                        # Limpiar el cache y re-ejecutar para refrescar la lista de lotes
                        st.cache_data.clear()
                        st.rerun()
