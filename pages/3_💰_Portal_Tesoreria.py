# pages/3_💰_Portal_Tesoreria.py
# -*- coding: utf-8 -*-
"""
Módulo de Portal de Tesorería (Versión 3.5 - Mejorado).

Esta página es para el uso exclusivo del equipo de Tesorería. Permite
visualizar los lotes de pago generados por Gerencia, inspeccionar el detalle,
descargar un soporte en Excel y, finalmente, marcar los lotes como 'Pagado'.

Esta versión corrige la lógica de carga de lotes pendientes y añade
funcionalidades clave para la gestión diaria.
"""

# --- 0. IMPORTACIÓN DE LIBRERÍAS ---
import streamlit as st
import pandas as pd
import gspread
import io
from common.utils import connect_to_google_sheets, GSHEET_REPORT_NAME

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
        
        # Cargar reporte consolidado para buscar las facturas
        reporte_ws = spreadsheet.worksheet(GSHEET_REPORT_NAME)
        # Usar get_all_values para evitar problemas con encabezados duplicados
        reporte_data = reporte_ws.get_all_values()
        reporte_headers = [str(h).strip() for h in reporte_data[0]]
        df_reporte = pd.DataFrame(reporte_data[1:], columns=reporte_headers)

        # Limpiar columnas duplicadas si existen
        df_reporte = df_reporte.loc[:, ~df_reporte.columns.duplicated(keep='first')]

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
        reporte_headers = [str(h).strip() for h in reporte_ws.row_values(1)]
        
        try:
            estado_factura_col_idx = reporte_headers.index('estado_factura') + 1
        except ValueError:
            st.error(f"No se encontró la columna 'estado_factura' en la hoja '{GSHEET_REPORT_NAME}'.")
            return False

        updates = []
        for index_str, _ in facturas_del_lote.iterrows():
            # El índice del DataFrame leído con gspread puede ser str, convertir a int
            # El índice en gspread es el índice del DataFrame + 2 (1 por header, 1 por base 0)
            row_to_update = int(index_str) + 2
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
st.title("💰 Portal de Pagos de Tesorería")
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
    for col in ['total_pagado_lote', 'ahorro_total_lote']:
        if col in df_lotes_pendientes.columns:
            df_lotes_pendientes[col] = pd.to_numeric(df_lotes_pendientes[col], errors='coerce')

    if df_lotes_pendientes.empty:
        st.success("🎉 ¡Excelente! No hay lotes de pago pendientes de procesar.")
        st.stop()

    # --- Sección de Visualización y Selección ---
    st.header("1. Lotes Pendientes de Pago")
    st.info("A continuación se muestran los lotes generados por Gerencia que requieren ser pagados.")
    
    # Ordenar lotes, poniendo los urgentes primero
    df_lotes_pendientes['es_urgente'] = df_lotes_pendientes['estado_lote'].apply(lambda x: 'URGENTE' in x)
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
            
            if st.button(f"Confirmar Pago del Lote {lote_seleccionado_id}", type="primary", use_container_width=True):
                with st.spinner("Procesando pago y actualizando estados en Google Sheets..."):
                    success = procesar_pago_lote(gs_client, lote_seleccionado_id, df_reporte)
                    
                    if success:
                        st.success("¡Lote procesado exitosamente! Los estados han sido actualizados.")
                        st.balloons()
                        # Limpiar el cache y re-ejecutar para refrescar la lista de lotes
                        st.cache_data.clear()
                        st.rerun()
