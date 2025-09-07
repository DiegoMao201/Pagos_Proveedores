# pages/3_💰_Portal_Tesoreria.py
# -*- coding: utf-8 -*-
"""
Módulo de Portal de Tesorería (Versión 3.0).

Esta página es para el uso exclusivo del equipo de Tesorería. Permite
visualizar los lotes de pago generados por Gerencia, inspeccionar el detalle
de las facturas contenidas y, finalmente, marcar los lotes como 'Pagado'.

Esta acción finaliza el ciclo de vida de las facturas, cambiando su estado a
'Pagada' en el reporte consolidado.
"""

# --- 0. IMPORTACIÓN DE LIBRERÍAS ---
import streamlit as st
import pandas as pd
import gspread
from common.utils import connect_to_google_sheets # Reutilizamos la conexión

# --- 1. CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(
    layout="wide",
    page_title="Portal de Tesorería",
    page_icon="💰"
)

# --- 2. FUNCIONES AUXILIARES ---

@st.cache_data(ttl=300, show_spinner="Cargando Lotes de Pago...")
def load_pending_lots(_gs_client: gspread.Client):
    """Carga los lotes y las facturas desde Google Sheets."""
    try:
        spreadsheet = _gs_client.open_by_key(st.secrets["google_sheet_id"])
        
        # Cargar historial de lotes
        historial_ws = spreadsheet.worksheet("Historial_Lotes_Pago")
        df_lotes = pd.DataFrame(historial_ws.get_all_records())
        
        # Cargar reporte consolidado para buscar las facturas
        reporte_ws = spreadsheet.worksheet("ReporteConsolidado_Activo")
        df_reporte = pd.DataFrame(reporte_ws.get_all_records())
        
        return df_lotes, df_reporte
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
        historial_ws = spreadsheet.worksheet("Historial_Lotes_Pago")
        # Encontrar la celda del lote por su ID
        cell = historial_ws.find(lote_id)
        if not cell:
            st.error(f"No se pudo encontrar el lote con ID {lote_id} en el historial.")
            return False
            
        # Encontrar la columna de 'estado_lote'
        headers = historial_ws.row_values(1)
        estado_lote_col = headers.index('estado_lote') + 1
        
        # Actualizar la celda
        historial_ws.update_cell(cell.row, estado_lote_col, 'Pagado')

        # --- 2. Actualizar el estado de las FACTURAS en 'ReporteConsolidado_Activo' ---
        facturas_del_lote = df_reporte[df_reporte['id_lote_pago'] == lote_id]
        if facturas_del_lote.empty:
            st.warning("El lote se marcó como pagado, pero no se encontraron facturas asociadas para actualizar.")
            return True

        reporte_ws = spreadsheet.worksheet("ReporteConsolidado_Activo")
        reporte_headers = reporte_ws.row_values(1)
        estado_factura_col_idx = reporte_headers.index('estado_factura') + 1
        
        updates = []
        for index, _ in facturas_del_lote.iterrows():
            # El índice en gspread es el índice del DataFrame + 2
            row_to_update = index + 2
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

# --- 3. INICIO DE LA APLICACIÓN ---
st.title("💰 Portal de Pagos de Tesorería")
st.markdown("Selecciona un lote de pago pendiente para ver su detalle y confirmar su pago.")

gs_client = connect_to_google_sheets()
if gs_client:
    df_lotes, df_reporte = load_pending_lots(gs_client)

    if df_lotes.empty:
        st.success("🎉 ¡Excelente! No hay lotes de pago pendientes de procesar.")
        st.stop()
    
    df_lotes_pendientes = df_lotes[df_lotes['estado_lote'] == 'Pendiente de Pago en Tesorería']

    if df_lotes_pendientes.empty:
        st.success("🎉 ¡Excelente! No hay lotes de pago pendientes de procesar.")
        st.stop()

    # --- Selección del Lote ---
    st.header("1. Lotes Pendientes de Pago")
    st.dataframe(df_lotes_pendientes, use_container_width=True, hide_index=True)
    
    lista_lotes = df_lotes_pendientes['id_lote'].tolist()
    lote_seleccionado_id = st.selectbox(
        "Selecciona el ID del lote que vas a procesar:",
        options=lista_lotes,
        index=None,
        placeholder="Elige un lote..."
    )

    st.divider()

    # --- Detalle del Lote y Acción de Pago ---
    if lote_seleccionado_id:
        st.header(f"2. Detalle del Lote: {lote_seleccionado_id}")
        
        lote_detalle = df_lotes[df_lotes['id_lote'] == lote_seleccionado_id].iloc[0]
        facturas_del_lote = df_reporte[df_reporte['id_lote_pago'] == lote_seleccionado_id]

        c1, c2, c3 = st.columns(3)
        c1.metric("Total a Pagar (COP)", f"{float(lote_detalle['total_pagado_lote']):,.0f}")
        c2.metric("Número de Facturas", lote_detalle['num_facturas'])
        c3.metric("Fecha de Creación", lote_detalle['fecha_creacion'])
        
        st.markdown("#### Facturas incluidas en este lote:")
        st.dataframe(facturas_del_lote, use_container_width=True, hide_index=True)
        
        st.divider()
        
        st.header("3. Confirmación de Pago")
        st.warning("⚠️ **Acción Irreversible:** Al confirmar, todas las facturas de este lote se marcarán como 'Pagada' y el lote se moverá al historial.")
        
        if st.button(f"✅ Confirmar Pago del Lote {lote_seleccionado_id}", type="primary", use_container_width=True):
            with st.spinner("Procesando pago y actualizando estados en Google Sheets..."):
                success = procesar_pago_lote(gs_client, lote_seleccionado_id, df_reporte)
                
                if success:
                    st.success("¡Lote procesado exitosamente! Los estados han sido actualizados.")
                    st.balloons()
                    # Limpiar el cache y re-ejecutar para refrescar la lista de lotes pendientes
                    st.cache_data.clear()
                    st.rerun()
