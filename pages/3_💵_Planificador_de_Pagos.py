# pages/3_💵_Planificador_de_Pagos.py
# -*- coding: utf-8 -*-
"""
Centro de Control de Pagos Inteligente para FERREINOX (Versión 3.2 - Módulo Gerencia).

Este módulo es utilizado por Gerencia para crear lotes de pago a partir de
facturas que están en estado 'Pendiente'. Ahora se basa en la carga de datos
robusta de utils.py para evitar errores de columnas.
"""

# --- 0. IMPORTACIÓN DE LIBRERÍAS ---
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import uuid
import gspread
import urllib.parse
import pytz

# Se importa desde el archivo utils.py ya actualizado.
from common.utils import connect_to_google_sheets, load_data_from_gsheet, GSHEET_REPORT_NAME, COLOMBIA_TZ

# --- 1. CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(
    layout="wide",
    page_title="Planificador de Pagos | Gerencia",
    page_icon="💵"
)

# --- 2. FUNCIONES AUXILIARES ---
def guardar_lote_en_gsheets(gs_client: gspread.Client, lote_info: dict, facturas_seleccionadas: pd.DataFrame):
    try:
        spreadsheet = gs_client.open_by_key(st.secrets["google_sheet_id"])
        historial_ws = spreadsheet.worksheet("Historial_Lotes_Pago")
        headers = historial_ws.row_values(1)
        valores_fila = [lote_info.get(col) for col in headers]
        historial_ws.append_row(valores_fila)

        reporte_ws = spreadsheet.worksheet(GSHEET_REPORT_NAME)
        reporte_data = reporte_ws.get_all_values()
        reporte_headers_list = [str(h).strip().lower().replace(' ', '_') for h in reporte_data[0]]
        reporte_df = pd.DataFrame(reporte_data[1:], columns=reporte_headers_list)

        # Se renombran las columnas del GSheet leído directamente para que coincida con el DataFrame de la app
        if 'nombre_proveedor_erp' in reporte_df.columns:
            reporte_df.rename(columns={'nombre_proveedor_erp': 'nombre_proveedor'}, inplace=True)

        reporte_df['valor_total_erp'] = pd.to_numeric(reporte_df['valor_total_erp'], errors='coerce')
        facturas_seleccionadas['valor_total_erp'] = pd.to_numeric(facturas_seleccionadas['valor_total_erp'], errors='coerce')

        try:
            estado_col_idx = reporte_headers_list.index('estado_factura') + 1
            lote_col_idx = reporte_headers_list.index('id_lote_pago') + 1
            id_unico_col_idx = reporte_headers_list.index('id_factura_unico') + 1
        except ValueError as e:
            error_col = str(e).split("'")[1]
            st.error(f"Error Crítico: La columna '{error_col}' no existe en '{GSHEET_REPORT_NAME}'.")
            return False, f"Falta la columna '{error_col}' en la hoja principal."

        updates = []
        for _, factura_a_actualizar in facturas_seleccionadas.iterrows():
            match = reporte_df[
                (reporte_df['nombre_proveedor'] == factura_a_actualizar['nombre_proveedor']) &
                (reporte_df['num_factura'] == factura_a_actualizar['num_factura']) &
                (np.isclose(reporte_df['valor_total_erp'].fillna(0), factura_a_actualizar['valor_total_erp']))
            ]
            if not match.empty:
                row_index_to_update = match.index[0] + 2
                updates.append({'range': gspread.utils.rowcol_to_a1(row_index_to_update, estado_col_idx), 'values': [['En Lote de Pago']]})
                updates.append({'range': gspread.utils.rowcol_to_a1(row_index_to_update, lote_col_idx), 'values': [[lote_info['id_lote']]]})
                updates.append({'range': gspread.utils.rowcol_to_a1(row_index_to_update, id_unico_col_idx), 'values': [[factura_a_actualizar['id_factura_unico']]]})
            else:
                st.warning(f"No se encontró la factura {factura_a_actualizar['num_factura']} de '{factura_a_actualizar['nombre_proveedor']}'. Se omitirá.")
        if updates:
            reporte_ws.batch_update(updates)
        return True, None
    except Exception as e:
        st.error(f"Error inesperado en Google Sheets: {e}")
        return False, str(e)

def generar_sugerencias(df: pd.DataFrame, presupuesto: float, estrategia: str) -> list:
    if presupuesto <= 0 or df.empty: return []
    df_sugerencias = df.copy()
    if estrategia == "Maximizar Ahorro": df_sugerencias = df_sugerencias.sort_values(by='valor_descuento', ascending=False)
    elif estrategia == "Evitar Vencimientos": df_sugerencias = df_sugerencias.sort_values(by='dias_para_vencer', ascending=True)
    elif estrategia == "Priorizar Antigüedad":
        if 'fecha_emision_erp' in df_sugerencias.columns: df_sugerencias = df_sugerencias.sort_values(by='fecha_emision_erp', ascending=True)
    total_acumulado = 0
    ids_sugeridos = []
    for _, row in df_sugerencias.iterrows():
        valor_a_considerar = row.get('valor_con_descuento', row['valor_total_erp']) if row.get('valor_con_descuento', 0) > 0 else row['valor_total_erp']
        if total_acumulado + valor_a_considerar <= presupuesto:
            total_acumulado += valor_a_considerar
            ids_sugeridos.append(row['id_factura_unico'])
    return ids_sugeridos

# --- 3. INICIO DE LA APLICACIÓN ---
st.title("💵 Planificador de Pagos | Gerencia")
st.markdown("Herramienta para crear lotes de pago a partir de la cartera pendiente.")

try:
    gs_client = connect_to_google_sheets()
    df_full = load_data_from_gsheet(gs_client)
except Exception as e:
    st.error(f"No se pudo conectar o cargar datos desde Google Sheets. Error: {e}")
    st.stop()

if df_full.empty:
    st.warning(f"No se encontraron datos válidos en la hoja '{GSHEET_REPORT_NAME}'. Por favor, revisa la hoja o los errores de carga mostrados arriba.")
    st.stop()

# --- 4. PRE-PROCESAMIENTO Y SEGMENTACIÓN DE DATOS ---
df_full['id_factura_unico'] = df_full.apply(
    lambda row: f"{row.get('nombre_proveedor', '')}-{row.get('num_factura', '')}-{row.get('valor_total_erp', '')}",
    axis=1
).str.replace(r'[\s/]+', '-', regex=True)

df_pendientes_full = df_full[df_full['estado_factura'] == 'Pendiente'].copy()

df_notas_credito = df_pendientes_full[df_pendientes_full['valor_total_erp'] < 0].copy()
df_vencidas = df_pendientes_full[(df_pendientes_full.get('estado_pago') == '🔴 Vencida') & (df_pendientes_full['valor_total_erp'] >= 0)].copy()
df_para_pagar = df_pendientes_full[(df_pendientes_full['valor_total_erp'] >= 0) & (df_pendientes_full.get('estado_pago', pd.Series(dtype=str)).isin(['🟢 Vigente', '🟠 Por Vencer (7 días)']))].copy()

# --- 5. BARRA LATERAL (SIDEBAR) ---
st.sidebar.header("⚙️ Filtros y Sugerencias")
st.sidebar.info("Los filtros y el motor de sugerencias se aplican únicamente a la pestaña 'Plan de Pagos'.")

proveedores_lista = sorted(df_para_pagar['nombre_proveedor'].dropna().unique().tolist())
selected_suppliers = st.sidebar.multiselect("Filtrar por Proveedor:", proveedores_lista)

estado_pago_lista = df_para_pagar.get('estado_pago', pd.Series(dtype=str)).dropna().unique().tolist()
selected_status = st.sidebar.multiselect("Filtrar por Estado de Pago:", estado_pago_lista, default=estado_pago_lista)

df_pagar_filtrado = df_para_pagar.copy()
if selected_suppliers: df_pagar_filtrado = df_pagar_filtrado[df_pagar_filtrado['nombre_proveedor'].isin(selected_suppliers)]
if selected_status and 'estado_pago' in df_pagar_filtrado.columns: df_pagar_filtrado = df_pagar_filtrado[df_pagar_filtrado['estado_pago'].isin(selected_status)]

st.sidebar.divider()
st.sidebar.subheader("🤖 Motor de Sugerencias")
presupuesto = st.sidebar.number_input("Ingresa tu Presupuesto de Pago:", min_value=0.0, value=20000000.0, step=1000000.0)
estrategia = st.sidebar.selectbox("Selecciona tu Estrategia de Pago:", ["Maximizar Ahorro", "Evitar Vencimientos", "Priorizar Antigüedad"])
if st.sidebar.button("💡 Generar Sugerencia", type="primary"):
    ids_sugeridos = generar_sugerencias(df_pagar_filtrado, presupuesto, estrategia)
    if ids_sugeridos: st.session_state['sugerencia_ids'] = ids_sugeridos
    else: st.session_state.pop('sugerencia_ids', None)

# --- 6. CUERPO PRINCIPAL CON PESTAÑAS ---
tab_pagos, tab_credito, tab_vencidas = st.tabs([
    f"✅ Plan de Pagos ({len(df_para_pagar)})",
    f"📝 Gestión de Notas Crédito ({len(df_notas_credito)})",
    f"🚨 Gestión de Facturas Críticas ({len(df_vencidas)})"
])

with tab_pagos:
    st.header("1. Selección de Facturas para el Plan de Pago")
    st.markdown("Marca las facturas que deseas incluir.")
    df_pagar_filtrado.insert(0, "seleccionar", False)
    if 'sugerencia_ids' in st.session_state:
        df_pagar_filtrado['seleccionar'] = df_pagar_filtrado['id_factura_unico'].isin(st.session_state['sugerencia_ids'])
    if df_pagar_filtrado.empty:
        st.info("No hay facturas para pagar que coincidan con los filtros actuales.")
    else:
        edited_df = st.data_editor(df_pagar_filtrado, key="data_editor_pagos", use_container_width=True, hide_index=True, column_config={"seleccionar": st.column_config.CheckboxColumn(required=True),"valor_total_erp": st.column_config.NumberColumn("Valor Original (COP)", format="%d"),"valor_con_descuento": st.column_config.NumberColumn("Valor a Pagar (COP)", format="%d"),"valor_descuento": st.column_config.NumberColumn("Ahorro (COP)", format="%d"),"fecha_emision_erp": st.column_config.DateColumn("Fecha Emisión", format="YYYY-MM-DD"),"fecha_limite_descuento": st.column_config.DateColumn("Límite Descuento", format="YYYY-MM-DD"),"dias_para_vencer": st.column_config.NumberColumn("Días Vence", format="%d días"),}, disabled=[col for col in df_pagar_filtrado.columns if col != 'seleccionar'])
        selected_rows = edited_df[edited_df['seleccionar']]
        st.divider()
        if not selected_rows.empty:
            selection_key = tuple(sorted(selected_rows['id_factura_unico'].tolist()))
            if st.session_state.get('current_selection_key') != selection_key:
                st.session_state['id_lote_propuesto'] = f"LOTE-{uuid.uuid4().hex[:8].upper()}"
                st.session_state['current_selection_key'] = selection_key
        elif 'id_lote_propuesto' in st.session_state:
            del st.session_state['id_lote_propuesto']
            if 'current_selection_key' in st.session_state: del st.session_state['current_selection_key']
        sub_tab1, sub_tab2 = st.tabs(["📊 Resumen del Plan de Pago", "🚀 Confirmar y Ejecutar Acciones"])
        with sub_tab1:
            st.subheader("Análisis del Lote Propuesto")
            if selected_rows.empty: st.info("Selecciona una o más facturas para ver el resumen del pago.")
            else:
                total_original, ahorro_total, total_a_pagar, num_facturas = selected_rows['valor_total_erp'].sum(), selected_rows['valor_descuento'].sum(), selected_rows['valor_con_descuento'].sum(), len(selected_rows)
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Nº Facturas Seleccionadas", f"{num_facturas}")
                c2.metric("Valor Original Total (COP)", f"{total_original:,.0f}")
                c3.metric("Ahorro Total (COP)", f"{ahorro_total:,.0f}")
                c4.metric("💰 TOTAL A PAGAR (COP)", f"{total_a_pagar:,.0f}", delta_color="off")
                st.dataframe(selected_rows[['nombre_proveedor', 'num_factura', 'valor_total_erp', 'estado_descuento','valor_descuento', 'valor_con_descuento', 'fecha_limite_descuento', 'estado_pago']], use_container_width=True, hide_index=True)
        with sub_tab2:
            st.subheader("Acciones Finales del Lote")
            if selected_rows.empty: st.warning("Debes seleccionar al menos una factura para poder generar un lote de pago.")
            else:
                col1, col2 = st.columns([1, 1])
                with col1:
                    st.markdown("#### ✅ Confirmación y Registro")
                    if st.button("Confirmar y Generar Lote de Vigentes", type="primary", use_container_width=True):
                        with st.spinner("Procesando y guardando lote..."):
                            id_lote = st.session_state.get('id_lote_propuesto', f"LOTE-ERROR-{uuid.uuid4().hex[:4]}")
                            lote_info = {"id_lote": id_lote, "fecha_creacion": datetime.now(COLOMBIA_TZ).strftime('%Y-%m-%d %H:%M:%S'), "num_facturas": len(selected_rows), "valor_original_total": selected_rows['valor_total_erp'].sum(), "ahorro_total_lote": selected_rows['valor_descuento'].sum(), "total_pagado_lote": selected_rows['valor_con_descuento'].sum(), "creado_por": "Usuario App (Gerencia)", "estado_lote": "Pendiente de Pago en Tesorería"}
                            success, error_msg = guardar_lote_en_gsheets(gs_client, lote_info, selected_rows)
                            if success: st.success(f"¡Éxito! Lote `{id_lote}` generado y estados actualizados."), st.balloons(), st.session_state.pop('sugerencia_ids', None), st.rerun()
                            else: st.error(f"Error Crítico: {error_msg}")
                with col2:
                    st.markdown("#### 📲 Notificación a Tesorería")
                    numero_tesoreria = st.text_input("Nº WhatsApp Tesorería", st.secrets.get("whatsapp_default_number", ""), key="whatsapp_num_vigentes")
                    id_lote_mensaje = st.session_state.get('id_lote_propuesto', 'LOTE-POR-CONFIRMAR')
                    mensaje = urllib.parse.quote(f"¡Hola! 👋 Se ha generado un nuevo lote de pago (Vigentes).\n\nID Lote: *{id_lote_mensaje}*\n\n🔹 Total a Pagar: COP {selected_rows['valor_con_descuento'].sum():,.0f}\n🔹 Nº Facturas: {len(selected_rows)}\nPor favor, revisa la plataforma para ver el detalle.")
                    st.link_button("📲 Enviar Notificación por WhatsApp", f"https://wa.me/{numero_tesoreria}?text={mensaje}", use_container_width=True)

with tab_credito:
    st.header("📝 Visor de Notas Crédito Pendientes")
    st.info("Aquí se listan todos los saldos a favor (notas crédito) pendientes por cruzar o aplicar.")
    if df_notas_credito.empty: st.success("¡Excelente! No hay notas crédito pendientes de gestión.")
    else:
        c1, c2 = st.columns(2)
        c1.metric("Saldo Total a Favor (COP)", f"{df_notas_credito['valor_total_erp'].sum():,.0f}")
        c2.metric("Cantidad de Notas Crédito", f"{len(df_notas_credito)}")
        st.dataframe(df_notas_credito[[col for col in ['nombre_proveedor', 'num_factura',
