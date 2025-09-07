# pages/3_💵_Planificador_de_Pagos.py
# -*- coding: utf-8 -*-
"""
Centro de Control de Pagos Inteligente para FERREINOX (Versión 3.5 - Módulo Gerencia).

Este módulo permite a Gerencia crear lotes de pago tanto para facturas
vigentes como para facturas críticas (vencidas), con notificaciones directas
a Tesorería. Lógica de sesión mejorada para prevenir NameError y código optimizado.
"""

# --- 0. IMPORTACIÓN DE LIBRERÍAS ---
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import uuid
import gspread
import urllib.parse

# Se importa desde el archivo utils.py ya actualizado y robusto.
from common.utils import connect_to_google_sheets, load_data_from_gsheet, GSHEET_REPORT_NAME, COLOMBIA_TZ

# --- 1. CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(
    layout="wide",
    page_title="Planificador de Pagos | Gerencia",
    page_icon="💵"
)

# --- 2. CONSTANTES Y CLAVES DE SESIÓN ---
# Mejora: Usar constantes para claves de estado de sesión previene errores de tipeo.
SESSION_KEY_SUGERENCIA_IDS = 'sugerencia_ids'
SESSION_KEY_LOTE_VIGENTES = 'id_lote_propuesto_vigentes'
SESSION_KEY_SELECTION_VIGENTES = 'current_selection_key_vigentes'
SESSION_KEY_LOTE_VENCIDAS = 'id_lote_propuesto_vencidas'
SESSION_KEY_SELECTION_VENCIDAS = 'current_selection_key_vencidas'


# --- 3. FUNCIONES AUXILIARES ---
def guardar_lote_en_gsheets(gs_client: gspread.Client, lote_info: dict, facturas_seleccionadas: pd.DataFrame):
    """
    Guarda la información de un nuevo lote en la hoja de historial y actualiza
    el estado de las facturas correspondientes en el reporte consolidado.
    """
    try:
        spreadsheet = gs_client.open_by_key(st.secrets["google_sheet_id"])
        
        # 1. Guardar en el historial de lotes
        historial_ws = spreadsheet.worksheet("Historial_Lotes_Pago")
        headers = historial_ws.row_values(1)
        # Asegura que solo se inserten los valores que coinciden con las cabeceras
        valores_fila = [lote_info.get(col) for col in headers]
        historial_ws.append_row(valores_fila)

        # 2. Actualizar el reporte principal
        reporte_ws = spreadsheet.worksheet(GSHEET_REPORT_NAME)
        reporte_data = reporte_ws.get_all_values()
        reporte_headers_list = [str(h).strip().lower().replace(' ', '_') for h in reporte_data[0]]
        
        reporte_df = pd.DataFrame(reporte_data[1:], columns=reporte_headers_list)
        reporte_df = reporte_df.loc[:, ~reporte_df.columns.duplicated(keep='first')]

        # Estandarización de nombres de columnas para consistencia
        if 'nombre_proveedor_erp' in reporte_df.columns:
            reporte_df.rename(columns={'nombre_proveedor_erp': 'nombre_proveedor'}, inplace=True)

        reporte_df['valor_total_erp'] = pd.to_numeric(reporte_df['valor_total_erp'], errors='coerce')
        facturas_seleccionadas['valor_total_erp'] = pd.to_numeric(facturas_seleccionadas['valor_total_erp'], errors='coerce')

        try:
            # Obtener los índices de las columnas a actualizar
            estado_col_idx = reporte_headers_list.index('estado_factura') + 1
            lote_col_idx = reporte_headers_list.index('id_lote_pago') + 1
        except ValueError as e:
            st.error(f"Error Crítico: La columna '{e.args[0].split("'")[1]}' no existe en '{GSHEET_REPORT_NAME}'.")
            return False, f"Falta la columna requerida '{e.args[0].split("'")[1]}' en la hoja principal."

        updates = []
        for _, factura_a_actualizar in facturas_seleccionadas.iterrows():
            # Búsqueda más robusta usando isclose para valores flotantes
            match = reporte_df[
                (reporte_df['nombre_proveedor'] == factura_a_actualizar['nombre_proveedor']) &
                (reporte_df['num_factura'] == factura_a_actualizar['num_factura']) &
                (np.isclose(reporte_df['valor_total_erp'].fillna(0), factura_a_actualizar['valor_total_erp']))
            ]
            
            if not match.empty:
                # Se suma 2: 1 por el encabezado y 1 porque los índices de DataFrame empiezan en 0
                row_index_to_update = match.index[0] + 2
                updates.append({'range': gspread.utils.rowcol_to_a1(row_index_to_update, estado_col_idx), 'values': [['En Lote de Pago']]})
                updates.append({'range': gspread.utils.rowcol_to_a1(row_index_to_update, lote_col_idx), 'values': [[lote_info['id_lote']]]})
            else:
                st.warning(f"No se encontró coincidencia para la factura {factura_a_actualizar['num_factura']} de '{factura_a_actualizar['nombre_proveedor']}'. Se omitirá la actualización para esta factura.")
        
        if updates:
            reporte_ws.batch_update(updates)
        return True, None
    except Exception as e:
        st.error(f"Error inesperado al actualizar Google Sheets: {e}")
        return False, str(e)

def generar_sugerencias(df: pd.DataFrame, presupuesto: float, estrategia: str) -> list:
    """Genera una lista de IDs de facturas sugeridas para pagar según una estrategia."""
    if presupuesto <= 0 or df.empty:
        return []

    df_sugerencias = df.copy()
    
    # Mejora: Se ordena el dataframe según la estrategia seleccionada de forma más segura
    if estrategia == "Maximizar Ahorro" and 'valor_descuento' in df_sugerencias.columns:
        df_sugerencias = df_sugerencias.sort_values(by='valor_descuento', ascending=False)
    elif estrategia == "Evitar Vencimientos" and 'dias_para_vencer' in df_sugerencias.columns:
        df_sugerencias = df_sugerencias.sort_values(by='dias_para_vencer', ascending=True)
    elif estrategia == "Priorizar Antigüedad" and 'fecha_emision_erp' in df_sugerencias.columns:
        df_sugerencias = df_sugerencias.sort_values(by='fecha_emision_erp', ascending=True)
    
    total_acumulado = 0
    ids_sugeridos = []
    for _, row in df_sugerencias.iterrows():
        # Considerar el valor con descuento si existe y es aplicable
        valor_a_considerar = row.get('valor_con_descuento', row['valor_total_erp']) if row.get('valor_con_descuento', 0) > 0 else row['valor_total_erp']
        
        if total_acumulado + valor_a_considerar <= presupuesto:
            total_acumulado += valor_a_considerar
            ids_sugeridos.append(row['id_factura_unico'])
            
    return ids_sugeridos

# --- 4. INICIO DE LA APLICACIÓN ---
st.title("💵 Planificador de Pagos | Gerencia")
st.markdown("Herramienta para crear lotes de pago a partir de la cartera pendiente.")

gs_client = connect_to_google_sheets()
df_full = load_data_from_gsheet(gs_client)

if df_full.empty:
    st.warning(f"No se encontraron datos válidos en la hoja de cálculo '{GSHEET_REPORT_NAME}'. Verifique la fuente de datos.")
    st.stop()

# --- 5. PRE-PROCESAMIENTO Y SEGMENTACIÓN DE DATOS ---
# Creación de un ID único y robusto para cada factura
df_full['id_factura_unico'] = df_full.apply(
    lambda row: f"{row.get('nombre_proveedor', '')}-{row.get('num_factura', '')}-{row.get('valor_total_erp', 0)}",
    axis=1
).str.replace(r'[\s/]+', '-', regex=True)

# Filtrado inicial de facturas 'Pendientes'
df_pendientes_full = df_full[df_full['estado_factura'] == 'Pendiente'].copy()

# Segmentación de datos en categorías claras
df_notas_credito = df_pendientes_full[df_pendientes_full['valor_total_erp'] < 0].copy()
df_vencidas = df_pendientes_full[(df_pendientes_full['estado_pago'] == '🔴 Vencida') & (df_pendientes_full['valor_total_erp'] >= 0)].copy()
df_para_pagar = df_pendientes_full[(df_pendientes_full['valor_total_erp'] >= 0) & (df_pendientes_full['estado_pago'].isin(['🟢 Vigente', '🟠 Por Vencer (7 días)']))].copy()

# --- 6. BARRA LATERAL (SIDEBAR) ---
with st.sidebar:
    st.header("⚙️ Filtros y Sugerencias")
    st.info("Los filtros y el motor de sugerencias se aplican únicamente a la pestaña 'Plan de Pagos (Vigentes)'.")

    proveedores_lista = sorted(df_para_pagar['nombre_proveedor'].dropna().unique().tolist())
    selected_suppliers = st.multiselect("Filtrar por Proveedor (Vigentes):", proveedores_lista, placeholder="Seleccione proveedores")

    # Aplicar filtros seleccionados
    df_pagar_filtrado = df_para_pagar.copy()
    if selected_suppliers:
        df_pagar_filtrado = df_pagar_filtrado[df_pagar_filtrado['nombre_proveedor'].isin(selected_suppliers)]

    st.divider()
    st.subheader("🤖 Motor de Sugerencias")
    presupuesto = st.number_input("Ingresa tu Presupuesto de Pago:", min_value=0.0, value=20000000.0, step=1000000.0, help="Presupuesto para calcular las sugerencias de pago.")
    estrategia = st.selectbox("Selecciona tu Estrategia de Pago:", ["Maximizar Ahorro", "Evitar Vencimientos", "Priorizar Antigüedad"])

    if st.button("💡 Generar Sugerencia de Pago", type="primary", use_container_width=True):
        ids_sugeridos = generar_sugerencias(df_pagar_filtrado, presupuesto, estrategia)
        st.session_state[SESSION_KEY_SUGERENCIA_IDS] = ids_sugeridos
        if not ids_sugeridos:
            st.warning("No se pudieron generar sugerencias con el presupuesto y filtros actuales.")

# --- 7. CUERPO PRINCIPAL CON PESTAÑAS ---
tab_pagos, tab_vencidas, tab_credito = st.tabs([
    f"✅ Plan de Pagos (Vigentes) ({len(df_para_pagar)})",
    f"🚨 Gestión de Facturas Críticas ({len(df_vencidas)})",
    f"📝 Gestión de Notas Crédito ({len(df_notas_credito)})"
])

# --- PESTAÑA 1: PLAN DE PAGOS (VIGENTES) ---
with tab_pagos:
    st.header("1. Selección de Facturas Vigentes para el Plan de Pago")
    st.markdown("Marca las facturas que deseas incluir en este lote.")
    
    # Insertar columna de selección al inicio
    df_pagar_filtrado.insert(0, "seleccionar", False)
    
    # Aplicar sugerencias si existen en la sesión
    if SESSION_KEY_SUGERENCIA_IDS in st.session_state:
        df_pagar_filtrado['seleccionar'] = df_pagar_filtrado['id_factura_unico'].isin(st.session_state[SESSION_KEY_SUGERENCIA_IDS])
        del st.session_state[SESSION_KEY_SUGERENCIA_IDS] # Limpiar para no pre-seleccionar en la siguiente recarga

    if df_pagar_filtrado.empty:
        st.info("No hay facturas vigentes para pagar que coincidan con los filtros actuales.")
    else:
        edited_df_vigentes = st.data_editor(
            df_pagar_filtrado, key="data_editor_pagos", use_container_width=True, hide_index=True, 
            column_config={
                "seleccionar": st.column_config.CheckboxColumn("Seleccionar", required=True),
                "valor_total_erp": st.column_config.NumberColumn("Valor Original (COP)", format="COP %d"),
                "valor_con_descuento": st.column_config.NumberColumn("Valor a Pagar (COP)", format="COP %d"),
                "valor_descuento": st.column_config.NumberColumn("Ahorro (COP)", format="COP %d")
            }, disabled=[col for col in df_pagar_filtrado.columns if col != 'seleccionar']
        )
        selected_rows_vigentes = edited_df_vigentes[edited_df_vigentes['seleccionar']]
        st.divider()

        # Lógica de sesión para generar un ID de lote estable y prevenir NameError
        if not selected_rows_vigentes.empty:
            selection_key = tuple(sorted(selected_rows_vigentes['id_factura_unico'].tolist()))
            if st.session_state.get(SESSION_KEY_SELECTION_VIGENTES) != selection_key:
                st.session_state[SESSION_KEY_LOTE_VIGENTES] = f"LOTE-VIG-{uuid.uuid4().hex[:6].upper()}"
                st.session_state[SESSION_KEY_SELECTION_VIGENTES] = selection_key
        
        sub_tab1_vig, sub_tab2_vig = st.tabs(["📊 Resumen del Lote (Vigentes)", "🚀 Confirmar y Notificar a Tesorería"])
        with sub_tab1_vig:
            st.subheader("Análisis del Lote de Pagos Vigentes")
            if selected_rows_vigentes.empty:
                st.info("Selecciona una o más facturas vigentes para ver el resumen del lote.")
            else:
                total_pagar = selected_rows_vigentes['valor_con_descuento'].sum()
                total_ahorro = selected_rows_vigentes['valor_descuento'].sum()
                num_facturas = len(selected_rows_vigentes)
                
                c1, c2, c3 = st.columns(3)
                c1.metric("Nº Facturas Seleccionadas", f"{num_facturas}")
                c2.metric("💰 TOTAL A PAGAR (COP)", f"{total_pagar:,.0f}")
                c3.metric("💸 AHORRO TOTAL (COP)", f"{total_ahorro:,.0f}")
                st.dataframe(selected_rows_vigentes[['nombre_proveedor', 'num_factura', 'valor_total_erp', 'valor_con_descuento', 'valor_descuento', 'fecha_vencimiento_erp']], use_container_width=True, hide_index=True)

        with sub_tab2_vig:
            st.subheader("Acciones Finales del Lote de Vigentes")
            if selected_rows_vigentes.empty:
                st.warning("Debes seleccionar al menos una factura para poder generar un lote de pago.")
            else:
                col1, col2 = st.columns([1, 1])
                with col1:
                    st.markdown("#### ✅ Confirmación y Registro")
                    if st.button("Confirmar y Generar Lote de VIGENTES", type="primary", use_container_width=True):
                        with st.spinner("Procesando y guardando lote..."):
                            id_lote = st.session_state.get(SESSION_KEY_LOTE_VIGENTES, f"LOTE-ERR-{uuid.uuid4().hex[:4]}")
                            lote_info = {
                                "id_lote": id_lote, "fecha_creacion": datetime.now(COLOMBIA_TZ).strftime('%Y-%m-%d %H:%M:%S'),
                                "num_facturas": len(selected_rows_vigentes), "valor_original_total": selected_rows_vigentes['valor_total_erp'].sum(),
                                "ahorro_total_lote": selected_rows_vigentes['valor_descuento'].sum(), "total_pagado_lote": selected_rows_vigentes['valor_con_descuento'].sum(),
                                "creado_por": "App Gerencia (Vigentes)", "estado_lote": "Pendiente de Pago"
                            }
                            success, error_msg = guardar_lote_en_gsheets(gs_client, lote_info, selected_rows_vigentes)
                            if success:
                                st.success(f"¡Éxito! Lote `{id_lote}` generado. La página se actualizará.")
                                st.balloons()
                                st.rerun()
                            else:
                                st.error(f"Error Crítico al guardar: {error_msg}")
                with col2:
                    st.markdown("#### 📲 Notificación a Tesorería")
                    id_lote_mensaje = st.session_state.get(SESSION_KEY_LOTE_VIGENTES, 'LOTE-POR-CONFIRMAR')
                    numero_tesoreria = st.text_input("Nº WhatsApp Tesorería", st.secrets.get("whatsapp_default_number", ""), key="whatsapp_num_vigentes")
                    mensaje = urllib.parse.quote(f"¡Hola! 👋 Se ha generado un nuevo lote de pago (VIGENTES).\n\n*ID Lote:* {id_lote_mensaje}\n*Total a Pagar:* COP {selected_rows_vigentes['valor_con_descuento'].sum():,.0f}\n*Nº Facturas:* {len(selected_rows_vigentes)}\n\nPor favor, revisa la plataforma para ver el detalle.")
                    st.link_button("📲 Enviar Notificación por WhatsApp", f"https://wa.me/{numero_tesoreria}?text={mensaje}", use_container_width=True, type="secondary")

# --- PESTAÑA 2: GESTIÓN DE FACTURAS CRÍTICAS (VENCIDAS) ---
with tab_vencidas:
    st.header("1. Selección de Facturas Críticas para Pago Inmediato")
    st.warning("¡Atención! Estás creando un lote de pago para facturas ya vencidas.")
    
    df_vencidas.insert(0, "seleccionar", False)

    if df_vencidas.empty:
        st.success("¡Excelente! No hay facturas críticas (vencidas) pendientes de gestión.")
    else:
        edited_df_vencidas = st.data_editor(
            df_vencidas, key="data_editor_vencidas", use_container_width=True, hide_index=True, 
            column_config={
                "seleccionar": st.column_config.CheckboxColumn("Seleccionar", required=True),
                "valor_total_erp": st.column_config.NumberColumn("Valor a Pagar (COP)", format="COP %d"),
                "dias_para_vencer": st.column_config.NumberColumn("Días Vencida", format="%d días"),
            },
            disabled=[col for col in df_vencidas.columns if col != 'seleccionar']
        )
        selected_rows_vencidas = edited_df_vencidas[edited_df_vencidas['seleccionar']]
        st.divider()

        # Lógica de sesión para el ID del lote de vencidas (previene NameError)
        if not selected_rows_vencidas.empty:
            selection_key_vencidas = tuple(sorted(selected_rows_vencidas['id_factura_unico'].tolist()))
            if st.session_state.get(SESSION_KEY_SELECTION_VENCIDAS) != selection_key_vencidas:
                st.session_state[SESSION_KEY_LOTE_VENCIDAS] = f"LOTE-CRI-{uuid.uuid4().hex[:6].upper()}"
                st.session_state[SESSION_KEY_SELECTION_VENCIDAS] = selection_key_vencidas

        sub_tab1_ven, sub_tab2_ven = st.tabs(["📊 Resumen del Lote (Críticos)", "🚀 Confirmar y Notificar a Tesorería"])
        with sub_tab1_ven:
            st.subheader("Análisis del Lote de Pagos Críticos")
            if selected_rows_vencidas.empty:
                st.info("Selecciona una o más facturas vencidas para crear un lote de pago.")
            else:
                total_a_pagar, num_facturas = selected_rows_vencidas['valor_total_erp'].sum(), len(selected_rows_vencidas)
                c1, c2 = st.columns(2)
                c1.metric("Nº Facturas Seleccionadas", f"{num_facturas}")
                c2.metric("💰 TOTAL A PAGAR (COP)", f"{total_a_pagar:,.0f}")
                st.dataframe(selected_rows_vencidas[['nombre_proveedor', 'num_factura', 'valor_total_erp', 'dias_para_vencer']], use_container_width=True, hide_index=True)
        
        with sub_tab2_ven:
            st.subheader("Acciones Finales del Lote de Críticos")
            if selected_rows_vencidas.empty:
                st.warning("Debes seleccionar al menos una factura vencida para generar el lote.")
            else:
                col1, col2 = st.columns([1, 1])
                with col1:
                    st.markdown("#### ✅ Confirmación y Registro")
                    if st.button("Confirmar y Generar Lote de CRÍTICOS", type="primary", use_container_width=True):
                        with st.spinner("Procesando y guardando lote de críticos..."):
                            id_lote = st.session_state.get(SESSION_KEY_LOTE_VENCIDAS, f"LOTE-ERR-{uuid.uuid4().hex[:4]}")
                            lote_info = {
                                "id_lote": id_lote, "fecha_creacion": datetime.now(COLOMBIA_TZ).strftime('%Y-%m-%d %H:%M:%S'),
                                "num_facturas": len(selected_rows_vencidas), "valor_original_total": selected_rows_vencidas['valor_total_erp'].sum(),
                                "ahorro_total_lote": 0, "total_pagado_lote": selected_rows_vencidas['valor_total_erp'].sum(),
                                "creado_por": "App Gerencia (Críticos)", "estado_lote": "Pendiente de Pago URGENTE"
                            }
                            success, error_msg = guardar_lote_en_gsheets(gs_client, lote_info, selected_rows_vencidas)
                            if success:
                                st.success(f"¡Éxito! Lote de críticos `{id_lote}` generado. La página se actualizará.")
                                st.balloons()
                                st.rerun()
                            else:
                                st.error(f"Error Crítico al guardar: {error_msg}")
                with col2:
                    st.markdown("#### 📲 Notificación a Tesorería")
                    id_lote_mensaje = st.session_state.get(SESSION_KEY_LOTE_VENCIDAS, 'LOTE-POR-CONFIRMAR')
                    numero_tesoreria = st.text_input("Nº WhatsApp Tesorería", st.secrets.get("whatsapp_default_number", ""), key="whatsapp_num_vencidas")
                    mensaje = urllib.parse.quote(f"¡URGENTE! 🚨 Se ha generado un lote de pago para FACTURAS CRÍTICAS (VENCIDAS).\n\n*ID Lote:* {id_lote_mensaje}\n*Total a Pagar:* COP {selected_rows_vencidas['valor_total_erp'].sum():,.0f}\n*Nº Facturas:* {len(selected_rows_vencidas)}\n\nPor favor, gestionar este pago con MÁXIMA PRIORIDAD.")
                    st.link_button("📲 Enviar Notificación URGENTE por WhatsApp", f"https://wa.me/{numero_tesoreria}?text={mensaje}", use_container_width=True, type="secondary")

# --- PESTAÑA 3: GESTIÓN DE NOTAS CRÉDITO ---
with tab_credito:
    st.header("📝 Visor de Notas Crédito Pendientes")
    st.info("Aquí se listan todos los saldos a favor (notas crédito) pendientes por cruzar o aplicar.")
    if df_notas_credito.empty:
        st.success("¡Excelente! No hay notas crédito pendientes de gestión.")
    else:
        c1, c2 = st.columns(2)
        total_favor = df_notas_credito['valor_total_erp'].sum()
        # El valor es negativo, así que lo multiplicamos por -1 para mostrarlo como un saldo a favor positivo
        c1.metric("Saldo Total a Favor (COP)", f"{abs(total_favor):,.0f}")
        c2.metric("Cantidad de Notas Crédito", f"{len(df_notas_credito)}")
        
        cols_to_display = ['nombre_proveedor', 'num_factura', 'valor_total_erp', 'fecha_emision_erp']
        existing_cols = [col for col in cols_to_display if col in df_notas_credito.columns]
        
        # FIX: Se eliminó el texto erróneo que causaba la IndentationError.
        # Esta es la línea que estaba causando el problema.
        st.dataframe(df_notas_credito[existing_cols], use_container_width=True, hide_index=True)
