# pages/2_🧠_Centro_de_Pagos.py
# -*- coding: utf-8 -*-
"""
Centro de Control de Pagos Inteligente para FERREINOX (Versión 2.1).

Esta herramienta permite la creación de lotes de pago optimizados y ahora
incluye módulos dedicados para la gestión de notas crédito y el seguimiento
y PAGO de facturas críticas (muy vencidas).

Funcionalidades Clave:
- Interfaz rediseñada con pestañas para mayor claridad:
  1. Plan de Pagos: Para facturas vigentes y por vencer.
  2. Notas Crédito: Para visualizar y gestionar saldos a favor.
  3. Facturas Críticas: Para aislar, seleccionar y PAGAR facturas muy vencidas.
- Motor de sugerencias para optimizar pagos según presupuesto y estrategia.
- Correcta visualización de valores numéricos en todas las tablas.
- Generación de lotes de pago desde Pestaña Principal y Pestaña de Críticas.
- Conexión directa con el reporte consolidado, asegurando datos actualizados.
"""

# --- 0. IMPORTACIÓN DE LIBRERías ---
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import uuid
import gspread
import urllib.parse
import pytz

# Se importa desde el archivo utils.py que ya existe en tu proyecto.
from common.utils import connect_to_google_sheets, load_data_from_gsheet

# --- 1. CONFIGURACIÓN DE PÁGINA Y CONSTANTES ---
st.set_page_config(
    layout="wide",
    page_title="Centro de Control de Pagos Inteligente",
    page_icon="🧠"
)

# Constantes (Sincronizadas con el Dashboard General)
GSHEET_REPORT_NAME = "ReporteConsolidado_Activo"
COLOMBIA_TZ = pytz.timezone('America/Bogota')

# --- 2. FUNCIONES AUXILIARES ---

def to_excel(df: pd.DataFrame) -> bytes:
    """Convierte un DataFrame a un archivo Excel en memoria (bytes)."""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='PlanDePagoGenerado')
    return output.getvalue()

def guardar_lote_en_gsheets(gs_client: gspread.Client, lote_info: dict, facturas_seleccionadas: pd.DataFrame):
    """
    Guarda el resumen del lote en el historial y actualiza el estado
    de las facturas seleccionadas en el reporte principal.
    """
    try:
        spreadsheet = gs_client.open_by_key(st.secrets["google_sheet_id"])

        # --- 1. Guardar el resumen del lote en la hoja de historial ---
        historial_ws = spreadsheet.worksheet("Historial_Lotes_Pago")
        headers = historial_ws.row_values(1)
        valores_fila = [lote_info.get(col) for col in headers]
        historial_ws.append_row(valores_fila)

        # --- 2. Actualizar el estado de las facturas en la hoja principal ---
        reporte_ws = spreadsheet.worksheet(GSHEET_REPORT_NAME)
        ids_a_actualizar = facturas_seleccionadas['id_factura_unico'].tolist()

        reporte_headers = reporte_ws.row_values(1)
        try:
            id_col_idx = reporte_headers.index('id_factura_unico') + 1
            estado_col_idx = reporte_headers.index('estado_factura') + 1
            lote_col_idx = reporte_headers.index('id_lote_pago') + 1
        except ValueError as e:
            st.error(f"Error Crítico: La columna '{e.args[0].split(' ')[0]}' no existe en la hoja '{GSHEET_REPORT_NAME}'. No se puede continuar.")
            return False, f"Falta la columna {e.args[0].split(' ')[0]} en la hoja principal."

        all_ids_in_sheet = reporte_ws.col_values(id_col_idx)

        updates = []
        for id_factura in ids_a_actualizar:
            try:
                row_index = all_ids_in_sheet.index(id_factura) + 1
                updates.append({
                    'range': gspread.utils.rowcol_to_a1(row_index, estado_col_idx),
                    'values': [['En Lote de Pago']]
                })
                updates.append({
                    'range': gspread.utils.rowcol_to_a1(row_index, lote_col_idx),
                    'values': [[lote_info['id_lote']]]
                })
            except ValueError:
                st.warning(f"No se encontró la factura con ID '{id_factura}' en la hoja principal. Se omitirá su actualización.")

        if updates:
            reporte_ws.batch_update(updates)
        return True, None
    except gspread.exceptions.WorksheetNotFound as e:
        return False, f"Error: No se encontró la hoja de cálculo requerida: '{e.args[0]}'."
    except Exception as e:
        st.error(f"Error inesperado en la comunicación con Google Sheets: {e}")
        return False, str(e)

def generar_sugerencias(df: pd.DataFrame, presupuesto: float, estrategia: str) -> list:
    """Motor de inteligencia para sugerir qué facturas pagar."""
    if presupuesto <= 0 or df.empty:
        return []

    df_sugerencias = df.copy()
    
    if estrategia == "Maximizar Ahorro":
        df_sugerencias = df_sugerencias.sort_values(by='valor_descuento', ascending=False)
    elif estrategia == "Evitar Vencimientos":
        df_sugerencias = df_sugerencias.sort_values(by='dias_para_vencer', ascending=True)
    elif estrategia == "Priorizar Antigüedad":
        if 'fecha_emision_erp' in df_sugerencias.columns and df_sugerencias['fecha_emision_erp'].notna().any():
            df_sugerencias = df_sugerencias.sort_values(by='fecha_emision_erp', ascending=True)

    total_acumulado = 0
    ids_sugeridos = []
    for _, row in df_sugerencias.iterrows():
        valor_a_considerar = row.get('valor_con_descuento', row['valor_total_erp']) if row.get('valor_con_descuento', 0) > 0 else row['valor_total_erp']
        if total_acumulado + valor_a_considerar <= presupuesto:
            total_acumulado += valor_a_considerar
            ids_sugeridos.append(row['id_factura_unico'])
            
    return ids_sugeridos

# --- 3. INICIO DE LA APLICACIÓN ---
st.title("🧠 Centro de Control de Pagos Inteligente v2.1")
st.markdown("Herramienta evolucionada para construir lotes de pago, gestionar notas crédito y auditar facturas críticas.")

# --- Carga y Cacheo de Datos ---
try:
    gs_client = connect_to_google_sheets()
    df_full = load_data_from_gsheet(gs_client)
except Exception as e:
    st.error(f"No se pudo conectar o cargar los datos desde Google Sheets. Error: {e}")
    st.stop()

if df_full.empty:
    st.warning(f"No hay datos disponibles en la hoja '{GSHEET_REPORT_NAME}'. Por favor, ejecuta una sincronización en el 'Dashboard General'.")
    st.stop()

# --- 4. PRE-PROCESAMIENTO Y SEGMENTACIÓN DE DATOS ---
required_cols = ['nombre_proveedor', 'num_factura', 'valor_total_erp', 'estado_factura', 'estado_pago']
for col in required_cols:
    if col not in df_full.columns:
        st.error(f"La columna requerida '{col}' no se encuentra en tu Google Sheet. La aplicación no puede continuar.")
        st.stop()
        
df_full['estado_factura'] = df_full['estado_factura'].replace('', 'Pendiente').fillna('Pendiente')
df_full['id_factura_unico'] = df_full.apply(
    lambda row: f"{row.get('nombre_proveedor', '')}-{row.get('num_factura', '')}-{row.get('valor_total_erp', '')}",
    axis=1
).str.replace(r'[\s/]+', '-', regex=True)

df_pendientes_full = df_full[df_full['estado_factura'] == 'Pendiente'].copy()

df_notas_credito = df_pendientes_full[df_pendientes_full['valor_total_erp'] < 0].copy()

df_vencidas = df_pendientes_full[
    (df_pendientes_full['estado_pago'] == '🔴 Vencida') & (df_pendientes_full['valor_total_erp'] >= 0)
].copy()

df_para_pagar = df_pendientes_full[
    (df_pendientes_full['valor_total_erp'] >= 0) & 
    (df_pendientes_full['estado_pago'].isin(['🟢 Vigente', '🟠 Por Vencer (7 días)']))
].copy()

# --- 5. BARRA LATERAL (SIDEBAR) ---
st.sidebar.header("⚙️ Filtros y Sugerencias")
st.sidebar.info("Los filtros y el motor de sugerencias se aplican únicamente a la pestaña 'Plan de Pagos'.")

proveedores_lista = sorted(df_para_pagar['nombre_proveedor'].dropna().unique().tolist())
selected_suppliers = st.sidebar.multiselect("Filtrar por Proveedor:", proveedores_lista)

estado_pago_lista = df_para_pagar['estado_pago'].unique().tolist()
selected_status = st.sidebar.multiselect("Filtrar por Estado de Pago:", estado_pago_lista, default=estado_pago_lista)

df_pagar_filtrado = df_para_pagar.copy()
if selected_suppliers:
    df_pagar_filtrado = df_pagar_filtrado[df_pagar_filtrado['nombre_proveedor'].isin(selected_suppliers)]
if selected_status:
    df_pagar_filtrado = df_pagar_filtrado[df_pagar_filtrado['estado_pago'].isin(selected_status)]

st.sidebar.divider()
st.sidebar.subheader("🤖 Motor de Sugerencias")
presupuesto = st.sidebar.number_input(
    "Ingresa tu Presupuesto de Pago:",
    min_value=0.0, value=20000000.0, step=1000000.0
)
estrategia = st.sidebar.selectbox(
    "Selecciona tu Estrategia de Pago:",
    ["Maximizar Ahorro", "Evitar Vencimientos", "Priorizar Antigüedad"],
    help="El motor seleccionará las facturas óptimas según esta regla y tu presupuesto."
)

if st.sidebar.button("💡 Generar Sugerencia", type="primary"):
    ids_sugeridos = generar_sugerencias(df_pagar_filtrado, presupuesto, estrategia)
    if ids_sugeridos:
        st.session_state['sugerencia_ids'] = ids_sugeridos
        st.toast(f"¡Sugerencia generada! Se han pre-seleccionado {len(ids_sugeridos)} facturas.", icon="💡")
    else:
        st.session_state.pop('sugerencia_ids', None)
        st.warning("No se pudieron generar sugerencias con los criterios actuales.")

# --- 6. CUERPO PRINCIPAL CON PESTAÑAS ---
tab_pagos, tab_credito, tab_vencidas = st.tabs([
    f"✅ Plan de Pagos ({len(df_para_pagar)})",
    f"📝 Gestión de Notas Crédito ({len(df_notas_credito)})",
    f"🚨 Gestión de Facturas Críticas ({len(df_vencidas)})"
])

# --- PESTAÑA 1: PLAN DE PAGOS (VIGENTES Y POR VENCER) ---
with tab_pagos:
    st.header("1. Selección de Facturas para el Plan de Pago")
    st.markdown("Marca las facturas que deseas incluir. Usa el **Motor de Sugerencias** en la barra lateral para una pre-selección inteligente.")
    
    df_pagar_filtrado.insert(0, "seleccionar", False)
    if 'sugerencia_ids' in st.session_state:
        df_pagar_filtrado['seleccionar'] = df_pagar_filtrado['id_factura_unico'].isin(st.session_state['sugerencia_ids'])

    if df_pagar_filtrado.empty:
        st.info("No hay facturas para pagar que coincidan con los filtros actuales.")
    else:
        edited_df = st.data_editor(
            df_pagar_filtrado, key="data_editor_pagos", use_container_width=True, hide_index=True,
            column_config={
                "seleccionar": st.column_config.CheckboxColumn(required=True),
                "valor_total_erp": st.column_config.NumberColumn("Valor Original (COP)", format="%d"),
                "valor_con_descuento": st.column_config.NumberColumn("Valor a Pagar (COP)", format="%d"),
                "valor_descuento": st.column_config.NumberColumn("Ahorro (COP)", format="%d"),
                "fecha_emision_erp": st.column_config.DateColumn("Fecha Emisión", format="YYYY-MM-DD"),
                "fecha_limite_descuento": st.column_config.DateColumn("Límite Descuento", format="YYYY-MM-DD"),
                "dias_para_vencer": st.column_config.NumberColumn("Días Vence", format="%d días"),
            },
            disabled=[col for col in df_pagar_filtrado.columns if col != 'seleccionar']
        )
        selected_rows = edited_df[edited_df['seleccionar']]
        st.divider()

        if not selected_rows.empty:
            selection_key = tuple(sorted(selected_rows['id_factura_unico'].tolist()))
            if st.session_state.get('current_selection_key') != selection_key:
                st.session_state['id_lote_propuesto'] = f"LOTE-{uuid.uuid4().hex[:8].upper()}"
                st.session_state['current_selection_key'] = selection_key
        elif 'id_lote_propuesto' in st.session_state:
            del st.session_state['id_lote_propuesto']
            if 'current_selection_key' in st.session_state:
                del st.session_state['current_selection_key']

        sub_tab1, sub_tab2 = st.tabs(["📊 Resumen del Plan de Pago", "🚀 Confirmar y Ejecutar Acciones"])
        with sub_tab1:
            st.subheader("Análisis del Lote Propuesto")
            if selected_rows.empty:
                st.info("Selecciona una o más facturas para ver el resumen del pago.")
            else:
                total_original = selected_rows['valor_total_erp'].sum()
                ahorro_total = selected_rows['valor_descuento'].sum()
                total_a_pagar = selected_rows['valor_con_descuento'].sum()
                num_facturas = len(selected_rows)

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Nº Facturas Seleccionadas", f"{num_facturas}")
                c2.metric("Valor Original Total (COP)", f"{total_original:,.0f}")
                c3.metric("Ahorro Total (COP)", f"{ahorro_total:,.0f}")
                c4.metric("💰 TOTAL A PAGAR (COP)", f"{total_a_pagar:,.0f}", delta_color="off")
                
                cols_to_show = [
                    'nombre_proveedor', 'num_factura', 'valor_total_erp', 'estado_descuento',
                    'valor_descuento', 'valor_con_descuento', 'fecha_limite_descuento', 'estado_pago'
                ]
                st.dataframe(selected_rows[cols_to_show], use_container_width=True, hide_index=True)
        with sub_tab2:
            st.subheader("Acciones Finales del Lote")
            if selected_rows.empty:
                 st.warning("Debes seleccionar al menos una factura para poder generar un lote de pago.")
            else:
                col1, col2 = st.columns([1, 1])
                with col1:
                    st.markdown("#### ✅ Confirmación y Registro")
                    if st.button("Confirmar y Generar Lote de Vigentes", type="primary", use_container_width=True):
                        with st.spinner("Procesando y guardando lote..."):
                            id_lote = st.session_state.get('id_lote_propuesto', f"LOTE-ERROR-{uuid.uuid4().hex[:4]}")
                            lote_info = {
                                "id_lote": id_lote, "fecha_creacion": datetime.now(COLOMBIA_TZ).strftime('%Y-%m-%d %H:%M:%S'),
                                "num_facturas": len(selected_rows), "valor_original_total": selected_rows['valor_total_erp'].sum(),
                                "ahorro_total_lote": selected_rows['valor_descuento'].sum(), "total_pagado_lote": selected_rows['valor_con_descuento'].sum(),
                                "creado_por": "Usuario App (Gerencia)", "estado_lote": "Pendiente de Pago en Tesorería"
                            }
                            success, error_msg = guardar_lote_en_gsheets(gs_client, lote_info, selected_rows)
                            if success:
                                st.success(f"¡Éxito! Lote `{id_lote}` generado.")
                                st.balloons()
                                st.session_state.pop('sugerencia_ids', None)
                                st.rerun()
                            else:
                                st.error(f"Error Crítico: {error_msg}")
                with col2:
                    st.markdown("#### 📲 Notificación a Tesorería")
                    numero_tesoreria = st.text_input("Nº WhatsApp Tesorería", st.secrets.get("whatsapp_default_number", ""), key="whatsapp_num_vigentes")
                    id_lote_mensaje = st.session_state.get('id_lote_propuesto', 'LOTE-POR-CONFIRMAR')
                    mensaje = urllib.parse.quote(
                        f"¡Hola! 👋 Se ha generado un nuevo lote de pago (Vigentes).\n\n"
                        f"ID Lote: *{id_lote_mensaje}*\n\n"
                        f"🔹 Total a Pagar: COP {selected_rows['valor_con_descuento'].sum():,.0f}\n"
                        f"🔹 Nº Facturas: {len(selected_rows)}\n"
                        "Por favor, revisa la plataforma para ver el detalle."
                    )
                    link_whatsapp = f"https://wa.me/{numero_tesoreria}?text={mensaje}"
                    st.link_button("📲 Enviar Notificación por WhatsApp", link_whatsapp, use_container_width=True)

# --- PESTAÑA 2: GESTIÓN DE NOTAS CRÉDITO ---
with tab_credito:
    st.header("📝 Visor de Notas Crédito Pendientes")
    st.info("Aquí se listan todos los saldos a favor (notas crédito) pendientes por cruzar o aplicar.")

    if df_notas_credito.empty:
        st.success("¡Excelente! No hay notas crédito pendientes de gestión.")
    else:
        c1, c2 = st.columns(2)
        total_nc = df_notas_credito['valor_total_erp'].sum()
        c1.metric("Saldo Total a Favor (COP)", f"{total_nc:,.0f}")
        c2.metric("Cantidad de Notas Crédito", f"{len(df_notas_credito)}")

        cols_nc_visibles = ['nombre_proveedor', 'num_factura', 'valor_total_erp', 'fecha_emision_erp', 'doc_erp', 'serie']
        cols_nc_a_mostrar = [col for col in cols_nc_visibles if col in df_notas_credito.columns]

        st.dataframe(
            df_notas_credito[cols_nc_a_mostrar].sort_values('fecha_emision_erp', ascending=False),
            use_container_width=True, hide_index=True,
            column_config={
                "valor_total_erp": st.column_config.NumberColumn("Valor Nota Crédito (COP)", format="%d"),
                "fecha_emision_erp": st.column_config.DateColumn("Fecha Emisión", format="YYYY-MM-DD"),
            }
        )

# --- PESTAÑA 3: GESTIÓN DE FACTURAS CRÍTICAS (VENCIDAS) ---
with tab_vencidas:
    st.header("🚨 Gestión y Pago de Facturas Críticas")
    st.warning("Esta sección aísla las facturas vencidas. Selecciónalas para generar un lote de pago y depurar la cartera pendiente.")

    if df_vencidas.empty:
        st.success("¡Muy bien! No hay facturas vencidas pendientes en el sistema.")
    else:
        df_vencidas.insert(0, "seleccionar", False)
        
        edited_df_vencidas = st.data_editor(
            df_vencidas, key="data_editor_vencidas", use_container_width=True, hide_index=True,
            column_config={
                "seleccionar": st.column_config.CheckboxColumn(required=True),
                "valor_total_erp": st.column_config.NumberColumn("Valor Factura (COP)", format="%d"),
                "fecha_emision_erp": st.column_config.DateColumn("Fecha Emisión", format="YYYY-MM-DD"),
                "fecha_vencimiento_erp": st.column_config.DateColumn("Fecha Vencimiento", format="YYYY-MM-DD"),
                "dias_para_vencer": st.column_config.NumberColumn("Días Vencida", format="%d días"),
            },
            disabled=[col for col in df_vencidas.columns if col != 'seleccionar']
        )
        selected_rows_vencidas = edited_df_vencidas[edited_df_vencidas['seleccionar']]
        st.divider()

        if not selected_rows_vencidas.empty:
            selection_key_vencidas = tuple(sorted(selected_rows_vencidas['id_factura_unico'].tolist()))
            if st.session_state.get('current_selection_key_vencidas') != selection_key_vencidas:
                st.session_state['id_lote_propuesto_vencidas'] = f"LOTE-VEN-{uuid.uuid4().hex[:6].upper()}"
                st.session_state['current_selection_key_vencidas'] = selection_key_vencidas
        elif 'id_lote_propuesto_vencidas' in st.session_state:
            del st.session_state['id_lote_propuesto_vencidas']
            if 'current_selection_key_vencidas' in st.session_state:
                del st.session_state['current_selection_key_vencidas']

        sub_tab_ven1, sub_tab_ven2 = st.tabs(["📊 Resumen del Lote de Vencidas", "🚀 Confirmar y Pagar Lote de Vencidas"])
        with sub_tab_ven1:
            st.subheader("Análisis del Lote de Vencidas Propuesto")
            if selected_rows_vencidas.empty:
                st.info("Selecciona una o más facturas vencidas para ver el resumen del pago.")
            else:
                total_a_pagar_vencidas = selected_rows_vencidas['valor_total_erp'].sum()
                num_facturas_vencidas = len(selected_rows_vencidas)

                c1, c2 = st.columns(2)
                c1.metric("Nº Facturas Vencidas Seleccionadas", f"{num_facturas_vencidas}")
                c2.metric("💰 TOTAL A PAGAR (COP)", f"{total_a_pagar_vencidas:,.0f}", delta_color="off")
                
                st.dataframe(selected_rows_vencidas, use_container_width=True, hide_index=True)

        with sub_tab_ven2:
            st.subheader("Acciones Finales del Lote de Vencidas")
            if selected_rows_vencidas.empty:
                 st.warning("Debes seleccionar al menos una factura vencida para poder generar un lote de pago.")
            else:
                col1, col2 = st.columns([1, 1])
                with col1:
                    st.markdown("#### ✅ Confirmación y Registro")
                    if st.button("Confirmar y Generar Lote de Vencidas", type="primary", use_container_width=True):
                        with st.spinner("Procesando y guardando lote de vencidas..."):
                            id_lote_vencidas = st.session_state.get('id_lote_propuesto_vencidas', f"LOTE-VEN-ERROR-{uuid.uuid4().hex[:4]}")
                            lote_info_vencidas = {
                                "id_lote": id_lote_vencidas, "fecha_creacion": datetime.now(COLOMBIA_TZ).strftime('%Y-%m-%d %H:%M:%S'),
                                "num_facturas": len(selected_rows_vencidas), "valor_original_total": selected_rows_vencidas['valor_total_erp'].sum(),
                                "ahorro_total_lote": 0, "total_pagado_lote": selected_rows_vencidas['valor_total_erp'].sum(),
                                "creado_por": "Usuario App (Gerencia)", "estado_lote": "Pendiente de Pago en Tesorería"
                            }
                            success, error_msg = guardar_lote_en_gsheets(gs_client, lote_info_vencidas, selected_rows_vencidas)
                            if success:
                                st.success(f"¡Éxito! Lote de Vencidas `{id_lote_vencidas}` generado.")
                                st.balloons()
                                st.rerun()
                            else:
                                st.error(f"Error Crítico: {error_msg}")
                with col2:
                    st.markdown("#### 📲 Notificación a Tesorería")
                    numero_tesoreria_vencidas = st.text_input("Nº WhatsApp Tesorería", st.secrets.get("whatsapp_default_number", ""), key="whatsapp_num_vencidas")
                    id_lote_mensaje_vencidas = st.session_state.get('id_lote_propuesto_vencidas', 'LOTE-VEN-POR-CONFIRMAR')
                    mensaje_vencidas = urllib.parse.quote(
                        f"¡Hola! 👋 Se ha generado un lote de pago de facturas VENCIDAS.\n\n"
                        f"ID Lote: *{id_lote_mensaje_vencidas}*\n\n"
                        f"🔹 Total a Pagar: COP {selected_rows_vencidas['valor_total_erp'].sum():,.0f}\n"
                        f"🔹 Nº Facturas: {len(selected_rows_vencidas)}\n"
                        "Este pago es crítico. Por favor, revisa la plataforma para ver el detalle."
                    )
                    link_whatsapp_vencidas = f"https://wa.me/{numero_tesoreria_vencidas}?text={mensaje_vencidas}"
                    st.link_button("📲 Enviar Notificación Urgente por WhatsApp", link_whatsapp_vencidas, use_container_width=True)
