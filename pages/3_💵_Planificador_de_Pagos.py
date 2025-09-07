# -*- coding: utf-8 -*-
"""
Centro de Control de Pagos Inteligente para FERREINOX (Versión 4.1 - Módulo Gerencia).

Este módulo permite a Gerencia crear lotes de pago para facturas vigentes, vencidas
y aplicar notas crédito en un solo flujo.

Mejoras en v4.1:
- Integración de Notas Crédito en el 'Plan de Pagos (Vigentes)' para selección y cruce.
- Aplicación consistente del filtro de proveedor a las pestañas de 'Vigentes' y 'Notas Crédito'.
- Funcionalidad para descargar el listado de Notas Crédito filtradas a un archivo Excel.
- Aclaraciones en la interfaz para mejorar la usabilidad y entendimiento de cada sección.
"""

# --- 0. IMPORTACIÓN DE LIBRERÍAS ---
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import uuid
import gspread
import urllib.parse
import pytz
from google.oauth2.service_account import Credentials
import io  # Necesario para la descarga de archivos en memoria

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
        if creds_dict.get("private_key_id") is None:
            creds_dict.pop("private_key_id", None)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
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

        # 1. Normalización
        df.columns = [str(col).strip().lower().replace(' ', '_') for col in df.columns]
        
        # 2. Renombrado
        if 'nombre_proveedor_erp' in df.columns:
            df.rename(columns={'nombre_proveedor_erp': 'nombre_proveedor'}, inplace=True)

        # 3. Eliminar columnas duplicadas
        df = df.loc[:, ~df.columns.duplicated(keep='first')]

        # 4. Garantizar columnas críticas
        required_cols = {
            'nombre_proveedor': 'N/A', 'valor_total_erp': 0, 
            'estado_factura': 'Pendiente', 'num_factura': 'N/A',
            'estado_pago': 'N/A'
        }
        for col, default in required_cols.items():
            if col not in df.columns:
                df[col] = default

        # 5. Limpieza y Conversión de Tipos
        df['estado_factura'] = df['estado_factura'].astype(str).str.strip().str.capitalize().replace('', 'Pendiente')
        
        numeric_cols = ['valor_total_erp', 'dias_para_vencer', 'valor_descuento', 'valor_con_descuento']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        date_cols = ['fecha_emision_erp', 'fecha_vencimiento_erp', 'fecha_limite_descuento']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return df

    except gspread.exceptions.WorksheetNotFound:
        st.error(f"❌ Error fatal: No se encontró la hoja '{GSHEET_REPORT_NAME}'.")
    except Exception as e:
        st.error(f"❌ Ocurrió un error inesperado al cargar los datos: {e}")
    return pd.DataFrame()
# --- FIN: Lógica de common/utils.py integrada ---


# --- 1. CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(
    layout="wide",
    page_title="Planificador de Pagos | Gerencia",
    page_icon="💵"
)

# --- 2. CONSTANTES Y CLAVES DE SESIÓN ---
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
    (Lógica original mantenida según solicitud).
    """
    try:
        spreadsheet = gs_client.open_by_key(st.secrets["google_sheet_id"])
        
        # 1. Guardar en el historial de lotes
        historial_ws = spreadsheet.worksheet("Historial_Lotes_Pago")
        headers = historial_ws.row_values(1)
        valores_fila = [lote_info.get(col) for col in headers]
        historial_ws.append_row(valores_fila)

        # 2. Actualizar el reporte principal
        reporte_ws = spreadsheet.worksheet(GSHEET_REPORT_NAME)
        reporte_data = reporte_ws.get_all_values()
        if len(reporte_data) < 2:
            st.error("La hoja de reporte está vacía. No se pueden actualizar las facturas.")
            return False, "Hoja de reporte vacía."

        # --- INICIO DE LA CORRECCIÓN MEJORADA ---
        original_headers = [str(h).strip().lower().replace(' ', '_') for h in reporte_data[0]]
        processed_headers = ['nombre_proveedor' if h == 'nombre_proveedor_erp' else h for h in original_headers]
        reporte_df = pd.DataFrame(reporte_data[1:], columns=processed_headers)
        reporte_df = reporte_df.loc[:, ~reporte_df.columns.duplicated(keep='first')]

        # Asegurar tipos de datos para la búsqueda
        reporte_df['valor_total_erp'] = pd.to_numeric(reporte_df['valor_total_erp'], errors='coerce').fillna(0)
        reporte_df['nombre_proveedor'] = reporte_df['nombre_proveedor'].astype(str).str.strip()
        reporte_df['num_factura'] = reporte_df['num_factura'].astype(str).str.strip()
        # --- FIN DE LA CORRECCIÓN MEJORADA ---

        try:
            estado_col_idx = reporte_df.columns.get_loc('estado_factura') + 1
            lote_col_idx = reporte_df.columns.get_loc('id_lote_pago') + 1
        except KeyError as e:
            error_msg = f"Error Crítico: La columna '{e.args[0]}' no se encuentra en la hoja '{GSHEET_REPORT_NAME}'."
            st.error(error_msg)
            return False, error_msg

        updates = []
        for _, factura_a_actualizar in facturas_seleccionadas.iterrows():
            match = reporte_df[
                (reporte_df['nombre_proveedor'] == str(factura_a_actualizar['nombre_proveedor']).strip()) &
                (reporte_df['num_factura'] == str(factura_a_actualizar['num_factura']).strip()) &
                (np.isclose(reporte_df['valor_total_erp'], float(factura_a_actualizar['valor_total_erp'])))
            ]
            
            if not match.empty:
                row_index_to_update = match.index[0] + 2 # +2 por encabezado y base 0
                updates.append({'range': gspread.utils.rowcol_to_a1(row_index_to_update, estado_col_idx), 'values': [['En Lote de Pago']]})
                updates.append({'range': gspread.utils.rowcol_to_a1(row_index_to_update, lote_col_idx), 'values': [[lote_info['id_lote']]]})
            else:
                st.warning(f"No se encontró coincidencia para factura '{factura_a_actualizar['num_factura']}' de '{factura_a_actualizar['nombre_proveedor']}'. No se actualizará.")
        
        if updates:
            reporte_ws.batch_update(updates)
        return True, None
    except Exception as e:
        error_msg = f"Error inesperado al actualizar Google Sheets: {e}"
        st.error(error_msg)
        return False, str(e)


def generar_sugerencias(df: pd.DataFrame, presupuesto: float, estrategia: str) -> list:
    """Genera una lista de IDs de facturas sugeridas para pagar según una estrategia."""
    if presupuesto <= 0 or df.empty:
        return []
    
    # Filtrar solo facturas (valores positivos) para sugerencias
    df_sugerencias = df[df['valor_total_erp'] > 0].copy()
    
    if estrategia == "Maximizar Ahorro" and 'valor_descuento' in df_sugerencias.columns:
        df_sugerencias = df_sugerencias.sort_values(by='valor_descuento', ascending=False)
    elif estrategia == "Evitar Vencimientos" and 'dias_para_vencer' in df_sugerencias.columns:
        df_sugerencias = df_sugerencias.sort_values(by='dias_para_vencer', ascending=True)
    elif estrategia == "Priorizar Antigüedad" and 'fecha_emision_erp' in df_sugerencias.columns:
        df_sugerencias = df_sugerencias.sort_values(by='fecha_emision_erp', ascending=True)
    
    total_acumulado = 0
    ids_sugeridos = []
    for _, row in df_sugerencias.iterrows():
        valor_a_considerar = row.get('valor_con_descuento', row['valor_total_erp']) if row.get('valor_con_descuento', 0) > 0 else row['valor_total_erp']
        
        if total_acumulado + valor_a_considerar <= presupuesto:
            total_acumulado += valor_a_considerar
            ids_sugeridos.append(row['id_factura_unico'])
            
    return ids_sugeridos

def to_excel(df: pd.DataFrame) -> bytes:
    """Convierte un DataFrame a un archivo Excel en memoria."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Notas_Credito')
    processed_data = output.getvalue()
    return processed_data

# --- 4. INICIO DE LA APLICACIÓN ---
st.title("💵 Planificador de Pagos | Gerencia")
st.markdown("Herramienta para crear lotes de pago a partir de la cartera pendiente.")

gs_client = connect_to_google_sheets()
if not gs_client:
    st.stop()

df_full = load_data_from_gsheet(gs_client)

if df_full.empty:
    st.warning(f"No se encontraron datos válidos en la hoja de cálculo '{GSHEET_REPORT_NAME}'. Verifique la fuente de datos.")
    st.stop()

# --- 5. PRE-PROCESAMIENTO Y SEGMENTACIÓN DE DATOS ---
df_full['id_factura_unico'] = df_full.apply(
    lambda row: f"{row.get('nombre_proveedor', '')}-{row.get('num_factura', '')}-{row.get('valor_total_erp', 0)}",
    axis=1
).str.replace(r'[\s/]+', '-', regex=True)

df_pendientes_full = df_full[df_full['estado_factura'] == 'Pendiente'].copy()

# Segmentación clara
df_notas_credito = df_pendientes_full[df_pendientes_full['valor_total_erp'] < 0].copy()
df_vencidas = df_pendientes_full[(df_pendientes_full['estado_pago'] == '🔴 Vencida') & (df_pendientes_full['valor_total_erp'] >= 0)].copy()
df_para_pagar_vigentes = df_pendientes_full[(df_pendientes_full['valor_total_erp'] >= 0) & (df_pendientes_full['estado_pago'].isin(['🟢 Vigente', '🟠 Por Vencer (7 días)']))].copy()

# --- 6. BARRA LATERAL (SIDEBAR) ---
with st.sidebar:
    st.header("⚙️ Filtros Globales")
    
    proveedores_lista = sorted(df_pendientes_full['nombre_proveedor'].dropna().unique().tolist())
    selected_suppliers = st.multiselect("Filtrar por Proveedor:", proveedores_lista, placeholder="Seleccione uno o más proveedores")

    # Aplicar filtros a los dataframes base
    df_pagar_filtrado = df_para_pagar_vigentes.copy()
    df_credito_filtrado = df_notas_credito.copy()
    df_vencidas_filtrado = df_vencidas.copy()

    if selected_suppliers:
        df_pagar_filtrado = df_pagar_filtrado[df_pagar_filtrado['nombre_proveedor'].isin(selected_suppliers)]
        df_credito_filtrado = df_credito_filtrado[df_credito_filtrado['nombre_proveedor'].isin(selected_suppliers)]
        df_vencidas_filtrado = df_vencidas_filtrado[df_vencidas_filtrado['nombre_proveedor'].isin(selected_suppliers)]
        st.info("Filtro aplicado a 'Plan de Pagos', 'Facturas Críticas' y 'Notas Crédito'.")

    st.divider()
    st.subheader("🤖 Motor de Sugerencias")
    st.info("Las sugerencias se calculan sobre las facturas vigentes que coinciden con el filtro de proveedor.")
    presupuesto = st.number_input("Ingresa tu Presupuesto de Pago:", min_value=0.0, value=20000000.0, step=1000000.0, help="Presupuesto para calcular las sugerencias de pago.")
    estrategia = st.selectbox("Selecciona tu Estrategia de Pago:", ["Maximizar Ahorro", "Evitar Vencimientos", "Priorizar Antigüedad"])

    if st.button("💡 Generar Sugerencia de Pago", type="primary", use_container_width=True):
        ids_sugeridos = generar_sugerencias(df_pagar_filtrado, presupuesto, estrategia)
        st.session_state[SESSION_KEY_SUGERENCIA_IDS] = ids_sugeridos
        if not ids_sugeridos:
            st.warning("No se pudieron generar sugerencias con el presupuesto y filtros actuales.")
        else:
            st.success(f"Sugerencia generada para {len(ids_sugeridos)} facturas.")

# --- 7. CUERPO PRINCIPAL CON PESTAÑAS ---
# NUEVO: Combinar vigentes y notas crédito para la primera pestaña
df_plan_pagos_completo = pd.concat([df_pagar_filtrado, df_credito_filtrado], ignore_index=True)


tab_pagos, tab_vencidas, tab_credito = st.tabs([
    f"✅ Plan de Pagos ({len(df_plan_pagos_completo)})",
    f"🚨 Gestión de Facturas Críticas ({len(df_vencidas_filtrado)})",
    f"📝 Visor de Notas Crédito ({len(df_credito_filtrado)})"
])

# --- PESTAÑA 1: PLAN DE PAGOS (VIGENTES + NOTAS CRÉDITO) ---
with tab_pagos:
    st.header("1. Selección de Facturas y Notas Crédito para el Plan de Pago")
    st.markdown("Marca las **facturas** que deseas pagar y las **notas crédito** que deseas cruzar en este lote.")
    
    df_plan_pagos_completo.insert(0, "seleccionar", False)
    
    # Aplicar sugerencias si existen
    if SESSION_KEY_SUGERENCIA_IDS in st.session_state:
        df_plan_pagos_completo['seleccionar'] = df_plan_pagos_completo['id_factura_unico'].isin(st.session_state[SESSION_KEY_SUGERENCIA_IDS])
        del st.session_state[SESSION_KEY_SUGERENCIA_IDS]

    if df_plan_pagos_completo.empty:
        st.info("No hay facturas vigentes ni notas crédito que coincidan con los filtros actuales.")
    else:
        edited_df_vigentes = st.data_editor(
            df_plan_pagos_completo, key="data_editor_pagos", use_container_width=True, hide_index=True,
            column_config={
                "seleccionar": st.column_config.CheckboxColumn("Seleccionar", required=True),
                "valor_total_erp": st.column_config.NumberColumn("Valor Original (COP)", format="COP %d"),
                "valor_con_descuento": st.column_config.NumberColumn("Valor a Pagar/Cruzar (COP)", format="COP %d"),
                "valor_descuento": st.column_config.NumberColumn("Ahorro (COP)", format="COP %d")
            }, disabled=[col for col in df_plan_pagos_completo.columns if col != 'seleccionar']
        )
        selected_rows_vigentes = edited_df_vigentes[edited_df_vigentes['seleccionar']]
        st.divider()

        if not selected_rows_vigentes.empty:
            selection_key = tuple(sorted(selected_rows_vigentes['id_factura_unico'].tolist()))
            if st.session_state.get(SESSION_KEY_SELECTION_VIGENTES) != selection_key:
                st.session_state[SESSION_KEY_LOTE_VIGENTES] = f"LOTE-VIG-{uuid.uuid4().hex[:6].upper()}"
                st.session_state[SESSION_KEY_SELECTION_VIGENTES] = selection_key
        
            sub_tab1_vig, sub_tab2_vig = st.tabs(["📊 Resumen del Lote", "🚀 Confirmar y Notificar a Tesorería"])
            with sub_tab1_vig:
                st.subheader("Análisis del Lote de Pagos")
                # El total a pagar ahora considera las notas crédito (valores negativos)
                total_pagar = selected_rows_vigentes['valor_con_descuento'].sum()
                total_ahorro = selected_rows_vigentes['valor_descuento'].sum()
                num_documentos = len(selected_rows_vigentes)
                
                c1, c2, c3 = st.columns(3)
                c1.metric("Nº Documentos Seleccionados", f"{num_documentos}")
                c2.metric("💰 TOTAL NETO A PAGAR (COP)", f"{total_pagar:,.0f}")
                c3.metric("💸 AHORRO TOTAL (COP)", f"{total_ahorro:,.0f}")
                st.dataframe(selected_rows_vigentes[['nombre_proveedor', 'num_factura', 'valor_total_erp', 'valor_con_descuento', 'valor_descuento', 'fecha_vencimiento_erp']], use_container_width=True, hide_index=True)

            with sub_tab2_vig:
                st.subheader("Acciones Finales del Lote")
                col1, col2 = st.columns([1, 1])
                with col1:
                    st.markdown("#### ✅ Confirmación y Registro")
                    if st.button("Confirmar y Generar Lote de PAGO", type="primary", use_container_width=True):
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
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error(f"Error Crítico al guardar: {error_msg}")
                with col2:
                    st.markdown("#### 📲 Notificación a Tesorería")
                    id_lote_mensaje = st.session_state.get(SESSION_KEY_LOTE_VIGENTES, 'LOTE-POR-CONFIRMAR')
                    numero_tesoreria = st.text_input("Nº WhatsApp Tesorería", st.secrets.get("whatsapp_default_number", ""), key="whatsapp_num_vigentes")
                    mensaje = urllib.parse.quote(f"¡Hola! 👋 Se ha generado un nuevo lote de pago.\n\n*ID Lote:* {id_lote_mensaje}\n*Total Neto a Pagar:* COP {selected_rows_vigentes['valor_con_descuento'].sum():,.0f}\n*Nº Documentos:* {len(selected_rows_vigentes)}\n\nPor favor, revisa la plataforma para ver el detalle.")
                    st.link_button("📲 Enviar Notificación por WhatsApp", f"https://wa.me/{numero_tesoreria}?text={mensaje}", use_container_width=True, type="secondary")

# --- PESTAÑA 2: GESTIÓN DE FACTURAS CRÍTICAS (VENCIDAS) ---
with tab_vencidas:
    st.header("1. Selección de Facturas Críticas para Pago Inmediato")
    st.warning("¡Atención! Estás creando un lote de pago para facturas ya vencidas. El filtro de proveedor también aplica aquí.")
    
    df_vencidas_filtrado.insert(0, "seleccionar", False)

    if df_vencidas_filtrado.empty:
        st.success("¡Excelente! No hay facturas críticas (vencidas) que coincidan con los filtros actuales.")
    else:
        edited_df_vencidas = st.data_editor(
            df_vencidas_filtrado, key="data_editor_vencidas", use_container_width=True, hide_index=True,
            column_config={
                "seleccionar": st.column_config.CheckboxColumn("Seleccionar", required=True),
                "valor_total_erp": st.column_config.NumberColumn("Valor a Pagar (COP)", format="COP %d"),
                "dias_para_vencer": st.column_config.NumberColumn("Días Vencida", format="%d días"),
            },
            disabled=[col for col in df_vencidas_filtrado.columns if col != 'seleccionar']
        )
        selected_rows_vencidas = edited_df_vencidas[edited_df_vencidas['seleccionar']]
        st.divider()

        if not selected_rows_vencidas.empty:
            selection_key_vencidas = tuple(sorted(selected_rows_vencidas['id_factura_unico'].tolist()))
            if st.session_state.get(SESSION_KEY_SELECTION_VENCIDAS) != selection_key_vencidas:
                st.session_state[SESSION_KEY_LOTE_VENCIDAS] = f"LOTE-CRI-{uuid.uuid4().hex[:6].upper()}"
                st.session_state[SESSION_KEY_SELECTION_VENCIDAS] = selection_key_vencidas

            sub_tab1_ven, sub_tab2_ven = st.tabs(["📊 Resumen del Lote (Críticos)", "🚀 Confirmar y Notificar a Tesorería"])
            with sub_tab1_ven:
                st.subheader("Análisis del Lote de Pagos Críticos")
                total_a_pagar, num_facturas = selected_rows_vencidas['valor_total_erp'].sum(), len(selected_rows_vencidas)
                c1, c2 = st.columns(2)
                c1.metric("Nº Facturas Seleccionadas", f"{num_facturas}")
                c2.metric("💰 TOTAL A PAGAR (COP)", f"{total_a_pagar:,.0f}")
                st.dataframe(selected_rows_vencidas[['nombre_proveedor', 'num_factura', 'valor_total_erp', 'dias_para_vencer']], use_container_width=True, hide_index=True)
            
            with sub_tab2_ven:
                st.subheader("Acciones Finales del Lote de Críticos")
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
                                st.cache_data.clear()
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
    st.info("Aquí se listan los saldos a favor (notas crédito) pendientes. El filtro de proveedor también aplica aquí.")
    if df_credito_filtrado.empty:
        st.success("¡Excelente! No hay notas crédito pendientes que coincidan con los filtros actuales.")
    else:
        c1, c2 = st.columns(2)
        total_favor = df_credito_filtrado['valor_total_erp'].sum()
        c1.metric("Saldo Total a Favor (COP)", f"{abs(total_favor):,.0f}")
        c2.metric("Cantidad de Notas Crédito", f"{len(df_credito_filtrado)}")
        
        cols_to_display = ['nombre_proveedor', 'num_factura', 'valor_total_erp', 'fecha_emision_erp']
        existing_cols = [col for col in cols_to_display if col in df_credito_filtrado.columns]
        
        st.dataframe(df_credito_filtrado[existing_cols], use_container_width=True, hide_index=True)
        
        st.divider()
        
        st.download_button(
           label="📥 Descargar listado a Excel",
           data=to_excel(df_credito_filtrado[existing_cols]),
           file_name=f"notas_credito_{datetime.now().strftime('%Y%m%d')}.xlsx",
           mime="application/vnd.ms-excel",
           use_container_width=True
        )
