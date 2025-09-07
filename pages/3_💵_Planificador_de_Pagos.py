# pages/2_🧠_Centro_de_Pagos.py
# -*- coding: utf-8 -*-
"""
Centro de Control de Pagos Inteligente para FERREINOX (Versión 2.0).

Esta herramienta permite la creación de lotes de pago optimizados y ahora
incluye módulos dedicados para la gestión de notas crédito y el seguimiento
de facturas críticas (muy vencidas).

Funcionalidades Clave:
- Interfaz rediseñada con pestañas para mayor claridad:
  1. Plan de Pagos: Para facturas vigentes y por vencer.
  2. Notas Crédito: Para visualizar y gestionar saldos a favor.
  3. Facturas Críticas: Para aislar y tomar acción sobre facturas muy vencidas.
- Motor de sugerencias para optimizar pagos según presupuesto y estrategia.
- Correcta visualización de valores en COP sin el símbolo '$'.
- Generación de lotes de pago con registro en Google Sheets y notificación a tesorería.
- Conexión directa con el reporte consolidado, asegurando datos actualizados.
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
        # Se añade un try-except para columnas opcionales que podrían no existir aún
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
        # Asegurarse de que la columna existe y no es NaT para ordenar
        if 'fecha_emision_erp' in df_sugerencias.columns and df_sugerencias['fecha_emision_erp'].notna().any():
            df_sugerencias = df_sugerencias.sort_values(by='fecha_emision_erp', ascending=True)

    total_acumulado = 0
    ids_sugeridos = []
    for _, row in df_sugerencias.iterrows():
        # Usa 'valor_con_descuento' si existe y es mayor que cero, sino 'valor_total_erp'
        valor_a_considerar = row.get('valor_con_descuento', row['valor_total_erp']) if row.get('valor_con_descuento', 0) > 0 else row['valor_total_erp']
        if total_acumulado + valor_a_considerar <= presupuesto:
            total_acumulado += valor_a_considerar
            ids_sugeridos.append(row['id_factura_unico'])
            
    return ids_sugeridos

# --- 3. INICIO DE LA APLICACIÓN ---
st.title("🧠 Centro de Control de Pagos Inteligente v2.0")
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
# Se asegura que las columnas requeridas existan
required_cols = ['nombre_proveedor', 'num_factura', 'valor_total_erp', 'estado_factura', 'estado_pago']
for col in required_cols:
    if col not in df_full.columns:
        st.error(f"La columna requerida '{col}' no se encuentra en tu Google Sheet. La aplicación no puede continuar.")
        st.stop()
        
# Limpieza de estado y creación de ID único
df_full['estado_factura'] = df_full['estado_factura'].replace('', 'Pendiente').fillna('Pendiente')
df_full['id_factura_unico'] = df_full.apply(
    lambda row: f"{row.get('nombre_proveedor', '')}-{row.get('num_factura', '')}-{row.get('valor_total_erp', '')}",
    axis=1
).str.replace(r'[\s/]+', '-', regex=True)

# Segmentación de datos en tres grupos principales
df_pendientes_full = df_full[df_full['estado_factura'] == 'Pendiente'].copy()

# Grupo 1: Notas Crédito (valores negativos)
df_notas_credito = df_pendientes_full[df_pendientes_full['valor_total_erp'] < 0].copy()

# Grupo 2: Facturas Críticas (vencidas)
df_vencidas = df_pendientes_full[
    (df_pendientes_full['estado_pago'] == '🔴 Vencida') & (df_pendientes_full['valor_total_erp'] >= 0)
].copy()

# Grupo 3: Facturas para Pagar (vigentes o por vencer, no negativas)
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
    min_value=0.0, value=20000000.0, step=1000000.0, format="%f"
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
    f"🚨 Facturas Críticas ({len(df_vencidas)})"
])

# --- PESTAÑA 1: PLAN DE PAGOS ---
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
                "seleccionar": st.column_config.CheckboxColumn(required=True, help="Selecciona las facturas a pagar"),
                "valor_total_erp": st.column_config.NumberColumn("Valor Original", format="COP {:,.0f}"),
                "valor_con_descuento": st.column_config.NumberColumn("Valor a Pagar", format="COP {:,.0f}"),
                "valor_descuento": st.column_config.NumberColumn("Ahorro", format="COP {:,.0f}"),
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
                c2.metric("Valor Original Total", f"COP {total_original:,.0f}")
                c3.metric("Ahorro Total por Descuento", f"COP {ahorro_total:,.0f}")
                c4.metric("💰 TOTAL A PAGAR", f"COP {total_a_pagar:,.0f}", delta_color="off")
                
                st.markdown("#### Detalle del Plan de Pago Propuesto")
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
                    st.info("Al confirmar, se registrará el lote en el historial y se actualizará el estado de las facturas en Google Sheets.")
                    if st.button("Confirmar y Generar Lote", type="primary", use_container_width=True):
                        with st.spinner("Procesando y guardando el lote... Este proceso es irreversible."):
                            id_lote = st.session_state.get('id_lote_propuesto', f"LOTE-ERROR-{uuid.uuid4().hex[:4]}")
                            lote_info = {
                                "id_lote": id_lote, "fecha_creacion": datetime.now(COLOMBIA_TZ).strftime('%Y-%m-%d %H:%M:%S'),
                                "num_facturas": len(selected_rows), "valor_original_total": selected_rows['valor_total_erp'].sum(),
                                "ahorro_total_lote": selected_rows['valor_descuento'].sum(), "total_pagado_lote": selected_rows['valor_con_descuento'].sum(),
                                "creado_por": "Usuario App (Gerencia)", "estado_lote": "Pendiente de Pago en Tesorería"
                            }
                            success, error_msg = guardar_lote_en_gsheets(gs_client, lote_info, selected_rows)
                            if success:
                                st.success(f"¡Éxito! Lote de pago `{id_lote}` generado y guardado.")
                                st.info("La página se recargará para reflejar los cambios.")
                                st.balloons()
                                st.session_state.pop('sugerencia_ids', None)
                                st.rerun()
                            else:
                                st.error(f"Error Crítico al guardar el lote: {error_msg}")
                with col2:
                    st.markdown("#### 📲 Notificación a Tesorería")
                    numero_tesoreria = st.text_input("Número de WhatsApp de Tesorería (ej: 573001234567)", st.secrets.get("whatsapp_default_number", ""), key="whatsapp_num")
                    
                    id_lote_mensaje = st.session_state.get('id_lote_propuesto', 'LOTE-POR-CONFIRMAR')
                    mensaje_base = (
                        f"¡Hola! 👋 Se ha generado un nuevo lote de pago para tu gestión.\n\n"
                        f"ID Lote: *{id_lote_mensaje}*\n\n"
                        f"🔹 *Total a Pagar:* COP {selected_rows['valor_con_descuento'].sum():,.0f}\n"
                        f"🔹 *Nº Facturas:* {len(selected_rows)}\n"
                        f"🔹 *Ahorro Obtenido:* COP {selected_rows['valor_descuento'].sum():,.0f}\n\n"
                        "Por favor, ingresa a la plataforma para revisar el detalle y descargar el soporte."
                    )
                    mensaje_codificado = urllib.parse.quote(mensaje_base)
                    link_whatsapp = f"https://wa.me/{numero_tesoreria}?text={mensaje_codificado}"
                    
                    st.markdown(f'<a href="{link_whatsapp}" target="_blank" class="button">📲 Enviar Notificación por WhatsApp</a>', unsafe_allow_html=True)
                    st.caption("Se abrirá una nueva pestaña con el mensaje listo para ser enviado.")
                    
                    st.markdown("""
                    <style>
                    .button {
                        display: inline-block; padding: 0.75rem 1.25rem; border-radius: 0.5rem;
                        background-color: #25D366; color: white; text-align: center;
                        text-decoration: none; font-weight: bold; width: 100%; box-sizing: border-box;
                    }
                    .button:hover { background-color: #128C7E; }
                    </style>
                    """, unsafe_allow_html=True)

# --- PESTAÑA 2: GESTIÓN DE NOTAS CRÉDITO ---
with tab_credito:
    st.header("📝 Visor de Notas Crédito Pendientes")
    st.info("Aquí se listan todos los saldos a favor (notas crédito) registrados en el ERP que están pendientes por cruzar o aplicar.")

    if df_notas_credito.empty:
        st.success("¡Excelente! No hay notas crédito pendientes de gestión.")
    else:
        c1, c2 = st.columns(2)
        total_nc = df_notas_credito['valor_total_erp'].sum()
        c1.metric("Saldo Total a Favor (NC)", f"COP {total_nc:,.0f}")
        c2.metric("Cantidad de Notas Crédito", f"{len(df_notas_credito)}")

        st.markdown("#### Detalle de Notas Crédito")
        cols_nc_visibles = [
            'nombre_proveedor', 'num_factura', 'valor_total_erp', 
            'fecha_emision_erp', 'doc_erp', 'serie'
        ]
        # Filtra para solo mostrar las columnas que realmente existen en el DataFrame
        cols_nc_a_mostrar = [col for col in cols_nc_visibles if col in df_notas_credito.columns]

        st.dataframe(
            df_notas_credito[cols_nc_a_mostrar].sort_values('fecha_emision_erp', ascending=False),
            use_container_width=True, hide_index=True,
            column_config={
                "valor_total_erp": st.column_config.NumberColumn("Valor Nota Crédito", format="COP {:,.0f}"),
                "fecha_emision_erp": st.column_config.DateColumn("Fecha Emisión", format="YYYY-MM-DD"),
                "doc_erp": "Documento ERP",
            }
        )

# --- PESTAÑA 3: GESTIÓN DE FACTURAS CRÍTICAS (VENCIDAS) ---
with tab_vencidas:
    st.header("🚨 Auditoría de Facturas Críticas (Vencidas)")
    st.warning("Esta sección aísla todas las facturas que ya han superado su fecha de vencimiento. Requieren acción y seguimiento inmediato para evitar problemas con proveedores.")

    if df_vencidas.empty:
        st.success("¡Muy bien! No hay facturas vencidas pendientes en el sistema.")
    else:
        c1, c2 = st.columns(2)
        monto_total_vencido = df_vencidas['valor_total_erp'].sum()
        c1.metric("Monto Total Vencido", f"COP {monto_total_vencido:,.0f}")
        c2.metric("Cantidad de Facturas Vencidas", f"{len(df_vencidas)}")

        st.markdown("#### Detalle de Facturas Vencidas (Más antigua primero)")
        cols_vencidas_visibles = [
            'nombre_proveedor', 'num_factura', 'valor_total_erp', 'fecha_vencimiento_erp',
            'dias_para_vencer', 'fecha_emision_erp'
        ]
        cols_vencidas_a_mostrar = [col for col in cols_vencidas_visibles if col in df_vencidas.columns]

        st.dataframe(
            df_vencidas[cols_vencidas_a_mostrar].sort_values('dias_para_vencer', ascending=True),
            use_container_width=True, hide_index=True,
            column_config={
                "valor_total_erp": st.column_config.NumberColumn("Valor Factura", format="COP {:,.0f}"),
                "fecha_emision_erp": st.column_config.DateColumn("Fecha Emisión", format="YYYY-MM-DD"),
                "fecha_vencimiento_erp": st.column_config.DateColumn("Fecha Vencimiento", format="YYYY-MM-DD"),
                "dias_para_vencer": st.column_config.NumberColumn("Días Vencida", format="%d días"),
            }
        )
