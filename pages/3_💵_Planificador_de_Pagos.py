import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import uuid
import gspread
import re
import urllib.parse

# Se importa directamente desde el archivo que ya tienes en tu proyecto.
# Asegúrate de que este archivo exista en la ruta common/utils.py
from common.utils import connect_to_google_sheets, load_data_from_gsheet

# --- CONFIGURACIÓN DE PÁGINA ---
# Usamos un layout ancho para mejor visualización de datos y un título descriptivo.
st.set_page_config(
    layout="wide",
    page_title="Centro de Control de Pagos Inteligente",
    page_icon="🧠"
)

# --- FUNCIONES AUXILIARES ---

def to_excel(df: pd.DataFrame) -> bytes:
    """
    Convierte un DataFrame a un archivo Excel en memoria (bytes).
    Esto es más eficiente que guardar en disco para luego leer.
    """
    output = BytesIO()
    # Usamos el motor 'xlsxwriter' que permite más personalización si fuera necesario.
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='PlanDePagoGenerado')
    # Devolvemos el contenido del archivo en memoria.
    return output.getvalue()

def guardar_lote_en_gsheets(gs_client: gspread.Client, lote_info: dict, facturas_seleccionadas: pd.DataFrame):
    """
    Guarda el resumen del lote en el historial y actualiza el estado
    de las facturas seleccionadas en el reporte principal de forma transaccional.
    """
    try:
        # Abre la hoja de cálculo usando el ID almacenado en los secretos de Streamlit.
        spreadsheet = gs_client.open_by_key(st.secrets["google_sheet_id"])

        # --- 1. Guardar el resumen del lote en la hoja de historial ---
        historial_ws = spreadsheet.worksheet("Historial_Lotes_Pago")
        # Obtenemos los encabezados de la hoja para asegurar el orden correcto de los datos.
        headers = historial_ws.row_values(1)
        # Creamos la fila de valores en el mismo orden que los encabezados.
        # Usamos .get(col, None) para evitar errores si alguna columna no está en lote_info.
        valores_fila = [lote_info.get(col) for col in headers]
        historial_ws.append_row(valores_fila)

        # --- 2. Actualizar el estado de las facturas en la hoja principal ---
        reporte_ws = spreadsheet.worksheet("ReporteConsolidado_Activo")

        # IDs únicos de las facturas que se van a actualizar.
        ids_a_actualizar = facturas_seleccionadas['id_factura_unico'].tolist()

        # Obtenemos las columnas de la hoja para encontrar los índices por nombre.
        reporte_headers = reporte_ws.row_values(1)
        id_col_idx = reporte_headers.index('id_factura_unico') + 1
        estado_col_idx = reporte_headers.index('estado_factura') + 1
        lote_col_idx = reporte_headers.index('id_lote_pago') + 1

        # Obtenemos todos los IDs de la hoja de cálculo en una sola llamada para eficiencia.
        all_ids_in_sheet = reporte_ws.col_values(id_col_idx)

        # Preparamos las actualizaciones en lote (batch) para ser más eficientes.
        updates = []
        for id_factura in ids_a_actualizar:
            try:
                # Encontramos la fila correspondiente a este ID.
                row_index = all_ids_in_sheet.index(id_factura) + 1

                # Añadir actualización para la columna 'estado_factura'.
                updates.append({
                    'range': gspread.utils.rowcol_to_a1(row_index, estado_col_idx),
                    'values': [['En Lote de Pago']]
                })
                # Añadir actualización para la columna 'id_lote_pago'.
                updates.append({
                    'range': gspread.utils.rowcol_to_a1(row_index, lote_col_idx),
                    'values': [[lote_info['id_lote']]]
                })
            except ValueError:
                # Si un ID no se encuentra, lo informamos pero continuamos con los demás.
                st.warning(f"No se encontró la factura con ID '{id_factura}' en la hoja principal. Se omitirá su actualización.")

        # Ejecuta todas las actualizaciones en una sola llamada a la API si hay algo que actualizar.
        if updates:
            reporte_ws.batch_update(updates)

        return True, None
    except gspread.exceptions.WorksheetNotFound:
        return False, "Error: No se encontró una de las hojas requeridas ('Historial_Lotes_Pago' o 'ReporteConsolidado_Activo')."
    except Exception as e:
        # Capturamos cualquier otro error para dar un feedback claro.
        st.error(f"Error inesperado en la comunicación con Google Sheets: {e}")
        return False, str(e)

def generar_sugerencias(df: pd.DataFrame, presupuesto: float, estrategia: str) -> list:
    """
    Motor de inteligencia para sugerir qué facturas pagar según una estrategia y presupuesto.
    """
    if presupuesto <= 0 or df.empty:
        return []

    df_sugerencias = df.copy()
    
    # Aplicar la estrategia de ordenamiento.
    if estrategia == "Maximizar Ahorro":
        df_sugerencias = df_sugerencias.sort_values(by='valor_descuento', ascending=False)
    elif estrategia == "Evitar Vencimientos":
        df_sugerencias = df_sugerencias.sort_values(by='dias_para_vencer', ascending=True)
    elif estrategia == "Priorizar Antigüedad":
        df_sugerencias = df_sugerencias.sort_values(by='fecha_factura', ascending=True)

    # Seleccionar facturas hasta alcanzar el presupuesto.
    total_acumulado = 0
    ids_sugeridos = []
    for _, row in df_sugerencias.iterrows():
        if total_acumulado + row['valor_con_descuento'] <= presupuesto:
            total_acumulado += row['valor_con_descuento']
            ids_sugeridos.append(row['id_factura_unico'])
            
    return ids_sugeridos

# --- INICIO DE LA APLICACIÓN ---
st.title("🧠 Centro de Control de Pagos Inteligente")
st.markdown("""
Esta herramienta te permite construir lotes de pago de forma interactiva e inteligente.
Utiliza el **Motor de Sugerencias** para optimizar tus pagos según tu presupuesto y estrategia.
""")

# --- Carga y Cacheo de Datos ---
# Se utiliza el caché de la función para eficiencia.
try:
    gs_client = connect_to_google_sheets()
    df_full = load_data_from_gsheet(gs_client)
except Exception as e:
    st.error(f"No se pudo conectar o cargar los datos desde Google Sheets. Error: {e}")
    st.stop()

if df_full.empty:
    st.warning("No hay datos disponibles en la hoja 'ReporteConsolidado_Activo'. Por favor, verifica la fuente de datos.")
    st.stop()

# --- PRE-PROCESAMIENTO Y LIMPIEZA DE DATOS ---
# Asegura que las columnas críticas existan.
required_cols = ['nombre_proveedor', 'num_factura', 'valor_total_erp', 'estado_factura']
for col in required_cols:
    if col not in df_full.columns:
        st.error(f"La columna requerida '{col}' no se encuentra en tu Google Sheet. La aplicación no puede continuar.")
        st.stop()
        
# Rellenar 'estado_factura' vacío con 'Pendiente' por defecto.
df_full['estado_factura'] = df_full['estado_factura'].replace('', 'Pendiente').fillna('Pendiente')

# Crear un ID único y robusto para cada factura. Es VITAL para la actualización.
df_full['id_factura_unico'] = df_full.apply(
    lambda row: f"{row['nombre_proveedor']}-{row['num_factura']}-{row['valor_total_erp']}-{row.get('fecha_factura', '')}",
    axis=1
).str.replace(r'\s+', '-', regex=True) # Reemplazar espacios para evitar problemas

# Conversión de tipos de datos para asegurar cálculos correctos.
numeric_cols = ['valor_total_erp', 'valor_con_descuento', 'valor_descuento', 'dias_para_vencer']
for col in numeric_cols:
    df_full[col] = pd.to_numeric(df_full[col], errors='coerce').fillna(0)

date_cols = ['fecha_factura', 'fecha_limite_descuento']
for col in date_cols:
    df_full[col] = pd.to_datetime(df_full[col], errors='coerce')


# Filtrar solo facturas PENDIENTES. Esta es la lógica principal del planificador.
df_pendientes = df_full[df_full['estado_factura'] == 'Pendiente'].copy()
# Excluir Notas de Crédito o valores negativos que no son pagos.
df_pendientes = df_pendientes[df_pendientes['valor_total_erp'] >= 0]


# --- BARRA LATERAL (SIDEBAR) CON FILTROS Y MOTOR DE SUGERENCIAS ---
st.sidebar.header("⚙️ Filtros y Sugerencias")

# Filtros estándar
proveedores_lista = sorted(df_pendientes['nombre_proveedor'].dropna().unique().tolist())
selected_suppliers = st.sidebar.multiselect("Filtrar por Proveedor:", proveedores_lista)

estado_pago_lista = df_pendientes['estado_pago'].unique().tolist()
default_status = [s for s in ["🟢 Vigente", "🟠 Por Vencer (7 días)"] if s in estado_pago_lista]
selected_status = st.sidebar.multiselect("Filtrar por Estado de Pago:", estado_pago_lista, default=default_status)

# Aplicar filtros al DataFrame
df_filtrado = df_pendientes.copy()
if selected_suppliers:
    df_filtrado = df_filtrado[df_filtrado['nombre_proveedor'].isin(selected_suppliers)]
if selected_status:
    df_filtrado = df_filtrado[df_filtrado['estado_pago'].isin(selected_status)]

st.sidebar.divider()

# Motor de Sugerencias Inteligente
st.sidebar.subheader("🤖 Motor de Sugerencias")
presupuesto = st.sidebar.number_input(
    "Ingresa tu Presupuesto de Pago:",
    min_value=0.0,
    value=10000000.0, # Valor por defecto
    step=500000.0,
    format="%.2f"
)

estrategia = st.sidebar.selectbox(
    "Selecciona tu Estrategia de Pago:",
    ["Maximizar Ahorro", "Evitar Vencimientos", "Priorizar Antigüedad"],
    help="El motor seleccionará las facturas óptimas según esta regla y tu presupuesto."
)

if st.sidebar.button("💡 Generar Sugerencia", type="primary"):
    ids_sugeridos = generar_sugerencias(df_filtrado, presupuesto, estrategia)
    if ids_sugeridos:
        # Guardamos los IDs sugeridos en el estado de la sesión para que persistan.
        st.session_state['sugerencia_ids'] = ids_sugeridos
        st.toast(f"¡Sugerencia generada! Se han pre-seleccionado {len(ids_sugeridos)} facturas.", icon="💡")
    else:
        st.session_state['sugerencia_ids'] = []
        st.warning("No se pudieron generar sugerencias con los criterios actuales.")

# --- CUERPO PRINCIPAL DE LA APLICACIÓN ---

# Insertamos la columna de selección.
df_filtrado.insert(0, "seleccionar", False)

# Pre-seleccionar filas basadas en la sugerencia del motor
if 'sugerencia_ids' in st.session_state and st.session_state['sugerencia_ids']:
    df_filtrado['seleccionar'] = df_filtrado['id_factura_unico'].isin(st.session_state['sugerencia_ids'])

st.header("1. Selección de Facturas para el Plan de Pago")
st.markdown("Marca las casillas de las facturas que deseas incluir. Puedes usar el **Motor de Sugerencias** en la barra lateral para una pre-selección inteligente.")

if df_filtrado.empty:
    st.info("No hay facturas pendientes que coincidan con los filtros actuales.")
else:
    # El data_editor es la herramienta principal para la interacción del usuario.
    edited_df = st.data_editor(
        df_filtrado,
        key="data_editor_pagos",
        use_container_width=True,
        hide_index=True,
        column_config={
            "seleccionar": st.column_config.CheckboxColumn(required=True, help="Selecciona las facturas a pagar"),
            "valor_total_erp": st.column_config.NumberColumn("Valor Original", format="$ {:,.0f}"),
            "valor_con_descuento": st.column_config.NumberColumn("Valor a Pagar", format="$ {:,.0f}"),
            "valor_descuento": st.column_config.NumberColumn("Ahorro", format="$ {:,.0f}"),
            "fecha_factura": st.column_config.DateColumn("Fecha Factura", format="YYYY-MM-DD"),
            "fecha_limite_descuento": st.column_config.DateColumn("Límite Descuento", format="YYYY-MM-DD"),
            "dias_para_vencer": st.column_config.NumberColumn("Días Vence", format="%d días"),
        },
        # Deshabilitamos la edición de todas las columnas excepto 'seleccionar'.
        disabled=[col for col in df_filtrado.columns if col != 'seleccionar']
    )

    # Filtramos las filas que el usuario ha seleccionado.
    selected_rows = edited_df[edited_df['seleccionar']]
    st.divider()

    # --- PESTAÑAS PARA RESUMEN Y ACCIONES ---
    tab1, tab2 = st.tabs(["📊 Resumen del Plan de Pago", "🚀 Confirmar y Ejecutar Acciones"])

    with tab1:
        st.subheader("Análisis del Lote Propuesto")
        if selected_rows.empty:
            st.info("Selecciona una o más facturas en la tabla de arriba para ver el resumen del pago.")
        else:
            total_original = selected_rows['valor_total_erp'].sum()
            ahorro_total = selected_rows['valor_descuento'].sum()
            total_a_pagar = selected_rows['valor_con_descuento'].sum()
            num_facturas = len(selected_rows)

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Nº Facturas Seleccionadas", f"{num_facturas}")
            c2.metric("Valor Original Total", f"${total_original:,.0f}")
            c3.metric("Ahorro Total por Descuento", f"${ahorro_total:,.0f}")
            c4.metric("💰 TOTAL A PAGAR", f"${total_a_pagar:,.0f}", delta_color="off")

            st.markdown("#### Detalle del Plan de Pago Propuesto")
            cols_to_show = [
                'nombre_proveedor', 'num_factura', 'valor_total_erp', 'estado_descuento',
                'valor_descuento', 'valor_con_descuento', 'fecha_limite_descuento', 'estado_pago'
            ]
            st.dataframe(selected_rows[cols_to_show], use_container_width=True, hide_index=True)

    with tab2:
        st.subheader("Acciones Finales del Lote")
        if selected_rows.empty:
             st.warning("Debes seleccionar al menos una factura para poder generar un lote de pago.")
        else:
            col1, col2 = st.columns([1, 1])

            with col1:
                st.markdown("#### ✅ Confirmación y Registro")
                st.info("Al confirmar, se registrará el lote en el historial y se actualizará el estado de las facturas en Google Sheets.")

                if st.button("Confirmar y Generar Lote", type="primary", use_container_width=True):
                    with st.spinner("Procesando y guardando el lote de pago... Este proceso es irreversible."):
                        # Crear la información del lote.
                        id_lote = f"LOTE-{uuid.uuid4().hex[:8].upper()}"
                        lote_info = {
                            "id_lote": id_lote,
                            "fecha_creacion": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            "num_facturas": num_facturas,
                            "valor_original_total": total_original,
                            "ahorro_total_lote": ahorro_total,
                            "total_pagado_lote": total_a_pagar,
                            "creado_por": "Usuario App (Gerencia)",
                            "estado_lote": "Pendiente de Pago en Tesorería"
                        }

                        # Llamar a la función para guardar en Google Sheets.
                        success, error_msg = guardar_lote_en_gsheets(gs_client, lote_info, selected_rows)

                        if success:
                            st.success(f"¡Éxito! Lote de pago `{id_lote}` generado y guardado correctamente.")
                            st.info("Las facturas seleccionadas ya no aparecerán como pendientes. La página se recargará para reflejar los cambios.")
                            st.balloons()
                            # Limpiar la selección para evitar re-envíos accidentales.
                            st.session_state['sugerencia_ids'] = []
                            # Forzar recarga con un botón para que el usuario controle
                            st.rerun()
                        else:
                            st.error(f"Error Crítico al guardar el lote: {error_msg}")

            with col2:
                st.markdown("#### 📲 Notificación a Tesorería")
                numero_tesoreria = st.text_input(
                    "Número de WhatsApp de Tesorería (ej: 573001234567)",
                    st.secrets.get("whatsapp_default_number", ""), # Usa un secreto para el número por defecto
                    key="whatsapp_num"
                )

                # Se define lote_info aquí también para que esté disponible aunque no se presione el botón de generar lote
                total_original = selected_rows['valor_total_erp'].sum()
                ahorro_total = selected_rows['valor_descuento'].sum()
                total_a_pagar = selected_rows['valor_con_descuento'].sum()
                num_facturas = len(selected_rows)
                
                mensaje_base = (
                    f"¡Hola! 👋 Se ha generado un nuevo lote de pago para tu gestión.\n\n"
                    f"*{'LOTE-POR-CONFIRMAR'}*\n\n"
                    f"🔹 *Total a Pagar:* ${total_a_pagar:,.0f}\n"
                    f"🔹 *Nº Facturas:* {num_facturas}\n"
                    f"🔹 *Ahorro Obtenido:* ${ahorro_total:,.0f}\n\n"
                    f"Por favor, ingresa a la pestaña 'Historial de Pagos' en la plataforma para revisar el detalle y descargar el soporte para la transacción."
                )
                
                # Codificar el mensaje para la URL de forma segura.
                mensaje_codificado = urllib.parse.quote(mensaje_base)
                link_whatsapp = f"https://wa.me/{numero_tesoreria}?text={mensaje_codificado}"
                
                st.markdown(f'<a href="{link_whatsapp}" target="_blank" class="button">📲 Enviar Notificación por WhatsApp</a>', unsafe_allow_html=True)
                st.caption("Se abrirá una nueva pestaña con el mensaje listo para ser enviado.")
                
                st.markdown("""
                <style>
                .button {
                    display: inline-block;
                    padding: 0.75rem 1.25rem;
                    border-radius: 0.5rem;
                    background-color: #25D366;
                    color: white;
                    text-align: center;
                    text-decoration: none;
                    font-weight: bold;
                    width: 100%;
                    box-sizing: border-box;
                }
                .button:hover {
                    background-color: #128C7E;
                }
                </style>
                """, unsafe_allow_html=True)
