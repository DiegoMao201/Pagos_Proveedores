# pages/3_💵_Planificador_de_Pagos.py
import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime
import uuid
import gspread

# Se importa directamente desde el archivo que ya tienes en tu proyecto
from common.utils import connect_to_google_sheets, load_data_from_gsheet

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(layout="wide", page_title="Planificador de Pagos")

# --- FUNCIONES AUXILIARES ---

def to_excel(df: pd.DataFrame) -> bytes:
    """Convierte un DataFrame a un archivo Excel en memoria."""
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

        # 1. Guardar el resumen del lote en la hoja de historial
        historial_ws = spreadsheet.worksheet("Historial_Lotes_Pago")
        # Asegura que el orden de los valores coincida con las columnas
        valores_fila = [lote_info.get(col) for col in historial_ws.row_values(1)]
        historial_ws.append_row(valores_fila)

        # 2. Actualizar el estado de las facturas en la hoja principal
        reporte_ws = spreadsheet.worksheet("ReporteConsolidado_Activo")

        # IDs únicos de las facturas a actualizar
        ids_a_actualizar = facturas_seleccionadas['id_factura_unico'].tolist()

        # Encontrar las filas correspondientes a estos IDs
        celdas_ids = reporte_ws.findall(r'{}'.format('|'.join(map(re.escape, ids_a_actualizar))), in_column=reporte_ws.find('id_factura_unico').col)

        # Prepara las actualizaciones en lote para ser más eficiente
        updates = []
        col_estado_idx = reporte_ws.find('estado_factura').col
        col_lote_idx = reporte_ws.find('id_lote_pago').col

        for cell in celdas_ids:
            # Añadir actualización para la columna 'estado_factura'
            updates.append({
                'range': f'{gspread.utils.rowcol_to_a1(cell.row, col_estado_idx)}',
                'values': [['En Lote de Pago']]
            })
            # Añadir actualización para la columna 'id_lote_pago'
            updates.append({
                'range': f'{gspread.utils.rowcol_to_a1(cell.row, col_lote_idx)}',
                'values': [[lote_info['id_lote']]]
            })

        # Ejecuta todas las actualizaciones en una sola llamada a la API
        if updates:
            reporte_ws.batch_update(updates)

        return True, None
    except Exception as e:
        return False, str(e)


# --- INICIO DE LA APLICACIÓN ---
st.title("💵 Centro de Operación y Planificación de Pagos")
st.markdown("""
Aquí puedes simular y construir lotes de pago. Selecciona las facturas **pendientes de pago**
para calcular totales, ahorros y exportar tu plan para aprobación.
""")

# --- Carga de Datos ---
# Se utiliza el caché de la función para eficiencia
gs_client = connect_to_google_sheets()
df_full = load_data_from_gsheet(gs_client)

if df_full.empty:
    st.warning("No hay datos disponibles. Por favor, ejecuta una sincronización en el 'Dashboard General'.")
    st.stop()

# --- CAMBIO CLAVE: Pre-procesamiento y filtrado inicial ---
# Rellenar 'estado_factura' vacío con 'Pendiente' por defecto
if 'estado_factura' not in df_full.columns:
    df_full['estado_factura'] = 'Pendiente'
else:
    df_full['estado_factura'] = df_full['estado_factura'].replace('', 'Pendiente')

# Crear un ID único si no existe, basado en proveedor, factura y valor.
# Esto es VITAL para poder actualizar la fila correcta después.
df_full['id_factura_unico'] = df_full.apply(
    lambda row: f"{row['nombre_proveedor']}-{row['num_factura']}-{row['valor_total_erp']}",
    axis=1
)

# Filtrar solo facturas PENDIENTES. Esta es la lógica principal.
df_pendientes = df_full[df_full['estado_factura'] == 'Pendiente'].copy()
# Filtrar facturas que no sean Notas de Crédito
df_pendientes = df_pendientes[df_pendientes['valor_total_erp'] >= 0]


# --- Filtros en la barra lateral ---
st.sidebar.header("Filtros del Plan")
proveedores_lista = sorted(df_pendientes['nombre_proveedor'].dropna().unique().tolist())
selected_suppliers = st.sidebar.multiselect("Filtrar por Proveedor:", proveedores_lista)

estado_pago_lista = df_pendientes['estado_pago'].unique().tolist()
# Por defecto se sugieren las que están vigentes o prontas a vencer.
default_status = [s for s in ["🟢 Vigente", "🟠 Por Vencer (7 días)"] if s in estado_pago_lista]
selected_status = st.sidebar.multiselect("Filtrar por Estado de Pago:", estado_pago_lista, default=default_status)

# Aplicar filtros
df_filtrado = df_pendientes.copy()
if selected_suppliers:
    df_filtrado = df_filtrado[df_filtrado['nombre_proveedor'].isin(selected_suppliers)]
if selected_status:
    df_filtrado = df_filtrado[df_filtrado['estado_pago'].isin(selected_status)]


# --- Tabla Interactiva para Selección de Pagos ---
df_filtrado.insert(0, "seleccionar", False)

st.subheader("Selecciona las Facturas para tu Plan de Pago")
if df_filtrado.empty:
    st.info("No hay facturas pendientes que coincidan con los filtros actuales.")
else:
    edited_df = st.data_editor(
        df_filtrado,
        key="data_editor_pagos",
        use_container_width=True,
        hide_index=True,
        column_config={
            "seleccionar": st.column_config.CheckboxColumn(required=True),
            "valor_total_erp": st.column_config.NumberColumn("Valor Original", format="$ %d"),
            "valor_con_descuento": st.column_config.NumberColumn("Valor a Pagar", format="$ %d"),
            "valor_descuento": st.column_config.NumberColumn("Ahorro", format="$ %d"),
            "fecha_limite_descuento": st.column_config.DateColumn("Límite Descuento", format="YYYY-MM-DD"),
            "dias_para_vencer": st.column_config.NumberColumn("Días Vence", format="%d días"),
        },
        # Deshabilita la edición de todas las columnas excepto 'seleccionar'
        disabled=[col for col in df_filtrado.columns if col != 'seleccionar']
    )

    # Filtrar las filas seleccionadas
    selected_rows = edited_df[edited_df['seleccionar']]
    st.divider()

    # --- Resumen del Lote de Pago (Reactivo) ---
    st.header("📊 Resumen del Plan de Pago")
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
        c4.metric("💰 TOTAL A PAGAR", f"${total_a_pagar:,.0f}")

        st.markdown("### Detalle del Plan de Pago Propuesto")
        # Columnas relevantes para la vista de resumen
        cols_to_show = [
            'nombre_proveedor', 'num_factura', 'valor_total_erp', 'estado_descuento',
            'valor_descuento', 'valor_con_descuento', 'fecha_limite_descuento', 'estado_pago'
        ]
        st.dataframe(selected_rows[cols_to_show], use_container_width=True, hide_index=True)

        st.divider()

        # --- SECCIÓN DE ACCIONES ---
        st.subheader("🚀 Acciones del Lote de Pago")
        col1, col2 = st.columns([1, 2])

        with col1:
            if st.button("✅ Confirmar y Generar Lote", type="primary", use_container_width=True):
                with st.spinner("Procesando y guardando el lote de pago..."):
                    # Crear la información del lote
                    id_lote = f"LOTE-{uuid.uuid4().hex[:8].upper()}"
                    lote_info = {
                        "id_lote": id_lote,
                        "fecha_creacion": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "num_facturas": num_facturas,
                        "valor_original_total": total_original,
                        "ahorro_total_lote": ahorro_total,
                        "total_pagado_lote": total_a_pagar,
                        "creado_por": "Gerente (App)", # Puedes hacerlo dinámico
                        "estado_lote": "Pendiente de Pago en Tesorería"
                    }

                    # Guardar en Google Sheets
                    success, error_msg = guardar_lote_en_gsheets(gs_client, lote_info, selected_rows)

                    if success:
                        st.success(f"¡Éxito! Lote de pago `{id_lote}` generado y guardado.")
                        st.info("Las facturas seleccionadas ya no aparecerán como pendientes. Actualiza la página para ver los cambios.")
                        st.balloons()
                        # Limpiar selección para evitar re-envíos
                        st.session_state.data_editor_pagos = None
                    else:
                        st.error(f"Error al guardar el lote: {error_msg}")

        with col2:
            st.markdown("**Notificar a Tesorería por WhatsApp**")
            numero_tesoreria = st.text_input("Número de WhatsApp de Tesorería (ej: +573001234567)", key="whatsapp_num")

            if st.button("📲 Enviar Notificación a Tesorería", use_container_width=True):
                if numero_tesoreria and not selected_rows.empty:
                    mensaje = (
                        f"¡Hola! 👋 Se ha creado un nuevo lote de pago para revisión en la plataforma.\n\n"
                        f"🔹 *Total a Pagar:* ${total_a_pagar:,.0f}\n"
                        f"🔹 *Facturas:* {num_facturas}\n"
                        f"🔹 *Ahorro Obtenido:* ${ahorro_total:,.0f}\n\n"
                        f"Por favor, ingresa al 'Historial de Pagos' para revisar el detalle y descargar el informe."
                    )
                    # La URL debe codificarse para que funcione en un navegador
                    link_whatsapp = f"https://wa.me/{numero_tesoreria.replace('+', '')}?text={mensaje.replace(' ', '%20').replace('á', '%C3%A1').replace('é', '%C3%A9').replace('í', '%C3%AD').replace('ó', '%C3%B3').replace('ú', '%C3%BA').replace('ñ', '%C3%B1')}"
                    st.markdown(f'<a href="{link_whatsapp}" target="_blank">Click aquí para enviar el mensaje por WhatsApp</a>', unsafe_allow_html=True)
                    st.info("Se abrirá una nueva pestaña con el mensaje listo para ser enviado.")
                else:
                    st.warning("Por favor, ingresa un número de teléfono y selecciona facturas para generar la notificación.")
