# pages/3_💵_Planificador_de_Pagos.py
import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime
from common.utils import load_data_from_gsheet, connect_to_google_sheets

st.set_page_config(layout="wide", page_title="Planificador de Pagos")

st.title("💵 Centro de Operación y Planificación de Pagos")
st.markdown("""
Aquí puedes simular y construir lotes de pago. Selecciona las facturas que deseas pagar
para calcular automáticamente los totales, ahorros por descuento y exportar tu plan.
""")

# --- Función para convertir DF a Excel en memoria ---
def to_excel(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='PlanDePago')
    processed_data = output.getvalue()
    return processed_data

# --- Carga de Datos ---
gs_client = connect_to_google_sheets()
df = load_data_from_gsheet(gs_client)

if df.empty:
    st.warning("No hay datos disponibles. Por favor, ejecuta una sincronización en el 'Dashboard General'.")
    st.stop()

# --- Filtros en la barra lateral ---
st.sidebar.header("Filtros del Plan")
proveedores_lista = sorted(df['nombre_proveedor'].dropna().unique().tolist())
selected_suppliers = st.sidebar.multiselect("Filtrar por Proveedor:", proveedores_lista)

estado_pago_lista = df['estado_pago'].unique().tolist()
selected_status = st.sidebar.multiselect("Filtrar por Estado de Pago:", estado_pago_lista, default=["🟢 Vigente", "🟠 Por Vencer (7 días)"])

# Aplicar filtros
if selected_suppliers:
    df = df[df['nombre_proveedor'].isin(selected_suppliers)]
if selected_status:
    df = df[df['estado_pago'].isin(selected_status)]

# --- Tabla Interactiva para Selección de Pagos ---
# Se añade una columna 'seleccionar' para el widget interactivo
df_selection = df.copy()
df_selection.insert(0, "seleccionar", False)

st.subheader("Selecciona las Facturas para tu Plan de Pago")
edited_df = st.data_editor(
    df_selection,
    use_container_width=True,
    hide_index=True,
    column_config={
        "seleccionar": st.column_config.CheckboxColumn(required=True),
        "valor_total_erp": st.column_config.NumberColumn("Valor Original", format="$ %,.2f"),
        "valor_con_descuento": st.column_config.NumberColumn("Valor a Pagar", format="$ %,.2f"),
        "valor_descuento": st.column_config.NumberColumn("Ahorro", format="$ %,.2f"),
        "fecha_limite_descuento": st.column_config.DateColumn("Límite Descuento", format="YYYY-MM-DD"),
    },
    # Deshabilita la edición de todas las columnas excepto 'seleccionar'
    disabled=df.columns
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
    c2.metric("Valor Original Total", f"${total_original:,.2f}")
    c3.metric("Ahorro Total por Descuento", f"${ahorro_total:,.2f}", delta_color="off")
    c4.metric("💰 TOTAL A PAGAR", f"${total_a_pagar:,.2f}")

    st.markdown("### Detalle del Plan de Pago")
    st.dataframe(selected_rows.drop(columns=['seleccionar']), use_container_width=True, hide_index=True)

    # --- Botón de Descarga ---
    excel_file = to_excel(selected_rows.drop(columns=['seleccionar']))
    st.download_button(
        label="📥 Descargar Plan de Pago en Excel",
        data=excel_file,
        file_name=f"plan_de_pago_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
