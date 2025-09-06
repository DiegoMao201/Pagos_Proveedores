# pages/2_🏢_Análisis_de_Proveedores.py
import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
from common.utils import load_data_from_gsheet, connect_to_google_sheets, COLOMBIA_TZ

st.set_page_config(layout="wide", page_title="Análisis de Proveedores")

st.title("🏢 Centro de Análisis de Proveedores")
st.markdown("Selecciona uno o más proveedores para analizar su comportamiento histórico, deudas y facturas.")

# --- Carga de Datos ---
gs_client = connect_to_google_sheets()
df = load_data_from_gsheet(gs_client)

if df.empty:
    st.warning("No hay datos disponibles. Por favor, ejecuta una sincronización en el 'Dashboard General'.")
    st.stop()

# --- Barra Lateral de Filtros ---
st.sidebar.header("Filtros de Análisis")
proveedores_lista = sorted(df['nombre_proveedor'].dropna().unique().tolist())
selected_supplier = st.sidebar.selectbox("Selecciona un Proveedor:", proveedores_lista)

# Filtrar el DataFrame principal por el proveedor seleccionado
supplier_df = df[df['nombre_proveedor'] == selected_supplier].copy()

if supplier_df.empty:
    st.info(f"No se encontraron datos para el proveedor '{selected_supplier}'.")
    st.stop()

# --- Métricas Clave del Proveedor ---
st.header(f"Resumen para: **{selected_supplier}**")

total_deuda = supplier_df['valor_total_erp'].sum()
monto_vencido = supplier_df[supplier_df['estado_pago'] == '🔴 Vencida']['valor_total_erp'].sum()
num_facturas = len(supplier_df)
avg_valor_factura = supplier_df['valor_total_erp'].mean()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Deuda Total", f"${total_deuda:,.2f}")
c2.metric("Monto Vencido", f"${monto_vencido:,.2f}")
c3.metric("Nº Facturas Registradas", f"{num_facturas}")
c4.metric("Valor Promedio Factura", f"${avg_valor_factura:,.2f}")

st.divider()

# --- Visualizaciones ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Distribución de Estados de Pago")
    pago_summary = supplier_df.groupby('estado_pago').agg(
        numero_facturas=('num_factura', 'count')
    ).reset_index()

    pie_chart = alt.Chart(pago_summary).mark_arc(innerRadius=50).encode(
        theta=alt.Theta(field="numero_facturas", type="quantitative"),
        color=alt.Color(field="estado_pago", type="nominal", title="Estado"),
        tooltip=['estado_pago', 'numero_facturas']
    ).properties(title="Facturas por Estado de Pago")
    st.altair_chart(pie_chart, use_container_width=True)

with col2:
    st.subheader("Historial de Facturación")
    # Asegurarse que la fecha no es nula para el gráfico
    chart_df = supplier_df.dropna(subset=['fecha_emision_erp']).copy()
    chart = alt.Chart(chart_df).mark_line(point=True).encode(
        x=alt.X('fecha_emision_erp:T', title='Fecha de Emisión'),
        y=alt.Y('valor_total_erp:Q', title='Valor Factura ($)'),
        tooltip=[
            alt.Tooltip('fecha_emision_erp:T', title='Emitida'),
            alt.Tooltip('num_factura:N', title='Factura'),
            alt.Tooltip('valor_total_erp:Q', title='Valor', format='$,.2f')
        ]
    ).properties(title="Valor de Facturas a lo Largo del Tiempo").interactive()
    st.altair_chart(chart, use_container_width=True)

# --- Tabla de Datos Detallada ---
st.divider()
st.subheader("Detalle de Facturas")
display_cols = [
    'num_factura', 'fecha_emision_erp', 'fecha_vencimiento_erp',
    'valor_total_erp', 'estado_pago', 'dias_para_vencer',
    'estado_conciliacion', 'estado_descuento'
]
st.dataframe(
    supplier_df[display_cols],
    use_container_width=True,
    hide_index=True,
    column_config={
        "valor_total_erp": st.column_config.NumberColumn("Valor ERP", format="$ %,.2f"),
        "fecha_emision_erp": st.column_config.DateColumn("Emitida", format="YYYY-MM-DD"),
        "fecha_vencimiento_erp": st.column_config.DateColumn("Vence", format="YYYY-MM-DD"),
    }
)
