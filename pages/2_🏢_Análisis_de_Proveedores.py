# pages/2_🏢_Análisis_de_Proveedores.py
import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, timedelta

# Asumimos que las funciones de conexión están en un directorio común.
# Si no es así, puedes copiar las funciones aquí directamente.
from common.utils import load_data_from_gsheet, connect_to_google_sheets, COLOMBIA_TZ

# --- 1. CONFIGURACIÓN DE LA PÁGINA ---
st.set_page_config(
    layout="wide",
    page_title="Análisis Inteligente de Proveedores",
    page_icon="🏢"
)

# --- 2. CARGA DE DATOS ---
# Usamos un decorador de cache para no recargar los datos en cada interacción.
@st.cache_data(ttl=600)
def get_master_data():
    """Conecta y carga los datos consolidados desde Google Sheets."""
    gs_client = connect_to_google_sheets()
    df = load_data_from_gsheet(gs_client, sheet_name="ReporteConsolidado_Activo")
    return df

master_df = get_master_data()

if master_df.empty:
    st.warning("🚨 No se pudieron cargar los datos. Asegúrate de haber ejecutado una sincronización en la página principal 'Dashboard General'.")
    st.stop()

# --- 3. BARRA LATERAL Y FILTROS ---
st.sidebar.header("Filtros de Análisis 🔎")
proveedores_lista = sorted(master_df['nombre_proveedor'].dropna().unique().tolist())

# Establecer "PINTUCO COLOMBIA S.A.S" como default si existe
default_provider = "PINTUCO COLOMBIA S.A.S"
if default_provider not in proveedores_lista:
    default_provider_index = 0
else:
    default_provider_index = proveedores_lista.index(default_provider)

selected_supplier = st.sidebar.selectbox(
    "Selecciona un Proveedor:",
    proveedores_lista,
    index=default_provider_index
)

# Filtrar el DataFrame para el proveedor seleccionado
supplier_df = master_df[master_df['nombre_proveedor'] == selected_supplier].copy()

# --- 4. TÍTULO PRINCIPAL Y VERIFICACIÓN DE DATOS ---
st.title(f"🏢 Centro de Análisis: {selected_supplier}")
st.markdown("Análisis 360° del comportamiento financiero, oportunidades de ahorro y estado de cuenta del proveedor.")

if supplier_df.empty:
    st.info(f"No se encontraron datos para el proveedor '{selected_supplier}'.")
    st.stop()

# --- 5. SECCIÓN DE SUGERENCIAS INTELIGENTES PARA TESORERÍA ---
st.header("🧠 Sugerencias Inteligentes para Tesorería")
st.markdown("Acciones prioritarias para optimizar el flujo de caja y maximizar ahorros.")

# Filtrar facturas con descuentos disponibles y ordenarlas por fecha límite
descuentos_df = supplier_df[
    (supplier_df['estado_descuento'] != 'No Aplica') &
    (supplier_df['fecha_limite_descuento'].notna())
].sort_values('fecha_limite_descuento')

# Filtrar facturas vencidas y ordenarlas por las más antiguas
vencidas_df = supplier_df[supplier_df['estado_pago'] == '🔴 Vencida'].sort_values('dias_para_vencer', ascending=True)

s1, s2 = st.columns(2)

with s1:
    with st.container(border=True):
        st.subheader("💰 Maximizar Ahorro")
        if not descuentos_df.empty:
            total_ahorro = descuentos_df['valor_descuento'].sum()
            st.success(f"¡Oportunidad de ahorrar **${total_ahorro:,.0f}**! Paga estas facturas antes de su fecha límite:")
            st.dataframe(
                descuentos_df[['num_factura', 'valor_con_descuento', 'fecha_limite_descuento', 'valor_descuento']],
                hide_index=True, use_container_width=True,
                column_config={
                    "num_factura": "N° Factura",
                    "valor_con_descuento": st.column_config.NumberColumn("Pagar", format="$ {:,.0f}"),
                    "fecha_limite_descuento": st.column_config.DateColumn("Fecha Límite", format="YYYY-MM-DD"),
                    "valor_descuento": st.column_config.NumberColumn("Ahorro", format="$ {:,.0f}")
                }
            )
        else:
            st.info("No hay descuentos por pronto pago activos para este proveedor.")

with s2:
    with st.container(border=True):
        st.subheader("⚠️ Acción Inmediata")
        if not vencidas_df.empty:
            st.error(f"Hay **{len(vencidas_df)} facturas vencidas**. Priorizar su pago para evitar problemas:")
            st.dataframe(
                vencidas_df[['num_factura', 'valor_total_erp', 'fecha_vencimiento_erp', 'dias_para_vencer']],
                hide_index=True, use_container_width=True,
                column_config={
                    "num_factura": "N° Factura",
                    "valor_total_erp": st.column_config.NumberColumn("Valor", format="$ {:,.0f}"),
                    "fecha_vencimiento_erp": st.column_config.DateColumn("Venció", format="YYYY-MM-DD"),
                    "dias_para_vencer": st.column_config.NumberColumn("Días Vencida")
                }
            )
        else:
            st.success("¡Excelente! No hay facturas vencidas con este proveedor.")
st.divider()


# --- 6. MÉTRICAS Y KPIs CLAVE ---
st.header("📊 Resumen Financiero y Operativo")

# Cálculos
total_deuda = supplier_df['valor_total_erp'].sum()
monto_vencido = vencidas_df['valor_total_erp'].sum()
num_facturas = len(supplier_df)
avg_valor_factura = supplier_df['valor_total_erp'].mean()
descuento_potencial = descuentos_df['valor_descuento'].sum()
# Calcular días promedio de pago (aproximación si no hay fecha de pago real)
supplier_df['plazo_dias'] = (supplier_df['fecha_vencimiento_erp'] - supplier_df['fecha_emision_erp']).dt.days
avg_plazo = supplier_df['plazo_dias'].mean()


kpi1, kpi2, kpi3, kpi4 = st.columns(4)
kpi1.metric("Deuda Total Actual", f"${total_deuda:,.0f}", help="Suma de todas las facturas pendientes de pago en el ERP.")
kpi2.metric("Monto Vencido", f"${monto_vencido:,.0f}", delta_color="inverse", help="Valor total de las facturas que ya pasaron su fecha de vencimiento.")
kpi3.metric("Ahorro Potencial", f"${descuento_potencial:,.0f}", delta_color="off", help="Suma de todos los descuentos por pronto pago disponibles.")
kpi4.metric("Plazo Promedio (Días)", f"{avg_plazo:.0f}" if pd.notna(avg_plazo) else "N/A", help="Días promedio entre la emisión y el vencimiento de las facturas.")
st.divider()

# --- 7. VISUALIZACIONES Y ANÁLISIS HISTÓRICO ---
st.header("📈 Análisis Histórico y Tendencias")
v1, v2 = st.columns([1, 2])

with v1:
    st.subheader("Estado de la Cartera")
    pago_summary = supplier_df.groupby('estado_pago').agg(
        numero_facturas=('num_factura', 'count'),
        valor_total=('valor_total_erp', 'sum')
    ).reset_index()

    pie_chart = alt.Chart(pago_summary).mark_arc(innerRadius=60).encode(
        theta=alt.Theta(field="valor_total", type="quantitative", title="Valor Total"),
        color=alt.Color(field="estado_pago", type="nominal", title="Estado",
                        scale=alt.Scale(domain=['🔴 Vencida', '🟠 Por Vencer (7 días)', '🟢 Vigente'],
                                        range=['#E74C3C', '#F39C12', '#2ECC71'])),
        tooltip=[
            alt.Tooltip('estado_pago', title='Estado'),
            alt.Tooltip('numero_facturas', title='N° Facturas'),
            alt.Tooltip('valor_total', title='Valor Total', format='$,.0f')
        ]
    ).properties(title="Distribución de la Deuda por Estado")
    st.altair_chart(pie_chart, use_container_width=True)

with v2:
    st.subheader("Facturación Mensual")
    chart_df = supplier_df.dropna(subset=['fecha_emision_erp']).copy()
    chart_df['mes_emision'] = chart_df['fecha_emision_erp'].dt.to_period('M').astype(str)

    monthly_summary = chart_df.groupby('mes_emision').agg(
        total_facturado=('valor_total_erp', 'sum'),
        numero_facturas=('num_factura', 'count')
    ).reset_index()

    base = alt.Chart(monthly_summary).encode(
        x=alt.X('mes_emision:O', title='Mes de Emisión', axis=alt.Axis(labelAngle=-45))
    )
    bars = base.mark_bar().encode(
        y=alt.Y('total_facturado:Q', title='Total Facturado ($)', axis=alt.Axis(format='$,.0f')),
        tooltip=[
            alt.Tooltip('mes_emision', title='Mes'),
            alt.Tooltip('total_facturado', title='Total Facturado', format='$,.0f'),
            alt.Tooltip('numero_facturas', title='N° Facturas')
        ]
    )
    st.altair_chart((bars).interactive(), use_container_width=True)


# --- 8. TABLA DE DATOS DETALLADA ---
st.divider()
with st.expander("Ver todas las facturas del proveedor", expanded=False):
    st.subheader("📑 Detalle Completo de Facturas")
    display_cols = [
        'num_factura', 'fecha_emision_erp', 'fecha_vencimiento_erp',
        'valor_total_erp', 'estado_pago', 'dias_para_vencer',
        'estado_conciliacion', 'estado_descuento', 'valor_descuento', 'fecha_limite_descuento'
    ]
    st.dataframe(
        supplier_df[display_cols],
        use_container_width=True,
        hide_index=True,
        column_config={
            "num_factura": st.column_config.TextColumn("N° Factura"),
            "valor_total_erp": st.column_config.NumberColumn("Valor Original", format="$ {:,.0f}"),
            "fecha_emision_erp": st.column_config.DateColumn("Emitida", format="YYYY-MM-DD"),
            "fecha_vencimiento_erp": st.column_config.DateColumn("Vence", format="YYYY-MM-DD"),
            "estado_pago": st.column_config.TextColumn("Estado Cartera"),
            "dias_para_vencer": st.column_config.ProgressColumn("Días para Vencer", format="%d días", min_value=-90, max_value=90),
            "estado_conciliacion": st.column_config.TextColumn("Estado Conciliación"),
            "estado_descuento": st.column_config.TextColumn("Descuento"),
            "valor_descuento": st.column_config.NumberColumn("Ahorro Potencial", format="$ {:,.0f}"),
            "fecha_limite_descuento": st.column_config.DateColumn("Pagar Antes de", format="YYYY-MM-DD"),
        }
    )
