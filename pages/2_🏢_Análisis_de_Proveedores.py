# pages/2_🏢_Análisis_de_Proveedores.py
# -*- coding: utf-8 -*-
"""
Módulo de Análisis Inteligente de Proveedores (Versión 3.5 - Corregido y Sincronizado).

Esta página se conecta con el archivo de utilidades para realizar un análisis
profundo de la cartera por proveedor o de forma consolidada. Se ha corregido
la indentación y se ha hecho más robusto ante la posible falta de columnas.
"""

import streamlit as st
import pandas as pd
import altair as alt
import io
from datetime import datetime
from common.utils import load_data_from_gsheet, connect_to_google_sheets

# --- 1. CONFIGURACIÓN DE LA PÁGINA ---
st.set_page_config(
    layout="wide",
    page_title="Análisis Inteligente de Proveedores",
    page_icon="🏢"
)

# --- FUNCIÓN DE UTILIDAD PARA DESCARGA DE EXCEL ---
@st.cache_data
def to_excel(df: pd.DataFrame) -> bytes:
    """Convierte un DataFrame a un archivo Excel en memoria, manejando las zonas horarias."""
    output = io.BytesIO()
    df_export = df.copy()

    # Itera sobre cada columna para remover la información de zona horaria
    for col in df_export.select_dtypes(include=['datetimetz']).columns:
        df_export[col] = df_export[col].dt.tz_localize(None)

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_export.to_excel(writer, index=False, sheet_name='DetalleProveedor')
        worksheet = writer.sheets['DetalleProveedor']
        # Auto-ajusta el ancho de las columnas en el Excel generado.
        for idx, col in enumerate(df_export):
            series = df_export[col]
            max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 2
            worksheet.set_column(idx, idx, max_len)
            
    return output.getvalue()

# --- 2. CARGA DE DATOS ---
@st.cache_data(ttl=600, show_spinner="Cargando datos para análisis...")
def get_master_data():
    """Conecta y carga los datos consolidados usando la función robusta de utils.py."""
    gs_client = connect_to_google_sheets()
    if gs_client:
        return load_data_from_gsheet(gs_client)
    return pd.DataFrame()

master_df = get_master_data()

if master_df.empty:
    st.warning("🚨 No se pudieron cargar datos para el análisis. Revisa la hoja de Google Sheets.")
    st.stop()

# --- 3. BARRA LATERAL Y FILTRO INTELIGENTE ---
st.sidebar.header("Filtros de Análisis 🔎")

# Se calculan los proveedores que tienen deuda real.
proveedores_con_deuda = master_df.groupby('nombre_proveedor')['valor_total_erp'].sum()
proveedores_activos = proveedores_con_deuda[proveedores_con_deuda > 0].index.tolist()
proveedores_lista_filtrada = sorted(proveedores_activos)

# Se añade la opción de consolidado
opciones_filtro = ["TODOS (Vista Consolidada)"] + proveedores_lista_filtrada

# Selección del proveedor
selected_supplier = st.sidebar.selectbox(
    "Selecciona un Proveedor:",
    opciones_filtro
)

# --- 4. LÓGICA DE FILTRADO Y TÍTULO DINÁMICO ---
if selected_supplier == "TODOS (Vista Consolidada)":
    supplier_df = master_df[master_df['nombre_proveedor'].isin(proveedores_lista_filtrada)].copy()
    titulo_pagina = "🏢 Centro de Análisis: Consolidado de Proveedores"
    nombre_archivo = f"Reporte_Consolidado_{datetime.now().strftime('%Y%m%d')}.xlsx"
else:
    supplier_df = master_df[master_df['nombre_proveedor'] == selected_supplier].copy()
    titulo_pagina = f"🏢 Centro de Análisis: {selected_supplier}"
    nombre_archivo = f"Reporte_{selected_supplier.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.xlsx"

st.title(titulo_pagina)

if supplier_df.empty:
    st.info("No se encontraron datos para la selección actual.")
    st.stop()

st.download_button(
    label="📥 Descargar Reporte en Excel",
    data=to_excel(supplier_df),
    file_name=nombre_archivo,
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# --- 5. ESTRUCTURA DE PESTAÑAS ---
tab1, tab2, tab3 = st.tabs(["📊 Resumen Ejecutivo", "💰 Diagnóstico Financiero Profundo", "📑 Detalle de Facturas"])

with tab1:
    st.header("🧠 Sugerencias Inteligentes para Tesorería")
    st.markdown("Acciones prioritarias para optimizar el flujo de caja y maximizar ahorros.")

    # DataFrames de análisis (se crean vacíos por si faltan columnas)
    descuentos_df = pd.DataFrame()
    vencidas_df = pd.DataFrame()

    if 'estado_descuento' in supplier_df.columns and 'fecha_limite_descuento' in supplier_df.columns:
        descuentos_df = supplier_df[
            (supplier_df['estado_descuento'] != 'No Aplica') & 
            (supplier_df['fecha_limite_descuento'].notna())
        ].sort_values('fecha_limite_descuento')

    if 'estado_pago' in supplier_df.columns:
        vencidas_df = supplier_df[
            supplier_df['estado_pago'] == '🔴 Vencida'
        ].sort_values('dias_para_vencer', ascending=True)

    s1, s2 = st.columns(2)
    with s1:
        with st.container(border=True):
            st.subheader("💰 Maximizar Ahorro")
            if not descuentos_df.empty and 'valor_descuento' in descuentos_df.columns:
                total_ahorro = descuentos_df['valor_descuento'].sum()
                st.success(f"Oportunidad de ahorrar **${int(total_ahorro):,}**! Pagar estas facturas antes de su fecha límite:")
                st.dataframe(
                    descuentos_df[['num_factura', 'valor_con_descuento', 'fecha_limite_descuento', 'valor_descuento']],
                    hide_index=True,
                    column_config={
                        "num_factura": "N° Factura",
                        "valor_con_descuento": st.column_config.NumberColumn("Pagar", format="$ %d"),
                        "fecha_limite_descuento": st.column_config.DateColumn("Fecha Límite", format="YYYY-MM-DD"),
                        "valor_descuento": st.column_config.NumberColumn("Ahorro", format="$ %d")
                    }
                )
            else:
                st.info("No hay descuentos por pronto pago activos para esta selección.")

    with s2:
        with st.container(border=True):
            st.subheader("⚠️ Acción Inmediata")
            if not vencidas_df.empty:
                st.error(f"Hay **{len(vencidas_df)} facturas vencidas**. Priorizar su pago para evitar problemas:")
                st.dataframe(
                    vencidas_df[['num_factura', 'valor_total_erp', 'fecha_vencimiento_erp', 'dias_para_vencer']],
                    hide_index=True,
                    column_config={
                        "num_factura": "N° Factura",
                        "valor_total_erp": st.column_config.NumberColumn("Valor", format="$ %d"),
                        "fecha_vencimiento_erp": st.column_config.DateColumn("Venció", format="YYYY-MM-DD"),
                        "dias_para_vencer": st.column_config.NumberColumn("Días Vencida")
                    }
                )
            else:
                st.success("¡Excelente! No hay facturas vencidas en esta selección.")
    
    st.divider()
    st.header("📊 Resumen Financiero y Operativo")

    total_deuda = supplier_df['valor_total_erp'].sum()
    monto_vencido = vencidas_df['valor_total_erp'].sum() if not vencidas_df.empty else 0
    descuento_potencial = descuentos_df['valor_descuento'].sum() if not descuentos_df.empty else 0

    avg_plazo = "N/A"
    if 'fecha_vencimiento_erp' in supplier_df.columns and 'fecha_emision_erp' in supplier_df.columns:
        plazos_validos = (supplier_df['fecha_vencimiento_erp'] - supplier_df['fecha_emision_erp']).dropna()
        if not plazos_validos.empty:
            avg_plazo = f"{plazos_validos.dt.days.mean():.0f}"

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("Deuda Total Actual", f"${int(total_deuda):,}", help="Suma de todas las facturas pendientes de pago.")
    kpi2.metric("Monto Vencido", f"${int(monto_vencido):,}", delta_color="inverse", help="Valor total de las facturas que ya pasaron su fecha de vencimiento.")
    kpi3.metric("Ahorro Potencial", f"${int(descuento_potencial):,}", delta_color="off", help="Suma de todos los descuentos por pronto pago disponibles.")
    kpi4.metric("Plazo Promedio (Días)", avg_plazo, help="Días promedio entre la emisión y el vencimiento de las facturas.")

with tab2:
    st.header("📈 Análisis de Antigüedad de Saldos (Aged Debt)")

    if 'dias_para_vencer' not in supplier_df.columns:
        st.warning("La columna 'dias_para_vencer' es necesaria para este análisis y no se encontró.")
    else:
        def categorize_age(days):
            if pd.isna(days): return "Sin Fecha"
            if days >= 0: return "1. Por Vencer"
            if days >= -30: return "2. Vencida (1-30 días)"
            if days >= -60: return "3. Vencida (31-60 días)"
            return "4. Vencida (+60 días)"

        supplier_df['categoria_antiguedad'] = supplier_df['dias_para_vencer'].apply(categorize_age)
        
        aging_summary = supplier_df.groupby('categoria_antiguedad').agg(
            valor_total=('valor_total_erp', 'sum'),
            numero_facturas=('num_factura', 'count')
        ).reset_index()

        total_deuda_tab2 = aging_summary['valor_total'].sum()
        st.markdown("Esta vista descompone la deuda total en bloques de tiempo para identificar riesgos.")
        
        kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
        
        monto_total_vencido = supplier_df[supplier_df['dias_para_vencer'] < 0]['valor_total_erp'].sum()
        porc_vencido = (monto_total_vencido / total_deuda_tab2 * 100) if total_deuda_tab2 > 0 else 0
        kpi_col1.metric("Porcentaje de Cartera Vencida", f"{porc_vencido:.1f}%")

        df_vencidas_calc = supplier_df[supplier_df['dias_para_vencer'] < 0]
        if not df_vencidas_calc.empty:
            avg_days_overdue = abs(df_vencidas_calc['dias_para_vencer']).mean()
            factura_critica = df_vencidas_calc.sort_values('dias_para_vencer').iloc[0]
            kpi_col2.metric("Días Promedio de Vencimiento", f"{avg_days_overdue:.0f} días")
            kpi_col3.metric("Factura más Crítica (N°)", f"{factura_critica.get('num_factura', 'N/A')}", help=f"Vencida hace {abs(int(factura_critica.get('dias_para_vencer', 0)))} días por ${int(factura_critica.get('valor_total_erp', 0)):,}")
        else:
            kpi_col2.metric("Días Promedio de Vencimiento", "0 días")
            kpi_col3.metric("Factura más Crítica (N°)", "N/A")

        chart = alt.Chart(aging_summary).mark_bar().encode(
            x=alt.X('valor_total:Q', title='Valor Total de la Deuda ($)', axis=alt.Axis(format='$,.0f')),
            y=alt.Y('categoria_antiguedad:N', title='Categoría de Antigüedad', sort='-x'),
            color=alt.Color('categoria_antiguedad:N', 
                legend=None,
                scale=alt.Scale(
                    domain=["1. Por Vencer", "2. Vencida (1-30 días)", "3. Vencida (31-60 días)", "4. Vencida (+60 días)", "Sin Fecha"],
                    range=['#2ECC71', '#F39C12', '#E67E22', '#C0392B', '#808080']
                )
            ),
            tooltip=['categoria_antiguedad', alt.Tooltip('valor_total', title='Valor Total', format='$,.0f'), 'numero_facturas']
        ).properties(title='Distribución de la Deuda por Antigüedad')
        
        text = chart.mark_text(align='left', baseline='middle', dx=3).encode(
            text=alt.condition(
                'datum.valor_total > 1000', 
                alt.Text('valor_total:Q', format='$,.1s'), 
                alt.value('')
            )
        )
        st.altair_chart((chart + text).interactive(), use_container_width=True)

with tab3:
    st.header("📑 Detalle Completo de Facturas")
    st.markdown("Explora, ordena y filtra todas las facturas registradas para esta selección.")
    
    display_cols = [
        'num_factura', 'fecha_emision_erp', 'fecha_vencimiento_erp',
        'valor_total_erp', 'estado_pago', 'dias_para_vencer',
        'estado_conciliacion', 'estado_descuento', 'valor_descuento', 'fecha_limite_descuento'
    ]
    if selected_supplier == "TODOS (Vista Consolidada)":
        display_cols.insert(1, 'nombre_proveedor')
    
    # Filtrar solo las columnas que realmente existen en el DataFrame
    existing_display_cols = [col for col in display_cols if col in supplier_df.columns]

    column_config_base = {
        "num_factura": "N° Factura",
        "nombre_proveedor": "Proveedor",
        "valor_total_erp": st.column_config.NumberColumn("Valor Original", format="$ %d"),
        "fecha_emision_erp": st.column_config.DateColumn("Emitida", format="YYYY-MM-DD"),
        "fecha_vencimiento_erp": st.column_config.DateColumn("Vence", format="YYYY-MM-DD"),
        "estado_pago": "Estado Cartera",
        "dias_para_vencer": st.column_config.ProgressColumn("Días para Vencer", format="%d días", min_value=-90, max_value=90),
        "estado_conciliacion": "Estado Conciliación",
        "estado_descuento": "Descuento",
        "valor_descuento": st.column_config.NumberColumn("Ahorro Potencial", format="$ %d"),
        "fecha_limite_descuento": st.column_config.DateColumn("Pagar Antes de", format="YYYY-MM-DD"),
    }
    
    st.dataframe(
        supplier_df[existing_display_cols],
        hide_index=True,
        column_config=column_config_base
    )
