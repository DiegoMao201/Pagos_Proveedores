# -*- coding: utf-8 -*-
"""
Módulo de Análisis Gerencial de Proveedores (Versión 4.0 - Dashboard Estratégico).

Este módulo ha sido rediseñado para ofrecer un tablero de control gerencial de alto nivel,
enfocado en KPIs estratégicos, análisis comparativo de proveedores y una visión clara 
para la toma de decisiones.

Mejoras en v4.0:
- **Análisis Comparativo de Proveedores:** Se ha añadido una nueva pestaña "Análisis por Proveedor" 
  que permite clasificar y comparar proveedores según su deuda total, porcentaje de cartera vencida y 
  oportunidades de ahorro.
- **Visualización de Proveedores Críticos:** Se han incorporado gráficos de barras para identificar 
  rápidamente a los proveedores con mayor deuda y mayor monto vencido.
- **Visibilidad Mejorada:** Se ha añadido el nombre del proveedor en las tablas de "Ahorro" y 
  "Cartera Vencida" en el resumen ejecutivo, como fue solicitado, para mayor claridad en la vista consolidada.
- **Gráfico de Composición de Deuda:** Se ha añadido un gráfico de dona para visualizar la concentración 
  de la deuda entre los principales proveedores.
- **KPIs Estratégicos:** Se han introducido nuevos KPIs como el porcentaje de deuda por proveedor y 
  la tasa de cartera vencida individual, ofreciendo una visión más profunda de la salud de las relaciones comerciales.
- **Experiencia de Usuario:** Se ha refinado la interfaz y la presentación de datos para que sea más 
  intuitiva y visualmente impactante.
"""

# --- 0. IMPORTACIÓN DE LIBRERÍAS ---
import streamlit as st
import pandas as pd
import altair as alt
import io
from datetime import datetime
# Asumimos que este archivo existe en la ruta especificada.
from common.utils import load_data_from_gsheet, connect_to_google_sheets

# ======================================================================================
# --- INICIO DEL BLOQUE DE SEGURIDAD ---
# ======================================================================================

# 1. Se asegura de que la variable de sesión exista para evitar errores.
if 'password_correct' not in st.session_state:
    st.session_state['password_correct'] = False

# 2. Verifica si la contraseña es correcta.
if not st.session_state["password_correct"]:
    st.error("🔒 Debes iniciar sesión para acceder a esta página.")
    st.info("Por favor, ve a la página principal 'Dashboard General' para ingresar la contraseña.")
    st.stop()

# --- FIN DEL BLOQUE DE SEGURIDAD ---

# --- 1. CONFIGURACIÓN DE LA PÁGINA ---
st.set_page_config(
    layout="wide",
    page_title="Análisis Estratégico de Proveedores",
    page_icon="📊"
)

# --- FUNCIÓN DE UTILIDAD PARA DESCARGA DE EXCEL ---
@st.cache_data
def to_excel(df: pd.DataFrame) -> bytes:
    """Convierte un DataFrame a un archivo Excel en memoria, manejando las zonas horarias."""
    output = io.BytesIO()
    df_export = df.copy()
    for col in df_export.select_dtypes(include=['datetimetz']).columns:
        df_export[col] = df_export[col].dt.tz_localize(None)
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_export.to_excel(writer, index=False, sheet_name='DetalleProveedor')
        worksheet = writer.sheets['DetalleProveedor']
        for idx, col in enumerate(df_export):
            series = df_export[col]
            max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 2
            worksheet.set_column(idx, idx, max_len)
    return output.getvalue()

# --- 2. CARGA Y FILTRADO INICIAL DE DATOS ---
@st.cache_data(ttl=300, show_spinner="Cargando y sincronizando datos de cartera...")
def get_master_data():
    """Conecta, carga y filtra los datos para asegurar que solo se analiza la cartera pendiente."""
    gs_client = connect_to_google_sheets()
    if not gs_client:
        return pd.DataFrame()
    
    master_df = load_data_from_gsheet(gs_client)
    if master_df.empty:
        return pd.DataFrame()
        
    # **MEJORA CLAVE**: Filtrar solo facturas pendientes para un análisis real.
    if 'estado_factura' in master_df.columns:
        master_df = master_df[master_df['estado_factura'] == 'Pendiente'].copy()
        
    return master_df

master_df = get_master_data()

if master_df.empty:
    st.success("✅ ¡Excelente! No hay facturas pendientes de pago en el sistema.", icon="🎉")
    st.stop()

# --- 3. BARRA LATERAL Y FILTRO INTELIGENTE ---
st.sidebar.header("Filtros de Análisis 🔎")

proveedores_net_debt = master_df.groupby('nombre_proveedor')['valor_total_erp'].sum()
proveedores_activos = proveedores_net_debt[proveedores_net_debt != 0].index.tolist()
proveedores_lista_filtrada = sorted(proveedores_activos)

opciones_filtro = ["TODOS (Vista Consolidada)"] + proveedores_lista_filtrada
selected_supplier = st.sidebar.selectbox("Selecciona un Proveedor:", opciones_filtro)

# --- 4. LÓGICA DE FILTRADO Y TÍTULO DINÁMICO ---
if selected_supplier == "TODOS (Vista Consolidada)":
    supplier_df = master_df[master_df['nombre_proveedor'].isin(proveedores_lista_filtrada)].copy()
    titulo_pagina = "📊 Panel Estratégico: Consolidado de Cartera Pendiente"
    nombre_archivo = f"Reporte_Consolidado_Pendiente_{datetime.now().strftime('%Y%m%d')}.xlsx"
else:
    supplier_df = master_df[master_df['nombre_proveedor'] == selected_supplier].copy()
    titulo_pagina = f"📊 Panel Estratégico: {selected_supplier}"
    nombre_archivo = f"Reporte_{selected_supplier.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.xlsx"

st.title(titulo_pagina)

if supplier_df.empty:
    st.info("No se encontraron datos pendientes para la selección actual.")
    st.stop()

st.download_button(
    label="📥 Descargar Reporte en Excel",
    data=to_excel(supplier_df),
    file_name=nombre_archivo,
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# --- 5. ESTRUCTURA DE PESTAÑAS ---
tab1, tab_proveedor, tab2, tab3 = st.tabs(["💡 Resumen Ejecutivo", "🏆 Análisis por Proveedor", "💰 Diagnóstico Financiero", "📑 Detalle de Documentos"])

with tab1:
    st.header("Fotografía Financiera de la Cartera Pendiente")

    # --- Cálculos para KPIs ---
    facturas_df = supplier_df[supplier_df['valor_total_erp'] >= 0]
    notas_credito_df = supplier_df[supplier_df['valor_total_erp'] < 0]
    
    deuda_bruta = facturas_df['valor_total_erp'].sum()
    saldo_a_favor = abs(notas_credito_df['valor_total_erp'].sum())
    deuda_neta = deuda_bruta - saldo_a_favor
    
    # --- DataFrames de análisis para acciones ---
    vencidas_df = pd.DataFrame()
    if 'estado_pago' in facturas_df.columns:
        vencidas_df = facturas_df[facturas_df['estado_pago'] == '🔴 Vencida'].copy()
    if not vencidas_df.empty and 'dias_para_vencer' in vencidas_df.columns:
        vencidas_df = vencidas_df.sort_values(by='dias_para_vencer')

    monto_vencido = vencidas_df['valor_total_erp'].sum()
    porc_vencido = (monto_vencido / deuda_bruta * 100) if deuda_bruta > 0 else 0
    salud_cartera = 100 - porc_vencido

    with st.container(border=True):
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        kpi1.metric("Deuda Neta (Valor Real a Pagar)", f"${int(deuda_neta):,}", help="La diferencia entre la deuda bruta y el saldo a favor.")
        kpi2.metric("Deuda Bruta (Facturas)", f"${int(deuda_bruta):,}", help="Suma de todas las facturas pendientes de pago.")
        kpi3.metric("Saldo a Favor (Notas Crédito)", f"${int(saldo_a_favor):,}", help="Suma de todas las notas crédito pendientes de aplicar.")
        kpi4.metric("Salud de Cartera", f"{salud_cartera:.1f}%", f"-{porc_vencido:.1f}% Vencido", delta_color="inverse", help="Porcentaje de la cartera que está al día. Un número más alto es mejor.")
    
    st.divider()
    st.header("🧠 Acciones Clave y Oportunidades")

    # --- CORRECCIÓN DE KEYERROR Y MEJORA DE ROBUSTEZ ---
    descuentos_df = pd.DataFrame()
    if 'estado_descuento' in facturas_df.columns:
        descuentos_df = facturas_df[facturas_df['estado_descuento'] != 'No Aplica'].copy()
    if not descuentos_df.empty and 'fecha_limite_descuento' in descuentos_df.columns:
        descuentos_df = descuentos_df.sort_values(by='fecha_limite_descuento')

    s1, s2 = st.columns(2)
    with s1:
        with st.container(border=True):
            total_ahorro = descuentos_df['valor_descuento'].sum() if 'valor_descuento' in descuentos_df.columns else 0
            st.subheader(f"💰 Oportunidad de Ahorro: ${int(total_ahorro):,}")
            if not descuentos_df.empty and total_ahorro > 0:
                st.success(f"Pagar estas **{len(descuentos_df)} facturas** antes de su fecha límite para maximizar el ahorro:")
                
                # **MEJORA SOLICITADA**: Añadir columna de proveedor
                display_cols_ahorro = ['nombre_proveedor', 'num_factura', 'valor_con_descuento', 'fecha_limite_descuento', 'valor_descuento']
                
                st.dataframe(
                    descuentos_df[display_cols_ahorro],
                    hide_index=True, height=250,
                    column_config={
                        "nombre_proveedor": "Proveedor",
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
            st.subheader(f"⚠️ Riesgo: Cartera Vencida ${int(monto_vencido):,}")
            if not vencidas_df.empty:
                st.error(f"Hay **{len(vencidas_df)} facturas vencidas**. Priorizar su pago para evitar problemas de suministro y cargos:")
                
                # **MEJORA SOLICITADA**: Añadir columna de proveedor
                display_cols_vencidas = ['nombre_proveedor', 'num_factura', 'valor_total_erp', 'fecha_vencimiento_erp', 'dias_para_vencer']
                
                st.dataframe(
                    vencidas_df[display_cols_vencidas],
                    hide_index=True, height=250,
                    column_config={
                        "nombre_proveedor": "Proveedor",
                        "num_factura": "N° Factura",
                        "valor_total_erp": st.column_config.NumberColumn("Valor", format="$ %d"),
                        "fecha_vencimiento_erp": st.column_config.DateColumn("Venció", format="YYYY-MM-DD"),
                        "dias_para_vencer": st.column_config.NumberColumn("Días Vencida")
                    }
                )
            else:
                st.success("¡Felicitaciones! No hay facturas vencidas en esta selección.")

with tab_proveedor:
    st.header("🏆 Ranking y Análisis Comparativo de Proveedores")
    st.markdown("Utilice esta vista para evaluar el estado de la cartera con cada proveedor, identificar riesgos y oportunidades.")

    if selected_supplier == "TODOS (Vista Consolidada)":
        # --- Lógica de cálculo para el ranking de proveedores ---
        # 1. Agrupar por proveedor
        proveedor_summary = master_df.groupby('nombre_proveedor').agg(
            deuda_total=('valor_total_erp', 'sum'),
            numero_documentos=('num_factura', 'count')
        ).reset_index()

        # 2. Calcular monto vencido por proveedor
        vencido_por_proveedor = master_df[master_df['estado_pago'] == '🔴 Vencida'].groupby('nombre_proveedor')['valor_total_erp'].sum().reset_index()
        vencido_por_proveedor.rename(columns={'valor_total_erp': 'monto_vencido'}, inplace=True)
        
        # 3. Calcular ahorro potencial por proveedor
        ahorro_por_proveedor = master_df[master_df['estado_descuento'] != 'No Aplica'].groupby('nombre_proveedor')['valor_descuento'].sum().reset_index()
        ahorro_por_proveedor.rename(columns={'valor_descuento': 'ahorro_potencial'}, inplace=True)
        
        # 4. Unir los datos
        proveedor_summary = pd.merge(proveedor_summary, vencido_por_proveedor, on='nombre_proveedor', how='left')
        proveedor_summary = pd.merge(proveedor_summary, ahorro_por_proveedor, on='nombre_proveedor', how='left')
        proveedor_summary.fillna(0, inplace=True)

        # 5. Calcular KPIs finales
        deuda_bruta_total = master_df[master_df['valor_total_erp'] >= 0]['valor_total_erp'].sum()
        proveedor_summary['deuda_bruta_proveedor'] = master_df[master_df['valor_total_erp'] >= 0].groupby('nombre_proveedor')['valor_total_erp'].sum().values
        
        proveedor_summary['%_deuda_total'] = (proveedor_summary['deuda_bruta_proveedor'] / deuda_bruta_total * 100)
        proveedor_summary['%_vencido'] = (proveedor_summary['monto_vencido'] / proveedor_summary['deuda_bruta_proveedor'] * 100).fillna(0)
        
        # 6. Mostrar la tabla de ranking
        st.dataframe(
            proveedor_summary[['nombre_proveedor', 'deuda_total', '%_deuda_total', 'monto_vencido', '%_vencido', 'ahorro_potencial', 'numero_documentos']].sort_values(by='deuda_total', ascending=False),
            use_container_width=True,
            hide_index=True,
            column_config={
                "nombre_proveedor": "Proveedor",
                "deuda_total": st.column_config.NumberColumn("Deuda Neta", format="$ %d"),
                "%_deuda_total": st.column_config.ProgressColumn("% Deuda Total", format="%.1f%%", min_value=0, max_value=100),
                "monto_vencido": st.column_config.NumberColumn("Monto Vencido", format="$ %d"),
                "%_vencido": st.column_config.ProgressColumn("% Vencido", format="%.1f%%", min_value=0, max_value=100),
                "ahorro_potencial": st.column_config.NumberColumn("Ahorro Potencial", format="$ %d"),
                "numero_documentos": "N° Docs"
            }
        )
        
        st.divider()
        
        # --- Visualizaciones de proveedores críticos ---
        st.subheader("Visualización de Proveedores Clave")
        
        top_n = 5
        g1, g2 = st.columns(2)
        
        with g1:
            st.write(f"**Top {top_n} Proveedores por Deuda Bruta**")
            top_deuda = proveedor_summary.nlargest(top_n, 'deuda_bruta_proveedor')
            chart_deuda = alt.Chart(top_deuda).mark_bar(cornerRadius=5, color='#1f77b4').encode(
                x=alt.X('deuda_bruta_proveedor:Q', title='Deuda Bruta ($)', axis=alt.Axis(format='$,.0f')),
                y=alt.Y('nombre_proveedor:N', title='Proveedor', sort='-x'),
                tooltip=[alt.Tooltip('nombre_proveedor', title='Proveedor'), alt.Tooltip('deuda_bruta_proveedor', title='Deuda Bruta', format='$,.0f')]
            ).properties(height=300)
            st.altair_chart(chart_deuda.interactive(), use_container_width=True)

        with g2:
            st.write(f"**Top {top_n} Proveedores por Monto Vencido**")
            top_vencido = proveedor_summary[proveedor_summary['monto_vencido'] > 0].nlargest(top_n, 'monto_vencido')
            if not top_vencido.empty:
                chart_vencido = alt.Chart(top_vencido).mark_bar(cornerRadius=5, color='#d62728').encode(
                    x=alt.X('monto_vencido:Q', title='Monto Vencido ($)', axis=alt.Axis(format='$,.0f')),
                    y=alt.Y('nombre_proveedor:N', title='Proveedor', sort='-x'),
                    tooltip=[alt.Tooltip('nombre_proveedor', title='Proveedor'), alt.Tooltip('monto_vencido', title='Monto Vencido', format='$,.0f')]
                ).properties(height=300)
                st.altair_chart(chart_vencido.interactive(), use_container_width=True)
            else:
                st.info("No hay cartera vencida para mostrar en el gráfico.")

    else:
        st.info("Esta pestaña muestra un análisis comparativo. Por favor, selecciona 'TODOS (Vista Consolidada)' en el filtro lateral para activar esta vista.")


with tab2:
    st.header("📈 Análisis de Antigüedad y Composición de Saldos")
    st.markdown("Esta vista descompone la deuda pendiente para identificar dónde se concentra el riesgo.")

    if 'dias_para_vencer' not in supplier_df.columns:
        st.warning("La columna 'dias_para_vencer' es necesaria para este análisis y no se encontró.")
    else:
        def categorize_age(days):
            if pd.isna(days): return "Sin Fecha"
            if days >= 0: return "1. Por Vencer"
            if days >= -30: return "2. Vencida (1-30 días)"
            if days >= -60: return "3. Vencida (31-60 días)"
            return "4. Vencida (+60 días)"

        aging_df = facturas_df.copy()
        aging_df['categoria_antiguedad'] = aging_df['dias_para_vencer'].apply(categorize_age)
        
        aging_summary = aging_df.groupby('categoria_antiguedad').agg(
            valor_total=('valor_total_erp', 'sum'),
            numero_facturas=('num_factura', 'count')
        ).reset_index()

        if not aging_summary.empty:
            avg_days_overdue = abs(vencidas_df['dias_para_vencer']).mean() if not vencidas_df.empty else 0
            
            kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
            kpi_col1.metric("Porcentaje de Cartera Vencida", f"{porc_vencido:.1f}%")
            kpi_col2.metric("Días Promedio de Vencimiento", f"{avg_days_overdue:.0f} días")
            
            if not vencidas_df.empty:
                 factura_critica = vencidas_df.iloc[0]
                 kpi_col3.metric("Factura más Crítica (N°)", f"{factura_critica.get('num_factura', 'N/A')}", help=f"Vencida hace {abs(int(factura_critica.get('dias_para_vencer', 0)))} días por ${int(factura_critica.get('valor_total_erp', 0)):,}")
            else:
                 kpi_col3.metric("Factura más Crítica (N°)", "N/A")

            # --- Gráfico de Antigüedad de Saldos ---
            chart = alt.Chart(aging_summary).mark_bar(cornerRadius=5).encode(
                x=alt.X('valor_total:Q', title='Valor Total de la Deuda ($)', axis=alt.Axis(format='$,.0f')),
                y=alt.Y('categoria_antiguedad:N', title='Categoría de Antigüedad', sort='-x'),
                color=alt.Color('categoria_antiguedad:N', legend=alt.Legend(title="Categorías"),
                    scale=alt.Scale(
                        domain=["1. Por Vencer", "2. Vencida (1-30 días)", "3. Vencida (31-60 días)", "4. Vencida (+60 días)", "Sin Fecha"],
                        range=['#2ECC71', '#F39C12', '#E67E22', '#C0392B', '#808080']
                    )),
                tooltip=['categoria_antiguedad', alt.Tooltip('valor_total', title='Valor Total', format='$,.0f'), 'numero_facturas']
            ).properties(title='Distribución de la Deuda por Antigüedad')
            
            text = chart.mark_text(align='left', baseline='middle', dx=3, color='white', fontWeight='bold').encode(
                text=alt.condition(
                    'datum.valor_total > 0', alt.Text('valor_total:Q', format='$,.1s'), alt.value('')
                )
            )
            st.altair_chart((chart + text).interactive(), use_container_width=True)
            
            st.divider()
            
            # --- NUEVO: Gráfico de Composición de Deuda por Proveedor ---
            if selected_supplier == "TODOS (Vista Consolidada)":
                st.subheader("Composición de la Deuda Bruta por Proveedor")
                
                # Usamos el summary ya calculado en la otra pestaña
                proveedor_pie_data = master_df[master_df['valor_total_erp'] >= 0].groupby('nombre_proveedor')['valor_total_erp'].sum().reset_index()
                proveedor_pie_data.rename(columns={'valor_total_erp': 'deuda_bruta'}, inplace=True)

                pie_chart = alt.Chart(proveedor_pie_data).mark_arc(innerRadius=50).encode(
                    theta=alt.Theta(field="deuda_bruta", type="quantitative"),
                    color=alt.Color(field="nombre_proveedor", type="nominal", legend=alt.Legend(title="Proveedores")),
                    tooltip=['nombre_proveedor', alt.Tooltip('deuda_bruta', title='Deuda Bruta', format='$,.0f')]
                ).properties(title='Distribución de la Deuda Bruta', height=400)
                
                st.altair_chart(pie_chart, use_container_width=True)

        else:
            st.info("No hay facturas pendientes para generar el gráfico de antigüedad.")

with tab3:
    st.header("📑 Detalle Completo de Documentos Pendientes")
    st.info("Haga clic en los encabezados de las columnas para ordenar los datos a su conveniencia.")
    
    display_cols = [
        'num_factura', 'fecha_emision_erp', 'fecha_vencimiento_erp',
        'valor_total_erp', 'estado_pago', 'dias_para_vencer',
        'estado_conciliacion', 'estado_descuento', 'valor_descuento', 'fecha_limite_descuento'
    ]
    if selected_supplier == "TODOS (Vista Consolidada)":
        display_cols.insert(1, 'nombre_proveedor')
    
    existing_display_cols = [col for col in display_cols if col in supplier_df.columns]
    
    # --- **MEJORA VISUAL**: Ajuste de la barra de progreso ---
    min_dias = int(supplier_df['dias_para_vencer'].min()) if 'dias_para_vencer' in supplier_df.columns and not supplier_df['dias_para_vencer'].empty else -90
    max_dias = int(supplier_df['dias_para_vencer'].max()) if 'dias_para_vencer' in supplier_df.columns and not supplier_df['dias_para_vencer'].empty else 90
    
    column_config_base = {
        "num_factura": "N° Documento",
        "nombre_proveedor": "Proveedor",
        "valor_total_erp": st.column_config.NumberColumn("Valor (NC en negativo)", format="$ %d"),
        "fecha_emision_erp": st.column_config.DateColumn("Emitida", format="YYYY-MM-DD"),
        "fecha_vencimiento_erp": st.column_config.DateColumn("Vence", format="YYYY-MM-DD"),
        "estado_pago": "Estado Cartera",
        "dias_para_vencer": st.column_config.ProgressColumn(
            "Días para Vencer", 
            help="Barra visual de días restantes. Los valores negativos indican días vencidos.",
            format="%d días", 
            min_value=min_dias, 
            max_value=max_dias
        ),
        "estado_conciliacion": "Estado Conciliación",
        "estado_descuento": "Descuento",
        "valor_descuento": st.column_config.NumberColumn("Ahorro Potencial", format="$ %d"),
        "fecha_limite_descuento": st.column_config.DateColumn("Pagar Antes de", format="YYYY-MM-DD"),
    }
    
    st.dataframe(
        supplier_df[existing_display_cols],
        hide_index=True,
        use_container_width=True,
        column_config=column_config_base
    )
