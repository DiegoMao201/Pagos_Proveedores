# -*- coding: utf-8 -*-
"""
Módulo de Análisis Gerencial de Proveedores (Versión 3.7 - Dashboard Ejecutivo).

Este módulo ha sido rediseñado para ofrecer un tablero de control gerencial,
enfocado en KPIs de alto impacto y una visión clara y actualizada de la cartera
pendiente.

Mejoras en v3.7:
- **Corrección de Error Crítico (KeyError):** Se solucionó un error que ocurría al intentar ordenar por columnas
  que no existían en los datos filtrados (ej. 'fecha_limite_descuento'). La lógica de ordenamiento ahora es robusta.
- **Enfoque Ejecutivo:** La interfaz ha sido rediseñada para ser más limpia y directa, hablando en el lenguaje de negocio
  para facilitar la toma de decisiones rápidas.
- **KPIs Mejorados:** Se ha añadido un indicador de "Salud de Cartera" y se ha mejorado la presentación de los KPIs financieros.
- **Guía al Usuario:** Se han añadido pequeños textos de guía para mejorar la experiencia y la comprensión de los datos.
- **Sincronización de Estado:** Se mantiene el enfoque EXCLUSIVO en facturas con estado 'Pendiente' para un reflejo fiel de la realidad.
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
# Este es el código que debes añadir al principio de cada página protegida.
# ======================================================================================

# 1. Se asegura de que la variable de sesión exista para evitar errores.
if 'password_correct' not in st.session_state:
    st.session_state['password_correct'] = False

# 2. Verifica si la contraseña es correcta (si el usuario ya inició sesión en la página principal).
#    Si no es correcta, muestra un mensaje de error y detiene la carga de la página.
if not st.session_state["password_correct"]:
    st.error("🔒 Debes iniciar sesión para acceder a esta página.")
    st.info("Por favor, ve a la página principal 'Dashboard General' para ingresar la contraseña.")
    st.stop() # ¡Este comando es clave! Detiene la ejecución del resto del script.

# --- FIN DEL BLOQUE DE SEGURIDAD ---

# --- 1. CONFIGURACIÓN DE LA PÁGINA ---
st.set_page_config(
    layout="wide",
    page_title="Análisis Gerencial de Proveedores",
    page_icon="🏢"
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
    titulo_pagina = "🏢 Panel de Control: Consolidado de Cartera Pendiente"
    nombre_archivo = f"Reporte_Consolidado_Pendiente_{datetime.now().strftime('%Y%m%d')}.xlsx"
else:
    supplier_df = master_df[master_df['nombre_proveedor'] == selected_supplier].copy()
    titulo_pagina = f"🏢 Panel de Control: {selected_supplier}"
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
tab1, tab2, tab3 = st.tabs(["💡 Resumen Ejecutivo", "💰 Diagnóstico Financiero", "📑 Detalle de Documentos"])

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

    # --- CORRECCIÓN DE KEYERROR ---
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
                st.dataframe(
                    descuentos_df[['num_factura', 'valor_con_descuento', 'fecha_limite_descuento', 'valor_descuento']],
                    hide_index=True, height=250,
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
            st.subheader(f"⚠️ Riesgo: Cartera Vencida ${int(monto_vencido):,}")
            if not vencidas_df.empty:
                st.error(f"Hay **{len(vencidas_df)} facturas vencidas**. Priorizar su pago para evitar problemas de suministro y cargos:")
                st.dataframe(
                    vencidas_df[['num_factura', 'valor_total_erp', 'fecha_vencimiento_erp', 'dias_para_vencer']],
                    hide_index=True, height=250,
                    column_config={
                        "num_factura": "N° Factura",
                        "valor_total_erp": st.column_config.NumberColumn("Valor", format="$ %d"),
                        "fecha_vencimiento_erp": st.column_config.DateColumn("Venció", format="YYYY-MM-DD"),
                        "dias_para_vencer": st.column_config.NumberColumn("Días Vencida")
                    }
                )
            else:
                st.success("¡Felicitaciones! No hay facturas vencidas en esta selección.")

with tab2:
    st.header("📈 Análisis de Antigüedad de Saldos (Aged Debt)")
    st.markdown("Esta vista descompone la deuda pendiente en bloques de tiempo para identificar dónde se concentra el riesgo.")

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
    
    column_config_base = {
        "num_factura": "N° Documento",
        "nombre_proveedor": "Proveedor",
        "valor_total_erp": st.column_config.NumberColumn("Valor (NC en negativo)", format="$ %d"),
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
        use_container_width=True,
        column_config=column_config_base
    )
