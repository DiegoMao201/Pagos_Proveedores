# pages/2_🏢_Análisis_de_Proveedores.py
import streamlit as st
import pandas as pd
import altair as alt
import io
from datetime import datetime

# Asumimos que las funciones de conexión están en un directorio común.
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
    
    # --- INICIO DE LA CORRECCIÓN (ValueError) ---
    # Se crea una copia para no modificar el DataFrame original que usa la app.
    df_export = df.copy()

    # Se itera sobre cada columna del DataFrame a exportar.
    for col in df_export.columns:
        # Se comprueba si la columna es de tipo datetime y tiene información de zona horaria.
        if pd.api.types.is_datetime64_any_dtype(df_export[col]) and df_export[col].dt.tz is not None:
            # Si cumple, se elimina la información de la zona horaria.
            # Esto convierte la fecha a "naive", que es compatible con Excel.
            df_export[col] = df_export[col].dt.tz_localize(None)
    # --- FIN DE LA CORRECCIÓN ---

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Se usa el DataFrame sin zonas horarias para la exportación.
        df_export.to_excel(writer, index=False, sheet_name='DetalleProveedor')
        
        # Auto-ajustar columnas
        worksheet = writer.sheets['DetalleProveedor']
        for idx, col in enumerate(df_export):
            series = df_export[col]
            max_len = max((
                series.astype(str).map(len).max(),
                len(str(series.name))
            )) + 2
            worksheet.set_column(idx, idx, max_len)
            
    processed_data = output.getvalue()
    return processed_data

# --- 2. CARGA DE DATOS ---
@st.cache_data(ttl=600)
def get_master_data():
    """Conecta y carga los datos consolidados desde Google Sheets."""
    gs_client = connect_to_google_sheets()
    df = load_data_from_gsheet(gs_client)
    return df

master_df = get_master_data()

if master_df.empty:
    st.warning("🚨 No se pudieron cargar los datos. Asegúrate de haber ejecutado una sincronización en la página principal 'Dashboard General'.")
    st.stop()

# --- 3. BARRA LATERAL Y FILTRO INTELIGENTE ---
st.sidebar.header("Filtros de Análisis 🔎")

# Se calculan los proveedores que tienen deuda real
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

# Se ajusta el DataFrame y el título según la selección
if selected_supplier == "TODOS (Vista Consolidada)":
    # Se usa el dataframe con todos los proveedores que tienen deuda
    supplier_df = master_df[master_df['nombre_proveedor'].isin(proveedores_lista_filtrada)].copy()
    titulo_pagina = "🏢 Centro de Análisis: Consolidado de Proveedores"
    nombre_archivo = f"Reporte_Consolidado_{datetime.now().strftime('%Y%m%d')}.xlsx"
else:
    # Se filtra por el proveedor específico
    supplier_df = master_df[master_df['nombre_proveedor'] == selected_supplier].copy()
    titulo_pagina = f"🏢 Centro de Análisis: {selected_supplier}"
    nombre_archivo = f"Reporte_{selected_supplier.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.xlsx"

st.title(titulo_pagina)

if supplier_df.empty:
    st.info("No se encontraron datos para la selección actual.")
    st.stop()

# Botón de descarga con nombre de archivo dinámico
excel_data = to_excel(supplier_df)
st.download_button(
    label="📥 Descargar Reporte en Excel",
    data=excel_data,
    file_name=nombre_archivo,
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# --- 5. ESTRUCTURA DE PESTAÑAS ---
tab1, tab2, tab3 = st.tabs(["📊 Resumen Ejecutivo", "💰 Diagnóstico Financiero Profundo", "📑 Detalle de Facturas"])

with tab1:
    st.header("🧠 Sugerencias Inteligentes para Tesorería")
    st.markdown("Acciones prioritarias para optimizar el flujo de caja y maximizar ahorros.")

    descuentos_df = supplier_df[
        (supplier_df['estado_descuento'] != 'No Aplica') &
        (supplier_df['fecha_limite_descuento'].notna())
    ].sort_values('fecha_limite_descuento')

    vencidas_df = supplier_df[supplier_df['estado_pago'] == '🔴 Vencida'].sort_values('dias_para_vencer', ascending=True)

    s1, s2 = st.columns(2)
    with s1:
        with st.container(border=True):
            st.subheader("💰 Maximizar Ahorro")
            if not descuentos_df.empty:
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
    monto_vencido = vencidas_df['valor_total_erp'].sum()
    descuento_potencial = descuentos_df['valor_descuento'].sum()
    supplier_df['plazo_dias'] = (supplier_df['fecha_vencimiento_erp'] - supplier_df['fecha_emision_erp']).dt.days
    avg_plazo = supplier_df['plazo_dias'].mean()

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("Deuda Total Actual", f"${int(total_deuda):,}", help="Suma de todas las facturas pendientes de pago.")
    kpi2.metric("Monto Vencido", f"${int(monto_vencido):,}", delta_color="inverse", help="Valor total de las facturas que ya pasaron su fecha de vencimiento.")
    kpi3.metric("Ahorro Potencial", f"${int(descuento_potencial):,}", delta_color="off", help="Suma de todos los descuentos por pronto pago disponibles.")
    kpi4.metric("Plazo Promedio (Días)", f"{avg_plazo:.0f}" if pd.notna(avg_plazo) else "N/A", help="Días promedio entre la emisión y el vencimiento de las facturas.")


with tab2:
    st.header("📈 Análisis de Antigüedad de Saldos (Aged Debt)")

    # --- Lógica de Antigüedad de Saldos ---
    def categorize_age(days):
        if days < 0:
            if days <= -61: return "4. Vencida (+60 días)"
            if days <= -31: return "3. Vencida (31-60 días)"
            return "2. Vencida (1-30 días)"
        return "1. Por Vencer"

    supplier_df['categoria_antiguedad'] = supplier_df['dias_para_vencer'].apply(categorize_age)
    
    aging_summary = supplier_df.groupby('categoria_antiguedad').agg(
        valor_total=('valor_total_erp', 'sum'),
        numero_facturas=('num_factura', 'count')
    ).reset_index()

    total_deuda_tab2 = aging_summary['valor_total'].sum()

    # --- Visualización Avanzada ---
    st.markdown("Esta vista descompone la deuda total en bloques de tiempo para identificar riesgos y priorizar pagos.")
    
    kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
    
    # Calcular % Vencido
    monto_total_vencido = supplier_df[supplier_df['dias_para_vencer'] < 0]['valor_total_erp'].sum()
    porc_vencido = (monto_total_vencido / total_deuda_tab2 * 100) if total_deuda_tab2 > 0 else 0
    kpi_col1.metric("Porcentaje de Cartera Vencida", f"{porc_vencido:.1f}%")

    # Calcular Días Promedio de Vencimiento
    df_vencidas_calc = supplier_df[supplier_df['dias_para_vencer'] < 0]
    if not df_vencidas_calc.empty:
        avg_days_overdue = abs(df_vencidas_calc['dias_para_vencer']).mean()
        kpi_col2.metric("Días Promedio de Vencimiento", f"{avg_days_overdue:.0f} días")
    else:
        kpi_col2.metric("Días Promedio de Vencimiento", "0 días")
        
    # Calcular Factura más crítica
    if not df_vencidas_calc.empty:
        factura_critica = df_vencidas_calc.sort_values('dias_para_vencer', ascending=True).iloc[0]
        kpi_col3.metric("Factura más Crítica (N°)", f"{factura_critica['num_factura']}", help=f"Vencida hace {abs(int(factura_critica['dias_para_vencer']))} días por un valor de ${int(factura_critica['valor_total_erp']):,}")
    else:
        kpi_col3.metric("Factura más Crítica (N°)", "N/A")


    # Gráfico de Antigüedad
    chart = alt.Chart(aging_summary).mark_bar().encode(
        x=alt.X('valor_total:Q', title='Valor Total de la Deuda ($)', axis=alt.Axis(format='$,.0f')),
        y=alt.Y('categoria_antiguedad:N', title='Categoría de Antigüedad', sort='descending'),
        color=alt.Color('categoria_antiguedad:N', 
            legend=None,
            scale=alt.Scale(
                domain=["1. Por Vencer", "2. Vencida (1-30 días)", "3. Vencida (31-60 días)", "4. Vencida (+60 días)"],
                range=['#2ECC71', '#F39C12', '#E67E22', '#C0392B']
            )
        ),
        tooltip=[
            alt.Tooltip('categoria_antiguedad', title='Categoría'),
            alt.Tooltip('valor_total', title='Valor Total', format='$,.0f'),
            alt.Tooltip('numero_facturas', title='N° Facturas')
        ]
    ).properties(
        title='Distribución de la Deuda por Antigüedad'
    )
    
    # --- BLOQUE CORREGIDO PARA EVITAR EL ERROR ---
    text = chart.mark_text(
        align='left',
        baseline='middle',
        dx=3,  # Desplaza el texto ligeramente a la derecha de la barra
    ).encode(
        # Usa alt.condition para mostrar el texto solo si el valor es significativo
        text=alt.condition(
            'datum.valor_total > 100000',  # Condición: si el valor es mayor a 100,000
            alt.Text('valor_total:Q', format='$,.1s'),  # Si es VERDADERO, muestra el valor formateado
            alt.value('')  # Si es FALSO, no muestres nada (texto vacío)
        )
    )

    st.altair_chart((chart + text).interactive(), use_container_width=True)

    # --- Comentarios y Diagnóstico Automático ---
    st.subheader("💡 Diagnóstico y Recomendaciones Automáticas")
    with st.container(border=True):
        # Encontrar la categoría con más dinero
        categoria_max_valor = aging_summary.loc[aging_summary['valor_total'].idxmax()]
        valor_max = categoria_max_valor['valor_total']
        cat_max = categoria_max_valor['categoria_antiguedad']
        porc_max = (valor_max / total_deuda_tab2) * 100 if total_deuda_tab2 > 0 else 0
        
        st.write(f"🔹 **Foco Principal**: La mayor concentración de la deuda, **${int(valor_max):,}** ({porc_max:.0f}%), se encuentra en la categoría **'{cat_max.split('. ')[1]}'**.")

        # Analizar la cartera vencida
        if monto_total_vencido > 0:
            st.write(f"🔸 **Salud de la Cartera**: Un **{porc_vencido:.0f}%** de la deuda está vencida. Esto representa un total de **${int(monto_total_vencido):,}** que requiere atención.")
            
            # Revisar deuda muy antigua
            deuda_muy_antigua_df = aging_summary[aging_summary['categoria_antiguedad'] == '4. Vencida (+60 días)']
            if not deuda_muy_antigua_df.empty:
                valor_muy_antiguo = deuda_muy_antigua_df['valor_total'].sum()
                if valor_muy_antiguo > 0:
                    porc_muy_antiguo = (valor_muy_antiguo / total_deuda_tab2) * 100
                    st.error(f"🚨 **Alerta Crítica**: Hay **${int(valor_muy_antiguo):,}** ({porc_muy_antiguo:.0f}%) en facturas con más de 60 días de vencimiento. Esto representa un alto riesgo financiero y para la relación con el proveedor. **Acción inmediata es requerida.**")
        else:
            st.success("✅ **¡Felicitaciones!** Toda la cartera se encuentra al día. La gestión de pagos es excelente.")
        
        # Analizar oportunidades de descuento
        if descuento_potencial > 0:
             st.info(f"📈 **Oportunidad de Ahorro**: No olvides que tienes un potencial de ahorro de **${int(descuento_potencial):,}** por pronto pago. Revisa el 'Resumen Ejecutivo' para ver los detalles.")


with tab3:
    st.header("📑 Detalle Completo de Facturas")
    st.markdown("Explora, ordena y filtra todas las facturas registradas para esta selección.")
    
    display_cols = [
        'num_factura', 'fecha_emision_erp', 'fecha_vencimiento_erp',
        'valor_total_erp', 'estado_pago', 'dias_para_vencer',
        'estado_conciliacion', 'estado_descuento', 'valor_descuento', 'fecha_limite_descuento'
    ]
    # Si estamos en vista consolidada, mostrar el nombre del proveedor
    if selected_supplier == "TODOS (Vista Consolidada)":
        display_cols.insert(1, 'nombre_proveedor')

    # Configuración de columnas base
    column_config_base = {
        "num_factura": st.column_config.TextColumn("N° Factura"),
        "valor_total_erp": st.column_config.NumberColumn("Valor Original", format="$ %d"),
        "fecha_emision_erp": st.column_config.DateColumn("Emitida", format="YYYY-MM-DD"),
        "fecha_vencimiento_erp": st.column_config.DateColumn("Vence", format="YYYY-MM-DD"),
        "estado_pago": st.column_config.TextColumn("Estado Cartera"),
        "dias_para_vencer": st.column_config.ProgressColumn("Días para Vencer", format="%d días", min_value=-90, max_value=90),
        "estado_conciliacion": st.column_config.TextColumn("Estado Conciliación"),
        "estado_descuento": st.column_config.TextColumn("Descuento"),
        "valor_descuento": st.column_config.NumberColumn("Ahorro Potencial", format="$ %d"),
        "fecha_limite_descuento": st.column_config.DateColumn("Pagar Antes de", format="YYYY-MM-DD"),
    }
    
    # Añadir configuración para la columna de proveedor si es necesario
    if selected_supplier == "TODOS (Vista Consolidada)":
        column_config_base["nombre_proveedor"] = st.column_config.TextColumn("Proveedor")

    st.dataframe(
        supplier_df[display_cols],
        hide_index=True,
        column_config=column_config_base
    )
