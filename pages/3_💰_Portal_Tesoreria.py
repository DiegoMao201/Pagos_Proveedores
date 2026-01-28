# -*- coding: utf-8 -*-
"""
Módulo de Auditoría de Facturación (Versión 5.0 - Intelligence Edition).
Enfoque: Conciliación Avanzada Correo vs ERP para Proveedores Objetivo.
"""

import streamlit as st
import pandas as pd
import io
import pytz
from datetime import datetime, timedelta
import plotly.express as px  # Agregamos gráficos para el "dashboard"

# Configuración de página para dar más espacio horizontal
st.set_page_config(page_title="Auditoría de Facturación", layout="wide", page_icon="🕵️‍♂️")

# ======================================================================================
# --- 0. SEGURIDAD Y CONFIGURACIÓN ---
# ======================================================================================

if 'password_correct' not in st.session_state:
    st.session_state['password_correct'] = False

if not st.session_state["password_correct"]:
    st.error("🔒 Acceso Denegado. Por favor inicia sesión en el Dashboard General.")
    st.stop()

COLOMBIA_TZ = pytz.timezone('America/Bogota')

# ======================================================================================
# --- 1. FUNCIONES DE UTILIDAD Y NORMALIZACIÓN (El Cerebro) ---
# ======================================================================================

def to_excel(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Auditoria_Facturas')
        # Ajuste automático de columnas
        worksheet = writer.sheets['Auditoria_Facturas']
        for idx, col in enumerate(df.columns):
            max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(idx, idx, max_len)
    return output.getvalue()

def normalizar_texto(texto):
    """Limpieza agresiva para maximizar coincidencias (Match Rate)."""
    if pd.isna(texto): return ""
    return str(texto).strip().upper().replace('.', '').replace(',', '').replace('-', '').replace(' ', '')

# ======================================================================================
# --- 2. LÓGICA DE NEGOCIO Y CARGA DE DATOS ---
# ======================================================================================

st.title("🕵️‍♂️ Auditoría Inteligente de Facturas")
st.markdown("### Control de Integridad: Correo vs ERP")

# --- A. Carga de Proveedores Objetivo ---
archivo_proveedores = "PROVEDORES_CORREO.xlsx"
df_proveedores_obj = pd.DataFrame()

try:
    df_proveedores_obj = pd.read_excel(archivo_proveedores)
    # Buscamos la columna correcta
    col_prov = next((c for c in df_proveedores_obj.columns if 'proveedor' in c.lower() or 'nombre' in c.lower()), None)
    if col_prov:
        lista_objetivo_raw = df_proveedores_obj[col_prov].dropna().unique().tolist()
        lista_objetivo_norm = [normalizar_texto(p) for p in lista_objetivo_raw]
    else:
        st.error("❌ El archivo de proveedores no tiene una columna 'Nombre' o 'Proveedor'.")
        st.stop()
except FileNotFoundError:
    st.error(f"⚠️ Falta el archivo maestro: '{archivo_proveedores}'.")
    st.stop()
except Exception as e:
    st.error(f"Error leyendo proveedores: {e}")
    st.stop()

# --- B. Recuperar Datos de Sesión (Sincronizados en Dashboard) ---
email_df = st.session_state.get("email_df", pd.DataFrame()).copy()
erp_df = st.session_state.get("erp_df", pd.DataFrame()).copy()

if email_df.empty or erp_df.empty:
    st.warning("⚠️ No hay datos sincronizados. Ve al Dashboard General y actualiza la data.")
    st.stop()

# --- C. Filtros de Proveedores Objetivo ---
# Normaliza la columna de proveedor del correo
if 'nombre_proveedor_correo' not in email_df.columns:
    st.error("El DataFrame de correo no tiene la columna 'nombre_proveedor_correo'. Revisa la sincronización.")
    st.stop()

email_analysis = email_df[email_df['nombre_proveedor_correo'].apply(normalizar_texto).isin(lista_objetivo_norm)]

# Filtramos por fecha si aplica
if isinstance(fechas_sel, list) and len(fechas_sel) == 2:
    start_d, end_d = fechas_sel
    email_analysis = email_analysis[
        (email_analysis['fecha_dt'] >= start_d) & 
        (email_analysis['fecha_dt'] <= end_d)
    ]

# --- PASO 2: El Cruce (Matching) ---
# Normalizamos llaves clave: Número de Factura
email_analysis['key_factura'] = email_analysis['num_factura'].apply(normalizar_texto)
erp_df['key_factura'] = erp_df['num_factura'].apply(normalizar_texto)

facturas_en_erp_set = set(erp_df['key_factura'].unique())

# Identificamos las que NO están en ERP
df_faltantes = email_analysis[~email_analysis['key_factura'].isin(facturas_en_erp_set)].copy()

# --- PASO 3: Enriquecimiento Inteligente ---
if not df_faltantes.empty:
    today = pd.Timestamp.now().date()
    
    # Calcular días desde recepción
    df_faltantes['dias_antiguedad'] = (pd.to_datetime(today) - pd.to_datetime(df_faltantes['fecha_dt'])).dt.days
    
    # CLASIFICACIÓN DEL ESTADO (La lógica de negocio)
    def clasificar_estado(dias):
        if dias <= 5:
            return "🟢 Reciente (Trámite Normal)"
        elif dias <= 15:
            return "🟡 Alerta (Seguimiento)"
        else:
            return "🔴 Crítico / Posiblemente Pagada"

    df_faltantes['estado_auditoria'] = df_faltantes['dias_antiguedad'].apply(clasificar_estado)
    
    # Formateo de moneda para visualización
    df_faltantes['valor_formato'] = df_faltantes['valor_total'].apply(lambda x: f"$ {x:,.0f}")

# ======================================================================================
# --- 5. DASHBOARD DE RESULTADOS ---
# ======================================================================================

if df_faltantes.empty:
    st.balloons()
    st.success("✅ **¡Integridad Total!** Todas las facturas de los proveedores seleccionados en este rango de fechas ya existen en el ERP.")
else:
    # --- A. Métricas KPI (Top Level) ---
    st.divider()
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    
    total_faltante = df_faltantes['valor_total'].sum()
    cant_facturas = len(df_faltantes)
    criticas = len(df_faltantes[df_faltantes['estado_auditoria'].str.contains("Crítico")])
    top_prov = df_faltantes['nombre_proveedor'].mode()[0] if not df_faltantes.empty else "N/A"

    kpi1.metric("💰 Valor 'Flotante'", f"$ {total_faltante:,.0f}", delta="No radicado en ERP", delta_color="inverse")
    kpi2.metric("📄 Facturas Faltantes", cant_facturas)
    kpi3.metric("🚨 Facturas Críticas (>15 días)", criticas, delta="- Prioridad Alta", delta_color="inverse")
    kpi4.metric("🏆 Proveedor con más pendientes", top_prov)

    st.divider()

    # --- B. Gráficos de Análisis Rápido ---
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        st.subheader("Distribución por Estado")
        fig_estado = px.pie(df_faltantes, names='estado_auditoria', title='Estado de Antigüedad', hole=0.4, 
                            color_discrete_map={
                                "🟢 Reciente (Trámite Normal)": "#2ecc71",
                                "🟡 Alerta (Seguimiento)": "#f1c40f",
                                "🔴 Crítico / Posiblemente Pagada": "#e74c3c"
                            })
        st.plotly_chart(fig_estado, use_container_width=True)
    
    with col_chart2:
        st.subheader("Monto Pendiente por Proveedor")
        df_group = df_faltantes.groupby('nombre_proveedor')['valor_total'].sum().reset_index().sort_values('valor_total', ascending=True)
        fig_bar = px.bar(df_group, x='valor_total', y='nombre_proveedor', orientation='h', text_auto='.2s')
        st.plotly_chart(fig_bar, use_container_width=True)

    # --- C. Tabla Detallada Interactiva ---
    st.subheader("📋 Detalle de Facturas para Gestión")
    st.info("💡 **Nota:** Si una factura es 'Crítica', verifica si ya fue pagada. Si es 'Reciente', es posible que Contabilidad aún no la haya ingresado.")

    # Preparamos el DF para el editor visual
    df_display = df_faltantes[[
        'estado_auditoria',
        'nombre_proveedor', 
        'num_factura', 
        'fecha_dt', 
        'dias_antiguedad', 
        'valor_total',
        'asunto_correo' 
    ]].sort_values(by='dias_antiguedad', ascending=False) # Las más antiguas primero

    # Usamos column_config para hacer la tabla hermosa y funcional
    st.data_editor(
        df_display,
        column_config={
            "estado_auditoria": st.column_config.TextColumn(
                "Diagnóstico IA",
                help="Clasificación basada en la fecha de recepción",
                width="medium"
            ),
            "nombre_proveedor": "Proveedor",
            "num_factura": "N° Factura",
            "fecha_dt": st.column_config.DateColumn("Fecha Recibido"),
            "dias_antiguedad": st.column_config.ProgressColumn(
                "Días en Limbo",
                help="Días desde que llegó al correo",
                format="%d días",
                min_value=0,
                max_value=60, # Escala visual
            ),
            "valor_total": st.column_config.NumberColumn(
                "Valor Total",
                format="$ %.2f"
            ),
            "asunto_correo": st.column_config.TextColumn("Contexto (Asunto)", width="large"),
        },
        hide_index=True,
        use_container_width=True,
        disabled=True # Solo lectura
    )

    # --- D. Exportación ---
    col_dl1, col_dl2 = st.columns([1, 4])
    with col_dl1:
        st.download_button(
            label="📥 Descargar Reporte de Auditoría",
            data=to_excel(df_faltantes),
            file_name=f"Auditoria_Facturacion_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.ms-excel",
        )

# ======================================================================================
# --- 6. SECCIÓN DE AYUDA / DEBUGGING (Opcional) ---
# ======================================================================================
with st.expander("🛠️ Herramientas de Diagnóstico (Si algo no cuadra)"):
    st.write("Si ves facturas aquí que SI están en el ERP, revisa cómo están escritas:")
    st.write(f"- Total Facturas en ERP (Cargadas): {len(erp_df)}")
    st.write(f"- Total Facturas en Correo (Filtradas): {len(email_analysis)}")
    
    col_dbg1, col_dbg2 = st.columns(2)
    with col_dbg1:
        txt_verif = st.text_input("Probar un N° de Factura específico:")
    if txt_verif:
        norm_verif = normalizar_texto(txt_verif)
        en_erp = norm_verif in facturas_en_erp_set
        st.write(f"🔍 Factura '{txt_verif}' (Norm: {norm_verif}) -> ¿Está en ERP?: **{'SÍ' if en_erp else 'NO'}**")