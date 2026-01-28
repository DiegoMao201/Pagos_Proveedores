# -*- coding: utf-8 -*-
"""
Módulo de Auditoría de Facturación (Versión 5.0 - Intelligence Edition).
Enfoque: Conciliación Avanzada Correo vs ERP para Proveedores Objetivo.
"""

import streamlit as st
import pandas as pd
import os
from datetime import datetime
import pytz

st.title("💰 Portal de Tesorería - Facturas Faltantes en ERP")
st.markdown(
    "A continuación se muestran **solo las facturas recibidas por correo de los proveedores objetivo** "
    "que aún **no están registradas en el ERP**. El listado de proveedores objetivo se toma de `PROVEDORES_CORREO.xlsx`."
)

# --- Leer archivo de proveedores objetivo desde la raíz ---
archivo_proveedores = "PROVEDORES_CORREO.xlsx"
if not os.path.exists(archivo_proveedores):
    st.error(f"No se encontró el archivo '{archivo_proveedores}' en la raíz del proyecto.")
    st.stop()

df_proveedores_obj = pd.read_excel(archivo_proveedores)

def normalizar_texto(texto):
    if pd.isna(texto): return ""
    return str(texto).strip().upper().replace('.', '').replace(',', '').replace('-', '').replace(' ', '')

# Buscar columna de proveedor objetivo
col_prov = next((c for c in df_proveedores_obj.columns if 'proveedor' in c.lower() or 'nombre' in c.lower()), None)
if not col_prov:
    st.error("El archivo debe tener una columna identificable como 'Proveedor'.")
    st.stop()

lista_objetivo_raw = df_proveedores_obj[col_prov].dropna().unique().tolist()
lista_objetivo_norm = [normalizar_texto(p) for p in lista_objetivo_raw]

# --- Cargar datos de facturas del correo y ERP desde sesión ---
email_df = st.session_state.get("email_df", pd.DataFrame()).copy()
erp_df = st.session_state.get("erp_df", pd.DataFrame()).copy()
if email_df.empty or erp_df.empty:
    st.warning("No hay datos de correo o ERP cargados. Realiza la sincronización desde el Dashboard General.")
    st.stop()

# --- Normalizar columna de proveedor en email_df ---
if 'nombre_proveedor_correo' not in email_df.columns:
    st.error("El DataFrame de correo no tiene la columna 'nombre_proveedor_correo'. Revisa la sincronización.")
    st.stop()

email_df['nombre_proveedor_correo'] = email_df['nombre_proveedor_correo'].astype(str).str.strip().str.upper()

# --- Detectar columna de fecha en email_df ---
fecha_cols = [c for c in email_df.columns if 'fecha' in c.lower()]
fecha_col = None
for c in ['fecha_dt', 'fecha_emision_correo', 'fecha_lectura', 'fecha']:
    if c in email_df.columns:
        fecha_col = c
        break
if not fecha_col and fecha_cols:
    fecha_col = fecha_cols[0]
if not fecha_col:
    st.error("No se encontró ninguna columna de fecha en los datos del correo.")
    st.stop()

# Renombrar la columna de fecha a 'fecha_dt' para el análisis
if fecha_col != 'fecha_dt':
    email_df = email_df.rename(columns={fecha_col: 'fecha_dt'})

# Limpieza previa: convierte todo a string y reemplaza valores vacíos/nulos
email_df['fecha_dt'] = email_df['fecha_dt'].astype(str).replace(['', ' ', 'NaT', 'None', None, pd.NaT, pd.NA], pd.NA)

# Solo intenta convertir si hay al menos un valor no nulo
if email_df['fecha_dt'].notna().any():
    email_df['fecha_dt'] = pd.to_datetime(email_df['fecha_dt'], errors='coerce')
else:
    st.error("No hay fechas válidas en los datos de correo para analizar.")
    st.stop()

# --- Filtro de fechas ---
if not email_df.empty and 'fecha_dt' in email_df.columns:
    email_df['fecha_dt'] = pd.to_datetime(email_df['fecha_dt'], errors='coerce')
    min_fecha = email_df['fecha_dt'].min().date()
    max_fecha = email_df['fecha_dt'].max().date()
    fechas_sel = st.date_input(
        "Filtrar por rango de fechas de recepción (correo):",
        value=(min_fecha, max_fecha),
        min_value=min_fecha,
        max_value=max_fecha
    )
else:
    fechas_sel = None

# --- PASO 2: El Cruce (Matching) ---
email_analysis = email_df[email_df['nombre_proveedor_correo'].apply(normalizar_texto).isin(lista_objetivo_norm)]

if isinstance(fechas_sel, list) and len(fechas_sel) == 2:
    start_d, end_d = fechas_sel
    email_analysis = email_analysis[
        (email_analysis['fecha_dt'] >= pd.to_datetime(start_d)) &
        (email_analysis['fecha_dt'] <= pd.to_datetime(end_d))
    ]

# --- PASO 3: Mostrar solo facturas que faltan en el ERP ---
if 'num_factura' not in email_analysis.columns or 'num_factura' not in erp_df.columns:
    st.error("Falta la columna 'num_factura' en los datos para el cruce.")
    st.stop()

email_analysis['num_factura'] = email_analysis['num_factura'].astype(str).str.strip()
erp_df['num_factura'] = erp_df['num_factura'].astype(str).str.strip()
facturas_en_erp_set = set(erp_df['num_factura'].unique())
email_analysis = email_analysis[~email_analysis['num_factura'].isin(facturas_en_erp_set)]

# --- Enriquecimiento: Días de antigüedad ---
from app import COLOMBIA_TZ  # Usa la constante global de tu app

if not email_analysis.empty:
    # Normaliza fechas y elimina nulos
    email_analysis = email_analysis[email_analysis['fecha_dt'].notna()].copy()
    email_analysis['fecha_dt'] = pd.to_datetime(email_analysis['fecha_dt'], errors='coerce').dt.tz_localize(COLOMBIA_TZ, ambiguous='infer') \
        if email_analysis['fecha_dt'].dt.tz is None else email_analysis['fecha_dt'].dt.tz_convert(COLOMBIA_TZ)
    today = pd.Timestamp.now(tz=COLOMBIA_TZ).normalize()
    email_analysis['dias_antiguedad'] = (today - email_analysis['fecha_dt'].dt.normalize()).dt.days

    def clasificar_estado(dias):
        if dias <= 5:
            return "🟢 Reciente (Trámite Normal)"
        elif dias <= 15:
            return "🟡 Alerta (Seguimiento)"
        else:
            return "🔴 Crítico / Posiblemente Pagada"
    email_analysis['estado_auditoria'] = email_analysis['dias_antiguedad'].apply(clasificar_estado)
    email_analysis['valor_total'] = email_analysis['valor_total_correo'] if 'valor_total_correo' in email_analysis.columns else email_analysis.get('valor_total', 0)
    email_analysis['nombre_proveedor'] = email_analysis['nombre_proveedor_correo']

# --- DASHBOARD DE RESULTADOS ---
if email_analysis.empty:
    st.balloons()
    st.success("✅ **¡Integridad Total!** Todas las facturas de los proveedores seleccionados en este rango de fechas ya existen en el ERP.")
else:
    st.divider()
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    total_faltante = email_analysis['valor_total'].sum()
    cant_facturas = len(email_analysis)
    criticas = len(email_analysis[email_analysis['estado_auditoria'].str.contains("Crítico")])
    top_prov = email_analysis['nombre_proveedor'].mode()[0] if not email_analysis.empty else "N/A"

    kpi1.metric("💰 Valor 'Flotante'", f"$ {total_faltante:,.0f}", delta="No radicado en ERP", delta_color="inverse")
    kpi2.metric("📄 Facturas Faltantes", cant_facturas)
    kpi3.metric("🚨 Facturas Críticas (>15 días)", criticas, delta="- Prioridad Alta", delta_color="inverse")
    kpi4.metric("🏆 Proveedor con más pendientes", top_prov)

    st.divider()
    st.subheader("📋 Detalle de Facturas para Gestión")
    st.dataframe(
        email_analysis[
            ['estado_auditoria', 'nombre_proveedor', 'num_factura', 'fecha_dt', 'dias_antiguedad', 'valor_total']
        ].sort_values(by='dias_antiguedad', ascending=False),
        use_container_width=True,
        hide_index=True,
        column_config={
            "estado_auditoria": st.column_config.TextColumn("Diagnóstico IA"),
            "nombre_proveedor": "Proveedor",
            "num_factura": "N° Factura",
            "fecha_dt": st.column_config.DateColumn("Fecha Recibido"),
            "dias_antiguedad": st.column_config.ProgressColumn("Días en Limbo", format="%d días", min_value=0, max_value=60),
            "valor_total": st.column_config.NumberColumn("Valor Total", format="$ %.2f"),
        }
    )