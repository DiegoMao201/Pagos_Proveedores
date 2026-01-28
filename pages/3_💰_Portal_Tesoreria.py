# -*- coding: utf-8 -*-
"""
Módulo de Auditoría de Facturación (Versión 5.0 - Intelligence Edition).
Enfoque: Conciliación Avanzada Correo vs ERP para Proveedores Objetivo.
"""

import streamlit as st
import pandas as pd
import os
from common.utils import COLOMBIA_TZ

st.title("💰 Portal de Tesorería - Facturas Faltantes en ERP")
st.markdown(
    """
    <style>
    .kpi-box {background: #f8fafc; border-radius: 12px; padding: 1.5em 1em; margin-bottom: 1.5em; box-shadow: 0 2px 8px #0001;}
    .kpi-title {font-size: 1.1em; color: #0C2D57; margin-bottom: 0.2em;}
    .kpi-value {font-size: 2.2em; font-weight: bold; color: #1f77b4;}
    .kpi-sub {font-size: 1em; color: #555;}
    </style>
    """,
    unsafe_allow_html=True,
)
st.markdown(
    "A continuación se muestran <b>todas las facturas recibidas por correo de los proveedores objetivo</b> "
    "que <b>aún no están registradas en el ERP</b>. El listado de proveedores objetivo se toma de <code>PROVEDORES_CORREO.xlsx</code>.",
    unsafe_allow_html=True
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

if 'nombre_proveedor_correo' not in email_df.columns:
    st.error("El DataFrame de correo no tiene la columna 'nombre_proveedor_correo'. Revisa la sincronización.")
    st.stop()
email_df['nombre_proveedor_correo'] = email_df['nombre_proveedor_correo'].astype(str).str.strip().str.upper()

# --- Matching: solo proveedores objetivo y facturas que faltan en ERP ---
email_analysis = email_df[email_df['nombre_proveedor_correo'].apply(normalizar_texto).isin(lista_objetivo_norm)]

if 'num_factura' not in email_analysis.columns or 'num_factura' not in erp_df.columns:
    st.error("Falta la columna 'num_factura' en los datos para el cruce.")
    st.stop()

email_analysis['num_factura'] = email_analysis['num_factura'].astype(str).str.strip()
erp_df['num_factura'] = erp_df['num_factura'].astype(str).str.strip()
facturas_en_erp_set = set(erp_df['num_factura'].unique())
email_analysis = email_analysis[~email_analysis['num_factura'].isin(facturas_en_erp_set)]

# --- Visualización profesional ---
if email_analysis.empty:
    st.balloons()
    st.success("✅ <b>¡Integridad Total!</b> Todas las facturas de los proveedores objetivo ya existen en el ERP.", unsafe_allow_html=True)
else:
    # KPIs
    total_faltante = email_analysis['valor_total_correo'].sum() if 'valor_total_correo' in email_analysis.columns else 0
    cant_facturas = len(email_analysis)
    top_prov = email_analysis['nombre_proveedor_correo'].mode()[0] if not email_analysis.empty else "N/A"

    st.markdown(
        f"""
        <div class="kpi-box">
            <div class="kpi-title">💰 Valor Total Faltante</div>
            <div class="kpi-value">${total_faltante:,.0f}</div>
            <div class="kpi-sub">No radicado en ERP</div>
        </div>
        <div class="kpi-box">
            <div class="kpi-title">📄 Facturas Faltantes</div>
            <div class="kpi-value">{cant_facturas}</div>
            <div class="kpi-sub">Documentos únicos</div>
        </div>
        <div class="kpi-box">
            <div class="kpi-title">🏆 Proveedor con más pendientes</div>
            <div class="kpi-value">{top_prov}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.divider()
    st.subheader("📋 Detalle de Facturas Faltantes en ERP")
    display_cols = [
        'nombre_proveedor_correo', 'num_factura', 'valor_total_correo',
        'fecha_emision_correo', 'fecha_vencimiento_correo'
    ]
    display_cols = [c for c in display_cols if c in email_analysis.columns]
    st.dataframe(
        email_analysis[display_cols].sort_values(by='nombre_proveedor_correo'),
        use_container_width=True,
        hide_index=True,
        column_config={
            "nombre_proveedor_correo": "Proveedor",
            "num_factura": "N° Factura",
            "valor_total_correo": st.column_config.NumberColumn("Valor", format="$ %d"),
            "fecha_emision_correo": st.column_config.DateColumn("Emitida", format="YYYY-MM-DD"),
            "fecha_vencimiento_correo": st.column_config.DateColumn("Vence", format="YYYY-MM-DD"),
        }
    )