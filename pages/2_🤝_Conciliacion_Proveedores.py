# -*- coding: utf-8 -*-
"""
Conciliación de Proveedores (Versión 5.1 - Intelligence Edition)

Objetivo: Conciliar facturas de proveedores objetivo entre correo electrónico, cartera (ICG) y ERP, automatizando el envío de correos para resolver discrepancias.

Reglas clave:
- Si una factura está en el correo y no en la cartera activa (ICG), se asume que ya fue pagada o no es relevante para conciliación.
- Si una factura está en el correo, está en cartera activa, pero no en el ERP, y tiene entre 5 y 8 días de antigüedad, se alerta que la mercancía no ha llegado y se envía correo automático.
- Si una factura está en el ERP y no en el correo, y pasan 5 días sin conciliar, se asume que el documento electrónico nunca llegó y se envía correo automático.
- Si una factura está en el correo y tiene más de 15 días, pero ya no está en cartera activa, se asume que fue pagada y no requiere acción.

Esta página muestra solo información de proveedores definidos en PROVEDORES_CORREO.xlsx y permite enviar correos de conciliación de forma clara y guiada.
"""

import streamlit as st
import pandas as pd
import os
from datetime import datetime, timedelta
from common.utils import COLOMBIA_TZ

st.title("🤝 Conciliación de Proveedores - Facturas y Documentos Electrónicos")

st.markdown(
    """
    <b>Conciliación automática de facturas de proveedores objetivo.</b><br>
    <ul>
    <li><b>Facturas en correo y en cartera activa, pero no en ERP:</b> Si tienen entre 5 y 8 días, se alerta que la mercancía no ha llegado y se envía correo automático.</li>
    <li><b>Facturas en ERP y no en correo:</b> Si pasan 5 días sin conciliar, se alerta que el documento electrónico no llegó y se envía correo automático.</li>
    <li><b>Facturas en correo pero no en cartera activa:</b> Se asume que ya fueron pagadas y no requieren acción.</li>
    </ul>
    Solo se muestran y procesan proveedores definidos en <code>PROVEDORES_CORREO.xlsx</code>.
    """,
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

    import smtplib
    from email.mime.text import MIMEText
col_prov = next((c for c in df_proveedores_obj.columns if 'proveedor' in c.lower() or 'nombre' in c.lower()), None)
if not col_prov:
    st.error("El archivo debe tener una columna identificable como 'Proveedor'.")
    st.stop()

lista_objetivo_raw = df_proveedores_obj[col_prov].dropna().unique().tolist()
lista_objetivo_norm = [normalizar_texto(p) for p in lista_objetivo_raw]
st.info("Solo se muestran y procesan proveedores objetivo definidos en PROVEDORES_CORREO.xlsx.")

# --- Cargar datos de facturas del correo, cartera y ERP desde sesión ---
email_df = st.session_state.get("email_df", pd.DataFrame()).copy()
erp_df = st.session_state.get("erp_df", pd.DataFrame()).copy()
cartera_df = st.session_state.get("cartera_df", pd.DataFrame()).copy()  # cartera activa ICG

if email_df.empty or erp_df.empty or cartera_df.empty:
    st.warning("No hay datos de correo, ERP o cartera cargados. Realiza la sincronización desde el Dashboard General.")
    st.stop()

# --- Normalización y filtrado por proveedores objetivo ---
email_df['nombre_proveedor_correo'] = email_df['nombre_proveedor_correo'].astype(str).apply(normalizar_texto)
erp_df['nombre_proveedor_erp'] = erp_df['nombre_proveedor_erp'].astype(str).apply(normalizar_texto)
cartera_df['nombre_proveedor_erp'] = cartera_df['nombre_proveedor_erp'].astype(str).apply(normalizar_texto)

    # --- Correos para facturas en correo y cartera activa, pero no en ERP ---
email_df = email_df[email_df['nombre_proveedor_correo'].isin(lista_objetivo_norm)]
erp_df = erp_df[erp_df['nombre_proveedor_erp'].isin(lista_objetivo_norm)]
cartera_df = cartera_df[cartera_df['nombre_proveedor_erp'].isin(lista_objetivo_norm)]

# --- Cruce principal: Facturas en correo, cartera y ERP ---
# 1. Facturas en correo y en cartera activa, pero no en ERP (posible mercancía no llegada)
cruce_1 = email_df.merge(cartera_df, left_on=['num_factura', 'nombre_proveedor_correo'], right_on=['num_factura', 'nombre_proveedor_erp'], how='inner', suffixes=('_correo', '_cartera'))
cruce_1 = cruce_1[~cruce_1['num_factura'].isin(erp_df['num_factura'])]
cruce_1['dias_desde_emision'] = (datetime.now(COLOMBIA_TZ) - pd.to_datetime(cruce_1['fecha_emision_correo'])).dt.days
cruce_1_alerta = cruce_1[(cruce_1['dias_desde_emision'] >= 5) & (cruce_1['dias_desde_emision'] <= 8)]

# 2. Facturas en ERP y no en correo (posible documento electrónico no recibido)
cruce_2 = erp_df[~erp_df['num_factura'].isin(email_df['num_factura'])]
cruce_2['dias_desde_emision'] = (datetime.now(COLOMBIA_TZ) - pd.to_datetime(cruce_2['fecha_emision_erp'])).dt.days
cruce_2_alerta = cruce_2[cruce_2['dias_desde_emision'] >= 5]

# 3. Facturas en correo pero no en cartera activa (probablemente ya pagadas)
cruce_3 = email_df[~email_df['num_factura'].isin(cartera_df['num_factura'])]
cruce_3['dias_desde_emision'] = (datetime.now(COLOMBIA_TZ) - pd.to_datetime(cruce_3['fecha_emision_correo'])).dt.days
cruce_3_pagadas = cruce_3[cruce_3['dias_desde_emision'] > 15]
    # --- Correos para facturas en ERP y no en correo ---

# --- Visualización y acciones ---
st.subheader("📋 Facturas en correo y cartera activa, pero no en ERP (Mercancía no llegada)")
st.dataframe(cruce_1_alerta[['nombre_proveedor_correo', 'num_factura', 'valor_total_correo', 'fecha_emision_correo', 'dias_desde_emision']], use_container_width=True)

st.subheader("📋 Facturas en ERP y no en correo (Documento electrónico no recibido)")
st.dataframe(cruce_2_alerta[['nombre_proveedor_erp', 'num_factura', 'valor_total_erp', 'fecha_emision_erp', 'dias_desde_emision']], use_container_width=True)

st.subheader("📋 Facturas en correo pero no en cartera activa (Probablemente ya pagadas)")
st.dataframe(cruce_3_pagadas[['nombre_proveedor_correo', 'num_factura', 'valor_total_correo', 'fecha_emision_correo', 'dias_desde_emision']], use_container_width=True)

# --- Envío de correos automáticos para conciliación ---
# Aquí puedes agregar botones y lógica para enviar correos automáticos según cada caso de alerta.
# Ejemplo:
# for idx, row in cruce_1_alerta.iterrows():
#     st.button(f"Enviar correo a {row['nombre_proveedor_correo']} por factura {row['num_factura']}")
# ...
