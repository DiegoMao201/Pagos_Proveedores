# -*- coding: utf-8 -*-
"""
Centro de Control de Pagos Inteligente para FERREINOX (Versión 4.1 - Módulo Gerencia).

Este módulo permite a Gerencia crear lotes de pago para facturas vigentes, vencidas
y aplicar notas crédito en un solo flujo.

Mejoras en v4.1:
- Integración de Notas Crédito en el 'Plan de Pagos (Vigentes)' para selección y cruce.
- Aplicación consistente del filtro de proveedor a las pestañas de 'Vigentes' y 'Notas Crédito'.
- Funcionalidad para descargar el listado de Notas Crédito filtradas a un archivo Excel.
- Aclaraciones en la interfaz para mejorar la usabilidad y entendimiento de cada sección.
"""

# --- 0. IMPORTACIÓN DE LIBRERÍAS ---
import streamlit as st
import pandas as pd
import numpy as np
import smtplib
from email.mime.text import MIMEText
import urllib.parse

# --- SEGURIDAD ---
if 'password_correct' not in st.session_state:
    st.session_state['password_correct'] = False
if not st.session_state["password_correct"]:
    st.error("🔒 Debes iniciar sesión para acceder a esta página.")
    st.info("Por favor, ve a la página principal 'Dashboard General' para ingresar la contraseña.")
    st.stop()

# --- CARGA DE DATOS ---
from common.utils import connect_to_google_sheets, load_data_from_gsheet

COLOMBIA_TZ = "America/Bogota"
GSHEET_REPORT_NAME = "ReporteConsolidado_Activo"

gs_client = connect_to_google_sheets()
df_full = load_data_from_gsheet(gs_client)
if df_full.empty:
    st.warning("No hay datos de cartera cargados.")
    st.stop()

# --- UI PRINCIPAL ---
st.title("🤝 Centro de Conciliación de Cuentas con Proveedores")
st.markdown("Conciliación automática entre tu cartera (ERP) y las facturas recibidas por correo electrónico.")

# --- CARGA DE DATOS CONCILIADOS ---
st.markdown("### Carga de datos conciliados")
st.info("Usamos el DataFrame conciliado que ya existe en sesión.")

master_df = st.session_state.get("master_df", pd.DataFrame())
if master_df.empty:
    st.warning("No hay datos conciliados cargados. Realiza la sincronización desde el Dashboard General.")
    st.stop()

# Selección de proveedor
proveedores = sorted(master_df['nombre_proveedor'].dropna().unique())
proveedor_sel = st.selectbox("Selecciona el proveedor para conciliar:", proveedores)

df_prov = master_df[master_df['nombre_proveedor'] == proveedor_sel].copy()
if df_prov.empty:
    st.info("No hay facturas para este proveedor.")
    st.stop()

# Mostramos la conciliación
st.markdown("### Estado de Conciliación")
cols_to_show = [
    'num_factura', 'valor_total_erp', 'valor_total_correo',
    'fecha_emision_erp', 'fecha_vencimiento_erp', 'estado_conciliacion'
]
cols_to_show = [c for c in cols_to_show if c in df_prov.columns]
st.dataframe(df_prov[cols_to_show], use_container_width=True)

# Facturas solo en correo (faltan en ERP)
faltan_en_erp = df_prov[df_prov['estado_conciliacion'] == '📧 Solo en Correo']
# Facturas solo en ERP (pendiente de correo)
faltan_en_correo = df_prov[df_prov['estado_conciliacion'] == '📬 Pendiente de Correo']
# Discrepancias de valor
discrepancias = df_prov[df_prov['estado_conciliacion'] == '⚠️ Discrepancia de Valor']

# --- MENSAJE DE CONCILIACIÓN ---
mensaje = f"Estimado proveedor {proveedor_sel},\n\n"
if not faltan_en_erp.empty or not faltan_en_correo.empty or not discrepancias.empty:
    mensaje += "Tras la revisión de nuestra cartera y su estado de cuenta, encontramos lo siguiente:\n\n"
    if not faltan_en_erp.empty:
        mensaje += "Facturas que aparecen en su estado de cuenta pero NO en nuestro sistema:\n"
        for _, row in faltan_en_erp.iterrows():
            mensaje += f"- Factura: {row['num_factura']} | Valor: {row.get('valor_total_correo', 'N/A')}\n"
        mensaje += "\n"
    if not faltan_en_correo.empty:
        mensaje += "Facturas que aparecen en nuestro sistema pero NO en su estado de cuenta:\n"
        for _, row in faltan_en_correo.iterrows():
            mensaje += f"- Factura: {row['num_factura']} | Valor: {row.get('valor_total_erp', 'N/A')}\n"
        mensaje += "\n"
    if not discrepancias.empty:
        mensaje += "Facturas con discrepancia de valor:\n"
        for _, row in discrepancias.iterrows():
            mensaje += f"- Factura: {row['num_factura']} | Valor ERP: {row.get('valor_total_erp', 'N/A')} | Valor Correo: {row.get('valor_total_correo', 'N/A')}\n"
        mensaje += "\n"
    mensaje += "Por favor, confirme o envíe los documentos faltantes o aclare las diferencias.\n\n"
else:
    mensaje += "¡Todas las facturas están conciliadas correctamente!\n\n"
mensaje += "Gracias por su colaboración.\nFERREINOX S.A.S. BIC"

st.markdown("#### ✉️ Mensaje de Conciliación para Enviar")
st.code(mensaje, language="text")

# --- ENVÍO DE CORREO ---
proveedor_email = st.text_input("Correo del proveedor para conciliación")
if st.button("📧 Enviar conciliación por correo", disabled=not proveedor_email):
    try:
        msg = MIMEText(mensaje)
        msg['Subject'] = "Conciliación de Cartera FERREINOX"
        msg['From'] = st.secrets.email["address"]
        msg['To'] = proveedor_email
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(st.secrets.email["address"], st.secrets.email["password"])
            server.sendmail(msg['From'], [msg['To']], msg.as_string())
        st.success("Conciliación enviada por correo.")
    except Exception as e:
        st.error(f"Error al enviar correo: {e}")

# --- ENVÍO DE WHATSAPP ---
telefono_proveedor = st.text_input("Número WhatsApp del proveedor (solo números, con código país)")
mensaje_wsp = urllib.parse.quote(mensaje)
if telefono_proveedor:
    url_wsp = f"https://wa.me/{telefono_proveedor}?text={mensaje_wsp}"
    st.link_button("📲 Enviar conciliación por WhatsApp", url_wsp, use_container_width=True)
