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
from datetime import datetime
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
st.markdown("Conciliación automática entre tu cartera y las facturas recibidas por correo electrónico.")

# --- SUBIDA DE FACTURAS DEL PROVEEDOR (DESDE CORREO) ---
uploaded_file = st.file_uploader("Sube el listado de facturas del proveedor (Excel/CSV exportado del correo)", type=["xlsx", "csv"])
if uploaded_file:
    if uploaded_file.name.endswith(".xlsx"):
        df_proveedor = pd.read_excel(uploaded_file)
    else:
        df_proveedor = pd.read_csv(uploaded_file)
    st.success("Listado de facturas del proveedor cargado correctamente.")

    # --- CONCILIACIÓN ---
    def conciliar_cartera(df_sistema, df_proveedor):
        # Normalizar columnas clave
        df_sistema['num_factura'] = df_sistema['num_factura'].astype(str).str.strip()
        df_proveedor['num_factura'] = df_proveedor['num_factura'].astype(str).str.strip()
        merged = pd.merge(
            df_proveedor, df_sistema, on='num_factura', how='left', suffixes=('_proveedor', '_sistema'), indicator=True
        )
        merged['estado_conciliacion'] = np.where(
            merged['_merge'] == 'both',
            'Conciliada',
            'Falta en Sistema'
        )
        return merged

    resultado_conciliacion = conciliar_cartera(df_full, df_proveedor)
    st.dataframe(resultado_conciliacion, use_container_width=True)

    # --- MENSAJE DE CONCILIACIÓN ---
    facturas_faltantes = resultado_conciliacion[resultado_conciliacion['estado_conciliacion'] == 'Falta en Sistema']
    if not facturas_faltantes.empty:
        lista_faltantes = "\n".join(
            f"- Factura: {row['num_factura']} | Valor: {row.get('valor_total_proveedor', 'N/A')}" for _, row in facturas_faltantes.iterrows()
        )
        mensaje_conciliacion = (
            "Estimado proveedor,\n\n"
            "Tras la revisión de nuestra cartera y su estado de cuenta, encontramos las siguientes facturas que aún no aparecen registradas en nuestro sistema:\n"
            f"{lista_faltantes}\n\n"
            "Por favor, confirme si estas facturas ya fueron enviadas o si requieren reenvío.\n\n"
            "Gracias por su colaboración.\nFERREINOX S.A.S. BIC"
        )
    else:
        mensaje_conciliacion = (
            "Estimado proveedor,\n\n"
            "Tras la revisión de nuestra cartera y su estado de cuenta, confirmamos que todas las facturas están conciliadas.\n\n"
            "Gracias por su colaboración.\nFERREINOX S.A.S. BIC"
        )

    st.markdown("#### ✉️ Mensaje de Conciliación para Enviar")
    st.code(mensaje_conciliacion, language="text")

    # --- ENVÍO DE CORREO ---
    proveedor_email = st.text_input("Correo del proveedor para conciliación")
    if st.button("📧 Enviar conciliación por correo", disabled=not proveedor_email):
        try:
            msg = MIMEText(mensaje_conciliacion)
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
    mensaje_wsp = urllib.parse.quote(mensaje_conciliacion)
    if telefono_proveedor:
        url_wsp = f"https://wa.me/{telefono_proveedor}?text={mensaje_wsp}"
        st.link_button("📲 Enviar conciliación por WhatsApp", url_wsp, use_container_width=True)
else:
    st.info("Por favor, sube el estado de cuenta del proveedor para iniciar la conciliación.")
