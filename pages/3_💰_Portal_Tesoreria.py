# -*- coding: utf-8 -*-
"""
Módulo de Portal de Tesorería (Versión 4.0 - Mejorado + Cruce Provedores).

Esta página es para el uso exclusivo del equipo de Tesorería. 
Funcionalidades principales:
1. CRUCE DE FACTURACIÓN: Identificar facturas de proveedores clave (definidos en Excel)
   que llegaron al correo pero no están en el ERP.
2. GESTIÓN DE PAGOS: Visualizar y pagar lotes generados por Gerencia.
"""

# --- 0. IMPORTACIÓN DE LIBRERÍAS ---
import streamlit as st
import pandas as pd
import gspread
import io
import pytz
from google.oauth2.service_account import Credentials
import os

# ======================================================================================
# --- INICIO DEL BLOQUE DE SEGURIDAD ---
# ======================================================================================

# 1. Se asegura de que la variable de sesión exista.
if 'password_correct' not in st.session_state:
    st.session_state['password_correct'] = False

# 2. Verifica si la contraseña es correcta.
if not st.session_state["password_correct"]:
    st.error("🔒 Debes iniciar sesión para acceder a esta página.")
    st.info("Por favor, ve a la página principal 'Dashboard General' para ingresar la contraseña.")
    st.stop()

# --- FIN DEL BLOQUE DE SEGURIDAD ---


# --- INICIO: Lógica de conexión y utilidades ---

COLOMBIA_TZ = pytz.timezone('America/Bogota')
GSHEET_REPORT_NAME = "ReporteConsolidado_Activo"
GSHEET_LOTES_NAME = "Historial_Lotes_Pago"
PENDING_STATUSES = ["Pendiente de Pago", "Pendiente de Pago URGENTE"]

@st.cache_resource(show_spinner="Conectando a Google Sheets...")
def connect_to_google_sheets() -> gspread.Client:
    """Establece la conexión con la API de Google Sheets de forma segura."""
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds_dict = st.secrets["google_credentials"]
        if creds_dict.get("private_key_id") is None:
            creds_dict.pop("private_key_id", None)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ Error crítico al autenticar con Google Sheets: {e}")
        return None

def to_excel(df: pd.DataFrame) -> bytes:
    """Convierte un DataFrame a un archivo Excel en memoria."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Detalle_Pago')
        for column in df:
            try:
                # Calcular ancho máximo
                col_len = max(df[column].astype(str).map(len).max(), len(column)) + 2
                col_idx = df.columns.get_loc(column)
                writer.sheets['Detalle_Pago'].set_column(col_idx, col_idx, col_len)
            except:
                pass 
    return output.getvalue()

@st.cache_data(ttl=300, show_spinner="Cargando Lotes de Pago...")
def load_data(_gs_client: gspread.Client):
    """Carga los lotes y las facturas desde Google Sheets."""
    try:
        spreadsheet = _gs_client.open_by_key(st.secrets["google_sheet_id"])
        
        # Cargar historial de lotes
        historial_ws = spreadsheet.worksheet(GSHEET_LOTES_NAME)
        df_lotes = pd.DataFrame(historial_ws.get_all_records())
        
        # Cargar reporte consolidado
        reporte_ws = spreadsheet.worksheet(GSHEET_REPORT_NAME)
        reporte_data = reporte_ws.get_all_values()
        
        if len(reporte_data) < 2:
            return df_lotes, pd.DataFrame()

        # Normalizar encabezados
        reporte_headers = [str(h).strip().lower().replace(' ', '_') for h in reporte_data[0]]
        df_reporte = pd.DataFrame(reporte_data[1:], columns=reporte_headers)

        if 'nombre_proveedor_erp' in df_reporte.columns:
            df_reporte.rename(columns={'nombre_proveedor_erp': 'nombre_proveedor'}, inplace=True)
            
        # Limpiar duplicados de columnas
        df_reporte = df_reporte.loc[:, ~df_reporte.columns.duplicated(keep='first')]

        return df_lotes, df_reporte
        
    except Exception as e:
        st.error(f"Error al cargar datos de Sheets: {e}")
        return pd.DataFrame(), pd.DataFrame()

def procesar_pago_lote(gs_client: gspread.Client, lote_id: str, df_reporte: pd.DataFrame):
    """Actualiza estados a 'Pagado'."""
    try:
        spreadsheet = gs_client.open_by_key(st.secrets["google_sheet_id"])
        historial_ws = spreadsheet.worksheet(GSHEET_LOTES_NAME)
        
        # 1. Actualizar Lote
        cell = historial_ws.find(lote_id)
        if not cell:
            st.error("No se encontró el ID del lote.")
            return False
        
        headers = historial_ws.row_values(1)
        estado_col = headers.index('estado_lote') + 1
        historial_ws.update_cell(cell.row, estado_col, 'Pagado')

        # 2. Actualizar Facturas
        facturas_lote = df_reporte[df_reporte['id_lote_pago'] == lote_id]
        if facturas_lote.empty:
            return True # Se pagó el lote (quizás vacío), éxito parcial

        reporte_ws = spreadsheet.worksheet(GSHEET_REPORT_NAME)
        rep_headers = reporte_ws.row_values(1)
        estado_fac_col = rep_headers.index('estado_factura') + 1
        
        updates = []
        for index, _ in facturas_lote.iterrows():
            row_idx = int(index) + 2
            updates.append({
                'range': gspread.utils.rowcol_to_a1(row_idx, estado_fac_col),
                'values': [['Pagada']]
            })
            
        if updates:
            reporte_ws.batch_update(updates)
            
        return True
    except Exception as e:
        st.error(f"Error al procesar pago: {e}")
        return False

# --- Funciones de Normalización para el Cruce ---
def normaliza_columna(col_name):
    """Limpia nombres de columnas: minúsculas, sin espacios, sin tildes."""
    return (
        col_name.strip()
        .lower()
        .replace('á', 'a').replace('é', 'e').replace('í', 'i')
        .replace('ó', 'o').replace('ú', 'u')
        .replace('ñ', 'n').replace(' ', '_')
        .replace('.', '')
    )

def normalizar_texto(texto):
    """Limpia texto de celdas para comparaciones."""
    if pd.isna(texto): return ""
    return str(texto).strip().upper()

# --- INICIO DE LA APLICACIÓN ---
st.title("💰 Portal de Tesorería")

# ==============================================================================
# SECCIÓN 1: CRUCE DE FACTURACIÓN (Lógica Solicitada)
# ==============================================================================
st.header("1. Verificación de Facturas Faltantes (Cruce Proveedores)")
st.markdown("Cruce entre facturas recibidas en **Correo** vs registradas en **ERP**, filtrado exclusivamente por tu archivo de **Proveedores Objetivo**.")

# 1. Cargar archivo de proveedores
archivo_proveedores = "PROVEDORES_CORREO.xlsx"

if not os.path.exists(archivo_proveedores):
    st.error(f"⚠️ No se encontró el archivo '{archivo_proveedores}' en la raíz. Por favor cárgalo.")
else:
    try:
        # Cargar Excel
        df_proveedores_obj = pd.read_excel(archivo_proveedores)
        
        # Normalizar columnas del Excel para evitar errores si cambian los nombres
        df_proveedores_obj.columns = [normaliza_columna(c) for c in df_proveedores_obj.columns]
        
        # Identificar la columna del nombre del proveedor
        col_prov = next((c for c in df_proveedores_obj.columns if 'proveedor' in c or 'nombre' in c), None)
        
        if not col_prov:
            st.error("El archivo de proveedores debe tener una columna llamada 'Proveedor' o 'Nombre'.")
        else:
            # 2. Obtener datos de Sesión (Correo y ERP)
            email_df = st.session_state.get("email_df", pd.DataFrame())
            erp_df = st.session_state.get("erp_df", pd.DataFrame())

            if email_df.empty or erp_df.empty:
                st.warning("⚠️ No hay datos cargados de Correo o ERP. Realiza la sincronización en el Dashboard General primero.")
            else:
                # 3. Preparar Listas para el Cruce
                
                # Lista de proveedores objetivo (desde el Excel) normalizada
                lista_proveedores_obj = df_proveedores_obj[col_prov].apply(normalizar_texto).unique().tolist()
                
                # Crear copias para no afectar los dataframes originales de la sesión
                email_analysis = email_df.copy()
                erp_analysis = erp_df.copy()
                
                # Asegurar columnas necesarias en Email
                if 'nombre_proveedor_correo' not in email_analysis.columns:
                    st.error("El DataFrame de correo no tiene la columna 'nombre_proveedor_correo'.")
                else:
                    # Normalizar nombres en el DF de correo
                    email_analysis['proveedor_norm'] = email_analysis['nombre_proveedor_correo'].apply(normalizar_texto)
                    
                    # 4. FILTRADO: Quedarnos SOLO con los proveedores del Excel
                    email_filtrado = email_analysis[email_analysis['proveedor_norm'].isin(lista_proveedores_obj)]
                    
                    if email_filtrado.empty:
                        st.info("ℹ️ No se encontraron facturas en el correo de los proveedores listados en el Excel.")
                    else:
                        # 5. EL CRUCE: Comparar facturas (usando número de factura)
                        
                        # Normalizar número de factura en ambos lados (quitar espacios, asegurar string)
                        email_filtrado['num_factura_clean'] = email_filtrado['num_factura'].astype(str).str.strip()
                        erp_analysis['num_factura_clean'] = erp_analysis['num_factura'].astype(str).str.strip()
                        
                        # Identificar facturas que están en Correo (filtrado) pero NO en ERP
                        facturas_en_erp = erp_analysis['num_factura_clean'].tolist()
                        
                        df_faltantes = email_filtrado[~email_filtrado['num_factura_clean'].isin(facturas_en_erp)].copy()
                        
                        # 6. MOSTRAR RESULTADOS
                        if df_faltantes.empty:
                            st.success("✅ ¡Todo al día! Todas las facturas de tus proveedores objetivo ya están en el ERP.")
                        else:
                            st.warning(f"🚨 Se encontraron **{len(df_faltantes)}** facturas en el correo que FALTAN en el ERP.")
                            
                            # Seleccionar columnas relevantes para mostrar
                            cols_to_show = [
                                'nombre_proveedor_correo', 
                                'num_factura', 
                                'valor_total_correo', 
                                'fecha_emision_correo', 
                                'asunto_correo'
                            ]
                            # Filtrar solo columnas que existan
                            cols_final = [c for c in cols_to_show if c in df_faltantes.columns]
                            
                            st.dataframe(
                                df_faltantes[cols_final],
                                use_container_width=True,
                                hide_index=True
                            )
                            
                            # Botón de descarga para el reporte de faltantes
                            st.download_button(
                                label="📥 Descargar Reporte de Faltantes (Excel)",
                                data=to_excel(df_faltantes[cols_final]),
                                file_name="Facturas_Faltantes_ERP.xlsx",
                                mime="application/vnd.ms-excel"
                            )

    except Exception as e:
        st.error(f"Error al procesar el archivo de proveedores o realizar el cruce: {e}")

st.divider()

# ==============================================================================
# SECCIÓN 2: GESTIÓN DE LOTES DE PAGO (Funcionalidad Original Tesorería)
# ==============================================================================
st.header("2. Gestión de Lotes de Pago")

gs_client = connect_to_google_sheets()
if gs_client:
    df_lotes, df_reporte = load_data(gs_client)

    # Filtrar lotes pendientes
    if not df_lotes.empty:
        df_pendientes = df_lotes[df_lotes['estado_lote'].isin(PENDING_STATUSES)].copy()
        
        # Ordenar (Urgentes primero)
        df_pendientes['es_urgente'] = df_pendientes['estado_lote'].apply(lambda x: 'URGENTE' in str(x))
        df_pendientes.sort_values(by=['es_urgente', 'fecha_creacion'], ascending=[False, False], inplace=True)

        if df_pendientes.empty:
            st.info("No hay lotes pendientes de pago.")
        else:
            st.markdown("Selecciona un lote para procesar su pago.")
            
            # Mostrar tabla resumen
            st.dataframe(
                df_pendientes[['id_lote', 'estado_lote', 'fecha_creacion', 'num_facturas', 'total_pagado_lote']],
                use_container_width=True, 
                hide_index=True
            )
            
            # Selector de Lote
            lote_id = st.selectbox("Seleccionar Lote:", df_pendientes['id_lote'].unique())
            
            if lote_id:
                lote_data = df_pendientes[df_pendientes['id_lote'] == lote_id].iloc[0]
                facturas_lote = df_reporte[df_reporte['id_lote_pago'] == lote_id].copy()
                
                st.subheader(f"Detalle Lote: {lote_id}")
                c1, c2 = st.columns(2)
                c1.metric("Total a Pagar", f"$ {float(lote_data.get('total_pagado_lote', 0)):,.0f}")
                c2.metric("Facturas", lote_data.get('num_facturas', 0))
                
                st.dataframe(facturas_lote, use_container_width=True)
                
                # Acciones
                col_btn1, col_btn2 = st.columns([1, 1])
                with col_btn1:
                    st.download_button(
                        "📄 Descargar Soporte Excel",
                        data=to_excel(facturas_lote),
                        file_name=f"Pago_{lote_id}.xlsx"
                    )
                with col_btn2:
                    if st.button("✅ Confirmar Pago Realizado", type="primary"):
                        if procesar_pago_lote(gs_client, lote_id, df_reporte):
                            st.success(f"Lote {lote_id} marcado como PAGADO.")
                            st.cache_data.clear()
                            st.rerun()