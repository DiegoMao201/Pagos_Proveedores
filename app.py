# 1_📈_Dashboard_General.py

# ... (MANTÉN TODAS LAS IMPORTACIONES Y FUNCIONES DESDE LA 0 HASTA LA 8)
# No necesitas cambiar nada en las funciones de backend (load_erp_data, fetch_new_invoices_from_email, etc.)

# ======================================================================================
# --- 9. APLICACIÓN PRINCIPAL (PUNTO DE ENTRADA) ---
# ======================================================================================

def main_app():
    """Función principal que construye y renderiza la interfaz de la aplicación."""
    load_css()
    # Pasa el DataFrame del estado de la sesión a la barra lateral
    master_df = st.session_state.get("master_df", pd.DataFrame())
    display_sidebar(master_df)

    st.title("Plataforma de Gestión Inteligente de Facturas")
    st.markdown("Bienvenido al centro de control de cuentas por pagar. **Esta es la página principal para actualizar los datos desde el correo y Dropbox.**")

    # Muestra un indicador de la última sincronización
    if 'last_sync_time' in st.session_state:
        st.success(f"Última sincronización completada a las: {st.session_state.last_sync_time}")

    if not st.session_state.data_loaded:
        st.info("👋 Presiona 'Sincronizar Todo' en la barra lateral para cargar y procesar los datos más recientes.")
        st.stop()

    # Usa el DataFrame filtrado desde el estado de la sesión
    filtered_df = st.session_state.get('filtered_df')
    if filtered_df is None or filtered_df.empty:
        st.warning("No hay datos que coincidan con los filtros seleccionados o no hay datos cargados.")
        st.stop()

    display_dashboard(filtered_df)

# --- MEJORA: Función de sincronización actualizada para guardar la hora ---
def run_full_sync():
    """Orquesta el proceso completo de sincronización de datos."""
    # ... (El interior de esta función permanece EXACTAMENTE IGUAL)
    # ...
    # Al final de la función, antes del st.balloons(), añade esto:
    st.session_state['last_sync_time'] = datetime.now(COLOMBIA_TZ).strftime('%Y-%m-%d %H:%M:%S')
    st.session_state.data_loaded = True
    st.balloons()


if __name__ == "__main__":
    initialize_session_state()
    if check_password():
        main_app()
