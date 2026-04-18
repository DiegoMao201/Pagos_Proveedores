# -*- coding: utf-8 -*-
"""Centro principal de control para cartera, correo y pagos a proveedores."""

from datetime import datetime

import pandas as pd
import streamlit as st

from common.treasury_core import (
    COLOMBIA_TZ,
    build_risk_alerts,
    connect_to_google_sheets,
    ensure_authenticated,
    format_currency,
    get_secret_value,
    load_operational_payload,
    safe_display,
    sync_treasury_data,
)


APP_PASSWORD = get_secret_value("password", "DEFAULT_PASSWORD")


st.set_page_config(
    page_title="Ferreinox BI | Centro Ejecutivo de Proveedores",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


def check_password() -> bool:
    if st.session_state.get("password_correct"):
        return True

    st.markdown(
        """
        <div style="background:linear-gradient(135deg,#0d2340 0%,#1c4e80 55%,#f3b221 100%);padding:26px 28px;border-radius:24px;color:white;margin-bottom:1rem;box-shadow:0 22px 56px rgba(13,35,64,.18);">
            <div style="font-size:.82rem;letter-spacing:.12em;text-transform:uppercase;opacity:.85;">Ferreinox BI</div>
            <div style="font-size:2.2rem;font-weight:800;line-height:1.05;margin-top:.35rem;">Centro Ejecutivo de Proveedores</div>
            <div style="margin-top:.8rem;max-width:720px;line-height:1.55;font-size:1rem;opacity:.95;">Tablero corporativo para tesoreria, conciliacion documental, priorizacion de descuentos y programacion profesional de pagos a proveedores.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    password = st.text_input("Contraseña", type="password")
    if st.button("Ingresar", type="primary", use_container_width=True):
        st.session_state["password_correct"] = password == APP_PASSWORD
        if not st.session_state["password_correct"]:
            st.error("Contraseña incorrecta.")
        st.rerun()
    return False


def inject_styles() -> None:
    st.markdown(
        """
        <style>
            :root {
                --fx-navy: #0d2340;
                --fx-blue: #1c4e80;
                --fx-red: #ef3737;
                --fx-gold: #f3b221;
                --fx-ink: #223548;
                --fx-soft: #eef3f8;
            }
            .main .block-container {
                padding-top: 1.2rem;
                padding-bottom: 2.8rem;
            }
            [data-testid="stSidebar"] {
                background: linear-gradient(180deg, #0a1a2f 0%, #102848 58%, #15365e 100%);
                border-right: 1px solid rgba(255,255,255,0.08);
            }
            [data-testid="stSidebar"] * {
                color: #f5f8fb;
            }
            [data-testid="stSidebar"] .stButton > button {
                background: linear-gradient(135deg, var(--fx-red) 0%, #ff6a3d 100%);
                color: white;
                border: 0;
                font-weight: 800;
                border-radius: 14px;
                box-shadow: 0 12px 28px rgba(239,55,55,.28);
            }
            [data-testid="stSidebar"] .stButton > button:hover {
                background: linear-gradient(135deg, #d82424 0%, #f08a16 100%);
                color: white;
            }
            .sidebar-shell {
                background: linear-gradient(180deg, rgba(255,255,255,.08) 0%, rgba(255,255,255,.03) 100%);
                border: 1px solid rgba(255,255,255,.09);
                border-radius: 22px;
                padding: 18px 16px;
                margin-bottom: 1rem;
                box-shadow: inset 0 1px 0 rgba(255,255,255,.08);
            }
            .sidebar-kicker {
                font-size: .72rem;
                letter-spacing: .14em;
                text-transform: uppercase;
                opacity: .72;
            }
            .sidebar-title {
                font-size: 1.35rem;
                font-weight: 800;
                line-height: 1.05;
                margin-top: .35rem;
            }
            .sidebar-copy {
                margin-top: .65rem;
                font-size: .92rem;
                line-height: 1.5;
                color: rgba(245,248,251,.82);
            }
            .sidebar-chip {
                display: inline-block;
                margin: 0 8px 8px 0;
                padding: 7px 10px;
                border-radius: 999px;
                font-size: .75rem;
                background: rgba(243,178,33,.16);
                border: 1px solid rgba(243,178,33,.22);
            }
            .hero-shell {
                background:
                    radial-gradient(circle at top right, rgba(243,178,33,.28), transparent 24%),
                    linear-gradient(135deg, #0d2340 0%, #1c4e80 50%, #ef3737 100%);
                border-radius: 32px;
                color: #ffffff;
                padding: 30px 34px;
                box-shadow: 0 28px 70px rgba(13, 35, 64, 0.22);
                margin-bottom: 1.2rem;
                position: relative;
                overflow: hidden;
            }
            .hero-kicker {
                text-transform: uppercase;
                letter-spacing: 0.12em;
                font-size: 0.82rem;
                font-weight: 700;
                opacity: 0.78;
            }
            .hero-title {
                font-size: 2.7rem;
                font-weight: 800;
                line-height: 1.02;
                margin: 0.35rem 0 0 0;
                max-width: 780px;
            }
            .hero-copy {
                font-size: 1.02rem;
                max-width: 880px;
                line-height: 1.55;
                margin-top: 0.9rem;
                opacity: 0.93;
            }
            .hero-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                gap: 0.8rem;
                margin-top: 1.1rem;
            }
            .hero-pill {
                background: rgba(255, 255, 255, 0.10);
                border: 1px solid rgba(255, 255, 255, 0.16);
                border-radius: 20px;
                padding: 1rem 1.05rem;
                backdrop-filter: blur(10px);
            }
            .hero-pill-label {
                font-size: 0.78rem;
                text-transform: uppercase;
                opacity: 0.74;
                margin-bottom: 0.22rem;
            }
            .hero-pill-value {
                font-size: 1.42rem;
                font-weight: 800;
            }
            .section-card {
                background: linear-gradient(180deg, #ffffff 0%, #fbfcfe 100%);
                border: 1px solid rgba(12, 45, 87, 0.08);
                border-radius: 24px;
                padding: 1.2rem 1.3rem;
                box-shadow: 0 14px 34px rgba(12, 45, 87, 0.06);
                margin-bottom: 1rem;
            }
            .section-title {
                color: var(--fx-navy);
                font-size: 1.08rem;
                font-weight: 800;
                margin-bottom: 0.25rem;
            }
            .section-copy {
                color: #5d7081;
                font-size: 0.93rem;
                margin-bottom: 0.9rem;
            }
            div[data-testid="metric-container"] {
                background: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
                border: 1px solid rgba(12,45,87,.08);
                border-radius: 20px;
                padding: .9rem 1rem;
                box-shadow: 0 10px 24px rgba(12,45,87,.05);
            }
            .overview-banner {
                background: linear-gradient(90deg, rgba(239,55,55,.09) 0%, rgba(243,178,33,.12) 100%);
                border: 1px solid rgba(239,55,55,.08);
                border-radius: 18px;
                padding: 14px 16px;
                margin-bottom: 1rem;
                color: var(--fx-ink);
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def payload_or_empty() -> dict:
    payload = load_operational_payload()
    payload.setdefault("master_df", pd.DataFrame())
    payload.setdefault("payment_plan_df", pd.DataFrame())
    payload.setdefault("risk_alerts_df", pd.DataFrame())
    payload.setdefault("provider_df", pd.DataFrame())
    payload.setdefault("email_history_df", pd.DataFrame())
    payload.setdefault("email_log_df", pd.DataFrame())
    payload.setdefault("lot_history_df", pd.DataFrame())
    payload.setdefault("pending_df", pd.DataFrame())
    payload.setdefault("paid_df", pd.DataFrame())
    payload.setdefault("sync_stats", {})
    payload.setdefault("has_snapshot", False)
    payload.setdefault("snapshot_rows", 0)
    payload.setdefault("snapshot_at", pd.NaT)
    payload.setdefault("snapshot_source", "sheets_cache")
    return payload


def summarize_operational_health(payload: dict) -> dict:
    master_df = payload["master_df"]
    provider_df = payload["provider_df"]
    plan_df = payload["payment_plan_df"]
    email_log_df = payload["email_log_df"]
    lot_history_df = payload["lot_history_df"]

    pending_rows = len(payload["pending_df"]) if not payload["pending_df"].empty else int((master_df.get("estado_erp", pd.Series(dtype=object)) == "Pendiente").sum())
    paid_rows = len(payload["paid_df"]) if not payload["paid_df"].empty else int((master_df.get("estado_erp", pd.Series(dtype=object)) == "Saldada").sum())
    providers_total = len(provider_df)
    active_providers = int(provider_df["activo"].fillna(True).sum()) if not provider_df.empty and "activo" in provider_df.columns else providers_total
    providers_with_payment_email = int(provider_df["email_pago"].fillna("").astype(str).str.strip().ne("").sum()) if not provider_df.empty and "email_pago" in provider_df.columns else 0
    providers_with_alert_email = int(provider_df["email_alertas"].fillna("").astype(str).str.strip().ne("").sum()) if not provider_df.empty and "email_alertas" in provider_df.columns else 0
    lots_registered = len(lot_history_df)
    emails_sent = int((email_log_df["estado_envio"].astype(str) == "Enviado").sum()) if not email_log_df.empty and "estado_envio" in email_log_df.columns else 0
    discount_amount = plan_df["valor_descuento"].sum() if not plan_df.empty else 0

    return {
        "pending_rows": pending_rows,
        "paid_rows": paid_rows,
        "providers_total": providers_total,
        "active_providers": active_providers,
        "providers_with_payment_email": providers_with_payment_email,
        "providers_with_alert_email": providers_with_alert_email,
        "lots_registered": lots_registered,
        "emails_sent": emails_sent,
        "discount_amount": discount_amount,
    }


def display_sidebar(payload: dict) -> None:
    with st.sidebar:
        st.image("LOGO FERREINOX SAS BIC 2024.png")
        st.markdown(
            """
            <div class="sidebar-shell">
                <div class="sidebar-kicker">Ferreinox BI</div>
                <div class="sidebar-title">Panel Ejecutivo de Proveedores</div>
                <div class="sidebar-copy">Conciliacion documental, descuentos de pronto pago, alertas de mora y lotes de comunicacion profesional en un solo centro operativo.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if st.button("🔄 Actualizar ahora", type="primary", use_container_width=True):
            result = sync_treasury_data()
            if result:
                st.rerun()

        snapshot_at = payload.get("snapshot_at")
        if payload.get("has_snapshot"):
            snapshot_label = snapshot_at.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(snapshot_at) else "fecha no disponible"
            st.success(f"Mostrando última foto guardada: {snapshot_label}")
            st.caption(
                f"Consulta inmediata sobre {payload.get('snapshot_rows', 0):,} registros guardados. Actualizar ahora solo trae novedades desde correo y Dropbox."
            )

        if st.session_state.get("last_treasury_sync"):
            st.success(f"Última sincronización: {st.session_state['last_treasury_sync']}")

        sync_stats = payload.get("sync_stats", {})
        if sync_stats:
            st.caption(
                " | ".join(
                    [
                        f"Correos: {sync_stats.get('emails_found', 0)}",
                        f"Adjuntos: {sync_stats.get('attachments_scanned', 0)}",
                        f"XML: {sync_stats.get('xml_files_scanned', 0)}",
                        f"Facturas detectadas: {sync_stats.get('invoice_rows_detected', 0)}",
                    ]
                )
            )
            st.caption(
                f"Sincronizacion incremental desde {sync_stats.get('started_from', payload.get('sync_started_from', 'inicio del ano'))}. El sistema relee una ventana corta para evitar omisiones y consolida sin duplicar."
            )
        else:
            if payload.get("has_snapshot"):
                st.caption("La app abre con la última foto guardada en Google Sheets. No necesitas sincronizar para consultar; usa actualizar solo cuando quieras traer novedades.")
            else:
                st.caption("Todavía no existe una foto guardada en Google Sheets. La primera actualización crea esa base; después la consulta ya será inmediata.")

        st.divider()
        st.markdown("**Fuentes activas**")
        st.markdown('<span class="sidebar-chip">Dropbox · cartera pendiente</span>', unsafe_allow_html=True)
        st.markdown('<span class="sidebar-chip">Dropbox · cartera saldada</span>', unsafe_allow_html=True)
        st.markdown('<span class="sidebar-chip">Gmail · XML y ZIP</span>', unsafe_allow_html=True)
        st.markdown('<span class="sidebar-chip">Google Sheets · trazabilidad</span>', unsafe_allow_html=True)
        st.divider()
        st.markdown("**Criterio ejecutivo**")
        st.write("Prioriza descuentos altos sin perder vencimientos.")
        st.write("Escala riesgo de mora dentro de 48 horas.")
        st.write("Registra cada lote y cada correo enviado.")


def hero_section(payload: dict) -> None:
    master_df = payload["master_df"]
    plan_df = payload["payment_plan_df"]
    alerts_df = payload["risk_alerts_df"]

    pending_amount = master_df.loc[master_df["estado_erp"] == "Pendiente", "valor_erp"].sum() if not master_df.empty else 0
    paid_amount = master_df.loc[master_df["estado_erp"] == "Saldada", "valor_erp"].sum() if not master_df.empty else 0
    potential_savings = plan_df["valor_descuento"].sum() if not plan_df.empty else 0
    providers_count = master_df["proveedor"].nunique() if not master_df.empty else 0

    st.markdown(
        f"""
        <div class="hero-shell">
            <div class="hero-kicker">Ferreinox BI · sociedades BIC · 2026</div>
            <h1 class="hero-title">Centro Ejecutivo de Pagos, Conciliación y Descuentos a Proveedores</h1>
            <div class="hero-copy">La plataforma integra cartera pendiente, cartera saldada, XML recibidos por correo y reglas comerciales para decidir con claridad qué pagar primero, qué ahorro capturar, qué discrepancias resolver y qué comunicaciones emitir con trazabilidad total.</div>
            <div class="hero-grid">
                <div class="hero-pill"><div class="hero-pill-label">Pendiente actual</div><div class="hero-pill-value">{format_currency(pending_amount)}</div></div>
                <div class="hero-pill"><div class="hero-pill-label">Saldado identificado</div><div class="hero-pill-value">{format_currency(paid_amount)}</div></div>
                <div class="hero-pill"><div class="hero-pill-label">Ahorro capturable</div><div class="hero-pill-value">{format_currency(potential_savings)}</div></div>
                <div class="hero-pill"><div class="hero-pill-label">Proveedores monitoreados</div><div class="hero-pill-value">{providers_count:,}</div></div>
                <div class="hero-pill"><div class="hero-pill-label">Alertas 48h</div><div class="hero-pill-value">{len(alerts_df):,}</div></div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def kpi_row(payload: dict) -> None:
    master_df = payload["master_df"]
    plan_df = payload["payment_plan_df"]
    alerts_df = payload["risk_alerts_df"]
    due_now = master_df[(master_df["estado_erp"] == "Pendiente") & (master_df["estado_vencimiento"].isin(["🔴 Vencida", "🟠 Riesgo 48h"]))] if not master_df.empty else pd.DataFrame()
    only_email = master_df[master_df["estado_conciliacion"] == "Solo correo"] if not master_df.empty else pd.DataFrame()
    pending_without_email = master_df[master_df["estado_conciliacion"] == "Pendiente sin correo"] if not master_df.empty else pd.DataFrame()
    conciliated = master_df[master_df["estado_conciliacion"].isin(["Pendiente conciliada", "Saldada conciliada", "Pendiente anterior a lectura", "Saldada anterior a lectura"])] if not master_df.empty else pd.DataFrame()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Que debo pagar ya", format_currency(due_now["valor_erp"].sum() if not due_now.empty else 0), f"{len(due_now):,} facturas")
    c2.metric("Solo en correo", f"{len(only_email):,}")
    c3.metric("Pendiente sin correo", f"{len(pending_without_email):,}")
    c4.metric("Cartera conciliada", f"{len(conciliated):,}")

    if not plan_df.empty:
        st.caption(
            f"Pago sugerido actual: {format_currency(plan_df['valor_a_pagar'].sum())}. Ahorro capturable: {format_currency(plan_df['valor_descuento'].sum())}. Alertas 48h: {len(alerts_df):,}."
        )


def display_source_health(payload: dict) -> None:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Salud de fuentes</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-copy">Validación cruzada entre cartera pendiente, cartera saldada y correo procesado.</div>', unsafe_allow_html=True)
    master_df = payload["master_df"]
    summary = summarize_operational_health(payload)

    source_summary = pd.DataFrame(
        [
            {"Fuente": "Cartera pendiente Dropbox", "Registros": summary["pending_rows"], "Observacion": "Base de facturas por pagar del año o pendientes ya consolidadas en maestro."},
            {"Fuente": "Cartera saldada Dropbox", "Registros": summary["paid_rows"], "Observacion": "Facturas ya canceladas para evitar falsas alarmas y cruces errados."},
            {"Fuente": "Histórico correo proveedores", "Registros": len(payload["email_history_df"]), "Observacion": "Facturas XML y ZIP detectadas en el buzón objetivo."},
            {"Fuente": "Maestro facturas", "Registros": len(master_df), "Observacion": "Consolidado final para control y programación."},
            {"Fuente": "Trazabilidad lotes/correos", "Registros": summary["lots_registered"] + len(payload["email_log_df"]), "Observacion": "Histórico de lotes programados y evidencia de comunicación enviada."},
        ]
    )
    st.dataframe(source_summary, use_container_width=True, hide_index=True)

    coverage_df = pd.DataFrame(
        [
            {"Control": "Proveedores activos", "Valor": f"{summary['active_providers']:,} / {summary['providers_total']:,}", "Lectura": "Cobertura de proveedores considerados en la operación."},
            {"Control": "Con correo de pago", "Valor": f"{summary['providers_with_payment_email']:,}", "Lectura": "Permite emitir lotes al proveedor sin reprocesos manuales."},
            {"Control": "Con correo de alertas", "Valor": f"{summary['providers_with_alert_email']:,}", "Lectura": "Facilita escalar excepciones internas cuando hay riesgo."},
            {"Control": "Ahorro capturable", "Valor": format_currency(summary['discount_amount']), "Lectura": "Descuento estimado hoy según reglas vigentes de pronto pago."},
        ]
    )
    st.dataframe(coverage_df, use_container_width=True, hide_index=True)
    st.markdown('</div>', unsafe_allow_html=True)


def display_operational_focus(payload: dict) -> None:
    master_df = payload["master_df"]
    plan_df = payload["payment_plan_df"]
    alerts_df = payload["risk_alerts_df"]
    email_log_df = payload["email_log_df"]
    lot_history_df = payload["lot_history_df"]

    if master_df.empty:
        return

    left_col, right_col = st.columns([1.2, 1])

    with left_col:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Foco operativo inmediato</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-copy">Proveedores con mayor concentración de valor pendiente, riesgo o ahorro disponible.</div>', unsafe_allow_html=True)

        focus_df = master_df.groupby("proveedor", dropna=False).agg(
            Facturas_Pendientes=("estado_erp", lambda values: (pd.Series(values) == "Pendiente").sum()),
            Valor_Pendiente=("valor_erp", lambda values: values[master_df.loc[values.index, "estado_erp"] == "Pendiente"].sum()),
            Riesgo_48h=("riesgo_mora_48h", "sum"),
            Sin_Soporte=("estado_conciliacion", lambda values: (pd.Series(values) == "Pendiente sin correo").sum()),
        ).reset_index()

        if not plan_df.empty:
            savings_df = plan_df.groupby("proveedor", dropna=False)["valor_descuento"].sum().reset_index(name="Ahorro_Potencial")
            focus_df = focus_df.merge(savings_df, on="proveedor", how="left")
        else:
            focus_df["Ahorro_Potencial"] = 0.0

        focus_df["Ahorro_Potencial"] = focus_df["Ahorro_Potencial"].fillna(0.0)
        focus_df.sort_values(by=["Riesgo_48h", "Ahorro_Potencial", "Valor_Pendiente"], ascending=[False, False, False], inplace=True)
        st.dataframe(
            focus_df.head(12),
            use_container_width=True,
            hide_index=True,
            column_config={
                "Valor_Pendiente": st.column_config.NumberColumn("Valor pendiente", format="$ %d"),
                "Ahorro_Potencial": st.column_config.NumberColumn("Ahorro potencial", format="$ %d"),
            },
        )
        st.markdown('</div>', unsafe_allow_html=True)

    with right_col:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Trazabilidad reciente</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-copy">Última actividad de lotes, correos y alertas críticas que explican el estado actual.</div>', unsafe_allow_html=True)

        if not lot_history_df.empty:
            lot_preview = lot_history_df.copy().tail(8)
            st.dataframe(
                lot_preview[[col for col in ["lote_id", "proveedor", "num_factura", "fecha_programada_pago", "estado_lote"] if col in lot_preview.columns]],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("Todavía no hay lotes registrados en el histórico.")

        if not email_log_df.empty:
            email_preview = email_log_df.copy().tail(8)
            st.dataframe(
                email_preview[[col for col in ["fecha_envio", "proveedor", "email_destino", "estado_envio", "detalle_envio"] if col in email_preview.columns]],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("Aún no hay registros de correos enviados desde el planificador.")

        if not alerts_df.empty:
            st.caption(f"Alertas activas hoy: {len(alerts_df):,}. El equipo debería revisar primero las facturas vencidas y las que caen dentro de 48 horas.")
        st.markdown('</div>', unsafe_allow_html=True)


def display_master_overview(payload: dict) -> None:
    master_df = payload["master_df"]
    if master_df.empty:
        st.info("Todavía no hay una foto guardada de cartera para mostrar. Cuando se ejecute la primera actualización, la consulta posterior ya abrirá con esa base sin reprocesar todos los correos.")
        return

    st.markdown(
        """
        <div class="overview-banner">
            <strong>Vista de decisión diaria.</strong> La lectura se reduce a cuatro preguntas: qué pagar ya, qué llegó por correo y falta ingresar, qué sigue sin conciliar y qué cartera ya quedó bien cruzada.
        </div>
        """,
        unsafe_allow_html=True,
    )

    pay_now_df = master_df[(master_df["estado_erp"] == "Pendiente") & (master_df["estado_vencimiento"].isin(["🔴 Vencida", "🟠 Riesgo 48h", "🟡 Proxima a vencer"]))].copy()
    only_email_df = master_df[master_df["estado_conciliacion"] == "Solo correo"].copy()
    unresolved_df = master_df[master_df["estado_conciliacion"].isin(["Pendiente sin correo", "Pendiente con valor por revisar", "Saldada con valor por revisar", "Inconsistencia entre pendiente y saldada"])].copy()
    conciliated_df = master_df[master_df["estado_conciliacion"].isin(["Pendiente conciliada", "Saldada conciliada", "Pendiente anterior a lectura", "Saldada anterior a lectura"])] .copy()

    tab1, tab2, tab3, tab4 = st.tabs(["💸 Que Debo Pagar", "📨 Falta Ingresar", "⚠️ No Conciliado", "✅ Conciliado"])

    with tab1:
        if pay_now_df.empty:
            st.success("No hay cartera pendiente con vencimiento cercano en este momento.")
        else:
            st.dataframe(
                safe_display(pay_now_df, [
                    "proveedor",
                    "num_factura",
                    "estado_conciliacion",
                    "estado_vencimiento",
                    "valor_erp",
                    "valor_a_pagar",
                    "valor_descuento",
                    "detalle_conciliacion",
                ], sort_by=["estado_vencimiento", "fecha_vencimiento_erp", "proveedor"]),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "valor_erp": st.column_config.NumberColumn("Valor ERP", format="$ %d"),
                    "valor_a_pagar": st.column_config.NumberColumn("Valor a pagar", format="$ %d"),
                    "valor_descuento": st.column_config.NumberColumn("Ahorro", format="$ %d"),
                },
            )

    with tab2:
        if only_email_df.empty:
            st.success("No hay facturas que estén solo en correo sin aparecer en ERP.")
        else:
            st.dataframe(
                safe_display(only_email_df, [
                    "proveedor_correo",
                    "num_factura",
                    "valor_total_correo",
                    "fecha_emision_correo",
                    "fecha_recepcion_correo",
                    "remitente_correo",
                    "detalle_conciliacion",
                ], sort_by=["fecha_recepcion_correo", "proveedor_correo"], ascending=[False, True]),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "valor_total_correo": st.column_config.NumberColumn("Valor correo", format="$ %d"),
                    "fecha_emision_correo": st.column_config.DateColumn("Fecha factura", format="YYYY-MM-DD"),
                    "fecha_recepcion_correo": st.column_config.DatetimeColumn("Fecha correo", format="YYYY-MM-DD HH:mm"),
                },
            )

    with tab3:
        if unresolved_df.empty:
            st.success("No hay facturas con cruce pendiente o diferencia por revisar.")
        else:
            st.dataframe(
                safe_display(unresolved_df, [
                    "proveedor",
                    "num_factura",
                    "estado_erp",
                    "estado_conciliacion",
                    "valor_erp",
                    "valor_total_correo",
                    "diferencia_valor",
                    "detalle_conciliacion",
                ], sort_by=["proveedor", "num_factura"]),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "valor_erp": st.column_config.NumberColumn("Valor ERP", format="$ %d"),
                    "valor_total_correo": st.column_config.NumberColumn("Valor correo", format="$ %d"),
                    "diferencia_valor": st.column_config.NumberColumn("Diferencia", format="$ %d"),
                },
            )

    with tab4:
        if conciliated_df.empty:
            st.info("Aún no hay cartera conciliada para mostrar.")
        else:
            st.dataframe(
                safe_display(conciliated_df, [
                    "proveedor",
                    "num_factura",
                    "estado_erp",
                    "estado_conciliacion",
                    "valor_erp",
                    "valor_total_correo",
                    "detalle_conciliacion",
                ], sort_by=["proveedor", "num_factura"]),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "valor_erp": st.column_config.NumberColumn("Valor ERP", format="$ %d"),
                    "valor_total_correo": st.column_config.NumberColumn("Valor correo", format="$ %d"),
                },
            )


def main_app() -> None:
    inject_styles()
    ensure_authenticated()
    payload = payload_or_empty()
    display_sidebar(payload)
    hero_section(payload)
    kpi_row(payload)
    display_source_health(payload)
    display_operational_focus(payload)
    display_master_overview(payload)


if __name__ == "__main__":
    if check_password():
        main_app()