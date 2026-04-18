# -*- coding: utf-8 -*-
"""Planificador operativo de pagos con trazabilidad y correo SendGrid."""

from datetime import date

import pandas as pd
import streamlit as st

from common.treasury_core import (
    EMAIL_LOG_COLUMNS,
    PAYMENT_LOT_COLUMNS,
    build_email_log_row,
    build_payment_email_html,
    connect_to_google_sheets,
    create_payment_lot,
    ensure_authenticated,
    format_currency,
    load_operational_payload,
    register_email_log,
    register_payment_lot,
    send_email_via_sendgrid,
)


st.set_page_config(page_title="Planificador de Pagos | Ferreinox", page_icon="💵", layout="wide")
ensure_authenticated()


def inject_styles() -> None:
    st.markdown(
        """
        <style>
            [data-testid="stSidebar"] {
                background: linear-gradient(180deg, #0a1a2f 0%, #102848 58%, #15365e 100%);
                border-right: 1px solid rgba(255,255,255,.08);
            }
            [data-testid="stSidebar"] * { color: #f5f8fb; }
            .planner-hero {
                background: linear-gradient(135deg, #0d2340 0%, #1c4e80 55%, #f3b221 100%);
                color: white;
                padding: 26px 30px;
                border-radius: 26px;
                margin-bottom: 1rem;
                box-shadow: 0 20px 48px rgba(13,35,64,.18);
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def prepare_selection_df(provider_df: pd.DataFrame) -> pd.DataFrame:
    table_df = provider_df.copy()
    table_df["Seleccionar"] = False
    ordered_columns = [
        "Seleccionar",
        "invoice_key",
        "num_factura",
        "valor_erp",
        "descuento_pct",
        "valor_descuento",
        "valor_a_pagar",
        "fecha_vencimiento_erp",
        "fecha_limite_descuento",
        "motivo_pago",
        "email_pago",
        "email_cc",
    ]
    for column in ordered_columns:
        if column not in table_df.columns:
            table_df[column] = ""
    return table_df[ordered_columns]


def first_non_empty(series: pd.Series) -> str:
    valid_values = series.dropna().astype(str).str.strip()
    valid_values = valid_values[valid_values.ne("")]
    return valid_values.iloc[0] if not valid_values.empty else ""


def main() -> None:
    inject_styles()
    payload = load_operational_payload()
    plan_df = payload.get("payment_plan_df", pd.DataFrame())
    lot_history_df = payload.get("lot_history_df", pd.DataFrame())
    email_log_df = payload.get("email_log_df", pd.DataFrame())
    if plan_df.empty:
        st.title("Planificador de Pagos")
        if payload.get("has_snapshot"):
            st.info("No hay facturas pendientes para programar en la última foto guardada. Solo actualiza si quieres traer novedades de correo o cartera.")
        else:
            st.info("Todavía no existe una foto guardada para construir el planificador. La primera actualización crea esa base y luego esta consulta será inmediata.")
        st.stop()

    st.markdown(
        """
        <div class="planner-hero">
            <div style="font-size:.82rem;text-transform:uppercase;letter-spacing:.08em;opacity:.86;">Ferreinox BI · Payments Engine</div>
            <div style="font-size:2.2rem;font-weight:800;line-height:1.05;margin-top:.35rem;">Planificador de Pagos Automatizados</div>
            <div style="margin-top:.85rem;max-width:860px;line-height:1.55;font-size:1rem;opacity:.95;">Selecciona facturas sugeridas, arma lotes de pago profesionales y registra el correo al proveedor con trazabilidad completa en Google Sheets.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    supplier_options = sorted(plan_df["proveedor"].dropna().astype(str).unique().tolist())
    selected_supplier = st.selectbox("Proveedor a programar", supplier_options)
    provider_plan_df = plan_df[plan_df["proveedor"] == selected_supplier].copy()
    provider_plan_df.sort_values(by=["prioridad_pago", "fecha_limite_descuento", "fecha_vencimiento_erp"], inplace=True)

    c1, c2, c3 = st.columns(3)
    c1.metric("Facturas candidatas", f"{len(provider_plan_df):,}")
    c2.metric("Ahorro potencial", format_currency(provider_plan_df["valor_descuento"].sum()))
    c3.metric("Valor programable", format_currency(provider_plan_df["valor_a_pagar"].sum()))

    recent_lots = lot_history_df[lot_history_df["proveedor"] == selected_supplier].copy() if not lot_history_df.empty and "proveedor" in lot_history_df.columns else pd.DataFrame()
    recent_emails = email_log_df[email_log_df["proveedor"] == selected_supplier].copy() if not email_log_df.empty and "proveedor" in email_log_df.columns else pd.DataFrame()
    if not recent_lots.empty or not recent_emails.empty:
        insight_col1, insight_col2 = st.columns(2)
        insight_col1.metric("Lotes historicos proveedor", f"{len(recent_lots):,}")
        insight_col2.metric("Correos historicos proveedor", f"{len(recent_emails):,}")

    editable_df = st.data_editor(
        prepare_selection_df(provider_plan_df),
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        column_config={
            "Seleccionar": st.column_config.CheckboxColumn("Seleccionar"),
            "valor_erp": st.column_config.NumberColumn("Valor factura", format="$ %d"),
            "descuento_pct": st.column_config.NumberColumn("Descuento %", format="%.2f"),
            "valor_descuento": st.column_config.NumberColumn("Descuento", format="$ %d"),
            "valor_a_pagar": st.column_config.NumberColumn("Valor a pagar", format="$ %d"),
            "fecha_vencimiento_erp": st.column_config.DateColumn("Vence", format="YYYY-MM-DD"),
            "fecha_limite_descuento": st.column_config.DateColumn("Limite descuento", format="YYYY-MM-DD"),
        },
        key="payment_selector",
    )

    selected_invoice_keys = editable_df[editable_df["Seleccionar"]]["invoice_key"].tolist()
    selected_df = provider_plan_df[provider_plan_df["invoice_key"].isin(selected_invoice_keys)].copy()

    if selected_df.empty:
        st.info("Marca al menos una factura para construir el lote.")
        st.stop()

    st.divider()
    st.subheader("Datos del lote y del correo")
    lot_col1, lot_col2, lot_col3 = st.columns(3)
    payment_date = lot_col1.date_input("Fecha programada de pago", value=date.today())
    responsible = lot_col2.text_input("Responsable del lote", value="Tesoreria Ferreinox")
    to_email_default = first_non_empty(selected_df["email_pago"]) if "email_pago" in selected_df.columns else ""
    cc_default = first_non_empty(selected_df["email_cc"]) if "email_cc" in selected_df.columns else ""
    to_email = lot_col3.text_input("Correo destino proveedor", value=to_email_default)
    cc_email = st.text_input("CC del correo", value=cc_default)
    email_notes = st.text_area("Mensaje adicional", value="Agradecemos validar cualquier novedad documental o financiera de este lote.")

    summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
    summary_col1.metric("Facturas seleccionadas", f"{len(selected_df):,}")
    summary_col2.metric("Valor original", format_currency(selected_df["valor_erp"].sum()))
    summary_col3.metric("Descuento a capturar", format_currency(selected_df["valor_descuento"].sum()))
    summary_col4.metric("Valor final a pagar", format_currency(selected_df["valor_a_pagar"].sum()))

    if selected_df["email_pago"].fillna("").astype(str).str.strip().eq("").any():
        st.warning("Hay facturas seleccionadas sin correo de pago diligenciado en el maestro. Revisa el destino antes de enviar.")

    if not recent_lots.empty:
        with st.expander("Historial reciente del proveedor", expanded=False):
            st.dataframe(
                recent_lots[[col for col in ["lote_id", "fecha_programada_pago", "num_factura", "valor_a_pagar", "estado_lote"] if col in recent_lots.columns]].tail(10),
                use_container_width=True,
                hide_index=True,
                column_config={"valor_a_pagar": st.column_config.NumberColumn("Valor a pagar", format="$ %d")},
            )
            if not recent_emails.empty:
                st.dataframe(
                    recent_emails[[col for col in ["fecha_envio", "asunto", "estado_envio", "detalle_envio"] if col in recent_emails.columns]].tail(10),
                    use_container_width=True,
                    hide_index=True,
                )

    selected_preview = selected_df[["num_factura", "valor_erp", "valor_descuento", "valor_a_pagar", "motivo_pago"]].copy()
    st.dataframe(
        selected_preview,
        use_container_width=True,
        hide_index=True,
        column_config={
            "valor_erp": st.column_config.NumberColumn("Valor factura", format="$ %d"),
            "valor_descuento": st.column_config.NumberColumn("Descuento", format="$ %d"),
            "valor_a_pagar": st.column_config.NumberColumn("Valor a pagar", format="$ %d"),
        },
    )

    html_preview = build_payment_email_html(selected_supplier, selected_df, payment_date, email_notes)
    st.caption(f"Asunto sugerido: Ferreinox | Programacion de pago {selected_supplier}")
    with st.expander("Vista previa del correo profesional", expanded=False):
        st.components.v1.html(html_preview, height=700, scrolling=True)

    if st.button("📨 Registrar lote y enviar correo", type="primary", use_container_width=True):
        if not to_email:
            st.error("Debes indicar el correo destino del proveedor.")
            st.stop()

        gs_client = connect_to_google_sheets()
        if not gs_client:
            st.error("No fue posible conectar con Google Sheets para registrar el lote.")
            st.stop()

        lot_df = create_payment_lot(selected_df, payment_date, responsible, to_email)
        if not register_payment_lot(gs_client, lot_df[PAYMENT_LOT_COLUMNS]):
            st.error("No se pudo registrar el lote en Google Sheets.")
            st.stop()

        subject = f"Ferreinox | Programacion de pago {selected_supplier}"
        ok, detail = send_email_via_sendgrid(
            to_email=to_email,
            cc_emails=[email.strip() for email in cc_email.split(",") if email.strip()],
            subject=subject,
            html_content=html_preview,
        )

        log_row = build_email_log_row(
            lote_id=lot_df["lote_id"].iloc[0],
            provider_name=selected_supplier,
            to_email=to_email,
            cc_email=cc_email,
            subject=subject,
            lot_df=lot_df,
            status="Enviado" if ok else "Fallido",
            detail=detail,
        )
        register_email_log(gs_client, log_row)

        if ok:
            st.success(f"Lote {lot_df['lote_id'].iloc[0]} registrado y correo enviado correctamente.")
        else:
            st.warning(f"Lote registrado, pero el correo no pudo enviarse. Detalle: {detail}")


main()