# -*- coding: utf-8 -*-
"""Portal ejecutivo de tesoreria para priorizacion y seguimiento de pagos."""

import pandas as pd
import streamlit as st

from common.treasury_core import ensure_authenticated, format_currency, load_operational_payload


st.set_page_config(page_title="Portal Tesoreria | Ferreinox", page_icon="💰", layout="wide")
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
            .main .block-container { padding-top: 1.6rem; padding-bottom: 2.4rem; }
            .hero {
                background: linear-gradient(135deg, #0d2340 0%, #1c4e80 58%, #ef3737 100%);
                color: white;
                padding: 26px 30px;
                border-radius: 26px;
                margin-bottom: 1rem;
                box-shadow: 0 20px 48px rgba(15, 44, 82, 0.18);
            }
            .card {
                background: white;
                border: 1px solid rgba(15,44,82,.08);
                border-radius: 22px;
                padding: 1.05rem 1.15rem;
                margin-bottom: 1rem;
                box-shadow: 0 12px 28px rgba(15,44,82,.06);
            }
            .bi-banner {
                background: linear-gradient(90deg, rgba(239,55,55,.08) 0%, rgba(243,178,33,.12) 100%);
                border: 1px solid rgba(15,44,82,.08);
                border-radius: 18px;
                padding: 14px 16px;
                margin-bottom: 1rem;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def display_hero(master_df: pd.DataFrame, plan_df: pd.DataFrame, alerts_df: pd.DataFrame) -> None:
    st.markdown(
        f"""
        <div class="hero">
            <div style="font-size:.82rem;text-transform:uppercase;letter-spacing:.08em;opacity:.88;">Mesa Ejecutiva de Tesoreria</div>
            <div style="font-size:2.25rem;font-weight:800;line-height:1.05;margin-top:.35rem;">Que pagar, por que pagarlo y donde esta el riesgo</div>
            <div style="margin-top:.85rem;max-width:920px;line-height:1.55;font-size:1rem;opacity:.95;">Este panel cruza estado de cartera, soporte recibido por correo, descuentos financieros disponibles y proximidad al vencimiento para priorizar decisiones de pago con criterio ejecutivo.</div>
            <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:.8rem;margin-top:1rem;">
                <div style="background:rgba(255,255,255,.12);padding:.95rem 1rem;border-radius:18px;">Pendiente conciliado<br><strong style="font-size:1.35rem;">{int((master_df['estado_conciliacion'] == 'Pendiente conciliada').sum()) if not master_df.empty else 0:,}</strong></div>
                <div style="background:rgba(255,255,255,.12);padding:.95rem 1rem;border-radius:18px;">Sin soporte de correo<br><strong style="font-size:1.35rem;">{int((master_df['estado_conciliacion'] == 'Pendiente sin correo').sum()) if not master_df.empty else 0:,}</strong></div>
                <div style="background:rgba(255,255,255,.12);padding:.95rem 1rem;border-radius:18px;">Ahorro en radar<br><strong style="font-size:1.35rem;">{format_currency(plan_df['valor_descuento'].sum() if not plan_df.empty else 0)}</strong></div>
                <div style="background:rgba(255,255,255,.12);padding:.95rem 1rem;border-radius:18px;">Riesgo 48h<br><strong style="font-size:1.35rem;">{len(alerts_df):,}</strong></div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def provider_summary(master_df: pd.DataFrame, plan_df: pd.DataFrame) -> pd.DataFrame:
    if master_df.empty:
        return pd.DataFrame()

    summary_df = master_df.groupby("proveedor", dropna=False).agg(
        Pendiente_ERP=("estado_erp", lambda values: (pd.Series(values) == "Pendiente").sum()),
        Solo_Correo=("estado_conciliacion", lambda values: (pd.Series(values) == "Solo correo").sum()),
        Pendiente_Sin_Correo=("estado_conciliacion", lambda values: (pd.Series(values) == "Pendiente sin correo").sum()),
        Riesgo_48h=("riesgo_mora_48h", "sum"),
        Valor_Pendiente=("valor_erp", lambda values: values[master_df.loc[values.index, "estado_erp"] == "Pendiente"].sum()),
    ).reset_index()

    if not plan_df.empty:
        savings_df = plan_df.groupby("proveedor", dropna=False)["valor_descuento"].sum().reset_index(name="Ahorro_Potencial")
        summary_df = summary_df.merge(savings_df, on="proveedor", how="left")
    else:
        summary_df["Ahorro_Potencial"] = 0.0

    summary_df["Ahorro_Potencial"] = summary_df["Ahorro_Potencial"].fillna(0.0)
    return summary_df.sort_values(by=["Riesgo_48h", "Ahorro_Potencial", "Valor_Pendiente"], ascending=[False, False, False])


def main() -> None:
    inject_styles()
    payload = load_operational_payload()
    master_df = payload.get("master_df", pd.DataFrame())
    plan_df = payload.get("payment_plan_df", pd.DataFrame())
    alerts_df = payload.get("risk_alerts_df", pd.DataFrame())

    st.title("Portal Ejecutivo de Tesoreria")
    if master_df.empty:
        st.info("Aun no hay informacion consolidada. Ejecuta la sincronizacion desde la pagina principal.")
        st.stop()

    display_hero(master_df, plan_df, alerts_df)
    st.markdown('<div class="bi-banner"><strong>Tablero de decision diaria.</strong> Prioriza descuentos capturables, anticipa mora y separa con claridad lo pendiente, lo saldado y lo que requiere validacion documental.</div>', unsafe_allow_html=True)

    supplier_options = ["Todos"] + sorted(master_df["proveedor"].dropna().astype(str).unique().tolist())
    filter_col1, filter_col2, filter_col3 = st.columns([1.2, 1, 1])
    selected_supplier = filter_col1.selectbox("Proveedor", supplier_options)
    selected_status = filter_col2.multiselect(
        "Estado conciliacion",
        sorted(master_df["estado_conciliacion"].dropna().astype(str).unique().tolist()),
        default=[],
    )
    selected_due = filter_col3.multiselect(
        "Ventana de vencimiento",
        sorted(master_df["estado_vencimiento"].dropna().astype(str).unique().tolist()),
        default=[],
    )

    filtered_master = master_df.copy()
    filtered_plan = plan_df.copy()
    filtered_alerts = alerts_df.copy()
    if selected_supplier != "Todos":
        filtered_master = filtered_master[filtered_master["proveedor"] == selected_supplier].copy()
        filtered_plan = filtered_plan[filtered_plan["proveedor"] == selected_supplier].copy()
        filtered_alerts = filtered_alerts[filtered_alerts["proveedor"] == selected_supplier].copy()
    if selected_status:
        filtered_master = filtered_master[filtered_master["estado_conciliacion"].isin(selected_status)].copy()
    if selected_due:
        filtered_master = filtered_master[filtered_master["estado_vencimiento"].isin(selected_due)].copy()
        filtered_plan = filtered_plan[filtered_plan["estado_vencimiento"].isin(selected_due)].copy()

    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
    metric_col1.metric("Valor pendiente visible", format_currency(filtered_master.loc[filtered_master["estado_erp"] == "Pendiente", "valor_erp"].sum() if not filtered_master.empty else 0))
    metric_col2.metric("Facturas sin soporte", f"{int((filtered_master['estado_conciliacion'] == 'Pendiente sin correo').sum()) if not filtered_master.empty else 0:,}")
    metric_col3.metric("Facturas con ahorro", f"{int((filtered_plan['descuento_pct'] > 0).sum()) if not filtered_plan.empty else 0:,}")
    metric_col4.metric("Riesgos criticos", f"{len(filtered_alerts):,}")

    if not filtered_plan.empty:
        st.caption(
            f"Ahorro potencial del filtro: {format_currency(filtered_plan['valor_descuento'].sum())}. Valor sugerido a pagar: {format_currency(filtered_plan['valor_a_pagar'].sum())}."
        )

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("Matriz de prioridad por proveedor")
    supplier_df = provider_summary(filtered_master, filtered_plan)
    st.dataframe(
        supplier_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Valor_Pendiente": st.column_config.NumberColumn("Valor pendiente", format="$ %d"),
            "Ahorro_Potencial": st.column_config.NumberColumn("Ahorro potencial", format="$ %d"),
        },
    )
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("Lectura ejecutiva del filtro")
    overdue_count = int((filtered_master["estado_vencimiento"] == "🔴 Vencida").sum()) if not filtered_master.empty else 0
    risk_count = int((filtered_master["estado_vencimiento"] == "🟠 Riesgo 48h").sum()) if not filtered_master.empty else 0
    missing_support = int((filtered_master["estado_conciliacion"] == "Pendiente sin correo").sum()) if not filtered_master.empty else 0
    savings_total = filtered_plan["valor_descuento"].sum() if not filtered_plan.empty else 0
    st.write(
        f"El filtro actual deja visibles {len(filtered_master):,} facturas. Hay {overdue_count:,} vencidas, {risk_count:,} en riesgo de mora dentro de 48 horas y {missing_support:,} pendientes sin soporte documental de correo."
    )
    st.write(
        f"En paralelo, el plan sugerido mantiene {len(filtered_plan):,} facturas programables con ahorro potencial de {format_currency(savings_total)}."
    )
    st.markdown('</div>', unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["💎 Descuentos", "⏱️ Riesgo mora", "📬 Conciliacion"])

    with tab1:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader("Facturas con beneficio financiero")
        if filtered_plan.empty:
            st.info("No hay facturas sugeridas en este filtro.")
        else:
            st.dataframe(
                filtered_plan[[
                    "prioridad_pago",
                    "proveedor",
                    "num_factura",
                    "valor_erp",
                    "descuento_pct",
                    "valor_descuento",
                    "valor_a_pagar",
                    "fecha_limite_descuento",
                    "motivo_pago",
                ]],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "valor_erp": st.column_config.NumberColumn("Valor factura", format="$ %d"),
                    "descuento_pct": st.column_config.NumberColumn("Descuento %", format="%.2f"),
                    "valor_descuento": st.column_config.NumberColumn("Descuento", format="$ %d"),
                    "valor_a_pagar": st.column_config.NumberColumn("Valor a pagar", format="$ %d"),
                    "fecha_limite_descuento": st.column_config.DateColumn("Limite descuento", format="YYYY-MM-DD"),
                },
            )
        st.markdown('</div>', unsafe_allow_html=True)

    with tab2:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader("Alertas operativas de mora")
        if filtered_alerts.empty:
            st.success("No hay alertas de mora en el rango actual.")
        else:
            st.dataframe(
                filtered_alerts,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "valor_erp": st.column_config.NumberColumn("Valor factura", format="$ %d"),
                    "fecha_vencimiento_erp": st.column_config.DateColumn("Fecha vencimiento", format="YYYY-MM-DD"),
                },
            )
        st.markdown('</div>', unsafe_allow_html=True)

    with tab3:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader("Conciliacion detallada")
        st.dataframe(
            filtered_master[[
                "proveedor",
                "num_factura",
                "estado_erp",
                "estado_conciliacion",
                "estado_vencimiento",
                "valor_erp",
                "valor_total_correo",
                "diferencia_valor",
                "fecha_recepcion_correo",
                "remitente_correo",
            ]].sort_values(by=["proveedor", "num_factura"]),
            use_container_width=True,
            hide_index=True,
            column_config={
                "valor_erp": st.column_config.NumberColumn("Valor ERP", format="$ %d"),
                "valor_total_correo": st.column_config.NumberColumn("Valor correo", format="$ %d"),
                "diferencia_valor": st.column_config.NumberColumn("Diferencia", format="$ %d"),
                "fecha_recepcion_correo": st.column_config.DatetimeColumn("Fecha correo", format="YYYY-MM-DD HH:mm"),
            },
        )
        st.markdown('</div>', unsafe_allow_html=True)


main()