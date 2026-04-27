# -*- coding: utf-8 -*-
"""Portal Ejecutivo de Tesorería — centro de control, análisis y decisión de pagos."""

from datetime import date

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from common.treasury_core import (
    DISCOUNT_PROVIDERS,
    connect_to_google_sheets,
    deactivate_invoice_exclusion,
    ensure_authenticated,
    export_df_to_excel,
    format_currency,
    get_discount_summary_for_suppliers,
    load_operational_payload,
    register_invoice_exclusion,
    register_invoice_exclusions,
    register_manual_reconciliation,
    safe_display,
)


st.set_page_config(page_title="Portal Tesoreria | Ferreinox", page_icon="💰", layout="wide")
ensure_authenticated()


# ─── Column safety ──────────────────────────────────────────────────
def ensure_columns(df: pd.DataFrame, defaults: dict) -> pd.DataFrame:
    prepared = df.copy()
    for column, default in defaults.items():
        if column not in prepared.columns:
            prepared[column] = default
    return prepared


# ─── Styles ─────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0a1a2f 0%, #102848 58%, #15365e 100%);
            border-right: 1px solid rgba(255,255,255,.08);
        }
        [data-testid="stSidebar"] * { color: #f5f8fb; }
        .main .block-container { padding-top: 1rem; padding-bottom: 2.4rem; }

        /* KPI row -------------------------------------------------- */
        .kpi-row { display: flex; gap: .7rem; flex-wrap: wrap; margin: .6rem 0 1rem 0; }
        .kpi-card {
            flex: 1 1 165px;
            background: linear-gradient(135deg, #f8fbff 0%, #eef3f8 100%);
            border: 1px solid rgba(12,45,87,.08);
            border-radius: 18px;
            padding: 1rem 1.1rem;
            min-width: 165px;
        }
        .kpi-card.accent { border-left: 4px solid #ef3737; }
        .kpi-card.gold { border-left: 4px solid #f3b221; }
        .kpi-card.green { border-left: 4px solid #0fa968; }
        .kpi-card.blue { border-left: 4px solid #1c4e80; }
        .kpi-card.purple { border-left: 4px solid #8b5cf6; }
        .kpi-label { font-size: .72rem; text-transform: uppercase; letter-spacing: .08em; color: #506070; margin-bottom: .15rem; }
        .kpi-value { font-size: 1.42rem; font-weight: 800; color: #0c2d57; line-height: 1.1; }
        .kpi-sub { font-size: .75rem; color: #6b7c8f; margin-top: .15rem; }

        /* Hero --------------------------------------------------- */
        .hero-treasury {
            background:
                radial-gradient(circle at top right, rgba(243,178,33,.28), transparent 24%),
                linear-gradient(135deg, #0d2340 0%, #1c4e80 50%, #ef3737 100%);
            color: white; padding: 28px 32px; border-radius: 28px; margin-bottom: 1rem;
            box-shadow: 0 22px 52px rgba(13,35,64,.20);
        }
        .hero-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: .8rem; margin-top: 1.1rem; }
        .hero-pill { background: rgba(255,255,255,.12); border: 1px solid rgba(255,255,255,.16); border-radius: 20px; padding: .95rem 1rem; backdrop-filter: blur(10px); }
        .hero-pill-label { font-size: .75rem; text-transform: uppercase; opacity: .74; margin-bottom: .18rem; }
        .hero-pill-value { font-size: 1.38rem; font-weight: 800; }

        /* Cards --------------------------------------------------- */
        .card { background: white; border: 1px solid rgba(15,44,82,.08); border-radius: 22px;
            padding: 1.05rem 1.15rem; margin-bottom: 1rem; box-shadow: 0 12px 28px rgba(15,44,82,.06); }
        .table-header { color: #0c2d57; font-size: 1.05rem; font-weight: 800; margin-bottom: .3rem; }
        .table-sub { color: #506070; font-size: .88rem; margin-bottom: .7rem; }

        /* Aging badge -------------------------------------------- */
        .aging-badge { display: inline-block; padding: 4px 12px; border-radius: 999px; font-size: .78rem; font-weight: 700; margin: 2px 4px 2px 0; }
        .aging-badge.red { background: rgba(239,55,55,.14); color: #c0392b; }
        .aging-badge.orange { background: rgba(243,178,33,.18); color: #8a6e10; }
        .aging-badge.yellow { background: rgba(241,196,15,.18); color: #7d6608; }
        .aging-badge.green { background: rgba(15,169,104,.14); color: #0a7c4f; }

        /* Banner ------------------------------------------------- */
        .bi-banner {
            background: linear-gradient(90deg, rgba(239,55,55,.09) 0%, rgba(243,178,33,.12) 100%);
            border: 1px solid rgba(15,44,82,.08); border-radius: 18px; padding: 14px 16px; margin-bottom: 1rem;
        }

        /* Plotly override for dark bg */
        .js-plotly-plot { border-radius: 16px; overflow: hidden; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ─── Helpers ────────────────────────────────────────────────────────
def kpi_html(label: str, value: str, sub: str = "", css: str = "") -> str:
    sub_html = f'<div class="kpi-sub">{sub}</div>' if sub else ""
    return f'<div class="kpi-card {css}"><div class="kpi-label">{label}</div><div class="kpi-value">{value}</div>{sub_html}</div>'


def display_ready(df: pd.DataFrame) -> pd.DataFrame:
    return safe_display(df, df.columns.tolist())


def reconciliation_option_label(row: pd.Series) -> str:
    provider = str(row.get("proveedor_correo") or row.get("proveedor") or "Sin proveedor")
    document = str(row.get("num_factura") or "Sin documento")
    amount = format_currency(row.get("valor_total_correo") or row.get("valor_erp") or 0)
    state = str(row.get("estado_conciliacion") or "")
    related = str(row.get("documento_relacionado_correo") or row.get("factura_compensada_correo") or "")
    related_suffix = f" -> {related}" if related else ""
    return f"{provider} | {document}{related_suffix} | {amount} | {state}"


def aging_bucket(days: float) -> str:
    if pd.isna(days):
        return "Sin fecha"
    d = int(days)
    if d < 0:
        return "Vencida"
    if d <= 15:
        return "0–15 dias"
    if d <= 30:
        return "16–30 dias"
    if d <= 60:
        return "31–60 dias"
    return "61+ dias"


def aging_bucket_order() -> dict:
    return {"Vencida": 0, "0–15 dias": 1, "16–30 dias": 2, "31–60 dias": 3, "61+ dias": 4, "Sin fecha": 5}


# ─── Load data ──────────────────────────────────────────────────────
payload = load_operational_payload()
master_df = ensure_columns(
    payload.get("master_df", pd.DataFrame()),
    {
        "detalle_conciliacion": "", "valor_descuento": 0.0, "valor_a_pagar": 0.0,
        "valor_base_descuento": 0.0,
        "origen_soporte": "",
        "tipo_documento_correo": "FACTURA",
        "documento_relacionado_correo": "",
        "descripcion_nota_correo": "",
        "factura_compensada_correo": "",
        "proveedor_correo": "", "fecha_recepcion_correo": pd.NaT, "remitente_correo": "",
        "valor_total_correo": 0.0, "estado_vencimiento": "", "estado_conciliacion": "",
        "estado_erp": "", "riesgo_mora_48h": False, "dias_para_vencer": 0,
        "descuento_pct": 0.0, "diferencia_valor": 0.0, "fecha_vencimiento_erp": pd.NaT,
        "fecha_emision_erp": pd.NaT, "valor_erp": 0.0, "num_factura": "", "proveedor": "",
        "condiciones_comerciales": "",
    },
)
master_df_all = ensure_columns(
    payload.get("master_df_all", payload.get("master_df", pd.DataFrame())),
    {
        "detalle_conciliacion": "", "valor_descuento": 0.0, "valor_a_pagar": 0.0,
        "valor_base_descuento": 0.0,
        "origen_soporte": "",
        "tipo_documento_correo": "FACTURA",
        "documento_relacionado_correo": "",
        "descripcion_nota_correo": "",
        "factura_compensada_correo": "",
        "proveedor_correo": "", "fecha_recepcion_correo": pd.NaT, "remitente_correo": "",
        "valor_total_correo": 0.0, "estado_vencimiento": "", "estado_conciliacion": "",
        "estado_erp": "", "riesgo_mora_48h": False, "dias_para_vencer": 0,
        "descuento_pct": 0.0, "diferencia_valor": 0.0, "fecha_vencimiento_erp": pd.NaT,
        "fecha_emision_erp": pd.NaT, "valor_erp": 0.0, "num_factura": "", "proveedor": "",
        "condiciones_comerciales": "", "invoice_key": "", "proveedor_norm": "",
        "excluir_de_calculos": False, "motivo_exclusion": "", "exclusion_id": "",
    },
)
master_df = master_df[master_df["estado_erp"] != "Saldada"].copy() if not master_df.empty else master_df
master_df_all = master_df_all[master_df_all["estado_erp"] != "Saldada"].copy() if not master_df_all.empty else master_df_all
plan_df = ensure_columns(
    payload.get("payment_plan_df", pd.DataFrame()),
    {"valor_descuento": 0.0, "valor_a_pagar": 0.0, "valor_base_descuento": 0.0, "estado_vencimiento": "", "descuento_pct": 0.0},
)
alerts_df = payload.get("risk_alerts_df", pd.DataFrame())
lot_history_df = payload.get("lot_history_df", pd.DataFrame())
email_log_df = payload.get("email_log_df", pd.DataFrame())

if master_df.empty:
    st.title("Portal Ejecutivo de Tesoreria")
    if payload.get("has_snapshot"):
        st.info("No hay facturas en la última foto guardada.")
    else:
        st.info("Todavía no existe una foto guardada. La primera actualización crea esa base.")
    st.stop()


# ─── Derived data ───────────────────────────────────────────────────
pending_df = master_df[master_df["estado_erp"] == "Pendiente"].copy()

pending_value = pending_df["valor_erp"].sum() if not pending_df.empty else 0
total_discount = plan_df["valor_descuento"].sum() if not plan_df.empty else 0
total_net = plan_df["valor_a_pagar"].sum() if not plan_df.empty else 0
n_overdue = int((master_df["estado_vencimiento"] == "🔴 Vencida").sum())
n_risk = int((master_df["estado_vencimiento"] == "🟠 Riesgo 48h").sum())
n_upcoming = int((master_df["estado_vencimiento"] == "🟡 Proxima a vencer").sum())
n_providers = master_df["proveedor"].nunique()
only_email_count = int((master_df["estado_conciliacion"] == "Solo correo").sum())
heuristic_email_count = int((master_df["estado_conciliacion"] == "Correo heuristico").sum())
credit_note_match_count = int(master_df["estado_conciliacion"].isin(["Solo correo compensado por NC", "NC/anulación compensada", "NC/anulación sin ERP"]).sum())
no_email_count = int((master_df["estado_conciliacion"] == "Pendiente sin correo").sum())
conciliated_count = int(master_df["estado_conciliacion"].isin(["Pendiente conciliada", "Saldada conciliada", "Pendiente anterior a lectura", "Saldada anterior a lectura"]).sum())

# Aging
if not pending_df.empty:
    pending_df["_aging"] = pending_df["dias_para_vencer"].apply(aging_bucket)
else:
    pending_df["_aging"] = pd.Series(dtype=str)


# ─── Hero ───────────────────────────────────────────────────────────
st.markdown(
    f"""
    <div class="hero-treasury">
        <div style="font-size:.82rem;text-transform:uppercase;letter-spacing:.1em;opacity:.82;">Ferreinox BI · Treasury Command Center</div>
        <div style="font-size:2.4rem;font-weight:800;line-height:1.05;margin-top:.35rem;">Portal Ejecutivo de Tesoreria</div>
        <div style="margin-top:.85rem;max-width:920px;line-height:1.55;font-size:1rem;opacity:.95;">
            Control integral de cartera pendiente, conciliación documental, riesgo de mora, oportunidades de descuento y trazabilidad de pagos.
            Esta vista se enfoca solo en lo pendiente y en el cruce real contra correo. Los descuentos financieros se calculan sobre la base antes de IVA.
        </div>
        <div class="hero-grid">
            <div class="hero-pill"><div class="hero-pill-label">Cartera pendiente</div><div class="hero-pill-value">{format_currency(pending_value)}</div></div>
            <div class="hero-pill"><div class="hero-pill-label">Pendiente sin correo</div><div class="hero-pill-value">{no_email_count:,}</div></div>
            <div class="hero-pill"><div class="hero-pill-label">Ahorro capturable</div><div class="hero-pill-value">{format_currency(total_discount)}</div></div>
            <div class="hero-pill"><div class="hero-pill-label">Proveedores</div><div class="hero-pill-value">{n_providers:,}</div></div>
            <div class="hero-pill"><div class="hero-pill-label">Vencidas + Riesgo 48h</div><div class="hero-pill-value">{n_overdue + n_risk:,}</div></div>
            <div class="hero-pill"><div class="hero-pill-label">Correo sin reflejo ERP</div><div class="hero-pill-value">{only_email_count:,}</div></div>
            <div class="hero-pill"><div class="hero-pill-label">Heurístico correo</div><div class="hero-pill-value">{heuristic_email_count:,}</div></div>
            <div class="hero-pill"><div class="hero-pill-label">NC por revisar</div><div class="hero-pill-value">{credit_note_match_count:,}</div></div>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ─── Global KPIs ────────────────────────────────────────────────────
st.markdown(
    f"""
    <div class="kpi-row">
        {kpi_html("Facturas pendientes", f"{len(pending_df):,}", f"Valor: {format_currency(pending_value)}", "blue")}
        {kpi_html("Vencidas", f"{n_overdue:,}", "Requieren pago inmediato", "accent")}
        {kpi_html("Riesgo 48h", f"{n_risk:,}", "Vencen dentro de 2 dias", "accent")}
        {kpi_html("Proximas a vencer", f"{n_upcoming:,}", "Ventana corta", "gold")}
        {kpi_html("Ahorro financiero", format_currency(total_discount), f"Neto: {format_currency(total_net)}", "green")}
        {kpi_html("Sin soporte correo", f"{no_email_count:,}", "Pendientes sin XML", "purple")}
        {kpi_html("Conciliadas", f"{conciliated_count:,}", "ERP + correo OK", "green")}
    </div>
    """,
    unsafe_allow_html=True,
)


# ─── Filters ────────────────────────────────────────────────────────
st.markdown("---")
st.markdown('<div class="table-header">🔍 Filtros globales</div>', unsafe_allow_html=True)
filter_c1, filter_c2, filter_c3, filter_c4 = st.columns([1.5, 1, 1, 1])
supplier_options = ["Todos"] + sorted(master_df["proveedor"].dropna().astype(str).unique().tolist())
selected_supplier = filter_c1.selectbox("Proveedor", supplier_options, key="portal_supplier")
selected_status = filter_c2.multiselect(
    "Estado conciliacion",
    sorted(master_df["estado_conciliacion"].dropna().astype(str).unique().tolist()),
    default=[],
    key="portal_conc",
)
selected_due = filter_c3.multiselect(
    "Ventana de vencimiento",
    sorted(master_df["estado_vencimiento"].dropna().astype(str).unique().tolist()),
    default=[],
    key="portal_due",
)
selected_erp = filter_c4.multiselect(
    "Estado ERP",
    sorted(master_df["estado_erp"].dropna().astype(str).unique().tolist()),
    default=[],
    key="portal_erp",
)

# Apply filters
filtered_master = master_df.copy()
filtered_master_all = master_df_all.copy()
filtered_plan = plan_df.copy()
filtered_alerts = alerts_df.copy()
if selected_supplier != "Todos":
    filtered_master = filtered_master[filtered_master["proveedor"] == selected_supplier].copy()
    filtered_master_all = filtered_master_all[filtered_master_all["proveedor"] == selected_supplier].copy()
    filtered_plan = filtered_plan[filtered_plan["proveedor"] == selected_supplier].copy() if "proveedor" in filtered_plan.columns else filtered_plan
    filtered_alerts = filtered_alerts[filtered_alerts["proveedor"] == selected_supplier].copy() if not filtered_alerts.empty and "proveedor" in filtered_alerts.columns else filtered_alerts
if selected_status:
    filtered_master = filtered_master[filtered_master["estado_conciliacion"].isin(selected_status)].copy()
    filtered_master_all = filtered_master_all[filtered_master_all["estado_conciliacion"].isin(selected_status)].copy()
if selected_due:
    filtered_master = filtered_master[filtered_master["estado_vencimiento"].isin(selected_due)].copy()
    filtered_master_all = filtered_master_all[filtered_master_all["estado_vencimiento"].isin(selected_due)].copy()
    filtered_plan = filtered_plan[filtered_plan["estado_vencimiento"].isin(selected_due)].copy() if "estado_vencimiento" in filtered_plan.columns else filtered_plan
if selected_erp:
    filtered_master = filtered_master[filtered_master["estado_erp"].isin(selected_erp)].copy()
    filtered_master_all = filtered_master_all[filtered_master_all["estado_erp"].isin(selected_erp)].copy()

# Filtered KPIs
if selected_supplier != "Todos" or selected_status or selected_due or selected_erp:
    f_pending_val = filtered_master.loc[filtered_master["estado_erp"] == "Pendiente", "valor_erp"].sum() if not filtered_master.empty else 0
    f_discount = filtered_plan["valor_descuento"].sum() if not filtered_plan.empty else 0
    st.markdown(
        f"""
        <div class="kpi-row">
            {kpi_html("Filtro: Facturas", f"{len(filtered_master):,}", "", "")}
            {kpi_html("Filtro: Valor pendiente", format_currency(f_pending_val), "", "blue")}
            {kpi_html("Filtro: Ahorro disponible", format_currency(f_discount), "", "gold")}
            {kpi_html("Filtro: Proveedores", f"{filtered_master['proveedor'].nunique():,}", "", "")}
            {kpi_html("Filtro: Heurístico", f"{int((filtered_master['estado_conciliacion'] == 'Correo heuristico').sum()):,}", "Baja confianza", "purple")}
        </div>
        """,
        unsafe_allow_html=True,
    )


# ─── Segment filtered data ──────────────────────────────────────────
pay_now_df = filtered_master[
    (filtered_master["estado_erp"] == "Pendiente")
    & (filtered_master["estado_vencimiento"].isin(["🔴 Vencida", "🟠 Riesgo 48h", "🟡 Proxima a vencer"]))
].copy()
only_email_df = filtered_master[filtered_master["estado_conciliacion"] == "Solo correo"].copy()
excluded_only_email_df = filtered_master_all[
    filtered_master_all["estado_conciliacion"].eq("Solo correo")
    & filtered_master_all["excluir_de_calculos"].fillna(False)
].copy()
heuristic_email_df = filtered_master[filtered_master["estado_conciliacion"] == "Correo heuristico"].copy()
credit_note_recon_df = filtered_master[
    filtered_master["estado_conciliacion"].isin(["Solo correo compensado por NC", "NC/anulación compensada", "NC/anulación sin ERP"])
].copy()
unresolved_df = filtered_master[
    (filtered_master["estado_erp"] == "Pendiente")
    & (filtered_master["estado_conciliacion"] == "Pendiente sin correo")
].copy()
conciliated_df = filtered_master[
    filtered_master["estado_conciliacion"].isin([
        "Pendiente conciliada", "Pendiente anterior a lectura",
    ])
].copy()


# ─── TABS ───────────────────────────────────────────────────────────
tab_overview, tab_pay, tab_email, tab_credit_note, tab_unrec, tab_conc, tab_aging, tab_provider, tab_trace = st.tabs([
    "📊 Resumen Ejecutivo",
    f"💸 Que pagar ({len(pay_now_df):,})",
    f"📨 Correo sin reflejo ERP ({len(only_email_df):,})",
    f"🧾 NC / Anulaciones ({len(credit_note_recon_df):,})",
    f"⚠️ No conciliado ({len(unresolved_df):,})",
    f"✅ Conciliado ({len(conciliated_df):,})",
    "📈 Analisis Aging",
    "🏢 Concentracion Proveedor",
    "📋 Trazabilidad",
])


# ── Tab 1: Resumen Ejecutivo ────────────────────────────────────────
with tab_overview:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="table-header">Lectura ejecutiva del filtro</div>', unsafe_allow_html=True)

    f_overdue = int((filtered_master["estado_vencimiento"] == "🔴 Vencida").sum()) if not filtered_master.empty else 0
    f_risk = int((filtered_master["estado_vencimiento"] == "🟠 Riesgo 48h").sum()) if not filtered_master.empty else 0
    f_missing = int(((filtered_master["estado_erp"] == "Pendiente") & (filtered_master["estado_conciliacion"] == "Pendiente sin correo")).sum()) if not filtered_master.empty else 0
    f_savings = filtered_plan["valor_descuento"].sum() if not filtered_plan.empty else 0

    st.write(
        f"El filtro actual deja visibles **{len(filtered_master):,} facturas**. "
        f"Hay **{f_overdue:,} vencidas**, **{f_risk:,} en riesgo de mora** dentro de 48 horas "
        f"y **{f_missing:,} pendientes sin soporte** documental de correo."
    )
    st.write(
        f"El plan sugerido tiene **{len(filtered_plan):,} facturas programables** con ahorro potencial de **{format_currency(f_savings)}**."
    )
    st.markdown('</div>', unsafe_allow_html=True)

    # ── Resumen por proveedor ──
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="table-header">Resumen por proveedor</div>', unsafe_allow_html=True)
    st.markdown('<div class="table-sub">Concentracion de riesgo, valor pendiente y ahorro disponible por proveedor.</div>', unsafe_allow_html=True)

    if not filtered_master.empty:
        summary_df = filtered_master.groupby("proveedor", dropna=False).agg(
            Facturas=("num_factura", "count"),
            Pendientes=("estado_erp", lambda v: (pd.Series(v) == "Pendiente").sum()),
            Vencidas=("estado_vencimiento", lambda v: (pd.Series(v) == "🔴 Vencida").sum()),
            Riesgo_48h=("riesgo_mora_48h", "sum"),
            Sin_Correo=("estado_conciliacion", lambda v: (pd.Series(v) == "Pendiente sin correo").sum()),
            Valor_Pendiente=("valor_erp", lambda v: v[filtered_master.loc[v.index, "estado_erp"] == "Pendiente"].sum()),
        ).reset_index()

        if not filtered_plan.empty:
            savings_agg = filtered_plan.groupby("proveedor", dropna=False).agg(
                Ahorro_Potencial=("valor_descuento", "sum"),
                Dcto_Max=("descuento_pct", "max"),
            ).reset_index()
            summary_df = summary_df.merge(savings_agg, on="proveedor", how="left")
        else:
            summary_df["Ahorro_Potencial"] = 0.0
            summary_df["Dcto_Max"] = 0.0
        summary_df["Ahorro_Potencial"] = summary_df["Ahorro_Potencial"].fillna(0.0)
        summary_df["Dcto_Max"] = summary_df["Dcto_Max"].fillna(0.0)
        summary_df.sort_values(by=["Riesgo_48h", "Vencidas", "Valor_Pendiente"], ascending=[False, False, False], inplace=True)

        st.dataframe(
            display_ready(summary_df),
            width="stretch",
            hide_index=True,
            column_config={
                "Valor_Pendiente": st.column_config.NumberColumn("Valor pendiente", format="$ %,.0f"),
                "Ahorro_Potencial": st.column_config.NumberColumn("Ahorro potencial", format="$ %,.0f"),
                "Dcto_Max": st.column_config.NumberColumn("Dcto max %", format="%.1f%%"),
            },
        )

        excel_summary = export_df_to_excel(summary_df, sheet_name="Resumen_Proveedor", title="Ferreinox — Resumen Ejecutivo por Proveedor")
        st.download_button(
            "📥 Descargar resumen por proveedor",
            excel_summary,
            file_name=f"Ferreinox_Resumen_Proveedores_{date.today()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_portal_summary",
        )
    st.markdown('</div>', unsafe_allow_html=True)

    # ── Distribucion por estado ──
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="table-header">Distribucion por estado de conciliacion</div>', unsafe_allow_html=True)

    if not filtered_master.empty:
        conc_dist = filtered_master.groupby("estado_conciliacion", dropna=False).agg(
            Facturas=("num_factura", "count"),
            Valor=("valor_erp", "sum"),
        ).reset_index().sort_values("Valor", ascending=False)

        dc1, dc2 = st.columns([1, 1])
        with dc1:
            st.dataframe(
                display_ready(conc_dist),
                width="stretch",
                hide_index=True,
                column_config={"Valor": st.column_config.NumberColumn("Valor total", format="$ %,.0f")},
            )
        with dc2:
            if len(conc_dist) > 0:
                fig_conc = px.pie(
                    conc_dist, values="Valor", names="estado_conciliacion",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                    hole=0.4,
                )
                fig_conc.update_layout(
                    margin=dict(t=10, b=10, l=10, r=10),
                    height=320,
                    legend=dict(orientation="h", yanchor="bottom", y=-0.2),
                    font=dict(size=12),
                )
                st.plotly_chart(fig_conc, width="stretch")
    st.markdown('</div>', unsafe_allow_html=True)


# ── Tab 2: Que pagar ───────────────────────────────────────────────
with tab_pay:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="table-header">Cartera que requiere pago o atencion inmediata</div>', unsafe_allow_html=True)
    st.markdown('<div class="table-sub">Facturas pendientes vencidas, en riesgo 48h o proximas a vencer.</div>', unsafe_allow_html=True)

    if pay_now_df.empty:
        st.info("No hay facturas por pagar con urgencia en este filtro. ✅")
    else:
        pc1, pc2, pc3, pc4 = st.columns(4)
        pc1.metric("Facturas urgentes", f"{len(pay_now_df):,}")
        pc2.metric("Valor urgente", format_currency(pay_now_df["valor_erp"].sum()))
        pc3.metric("Vencidas 🔴", f"{int((pay_now_df['estado_vencimiento'] == '🔴 Vencida').sum()):,}")
        pc4.metric("Riesgo 48h 🟠", f"{int((pay_now_df['estado_vencimiento'] == '🟠 Riesgo 48h').sum()):,}")

        st.dataframe(
            safe_display(pay_now_df, [
                "proveedor", "num_factura", "valor_erp", "valor_descuento", "valor_a_pagar",
                "fecha_vencimiento_erp", "dias_para_vencer", "estado_vencimiento", "detalle_conciliacion",
            ], sort_by=["estado_vencimiento", "fecha_vencimiento_erp", "proveedor"]),
            width="stretch",
            hide_index=True,
            column_config={
                "valor_erp": st.column_config.NumberColumn("Valor factura", format="$ %,.0f"),
                "valor_descuento": st.column_config.NumberColumn("Descuento", format="$ %,.0f"),
                "valor_a_pagar": st.column_config.NumberColumn("Valor a pagar", format="$ %,.0f"),
                "fecha_vencimiento_erp": st.column_config.DateColumn("Vence", format="YYYY-MM-DD"),
            },
        )

        excel_pay = export_df_to_excel(
            pay_now_df[["proveedor", "num_factura", "valor_erp", "valor_descuento", "valor_a_pagar",
                         "fecha_vencimiento_erp", "dias_para_vencer", "estado_vencimiento"]],
            sheet_name="Que_Pagar", title="Ferreinox — Cartera Urgente por Pagar",
        )
        st.download_button("📥 Descargar cartera urgente", excel_pay,
                           file_name=f"Ferreinox_Urgente_{date.today()}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_portal_pay")
    st.markdown('</div>', unsafe_allow_html=True)


# ── Tab 3: Solo correo ─────────────────────────────────────────────
with tab_email:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="table-header">Facturas con correo pero sin reflejo en ERP</div>', unsafe_allow_html=True)
    st.markdown('<div class="table-sub">Documentos XML detectados en el buzón que no aparecen en las fuentes ERP descargadas desde Dropbox.</div>', unsafe_allow_html=True)

    if only_email_df.empty:
        if excluded_only_email_df.empty:
            st.success("No hay facturas con correo sin reflejo en ERP para este filtro.")
        else:
            st.info("No hay facturas activas en solo correo para este filtro, pero sí hay facturas que ya fueron excluidas desde este flujo.")
    else:
        ec1, ec2, ec3 = st.columns(3)
        ec1.metric("Facturas sin reflejo ERP", f"{len(only_email_df):,}")
        ec2.metric("Valor total correo", format_currency(only_email_df["valor_total_correo"].sum()))
        ec3.metric("Ya excluidas", f"{len(excluded_only_email_df):,}")
        st.caption("Estas facturas no fueron encontradas en la cartera pendiente ni en la cartera saldada que la app descargó desde Dropbox.")

        st.dataframe(
            safe_display(only_email_df, [
                "proveedor_correo", "num_factura", "valor_total_correo",
                "fecha_recepcion_correo", "remitente_correo", "invoice_key", "detalle_conciliacion",
            ], sort_by=["fecha_recepcion_correo", "proveedor_correo"], ascending=[False, True]),
            width="stretch",
            hide_index=True,
            column_config={
                "valor_total_correo": st.column_config.NumberColumn("Valor correo", format="$ %,.0f"),
                "fecha_recepcion_correo": st.column_config.DatetimeColumn("Fecha correo", format="YYYY-MM-DD HH:mm"),
                "invoice_key": st.column_config.TextColumn("Clave interna"),
            },
        )

        st.markdown("---")
        st.markdown("**Excluir estas facturas de toda la app**")
        st.caption("Usa este bloque cuando confirmes que una factura de correo no va a ingresar al ERP y no debe sumar en rebate, tesorería ni planificador.")

        exclusion_options = {
            reconciliation_option_label(row): row["invoice_key"]
            for _, row in only_email_df.iterrows()
        }
        exclude_col1, exclude_col2 = st.columns([1.6, 1])
        selected_exclusion_labels = exclude_col1.multiselect(
            "Facturas a excluir",
            list(exclusion_options.keys()),
            default=[],
            key="portal_only_email_exclude",
        )
        exclusion_reason = exclude_col2.text_input(
            "Motivo común del lote",
            value="Factura promocional / no tener en cuenta",
            key="portal_only_email_exclude_reason",
        )

        selected_exclusion_keys = [exclusion_options[label] for label in selected_exclusion_labels]
        selected_exclusion_rows = only_email_df[only_email_df["invoice_key"].isin(selected_exclusion_keys)].copy()
        if not selected_exclusion_rows.empty:
            st.caption(
                f"Lote seleccionado: {len(selected_exclusion_rows):,} facturas por {format_currency(selected_exclusion_rows['valor_total_correo'].sum())}."
            )
        if st.button("Excluir lote de todos los cálculos", type="primary", width="stretch", key="portal_save_invoice_exclusion"):
            gs_client = connect_to_google_sheets()
            if not gs_client:
                st.error("No fue posible conectar con Google Sheets para guardar la exclusión.")
            elif selected_exclusion_rows.empty:
                st.error("Debes seleccionar al menos una factura para excluir.")
            elif register_invoice_exclusions(
                gs_client,
                [
                    {
                        "invoice_key": str(row.get("invoice_key", "") or ""),
                        "proveedor_norm": str(row.get("proveedor_norm", "") or ""),
                        "num_factura": str(row.get("num_factura", "") or ""),
                        "reason": exclusion_reason,
                        "source": "portal_tesoreria_only_email",
                    }
                    for _, row in selected_exclusion_rows.iterrows()
                ],
            ):
                st.session_state.pop("treasury_payload", None)
                st.success(f"Se excluyeron {len(selected_exclusion_rows):,} facturas. Ya no volverán a sumar en ninguna parte de la app.")
                st.rerun()
            else:
                st.error("No se pudo guardar la exclusión.")

        excel_email = export_df_to_excel(
            only_email_df[["proveedor_correo", "num_factura", "valor_total_correo", "fecha_recepcion_correo", "remitente_correo"]],
            sheet_name="Solo_Correo", title="Ferreinox — Facturas Solo en Correo",
        )
        st.download_button("📥 Descargar solo correo", excel_email,
                           file_name=f"Ferreinox_Solo_Correo_{date.today()}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_portal_email")
    if not excluded_only_email_df.empty:
        st.markdown("---")
        st.markdown("**Facturas ya excluidas desde este flujo**")
        st.dataframe(
            safe_display(excluded_only_email_df, [
                "proveedor_correo", "num_factura", "valor_total_correo", "fecha_recepcion_correo",
                "motivo_exclusion", "exclusion_id",
            ], sort_by=["fecha_recepcion_correo", "proveedor_correo"], ascending=[False, True]),
            width="stretch",
            hide_index=True,
            column_config={
                "valor_total_correo": st.column_config.NumberColumn("Valor correo", format="$ %,.0f"),
                "fecha_recepcion_correo": st.column_config.DatetimeColumn("Fecha correo", format="YYYY-MM-DD HH:mm"),
            },
        )

        restore_options = {
            reconciliation_option_label(row): row["exclusion_id"]
            for _, row in excluded_only_email_df.iterrows()
        }
        restore_col1, restore_col2 = st.columns([1.6, 1])
        selected_restore_labels = restore_col1.multiselect(
            "Facturas a reincluir",
            list(restore_options.keys()),
            default=[],
            key="portal_only_email_restore",
        )
        restore_col2.caption("La reinclusión hace que la factura vuelva a aparecer en todos los cálculos y vistas.")
        selected_restore_ids = [restore_options[label] for label in selected_restore_labels]
        if st.button("Quitar exclusión del lote", width="stretch", key="portal_restore_invoice_exclusion"):
            gs_client = connect_to_google_sheets()
            if not gs_client:
                st.error("No fue posible conectar con Google Sheets para revertir la exclusión.")
            elif not selected_restore_ids:
                st.error("Debes seleccionar al menos una factura para reincluir.")
            elif all(deactivate_invoice_exclusion(gs_client, exclusion_id) for exclusion_id in selected_restore_ids):
                st.session_state.pop("treasury_payload", None)
                st.success(f"Se reincluyeron {len(selected_restore_ids):,} facturas en toda la app.")
                st.rerun()
            else:
                st.error("No se pudo revertir la exclusión.")

    if only_email_df.empty and not excluded_only_email_df.empty:
        ec1, ec2, ec3 = st.columns(3)
        ec1.metric("Facturas sin reflejo ERP", "0")
        ec2.metric("Valor total correo", format_currency(0))
        ec3.metric("Ya excluidas", f"{len(excluded_only_email_df):,}")

        if not heuristic_email_df.empty:
            st.warning("Los casos heurísticos detectados solo desde el cuerpo del correo no cuentan en este indicador ejecutivo. Se muestran abajo solo para auditoría.")
            st.dataframe(
                safe_display(heuristic_email_df, [
                    "proveedor_correo", "num_factura", "valor_total_correo", "origen_soporte",
                    "fecha_recepcion_correo", "remitente_correo", "detalle_conciliacion",
                ], sort_by=["fecha_recepcion_correo", "proveedor_correo"], ascending=[False, True]),
                width="stretch",
                hide_index=True,
                column_config={
                    "valor_total_correo": st.column_config.NumberColumn("Valor correo", format="$ %,.0f"),
                    "fecha_recepcion_correo": st.column_config.DatetimeColumn("Fecha correo", format="YYYY-MM-DD HH:mm"),
                },
            )
    st.markdown('</div>', unsafe_allow_html=True)


# ── Tab 4: No conciliado ───────────────────────────────────────────
with tab_credit_note:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="table-header">Facturas y notas crédito/anulaciones detectadas por correo</div>', unsafe_allow_html=True)
    st.markdown('<div class="table-sub">Aquí se ve qué ya quedó compensado, qué NC sigue sin match y cómo registrar un cruce manual persistente para sacar esos casos de la lectura operativa.</div>', unsafe_allow_html=True)

    if credit_note_recon_df.empty:
        st.success("No hay cruces de nota crédito/anulación para este filtro.")
    else:
        compensated_invoice_df = credit_note_recon_df[credit_note_recon_df["estado_conciliacion"] == "Solo correo compensado por NC"].copy()
        credit_note_only_df = credit_note_recon_df[credit_note_recon_df["tipo_documento_correo"] == "NOTA_CREDITO"].copy()
        unresolved_credit_note_df = credit_note_recon_df[credit_note_recon_df["estado_conciliacion"] == "NC/anulación sin ERP"].copy()

        cn1, cn2, cn3 = st.columns(3)
        cn1.metric("Registros vinculados", f"{len(credit_note_recon_df):,}")
        cn2.metric("Facturas compensadas", f"{len(compensated_invoice_df):,}")
        cn3.metric("NC sin match", f"{len(unresolved_credit_note_df):,}")

        st.info("Uso recomendado: primero revisa las facturas ya compensadas. Si una NC no quedó enlazada sola, baja al bloque de cruce manual, selecciona la factura y la NC, guarda y ambas saldrán del ruido operativo en adelante.")

        if not compensated_invoice_df.empty:
            st.markdown("**Facturas de correo ya compensadas por una NC/anulación**")
            st.dataframe(
                safe_display(compensated_invoice_df, [
                    "proveedor_correo", "num_factura", "factura_compensada_correo", "valor_total_correo",
                    "fecha_recepcion_correo", "detalle_conciliacion",
                ], sort_by=["fecha_recepcion_correo", "proveedor_correo"], ascending=[False, True]),
                width="stretch",
                hide_index=True,
                column_config={
                    "valor_total_correo": st.column_config.NumberColumn("Valor correo", format="$ %,.0f"),
                    "fecha_recepcion_correo": st.column_config.DatetimeColumn("Fecha correo", format="YYYY-MM-DD HH:mm"),
                },
            )

        st.markdown("**Notas crédito / anulaciones detectadas**")

        st.dataframe(
            safe_display(credit_note_only_df, [
                "proveedor_correo", "num_factura", "tipo_documento_correo", "documento_relacionado_correo",
                "factura_compensada_correo", "valor_total_correo", "fecha_recepcion_correo",
                "remitente_correo", "estado_conciliacion", "descripcion_nota_correo", "detalle_conciliacion",
            ], sort_by=["fecha_recepcion_correo", "proveedor_correo"], ascending=[False, True]),
            width="stretch",
            hide_index=True,
            column_config={
                "valor_total_correo": st.column_config.NumberColumn("Valor correo", format="$ %,.0f"),
                "fecha_recepcion_correo": st.column_config.DatetimeColumn("Fecha correo", format="YYYY-MM-DD HH:mm"),
            },
        )

        st.markdown("---")
        st.markdown("**Cruce manual persistente**")
        st.caption("Si sabes que una factura de correo y una NC/anulación se compensan, puedes registrar el cruce aquí. Quedará guardado en Google Sheets y esos documentos dejarán de salir como no conciliados.")

        manual_invoice_candidates = filtered_master[
            filtered_master["estado_conciliacion"].isin(["Solo correo", "Solo correo compensado por NC"])
        ].copy()
        manual_credit_candidates = filtered_master[
            filtered_master["estado_conciliacion"].isin(["NC/anulación compensada", "NC/anulación sin ERP"])
            & filtered_master["tipo_documento_correo"].eq("NOTA_CREDITO")
        ].copy()

        if manual_invoice_candidates.empty or manual_credit_candidates.empty:
            st.info("No hay suficientes candidatos visibles para registrar un cruce manual en este filtro.")
        else:
            invoice_options = {reconciliation_option_label(row): row["invoice_key"] for _, row in manual_invoice_candidates.iterrows()}
            credit_options = {reconciliation_option_label(row): row["invoice_key"] for _, row in manual_credit_candidates.iterrows()}

            manual_col1, manual_col2 = st.columns(2)
            selected_invoice_label = manual_col1.selectbox("Factura a compensar", list(invoice_options.keys()), key="manual_recon_invoice")
            selected_credit_label = manual_col2.selectbox("NC / anulación relacionada", list(credit_options.keys()), key="manual_recon_credit")
            manual_note = st.text_input("Nota de auditoría", value="Cruce manual confirmado por tesorería", key="manual_recon_note")

            selected_invoice_key = invoice_options[selected_invoice_label]
            selected_credit_key = credit_options[selected_credit_label]
            selected_invoice_row = manual_invoice_candidates[manual_invoice_candidates["invoice_key"] == selected_invoice_key].iloc[0]
            selected_credit_row = manual_credit_candidates[manual_credit_candidates["invoice_key"] == selected_credit_key].iloc[0]

            preview_df = pd.DataFrame([
                {
                    "Rol": "Factura",
                    "Proveedor": selected_invoice_row.get("proveedor_correo") or selected_invoice_row.get("proveedor"),
                    "Documento": selected_invoice_row.get("num_factura"),
                    "Valor": selected_invoice_row.get("valor_total_correo") or selected_invoice_row.get("valor_erp"),
                    "Estado actual": selected_invoice_row.get("estado_conciliacion"),
                },
                {
                    "Rol": "NC / anulación",
                    "Proveedor": selected_credit_row.get("proveedor_correo") or selected_credit_row.get("proveedor"),
                    "Documento": selected_credit_row.get("num_factura"),
                    "Valor": selected_credit_row.get("valor_total_correo") or selected_credit_row.get("valor_erp"),
                    "Estado actual": selected_credit_row.get("estado_conciliacion"),
                },
            ])
            st.dataframe(display_ready(preview_df), width="stretch", hide_index=True, column_config={"Valor": st.column_config.NumberColumn("Valor", format="$ %,.0f")})

            if st.button("Guardar cruce manual y sacar de la vista operativa", type="primary", width="stretch", key="save_manual_recon"):
                gs_client = connect_to_google_sheets()
                if not gs_client:
                    st.error("No fue posible conectar con Google Sheets para guardar el cruce manual.")
                elif selected_invoice_key == selected_credit_key:
                    st.error("Debes escoger dos documentos distintos.")
                elif register_manual_reconciliation(gs_client, selected_invoice_row.to_dict(), selected_credit_row.to_dict(), manual_note):
                    st.session_state.pop("treasury_payload", None)
                    st.success("Cruce manual guardado. Estos documentos ya no volverán a aparecer como no conciliados en la lectura operativa.")
                    st.rerun()
                else:
                    st.error("No se pudo guardar el cruce manual.")
    st.markdown('</div>', unsafe_allow_html=True)


# ── Tab 4: No conciliado ───────────────────────────────────────────
with tab_unrec:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="table-header">Facturas pendientes sin soporte de correo</div>', unsafe_allow_html=True)
    st.markdown('<div class="table-sub">Solo se muestran facturas pendientes que todavía no tienen soporte documental encontrado en el buzón.</div>', unsafe_allow_html=True)

    if unresolved_df.empty:
        st.success("No hay facturas pendientes sin correo en este filtro.")
    else:
        uc1, uc2, uc3 = st.columns(3)
        uc1.metric("Pendientes sin correo", f"{len(unresolved_df):,}")
        uc2.metric("Valor ERP involucrado", format_currency(unresolved_df["valor_erp"].sum()))
        uc3.metric("Proveedores impactados", f"{unresolved_df['proveedor'].nunique():,}")

        st.dataframe(
            safe_display(unresolved_df, [
                "proveedor", "num_factura", "estado_conciliacion",
                "valor_erp", "fecha_emision_erp", "fecha_vencimiento_erp", "detalle_conciliacion",
            ], sort_by=["proveedor", "num_factura"]),
            width="stretch",
            hide_index=True,
            column_config={
                "valor_erp": st.column_config.NumberColumn("Valor ERP", format="$ %,.0f"),
                "fecha_emision_erp": st.column_config.DateColumn("Emisión", format="YYYY-MM-DD"),
                "fecha_vencimiento_erp": st.column_config.DateColumn("Vence", format="YYYY-MM-DD"),
            },
        )

        excel_unrec = export_df_to_excel(
            unresolved_df[["proveedor", "num_factura", "estado_conciliacion",
                           "valor_erp", "fecha_emision_erp", "fecha_vencimiento_erp", "detalle_conciliacion"]],
            sheet_name="No_Conciliado", title="Ferreinox — Facturas No Conciliadas",
        )
        st.download_button("📥 Descargar no conciliadas", excel_unrec,
                           file_name=f"Ferreinox_No_Conciliado_{date.today()}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_portal_unrec")
    st.markdown('</div>', unsafe_allow_html=True)


# ── Tab 5: Conciliado ──────────────────────────────────────────────
with tab_conc:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="table-header">Cartera pendiente ya conciliada — ERP + Correo verificados</div>', unsafe_allow_html=True)

    if conciliated_df.empty:
        st.info("No hay facturas conciliadas en este filtro.")
    else:
        cc1, cc2, cc3 = st.columns(3)
        cc1.metric("Facturas conciliadas", f"{len(conciliated_df):,}")
        cc2.metric("Valor ERP conciliado", format_currency(conciliated_df["valor_erp"].sum()))
        cc3.metric("Pendientes conciliadas", f"{int((conciliated_df['estado_conciliacion'] == 'Pendiente conciliada').sum()):,}")

        st.dataframe(
            safe_display(conciliated_df, [
                "proveedor", "num_factura", "estado_erp", "estado_conciliacion",
                "valor_erp", "valor_total_correo", "detalle_conciliacion",
            ], sort_by=["proveedor", "num_factura"]),
            width="stretch",
            hide_index=True,
            column_config={
                "valor_erp": st.column_config.NumberColumn("Valor ERP", format="$ %,.0f"),
                "valor_total_correo": st.column_config.NumberColumn("Valor correo", format="$ %,.0f"),
            },
        )

        excel_conc = export_df_to_excel(
            conciliated_df[["proveedor", "num_factura", "estado_erp", "estado_conciliacion",
                            "valor_erp", "valor_total_correo", "detalle_conciliacion"]],
            sheet_name="Conciliada", title="Ferreinox — Cartera Conciliada",
        )
        st.download_button("📥 Descargar conciliadas", excel_conc,
                           file_name=f"Ferreinox_Conciliada_{date.today()}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_portal_conc")
    st.markdown('</div>', unsafe_allow_html=True)


# ── Tab 6: Análisis Aging ──────────────────────────────────────────
with tab_aging:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="table-header">Analisis de Aging — Antigüedad de cartera pendiente</div>', unsafe_allow_html=True)
    st.markdown('<div class="table-sub">Distribucion de facturas pendientes por ventana de dias para vencer. Permite identificar concentraciones de riesgo.</div>', unsafe_allow_html=True)

    if pending_df.empty:
        st.info("No hay cartera pendiente para analizar.")
    else:
        # Aging summary
        aging_agg = pending_df.groupby("_aging", dropna=False).agg(
            Facturas=("num_factura", "count"),
            Valor=("valor_erp", "sum"),
            Proveedores=("proveedor", "nunique"),
        ).reset_index().rename(columns={"_aging": "Rango"})

        order_map = aging_bucket_order()
        aging_agg["_order"] = aging_agg["Rango"].map(order_map).fillna(99)
        aging_agg.sort_values("_order", inplace=True)
        aging_agg.drop(columns=["_order"], inplace=True)

        # KPIs por bucket
        bucket_kpis = ""
        colors = {"Vencida": "accent", "0–15 dias": "accent", "16–30 dias": "gold", "31–60 dias": "blue", "61+ dias": "green"}
        for _, row in aging_agg.iterrows():
            css = colors.get(row["Rango"], "")
            bucket_kpis += kpi_html(row["Rango"], format_currency(row["Valor"]), f"{int(row['Facturas']):,} facturas · {int(row['Proveedores']):,} proveedores", css)
        st.markdown(f'<div class="kpi-row">{bucket_kpis}</div>', unsafe_allow_html=True)

        ac1, ac2 = st.columns([1, 1])
        with ac1:
            st.dataframe(
                display_ready(aging_agg),
                width="stretch",
                hide_index=True,
                column_config={"Valor": st.column_config.NumberColumn("Valor total", format="$ %,.0f")},
            )
        with ac2:
            color_map = {"Vencida": "#ef3737", "0–15 dias": "#f59e0b", "16–30 dias": "#3b82f6", "31–60 dias": "#8b5cf6", "61+ dias": "#10b981", "Sin fecha": "#9ca3af"}
            fig_aging = px.bar(
                aging_agg, x="Rango", y="Valor", color="Rango",
                color_discrete_map=color_map,
                text=aging_agg["Valor"].apply(lambda v: format_currency(v)),
            )
            fig_aging.update_layout(
                margin=dict(t=20, b=10, l=10, r=10), height=340,
                showlegend=False, yaxis_title="", xaxis_title="",
                font=dict(size=12),
            )
            fig_aging.update_traces(textposition="outside")
            st.plotly_chart(fig_aging, width="stretch")

        # Aging by provider (top 10)
        st.markdown("---")
        st.markdown("**Top 10 proveedores por valor pendiente con desglose de aging**")

        top_providers = pending_df.groupby("proveedor")["valor_erp"].sum().nlargest(10).index.tolist()
        top_aging_df = pending_df[pending_df["proveedor"].isin(top_providers)].copy()

        if not top_aging_df.empty:
            aging_pivot = top_aging_df.groupby(["proveedor", "_aging"])["valor_erp"].sum().reset_index()
            aging_pivot.rename(columns={"_aging": "Rango", "valor_erp": "Valor"}, inplace=True)

            fig_stack = px.bar(
                aging_pivot, x="proveedor", y="Valor", color="Rango",
                color_discrete_map=color_map,
                barmode="stack",
            )
            fig_stack.update_layout(
                margin=dict(t=20, b=10, l=10, r=10), height=400,
                xaxis_title="", yaxis_title="",
                legend=dict(orientation="h", yanchor="bottom", y=-0.25),
                font=dict(size=11),
            )
            st.plotly_chart(fig_stack, width="stretch")

        excel_aging = export_df_to_excel(aging_agg, sheet_name="Aging", title="Ferreinox — Analisis de Aging")
        st.download_button("📥 Descargar aging", excel_aging,
                           file_name=f"Ferreinox_Aging_{date.today()}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_portal_aging")
    st.markdown('</div>', unsafe_allow_html=True)


# ── Tab 7: Concentración por proveedor ──────────────────────────────
with tab_provider:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="table-header">Concentracion de cartera por proveedor</div>', unsafe_allow_html=True)
    st.markdown('<div class="table-sub">Analisis de Pareto: cuanto del valor pendiente esta concentrado en los principales proveedores.</div>', unsafe_allow_html=True)

    if pending_df.empty:
        st.info("No hay cartera pendiente para analizar concentración.")
    else:
        prov_val = pending_df.groupby("proveedor").agg(
            Facturas=("num_factura", "count"),
            Valor_Pendiente=("valor_erp", "sum"),
        ).reset_index().sort_values("Valor_Pendiente", ascending=False)

        total_pending = prov_val["Valor_Pendiente"].sum()
        prov_val["Pct_Cartera"] = prov_val["Valor_Pendiente"] / total_pending if total_pending > 0 else 0
        prov_val["Pct_Acumulado"] = prov_val["Pct_Cartera"].cumsum()

        pc1, pc2 = st.columns([1, 1])
        with pc1:
            # Pareto chart
            fig_pareto = go.Figure()
            fig_pareto.add_trace(go.Bar(
                x=prov_val["proveedor"].head(15),
                y=prov_val["Valor_Pendiente"].head(15),
                name="Valor pendiente",
                marker_color="#1c4e80",
            ))
            fig_pareto.add_trace(go.Scatter(
                x=prov_val["proveedor"].head(15),
                y=prov_val["Pct_Acumulado"].head(15) * total_pending,
                name="% acumulado",
                yaxis="y2",
                line=dict(color="#ef3737", width=3),
                mode="lines+markers",
            ))
            fig_pareto.update_layout(
                yaxis=dict(title="Valor pendiente"),
                yaxis2=dict(title="% acumulado", overlaying="y", side="right",
                            tickformat=".0%", range=[0, total_pending * 1.1]),
                margin=dict(t=20, b=10, l=10, r=60), height=380,
                showlegend=True, legend=dict(orientation="h", yanchor="bottom", y=-0.35),
                font=dict(size=11),
            )
            st.plotly_chart(fig_pareto, width="stretch")

        with pc2:
            # Treemap
            prov_top = prov_val.head(20).copy()
            fig_tree = px.treemap(
                prov_top, path=["proveedor"], values="Valor_Pendiente",
                color="Valor_Pendiente",
                color_continuous_scale="Blues",
            )
            fig_tree.update_layout(margin=dict(t=20, b=10, l=10, r=10), height=380)
            st.plotly_chart(fig_tree, width="stretch")

        st.dataframe(
            display_ready(prov_val),
            width="stretch",
            hide_index=True,
            column_config={
                "Valor_Pendiente": st.column_config.NumberColumn("Valor pendiente", format="$ %,.0f"),
                "Pct_Cartera": st.column_config.NumberColumn("% cartera", format="%.1f%%"),
                "Pct_Acumulado": st.column_config.NumberColumn("% acumulado", format="%.1f%%"),
            },
        )

        # Concentration insight
        top5_pct = prov_val["Pct_Cartera"].head(5).sum()
        top10_pct = prov_val["Pct_Cartera"].head(10).sum()
        st.markdown(
            f"**Insight:** Los top 5 proveedores concentran el **{top5_pct:.1%}** de la cartera pendiente. "
            f"Los top 10 representan el **{top10_pct:.1%}**."
        )

        excel_conc_prov = export_df_to_excel(prov_val, sheet_name="Concentracion", title="Ferreinox — Concentracion por Proveedor")
        st.download_button("📥 Descargar concentración", excel_conc_prov,
                           file_name=f"Ferreinox_Concentracion_{date.today()}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_portal_conc_prov")
    st.markdown('</div>', unsafe_allow_html=True)

    # Discount conditions summary
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="table-header">Condiciones de descuento pronto pago configuradas</div>', unsafe_allow_html=True)

    discount_df = get_discount_summary_for_suppliers()
    if discount_df.empty:
        st.info("No hay descuentos configurados.")
    else:
        st.dataframe(
            display_ready(discount_df),
            width="stretch",
            hide_index=True,
            column_config={
                "Descuento %": st.column_config.NumberColumn("Descuento %", format="%.1f%%"),
                "Días límite": st.column_config.NumberColumn("Días desde emisión"),
            },
        )
    st.markdown('</div>', unsafe_allow_html=True)


# ── Tab 8: Trazabilidad ────────────────────────────────────────────
with tab_trace:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="table-header">Trazabilidad de lotes y correos de pago</div>', unsafe_allow_html=True)
    st.markdown('<div class="table-sub">Historico de lotes registrados y correos enviados desde el Planificador de Pagos.</div>', unsafe_allow_html=True)

    if lot_history_df.empty and email_log_df.empty:
        st.info("No hay trazabilidad registrada todavía. Los lotes y correos aparecerán aquí cuando se generen desde el Planificador.")
    else:
        if not lot_history_df.empty:
            st.markdown("**📦 Lotes de pago registrados**")
            lot_filtered = lot_history_df.copy()
            if selected_supplier != "Todos" and "proveedor" in lot_filtered.columns:
                lot_filtered = lot_filtered[lot_filtered["proveedor"] == selected_supplier]

            if lot_filtered.empty:
                st.info(f"No hay lotes para {selected_supplier}.")
            else:
                tl1, tl2, tl3 = st.columns(3)
                tl1.metric("Lotes únicos", f"{lot_filtered['lote_id'].nunique() if 'lote_id' in lot_filtered.columns else 0:,}")
                if "valor_a_pagar" in lot_filtered.columns:
                    tl2.metric("Valor programado", format_currency(lot_filtered["valor_a_pagar"].sum()))
                if "valor_descuento" in lot_filtered.columns:
                    tl3.metric("Descuento capturado", format_currency(lot_filtered["valor_descuento"].sum()))

                lot_cols = [c for c in ["lote_id", "fecha_programada_pago", "proveedor", "num_factura",
                                        "valor_factura", "valor_descuento", "valor_a_pagar", "estado_lote", "responsable"]
                            if c in lot_filtered.columns]
                st.dataframe(
                    display_ready(
                        lot_filtered[lot_cols].sort_values(
                            by=[c for c in ["fecha_programada_pago", "lote_id"] if c in lot_filtered.columns],
                            ascending=False,
                        )
                    ),
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "valor_factura": st.column_config.NumberColumn("Valor factura", format="$ %,.0f"),
                        "valor_descuento": st.column_config.NumberColumn("Descuento", format="$ %,.0f"),
                        "valor_a_pagar": st.column_config.NumberColumn("Valor pago", format="$ %,.0f"),
                        "fecha_programada_pago": st.column_config.DateColumn("Fecha pago", format="YYYY-MM-DD"),
                    },
                )

                excel_lots = export_df_to_excel(lot_filtered[lot_cols], sheet_name="Lotes", title="Ferreinox — Historico de Lotes")
                st.download_button("📥 Descargar lotes", excel_lots,
                                   file_name=f"Ferreinox_Lotes_{date.today()}.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_portal_lots")

        if not email_log_df.empty:
            st.markdown("---")
            st.markdown("**📧 Correos de pago enviados**")
            email_filtered = email_log_df.copy()
            if selected_supplier != "Todos" and "proveedor" in email_filtered.columns:
                email_filtered = email_filtered[email_filtered["proveedor"] == selected_supplier]

            if not email_filtered.empty:
                te1, te2 = st.columns(2)
                te1.metric("Correos enviados", f"{int((email_filtered.get('estado_envio', pd.Series(dtype=str)) == 'Enviado').sum()):,}")
                te2.metric("Correos fallidos", f"{int((email_filtered.get('estado_envio', pd.Series(dtype=str)) == 'Fallido').sum()):,}")

                email_cols = [c for c in ["fecha_envio", "proveedor", "email_destino", "asunto",
                                          "facturas", "ahorro_total", "estado_envio", "detalle_envio"]
                              if c in email_filtered.columns]
                st.dataframe(display_ready(email_filtered[email_cols].tail(30)), width="stretch", hide_index=True)
            else:
                st.info(f"No hay correos enviados para {selected_supplier}.")
    st.markdown('</div>', unsafe_allow_html=True)


# ─── Full master Excel export ────────────────────────────────────────
st.markdown("---")
with st.expander("📊 Exportar maestro completo de cartera"):
    if not filtered_master.empty:
        export_cols = ["proveedor", "num_factura", "valor_erp", "estado_erp", "estado_conciliacion",
                       "estado_vencimiento", "valor_descuento", "valor_a_pagar", "descuento_pct",
                       "valor_total_correo", "diferencia_valor", "fecha_vencimiento_erp",
                       "fecha_emision_erp", "dias_para_vencer", "detalle_conciliacion", "condiciones_comerciales"]
        valid_export_cols = [c for c in export_cols if c in filtered_master.columns]
        excel_master = export_df_to_excel(
            filtered_master[valid_export_cols],
            sheet_name="Maestro_Cartera",
            title=f"Ferreinox — Maestro de Cartera ({date.today().strftime('%Y-%m-%d')})",
        )
        st.download_button("📥 Descargar maestro completo", excel_master,
                           file_name=f"Ferreinox_Maestro_Cartera_{date.today()}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_portal_master")
    else:
        st.info("No hay datos para exportar.")