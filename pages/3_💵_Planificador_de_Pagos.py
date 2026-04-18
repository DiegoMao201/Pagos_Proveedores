# -*- coding: utf-8 -*-
"""Planificador profesional de pagos con tres mesas: Crítico, Financiero, Neto."""

from datetime import date, timedelta

import pandas as pd
import streamlit as st

from common.treasury_core import (
    DISCOUNT_PROVIDERS,
    EMAIL_LOG_COLUMNS,
    PAYMENT_LOT_COLUMNS,
    build_email_log_row,
    build_payment_email_html,
    connect_to_google_sheets,
    create_payment_lot,
    ensure_authenticated,
    export_df_to_excel,
    format_currency,
    get_discount_summary_for_suppliers,
    load_operational_payload,
    register_email_log,
    register_payment_lot,
    safe_display,
    send_email_via_sendgrid,
)


st.set_page_config(page_title="Planificador de Pagos | Ferreinox", page_icon="💵", layout="wide")
ensure_authenticated()

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
        .kpi-row { display: flex; gap: .7rem; flex-wrap: wrap; margin: .6rem 0 1rem 0; }
        .kpi-card {
            flex: 1 1 170px;
            background: linear-gradient(135deg, #f8fbff 0%, #eef3f8 100%);
            border: 1px solid rgba(12,45,87,.08);
            border-radius: 18px;
            padding: 1rem 1.1rem;
            min-width: 170px;
        }
        .kpi-card.accent { border-left: 4px solid #ef3737; }
        .kpi-card.gold { border-left: 4px solid #f3b221; }
        .kpi-card.green { border-left: 4px solid #0fa968; }
        .kpi-card.blue { border-left: 4px solid #1c4e80; }
        .kpi-label { font-size: .72rem; text-transform: uppercase; letter-spacing: .08em; color: #506070; margin-bottom: .15rem; }
        .kpi-value { font-size: 1.45rem; font-weight: 800; color: #0c2d57; line-height: 1.1; }
        .kpi-sub { font-size: .75rem; color: #6b7c8f; margin-top: .15rem; }
        .hero-planner {
            background: linear-gradient(135deg, #0d2340 0%, #1c4e80 55%, #f3b221 100%);
            color: white; padding: 26px 30px; border-radius: 26px; margin-bottom: 1rem;
            box-shadow: 0 20px 48px rgba(13,35,64,.18);
        }
        .card { background: white; border: 1px solid rgba(15,44,82,.08); border-radius: 22px;
            padding: 1.05rem 1.15rem; margin-bottom: 1rem; box-shadow: 0 12px 28px rgba(15,44,82,.06); }
        .table-header { color: #0c2d57; font-size: 1.05rem; font-weight: 800; margin-bottom: .3rem; }
        .table-sub { color: #506070; font-size: .88rem; margin-bottom: .7rem; }
        .discount-chip {
            display: inline-block; padding: 4px 10px; border-radius: 999px; font-size: .78rem; font-weight: 600;
            margin: 2px 4px 2px 0;
        }
        .discount-chip.high { background: rgba(15,169,104,.12); color: #0a7c4f; }
        .discount-chip.med { background: rgba(243,178,33,.15); color: #8a6e10; }
        .discount-chip.low { background: rgba(108,130,155,.12); color: #506070; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ─── Helpers ────────────────────────────────────────────────────────
def first_non_empty(series: pd.Series) -> str:
    valid = series.dropna().astype(str).str.strip()
    valid = valid[valid.ne("")]
    return valid.iloc[0] if not valid.empty else ""


def kpi_html(label: str, value: str, sub: str = "", css: str = "") -> str:
    sub_html = f'<div class="kpi-sub">{sub}</div>' if sub else ""
    return f'<div class="kpi-card {css}"><div class="kpi-label">{label}</div><div class="kpi-value">{value}</div>{sub_html}</div>'


# ─── Load data ──────────────────────────────────────────────────────
payload = load_operational_payload()
master_df = payload.get("master_df", pd.DataFrame())
plan_df = payload.get("payment_plan_df", pd.DataFrame())
lot_history_df = payload.get("lot_history_df", pd.DataFrame())
email_log_df = payload.get("email_log_df", pd.DataFrame())

if master_df.empty and plan_df.empty:
    st.title("Planificador de Pagos")
    if payload.get("has_snapshot"):
        st.info("No hay facturas pendientes para programar en la última foto guardada.")
    else:
        st.info("Todavía no existe una foto guardada. La primera actualización crea esa base.")
    st.stop()

# Ensure columns
for col, default in [("valor_descuento", 0.0), ("valor_a_pagar", 0.0), ("descuento_pct", 0.0),
                      ("estado_descuento", ""), ("estado_vencimiento", ""), ("estado_conciliacion", ""),
                      ("dias_para_vencer", 0), ("dias_para_objetivo", 0), ("motivo_pago", ""),
                      ("lote_id", ""), ("estado_lote", ""), ("email_pago", ""), ("email_cc", ""),
                      ("fecha_limite_descuento", pd.NaT), ("fecha_vencimiento_erp", pd.NaT),
                      ("fecha_emision_erp", pd.NaT), ("valor_erp", 0.0), ("proveedor", ""),
                      ("num_factura", ""), ("invoice_key", ""), ("registrada_para_pago", False),
                      ("riesgo_mora_48h", False), ("prioridad_pago", 0), ("condiciones_comerciales", ""),
                      ("valor_total_correo", 0.0), ("diferencia_valor", 0.0), ("estado_erp", ""),
                      ("detalle_conciliacion", "")]:
    if col not in plan_df.columns:
        plan_df[col] = default
    if col not in master_df.columns:
        master_df[col] = default

# ─── Segment data into three payment categories ────────────────────
today = pd.Timestamp.now(tz="America/Bogota").normalize()

# Pagos Críticos: vencidas + riesgo 48h
critical_df = plan_df[plan_df["estado_vencimiento"].isin(["🔴 Vencida", "🟠 Riesgo 48h"])].copy()

# Pagos Financiero: tiene descuento disponible y no está vencida
financial_df = plan_df[
    (plan_df["descuento_pct"] > 0)
    & (~plan_df["estado_vencimiento"].isin(["🔴 Vencida", "🟠 Riesgo 48h"]))
].copy()

# Pagos Neto: todo lo demás pendiente de pago
neto_keys = set(plan_df["invoice_key"]) - set(critical_df["invoice_key"]) - set(financial_df["invoice_key"])
neto_df = plan_df[plan_df["invoice_key"].isin(neto_keys)].copy()

# Notas crédito from master (negative values)
credit_notes_df = master_df[
    (master_df["valor_erp"] < 0) | (master_df["num_factura"].astype(str).str.startswith("NC-"))
].copy() if not master_df.empty else pd.DataFrame()

# Pagos programados (lotes históricos)
scheduled_df = lot_history_df.copy() if not lot_history_df.empty else pd.DataFrame()

# Cartera conciliada
reconciled_df = master_df[
    master_df["estado_conciliacion"].isin(["Pendiente conciliada", "Pendiente anterior a lectura"])
].copy() if not master_df.empty else pd.DataFrame()


# ─── Hero ───────────────────────────────────────────────────────────
st.markdown(
    f"""
    <div class="hero-planner">
        <div style="font-size:.82rem;text-transform:uppercase;letter-spacing:.08em;opacity:.86;">Ferreinox BI · Payments Engine</div>
        <div style="font-size:2.2rem;font-weight:800;line-height:1.05;margin-top:.35rem;">Planificador Profesional de Pagos</div>
        <div style="margin-top:.85rem;max-width:920px;line-height:1.55;font-size:1rem;opacity:.95;">
            Tres mesas de decision: <strong>Pagos Criticos</strong> (facturas vencidas y en riesgo),
            <strong>Pagos Financiero</strong> (oportunidades de descuento pronto pago) y
            <strong>Pagos Neto</strong> (cartera pendiente sin descuento). Selecciona facturas, genera lotes profesionales y exporta a Excel.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ─── Global KPIs ────────────────────────────────────────────────────
total_payable = plan_df["valor_erp"].sum()
total_discount = plan_df["valor_descuento"].sum()
total_net = plan_df["valor_a_pagar"].sum()
critical_value = critical_df["valor_erp"].sum() if not critical_df.empty else 0
financial_savings = financial_df["valor_descuento"].sum() if not financial_df.empty else 0
n_overdue = int((plan_df["estado_vencimiento"] == "🔴 Vencida").sum())
n_risk = int((plan_df["estado_vencimiento"] == "🟠 Riesgo 48h").sum())
n_credit = len(credit_notes_df)
credit_total = credit_notes_df["valor_erp"].sum() if not credit_notes_df.empty else 0

st.markdown(
    f"""
    <div class="kpi-row">
        {kpi_html("Cartera total programable", format_currency(total_payable), f"{len(plan_df):,} facturas", "blue")}
        {kpi_html("Criticos por pagar", format_currency(critical_value), f"{n_overdue} vencidas · {n_risk} riesgo 48h", "accent")}
        {kpi_html("Ahorro financiero disponible", format_currency(financial_savings), f"{len(financial_df):,} facturas con descuento", "gold")}
        {kpi_html("Descuento total capturado", format_currency(total_discount), f"Neto a pagar: {format_currency(total_net)}", "green")}
        {kpi_html("Notas credito", format_currency(abs(credit_total)), f"{n_credit} documentos", "")}
    </div>
    """,
    unsafe_allow_html=True,
)


# ─── Filters ────────────────────────────────────────────────────────
st.markdown("---")
filter_col1, filter_col2, filter_col3 = st.columns([1.5, 1, 1])
all_suppliers = sorted(plan_df["proveedor"].dropna().astype(str).unique().tolist())
selected_supplier = filter_col1.selectbox("🔍 Filtrar por proveedor", ["Todos"] + all_suppliers, key="planner_supplier")
date_range = filter_col2.selectbox("Ventana de vencimiento", ["Todas", "🔴 Vencida", "🟠 Riesgo 48h", "🟡 Proxima a vencer", "🟢 Vigente"], key="planner_due")
sort_option = filter_col3.selectbox("Ordenar por", ["Prioridad", "Mayor descuento", "Mayor valor", "Proveedor A-Z", "Fecha vencimiento"], key="planner_sort")


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    if selected_supplier != "Todos":
        result = result[result["proveedor"] == selected_supplier]
    if date_range != "Todas":
        result = result[result["estado_vencimiento"] == date_range]
    sort_map = {
        "Prioridad": (["prioridad_pago"], [True]),
        "Mayor descuento": (["descuento_pct", "valor_descuento"], [False, False]),
        "Mayor valor": (["valor_erp"], [False]),
        "Proveedor A-Z": (["proveedor", "num_factura"], [True, True]),
        "Fecha vencimiento": (["fecha_vencimiento_erp", "proveedor"], [True, True]),
    }
    sort_cols, sort_asc = sort_map.get(sort_option, (["prioridad_pago"], [True]))
    valid_cols = [c for c in sort_cols if c in result.columns]
    valid_asc = sort_asc[:len(valid_cols)]
    if valid_cols:
        result = result.sort_values(by=valid_cols, ascending=valid_asc)
    return result


f_critical = apply_filters(critical_df)
f_financial = apply_filters(financial_df)
f_neto = apply_filters(neto_df)


# ─── Filtered KPIs after filter ─────────────────────────────────────
if selected_supplier != "Todos" or date_range != "Todas":
    combined_filtered = pd.concat([f_critical, f_financial, f_neto], ignore_index=True)
    st.markdown(
        f"""
        <div class="kpi-row">
            {kpi_html("Filtro: Facturas visibles", f"{len(combined_filtered):,}", "", "")}
            {kpi_html("Filtro: Valor total", format_currency(combined_filtered['valor_erp'].sum()), "", "blue")}
            {kpi_html("Filtro: Ahorro disponible", format_currency(combined_filtered['valor_descuento'].sum()), "", "gold")}
            {kpi_html("Filtro: Neto a pagar", format_currency(combined_filtered['valor_a_pagar'].sum()), "", "green")}
        </div>
        """,
        unsafe_allow_html=True,
    )


# ─── Display columns config ─────────────────────────────────────────
DISPLAY_COLS = [
    "Seleccionar", "proveedor", "num_factura", "valor_erp",
    "descuento_pct", "valor_descuento", "valor_a_pagar",
    "fecha_vencimiento_erp", "fecha_limite_descuento",
    "dias_para_vencer", "estado_vencimiento", "estado_conciliacion",
    "motivo_pago", "invoice_key",
]

COLUMN_CONFIG = {
    "Seleccionar": st.column_config.CheckboxColumn("✔"),
    "proveedor": st.column_config.TextColumn("Proveedor", width="medium"),
    "num_factura": st.column_config.TextColumn("# Factura"),
    "valor_erp": st.column_config.NumberColumn("Valor factura", format="$ %,.0f"),
    "descuento_pct": st.column_config.NumberColumn("Dcto %", format="%.1f%%"),
    "valor_descuento": st.column_config.NumberColumn("Descuento", format="$ %,.0f"),
    "valor_a_pagar": st.column_config.NumberColumn("Valor a pagar", format="$ %,.0f"),
    "fecha_vencimiento_erp": st.column_config.DateColumn("Vence", format="YYYY-MM-DD"),
    "fecha_limite_descuento": st.column_config.DateColumn("Límite dcto", format="YYYY-MM-DD"),
    "dias_para_vencer": st.column_config.NumberColumn("Días vence"),
    "estado_vencimiento": st.column_config.TextColumn("Estado"),
    "estado_conciliacion": st.column_config.TextColumn("Conciliación"),
    "motivo_pago": st.column_config.TextColumn("Motivo"),
    "invoice_key": st.column_config.TextColumn("Clave", width="small"),
}


def prepare_table(df: pd.DataFrame) -> pd.DataFrame:
    table = df.copy()
    table["Seleccionar"] = False
    for col in DISPLAY_COLS:
        if col not in table.columns:
            table[col] = ""
    return table[DISPLAY_COLS]


def render_tab_table(df: pd.DataFrame, tab_key: str, empty_msg: str) -> pd.DataFrame:
    if df.empty:
        st.info(empty_msg)
        return pd.DataFrame()

    select_all = st.checkbox(f"Seleccionar todas ({len(df):,})", key=f"select_all_{tab_key}")
    table = prepare_table(df)
    if select_all:
        table["Seleccionar"] = True

    edited = st.data_editor(
        table,
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        column_config=COLUMN_CONFIG,
        key=f"editor_{tab_key}",
    )

    selected_keys = edited[edited["Seleccionar"]]["invoice_key"].tolist()
    if selected_keys:
        sel_df = df[df["invoice_key"].isin(selected_keys)]
        sc1, sc2, sc3, sc4 = st.columns(4)
        sc1.metric("Seleccionadas", f"{len(sel_df):,}")
        sc2.metric("Valor original", format_currency(sel_df["valor_erp"].sum()))
        sc3.metric("Descuento", format_currency(sel_df["valor_descuento"].sum()))
        sc4.metric("Neto a pagar", format_currency(sel_df["valor_a_pagar"].sum()))

    return edited[edited["Seleccionar"]]


# ─── TABS ───────────────────────────────────────────────────────────
tab_crit, tab_fin, tab_neto, tab_notes, tab_sched, tab_recon, tab_discounts = st.tabs([
    f"🔴 Pagos Criticos ({len(f_critical):,})",
    f"💰 Pagos Financiero ({len(f_financial):,})",
    f"📋 Pagos Neto ({len(f_neto):,})",
    f"📝 Notas Credito ({n_credit:,})",
    "📅 Pagos Programados",
    "✅ Cartera Conciliada",
    "📊 Descuentos Proveedor",
])


# ── Tab 1: Pagos Críticos ──────────────────────────────────────────
with tab_crit:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="table-header">Pagos Criticos — Facturas vencidas y en riesgo de mora</div>', unsafe_allow_html=True)
    st.markdown('<div class="table-sub">Estas facturas requieren atencion inmediata para evitar cargos por mora o afectar la relacion comercial.</div>', unsafe_allow_html=True)

    if not f_critical.empty:
        vc1, vc2, vc3 = st.columns(3)
        vc1.metric("Vencidas 🔴", f"{int((f_critical['estado_vencimiento'] == '🔴 Vencida').sum()):,}")
        vc2.metric("Riesgo 48h 🟠", f"{int((f_critical['estado_vencimiento'] == '🟠 Riesgo 48h').sum()):,}")
        vc3.metric("Valor urgente", format_currency(f_critical["valor_erp"].sum()))

    selected_crit = render_tab_table(f_critical, "critical", "No hay facturas criticas en este filtro. ✅")

    if not f_critical.empty:
        excel_crit = export_df_to_excel(
            f_critical[["proveedor", "num_factura", "valor_erp", "valor_descuento", "valor_a_pagar", "fecha_vencimiento_erp", "dias_para_vencer", "estado_vencimiento", "motivo_pago"]],
            sheet_name="Pagos_Criticos",
            title="Ferreinox — Pagos Criticos",
        )
        st.download_button("📥 Descargar Criticos en Excel", excel_crit, file_name="Ferreinox_Pagos_Criticos.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_crit")
    st.markdown('</div>', unsafe_allow_html=True)


# ── Tab 2: Pagos Financiero ────────────────────────────────────────
with tab_fin:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="table-header">Pagos Financiero — Oportunidades de descuento pronto pago</div>', unsafe_allow_html=True)
    st.markdown('<div class="table-sub">Facturas con ventana de descuento abierta. Pagar dentro del plazo captura ahorro financiero directo.</div>', unsafe_allow_html=True)

    if not f_financial.empty:
        vf1, vf2, vf3, vf4 = st.columns(4)
        vf1.metric("Facturas con descuento", f"{len(f_financial):,}")
        vf2.metric("Ahorro potencial", format_currency(f_financial["valor_descuento"].sum()))
        vf3.metric("Mejor % disponible", f"{f_financial['descuento_pct'].max():.1%}" if not f_financial.empty else "0%")
        vf4.metric("Dias promedio para limite", f"{f_financial['dias_para_vencer'].mean():.0f}" if not f_financial.empty else "—")

        # Top opportunities
        top5 = f_financial.nlargest(5, "valor_descuento")
        st.markdown("**🏆 Top 5 oportunidades de ahorro:**")
        for _, row in top5.iterrows():
            pct_label = f"{row['descuento_pct']:.1%}"
            st.markdown(
                f"- **{row['proveedor']}** — {row['num_factura']}: descuento {pct_label} = "
                f"**{format_currency(row['valor_descuento'])}** ahorro (vence {str(row.get('fecha_limite_descuento', ''))[:10]})"
            )

    selected_fin = render_tab_table(f_financial, "financial", "No hay facturas con descuento financiero en este filtro.")

    if not f_financial.empty:
        excel_fin = export_df_to_excel(
            f_financial[["proveedor", "num_factura", "valor_erp", "descuento_pct", "valor_descuento", "valor_a_pagar",
                         "fecha_vencimiento_erp", "fecha_limite_descuento", "dias_para_vencer", "estado_descuento", "motivo_pago"]],
            sheet_name="Pagos_Financiero",
            title="Ferreinox — Oportunidades Financieras Pronto Pago",
        )
        st.download_button("📥 Descargar Financiero en Excel", excel_fin, file_name="Ferreinox_Pagos_Financiero.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_fin")
    st.markdown('</div>', unsafe_allow_html=True)


# ── Tab 3: Pagos Neto ──────────────────────────────────────────────
with tab_neto:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="table-header">Pagos Neto — Cartera pendiente sin descuento especial</div>', unsafe_allow_html=True)
    st.markdown('<div class="table-sub">Facturas pendientes que no tienen ventana de descuento ni urgencia critica. Pago a valor total.</div>', unsafe_allow_html=True)

    if not f_neto.empty:
        vn1, vn2, vn3 = st.columns(3)
        vn1.metric("Facturas neto", f"{len(f_neto):,}")
        vn2.metric("Valor total neto", format_currency(f_neto["valor_erp"].sum()))
        vn3.metric("Promedio dias para vencer", f"{f_neto['dias_para_vencer'].mean():.0f}" if not f_neto.empty else "—")

    selected_neto = render_tab_table(f_neto, "neto", "No hay facturas en pagos neto para este filtro.")

    if not f_neto.empty:
        excel_neto = export_df_to_excel(
            f_neto[["proveedor", "num_factura", "valor_erp", "valor_a_pagar", "fecha_vencimiento_erp",
                     "dias_para_vencer", "estado_vencimiento", "estado_conciliacion", "motivo_pago"]],
            sheet_name="Pagos_Neto",
            title="Ferreinox — Pagos Neto",
        )
        st.download_button("📥 Descargar Neto en Excel", excel_neto, file_name="Ferreinox_Pagos_Neto.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_neto")
    st.markdown('</div>', unsafe_allow_html=True)


# ── Tab 4: Notas Crédito ───────────────────────────────────────────
with tab_notes:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="table-header">Notas Credito registradas</div>', unsafe_allow_html=True)
    st.markdown('<div class="table-sub">Documentos con valor negativo o prefijo NC- identificados en la cartera.</div>', unsafe_allow_html=True)

    if credit_notes_df.empty:
        st.info("No se encontraron notas crédito en la cartera actual.")
    else:
        nc_filtered = credit_notes_df.copy()
        if selected_supplier != "Todos":
            nc_filtered = nc_filtered[nc_filtered["proveedor"] == selected_supplier]

        if nc_filtered.empty:
            st.info(f"No hay notas crédito para {selected_supplier}.")
        else:
            nc1, nc2 = st.columns(2)
            nc1.metric("Notas crédito", f"{len(nc_filtered):,}")
            nc2.metric("Valor total NC", format_currency(abs(nc_filtered["valor_erp"].sum())))

            st.dataframe(
                safe_display(nc_filtered, [
                    "proveedor", "num_factura", "valor_erp", "fecha_emision_erp",
                    "fecha_vencimiento_erp", "estado_erp", "estado_conciliacion",
                ], sort_by=["proveedor", "fecha_emision_erp"]),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "valor_erp": st.column_config.NumberColumn("Valor NC", format="$ %,.0f"),
                    "fecha_emision_erp": st.column_config.DateColumn("Emisión", format="YYYY-MM-DD"),
                    "fecha_vencimiento_erp": st.column_config.DateColumn("Vence", format="YYYY-MM-DD"),
                },
            )

            excel_nc = export_df_to_excel(
                nc_filtered[["proveedor", "num_factura", "valor_erp", "fecha_emision_erp", "fecha_vencimiento_erp", "estado_erp"]],
                sheet_name="Notas_Credito",
                title="Ferreinox — Notas Crédito",
            )
            st.download_button("📥 Descargar Notas Crédito en Excel", excel_nc, file_name="Ferreinox_Notas_Credito.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_nc")
    st.markdown('</div>', unsafe_allow_html=True)


# ── Tab 5: Pagos Programados ───────────────────────────────────────
with tab_sched:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="table-header">Historial de Lotes de Pago Programados</div>', unsafe_allow_html=True)

    if scheduled_df.empty:
        st.info("No hay lotes de pago registrados todavía.")
    else:
        sched_filtered = scheduled_df.copy()
        if selected_supplier != "Todos" and "proveedor" in sched_filtered.columns:
            sched_filtered = sched_filtered[sched_filtered["proveedor"] == selected_supplier]

        if sched_filtered.empty:
            st.info(f"No hay lotes registrados para {selected_supplier}.")
        else:
            sp1, sp2, sp3 = st.columns(3)
            sp1.metric("Lotes totales", f"{sched_filtered['lote_id'].nunique() if 'lote_id' in sched_filtered.columns else len(sched_filtered):,}")
            if "valor_a_pagar" in sched_filtered.columns:
                sp2.metric("Valor programado", format_currency(sched_filtered["valor_a_pagar"].sum()))
            if "valor_descuento" in sched_filtered.columns:
                sp3.metric("Descuento capturado", format_currency(sched_filtered["valor_descuento"].sum()))

            sched_cols = [c for c in ["lote_id", "fecha_programada_pago", "proveedor", "num_factura",
                                       "valor_factura", "valor_descuento", "valor_a_pagar", "estado_lote",
                                       "responsable", "motivo_pago"] if c in sched_filtered.columns]
            st.dataframe(
                sched_filtered[sched_cols].sort_values(by=[c for c in ["fecha_programada_pago", "lote_id"] if c in sched_filtered.columns], ascending=False),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "valor_factura": st.column_config.NumberColumn("Valor factura", format="$ %,.0f"),
                    "valor_descuento": st.column_config.NumberColumn("Descuento", format="$ %,.0f"),
                    "valor_a_pagar": st.column_config.NumberColumn("Valor pago", format="$ %,.0f"),
                    "fecha_programada_pago": st.column_config.DateColumn("Fecha pago", format="YYYY-MM-DD"),
                },
            )

            excel_sched = export_df_to_excel(
                sched_filtered[sched_cols],
                sheet_name="Pagos_Programados",
                title="Ferreinox — Lotes de Pago Programados",
            )
            st.download_button("📥 Descargar Programados en Excel", excel_sched, file_name="Ferreinox_Pagos_Programados.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_sched")

    # Email log
    if not email_log_df.empty:
        st.markdown("---")
        st.markdown("**📧 Historial de correos de pago enviados**")
        email_filtered = email_log_df.copy()
        if selected_supplier != "Todos" and "proveedor" in email_filtered.columns:
            email_filtered = email_filtered[email_filtered["proveedor"] == selected_supplier]
        if not email_filtered.empty:
            email_cols = [c for c in ["fecha_envio", "proveedor", "asunto", "facturas", "ahorro_total", "estado_envio"] if c in email_filtered.columns]
            st.dataframe(email_filtered[email_cols].tail(20), use_container_width=True, hide_index=True)
    st.markdown('</div>', unsafe_allow_html=True)


# ── Tab 6: Cartera Conciliada ──────────────────────────────────────
with tab_recon:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="table-header">Cartera Conciliada — ERP + Correo verificados</div>', unsafe_allow_html=True)
    st.markdown('<div class="table-sub">Facturas donde el valor de ERP y el soporte de correo estan alineados.</div>', unsafe_allow_html=True)

    if reconciled_df.empty:
        st.info("No hay facturas conciliadas disponibles.")
    else:
        recon_filtered = reconciled_df.copy()
        if selected_supplier != "Todos":
            recon_filtered = recon_filtered[recon_filtered["proveedor"] == selected_supplier]

        if recon_filtered.empty:
            st.info(f"No hay facturas conciliadas para {selected_supplier}.")
        else:
            rc1, rc2, rc3 = st.columns(3)
            rc1.metric("Facturas conciliadas", f"{len(recon_filtered):,}")
            rc2.metric("Valor ERP conciliado", format_currency(recon_filtered["valor_erp"].sum()))
            rc3.metric("Pendientes conciliadas", f"{int((recon_filtered['estado_conciliacion'] == 'Pendiente conciliada').sum()):,}")

            st.dataframe(
                safe_display(recon_filtered, [
                    "proveedor", "num_factura", "valor_erp", "valor_total_correo",
                    "diferencia_valor", "estado_erp", "estado_conciliacion",
                    "fecha_vencimiento_erp", "detalle_conciliacion",
                ], sort_by=["proveedor", "num_factura"]),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "valor_erp": st.column_config.NumberColumn("Valor ERP", format="$ %,.0f"),
                    "valor_total_correo": st.column_config.NumberColumn("Valor correo", format="$ %,.0f"),
                    "diferencia_valor": st.column_config.NumberColumn("Diferencia", format="$ %,.0f"),
                    "fecha_vencimiento_erp": st.column_config.DateColumn("Vence", format="YYYY-MM-DD"),
                },
            )

            excel_recon = export_df_to_excel(
                recon_filtered[["proveedor", "num_factura", "valor_erp", "valor_total_correo", "diferencia_valor",
                                "estado_erp", "estado_conciliacion", "fecha_vencimiento_erp"]],
                sheet_name="Conciliada",
                title="Ferreinox — Cartera Conciliada",
            )
            st.download_button("📥 Descargar Conciliada en Excel", excel_recon, file_name="Ferreinox_Cartera_Conciliada.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_recon")
    st.markdown('</div>', unsafe_allow_html=True)


# ── Tab 7: Descuentos por proveedor ────────────────────────────────
with tab_discounts:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="table-header">Configuracion de Descuentos Pronto Pago por Proveedor</div>', unsafe_allow_html=True)
    st.markdown('<div class="table-sub">Resumen de las condiciones negociadas con cada proveedor. Si un proveedor no aparece aqui, no tiene descuento configurado.</div>', unsafe_allow_html=True)

    discount_df = get_discount_summary_for_suppliers()
    if discount_df.empty:
        st.info("No hay descuentos configurados.")
    else:
        st.dataframe(
            discount_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Descuento %": st.column_config.NumberColumn("Descuento %", format="%.1f%%"),
                "Días límite": st.column_config.NumberColumn("Días desde emisión"),
            },
        )

        st.markdown("---")
        st.markdown("**Resumen visual de condiciones:**")
        for supplier, rules in DISCOUNT_PROVIDERS.items():
            chips = ""
            for r in sorted(rules, key=lambda x: x["days"]):
                pct = r["rate"]
                css_class = "high" if pct >= 0.03 else "med" if pct >= 0.015 else "low"
                chips += f'<span class="discount-chip {css_class}">{pct:.1%} a {r["days"]}d</span>'
            st.markdown(f"**{supplier}**: {chips}", unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


# ─── PAYMENT LOT BUILDER (floating section) ─────────────────────────
st.markdown("---")
st.markdown("## 📨 Generar Lote de Pago")
st.markdown("Las facturas seleccionadas en cualquier pestaña (Criticos, Financiero o Neto) se consolidan aqui para armar el lote.")

# Collect all selected invoice keys from all tabs
all_selected_keys = set()
for tab_key in ["critical", "financial", "neto"]:
    editor_key = f"editor_{tab_key}"
    if editor_key in st.session_state:
        editor_data = st.session_state[editor_key]
        if isinstance(editor_data, pd.DataFrame) and "Seleccionar" in editor_data.columns:
            sel = editor_data[editor_data["Seleccionar"]]
            if "invoice_key" in sel.columns:
                all_selected_keys.update(sel["invoice_key"].tolist())

selected_for_lot = plan_df[plan_df["invoice_key"].isin(all_selected_keys)].copy() if all_selected_keys else pd.DataFrame()

if selected_for_lot.empty:
    st.info("Marca facturas en las pestañas Criticos, Financiero o Neto para construir un lote de pago.")
else:
    st.markdown(f"**{len(selected_for_lot):,} facturas seleccionadas** de {selected_for_lot['proveedor'].nunique()} proveedores")

    lm1, lm2, lm3, lm4 = st.columns(4)
    lm1.metric("Facturas en lote", f"{len(selected_for_lot):,}")
    lm2.metric("Valor original", format_currency(selected_for_lot["valor_erp"].sum()))
    lm3.metric("Descuento capturado", format_currency(selected_for_lot["valor_descuento"].sum()))
    lm4.metric("Valor final a pagar", format_currency(selected_for_lot["valor_a_pagar"].sum()))

    st.dataframe(
        selected_for_lot[["proveedor", "num_factura", "valor_erp", "descuento_pct", "valor_descuento", "valor_a_pagar", "motivo_pago"]],
        use_container_width=True,
        hide_index=True,
        column_config={
            "valor_erp": st.column_config.NumberColumn("Valor factura", format="$ %,.0f"),
            "descuento_pct": st.column_config.NumberColumn("Dcto %", format="%.1f%%"),
            "valor_descuento": st.column_config.NumberColumn("Descuento", format="$ %,.0f"),
            "valor_a_pagar": st.column_config.NumberColumn("Neto", format="$ %,.0f"),
        },
    )

    excel_lot = export_df_to_excel(
        selected_for_lot[["proveedor", "num_factura", "valor_erp", "descuento_pct", "valor_descuento",
                          "valor_a_pagar", "fecha_vencimiento_erp", "fecha_limite_descuento", "motivo_pago"]],
        sheet_name="Lote_Pago",
        title=f"Ferreinox — Lote de Pago ({date.today().strftime('%Y-%m-%d')})",
    )
    st.download_button("📥 Descargar Lote seleccionado en Excel", excel_lot, file_name=f"Ferreinox_Lote_{date.today()}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_lot")

    # ─── Email + lot registration ───────────────────────────────────
    st.markdown("---")
    st.subheader("Datos del correo y registro")

    lot_providers = sorted(selected_for_lot["proveedor"].unique().tolist())
    target_provider = st.selectbox("Proveedor destino del correo", lot_providers, key="lot_target_provider")
    provider_lot = selected_for_lot[selected_for_lot["proveedor"] == target_provider].copy()

    lc1, lc2, lc3 = st.columns(3)
    payment_date = lc1.date_input("Fecha programada de pago", value=date.today(), key="lot_payment_date")
    responsible = lc2.text_input("Responsable", value="Tesoreria Ferreinox", key="lot_responsible")
    to_email_default = first_non_empty(provider_lot["email_pago"]) if "email_pago" in provider_lot.columns else ""
    cc_default = first_non_empty(provider_lot["email_cc"]) if "email_cc" in provider_lot.columns else ""
    to_email = lc3.text_input("Correo destino", value=to_email_default, key="lot_to_email")
    cc_email = st.text_input("CC", value=cc_default, key="lot_cc")
    email_notes = st.text_area("Mensaje adicional", value="Agradecemos validar cualquier novedad documental o financiera de este lote.", key="lot_notes")

    html_preview = build_payment_email_html(target_provider, provider_lot, payment_date, email_notes)
    with st.expander("Vista previa del correo profesional", expanded=False):
        st.components.v1.html(html_preview, height=700, scrolling=True)

    if st.button("📨 Registrar lote y enviar correo", type="primary", use_container_width=True, key="btn_send_lot"):
        if not to_email:
            st.error("Indica el correo destino del proveedor.")
            st.stop()

        gs_client = connect_to_google_sheets()
        if not gs_client:
            st.error("No fue posible conectar con Google Sheets.")
            st.stop()

        lot_df = create_payment_lot(provider_lot, payment_date, responsible, to_email)
        if not register_payment_lot(gs_client, lot_df[PAYMENT_LOT_COLUMNS]):
            st.error("No se pudo registrar el lote.")
            st.stop()

        subject = f"Ferreinox | Programacion de pago {target_provider}"
        ok, detail = send_email_via_sendgrid(
            to_email=to_email,
            cc_emails=[e.strip() for e in cc_email.split(",") if e.strip()],
            subject=subject,
            html_content=html_preview,
        )

        log_row = build_email_log_row(
            lote_id=lot_df["lote_id"].iloc[0],
            provider_name=target_provider,
            to_email=to_email,
            cc_email=cc_email,
            subject=subject,
            lot_df=lot_df,
            status="Enviado" if ok else "Fallido",
            detail=detail,
        )
        register_email_log(gs_client, log_row)

        if ok:
            st.success(f"✅ Lote **{lot_df['lote_id'].iloc[0]}** registrado y correo enviado a {to_email}.")
        else:
            st.warning(f"Lote registrado, pero el correo fallo: {detail}")


# ─── Full master Excel export ────────────────────────────────────────
st.markdown("---")
with st.expander("📊 Exportar reporte completo del plan de pagos"):
    if not plan_df.empty:
        full_export_cols = ["proveedor", "num_factura", "valor_erp", "descuento_pct", "valor_descuento",
                           "valor_a_pagar", "fecha_vencimiento_erp", "fecha_limite_descuento",
                           "dias_para_vencer", "estado_vencimiento", "estado_conciliacion", "motivo_pago"]
        excel_full = export_df_to_excel(
            plan_df[[c for c in full_export_cols if c in plan_df.columns]],
            sheet_name="Plan_Completo",
            title=f"Ferreinox — Plan de Pagos Completo ({date.today().strftime('%Y-%m-%d')})",
        )
        st.download_button("📥 Descargar Plan Completo en Excel", excel_full,
                           file_name=f"Ferreinox_Plan_Pagos_Completo_{date.today()}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_full")
    else:
        st.info("No hay datos en el plan de pagos para exportar.")