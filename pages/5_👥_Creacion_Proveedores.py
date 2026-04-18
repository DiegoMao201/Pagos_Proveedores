# -*- coding: utf-8 -*-
"""Maestro operativo de proveedores para pagos, alertas y contactos."""

import pandas as pd
import streamlit as st

from common.treasury_core import connect_to_google_sheets, ensure_authenticated, load_provider_master, save_provider_master


st.set_page_config(page_title="Maestro de Proveedores | Ferreinox", page_icon="👥", layout="wide")
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
            .provider-hero {
                background: linear-gradient(135deg, #0d2340 0%, #173d67 52%, #ef3737 100%);
                color: white;
                padding: 24px 28px;
                border-radius: 24px;
                margin-bottom: 1rem;
                box-shadow: 0 18px 42px rgba(13,35,64,.18);
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def main() -> None:
    inject_styles()
    st.markdown(
        """
        <div class="provider-hero">
            <div style="font-size:.82rem;text-transform:uppercase;letter-spacing:.08em;opacity:.86;">Ferreinox BI · Supplier Master</div>
            <div style="font-size:2.1rem;font-weight:800;line-height:1.05;margin-top:.35rem;">Maestro Operativo de Proveedores</div>
            <div style="margin-top:.8rem;max-width:860px;line-height:1.55;font-size:1rem;opacity:.95;">Aqui se administran correos de pago, contactos, alertas internas y condiciones comerciales que alimentan la conciliacion y la programacion automatizada.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    gs_client = connect_to_google_sheets()
    if not gs_client:
        st.error("No fue posible conectar con Google Sheets.")
        st.stop()

    provider_df = load_provider_master(gs_client)
    if provider_df.empty:
        st.warning("No se encontro base de proveedores para editar.")
        st.stop()

    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
    metric_col1.metric("Proveedores en maestro", f"{len(provider_df):,}")
    metric_col2.metric("Con correo de pago", f"{int(provider_df['email_pago'].astype(str).str.strip().ne('').sum()):,}")
    metric_col3.metric("Con correo de alertas", f"{int(provider_df['email_alertas'].astype(str).str.strip().ne('').sum()):,}")
    metric_col4.metric("Activos", f"{int(provider_df['activo'].fillna(True).sum()):,}")

    gaps_df = provider_df.copy()
    gaps_df["falta_correo_pago"] = gaps_df["email_pago"].fillna("").astype(str).str.strip().eq("")
    gaps_df["falta_alerta"] = gaps_df["email_alertas"].fillna("").astype(str).str.strip().eq("")
    gaps_df["falta_contacto"] = gaps_df["contacto_pagos"].fillna("").astype(str).str.strip().eq("")

    st.info(
        f"Pendientes de calidad del maestro: {int(gaps_df['falta_correo_pago'].sum()):,} proveedores sin correo de pago, {int(gaps_df['falta_alerta'].sum()):,} sin correo de alertas y {int(gaps_df['falta_contacto'].sum()):,} sin contacto de pagos."
    )

    quality_tab, editor_tab = st.tabs(["🔎 Brechas de calidad", "📝 Edicion operativa"])

    with quality_tab:
        st.dataframe(
            gaps_df.loc[
                gaps_df[["falta_correo_pago", "falta_alerta", "falta_contacto"]].any(axis=1),
                ["proveedor", "activo", "email_pago", "email_alertas", "contacto_pagos", "telefono", "observaciones"],
            ],
            use_container_width=True,
            hide_index=True,
        )

    editable_columns = [
        "activo",
        "codigo_proveedor",
        "nif",
        "proveedor",
        "email_pago",
        "email_cc",
        "email_alertas",
        "contacto_pagos",
        "contacto_tesoreria",
        "telefono",
        "condiciones_comerciales",
        "observaciones",
    ]

    with editor_tab:
        edited_df = st.data_editor(
            provider_df[editable_columns],
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            column_config={
                "activo": st.column_config.CheckboxColumn("Activo"),
                "codigo_proveedor": "Codigo",
                "nif": "NIF/NIT",
                "proveedor": st.column_config.TextColumn("Proveedor", width="large"),
                "email_pago": st.column_config.TextColumn("Correo pagos", width="medium"),
                "email_cc": st.column_config.TextColumn("CC", width="medium"),
                "email_alertas": st.column_config.TextColumn("Correo alertas internas", width="medium"),
                "contacto_pagos": st.column_config.TextColumn("Contacto pagos"),
                "contacto_tesoreria": st.column_config.TextColumn("Contacto tesoreria"),
                "telefono": st.column_config.TextColumn("Telefono"),
                "condiciones_comerciales": st.column_config.TextColumn("Condiciones comerciales", width="large"),
                "observaciones": st.column_config.TextColumn("Observaciones", width="large"),
            },
            key="provider_master_editor",
        )

        if st.button("💾 Guardar maestro de proveedores", type="primary", use_container_width=True):
            updated_df = provider_df.copy()
            for column in edited_df.columns:
                updated_df[column] = edited_df[column]
            if save_provider_master(gs_client, updated_df):
                st.success("Maestro de proveedores actualizado correctamente.")
            else:
                st.error("No se pudo guardar el maestro de proveedores.")


main()