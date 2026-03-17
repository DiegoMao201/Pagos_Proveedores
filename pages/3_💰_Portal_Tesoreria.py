# -*- coding: utf-8 -*-
"""
Centro operativo de alertas de Tesoreria para Ferreinox.
Enfocado en conciliacion ERP vs correo y envio guiado de correos.
"""

import os
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape

import pandas as pd
import streamlit as st

from common.utils import COLOMBIA_TZ, connect_to_google_sheets, load_data_from_gsheet


SOLO_CORREO = "Correo recibido sin radicar en ERP"
SOLO_ERP = "ERP sin soporte electronico en correo"
DISCREPANCIA = "Discrepancia entre ERP y correo"

ALERTA_SOLO_CORREO_DIAS = 5
CRITICO_SOLO_CORREO_DIAS = 8
ALERTA_SOLO_ERP_DIAS = 5
CRITICO_SOLO_ERP_DIAS = 10
VENTANA_OPERATIVA_DIAS = 40


def require_authentication() -> None:
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False
    if not st.session_state["password_correct"]:
        st.error("Debes iniciar sesion desde Dashboard General para acceder a esta pagina.")
        st.stop()


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        .main .block-container {
            padding-top: 1.6rem;
            padding-bottom: 2.5rem;
        }
        .portal-shell {
            background:
                radial-gradient(circle at top right, rgba(199, 228, 255, 0.85), transparent 30%),
                linear-gradient(135deg, #f6fbff 0%, #eef4fb 55%, #f8fafc 100%);
            border: 1px solid rgba(12, 45, 87, 0.08);
            border-radius: 28px;
            padding: 1.6rem 1.8rem;
            box-shadow: 0 20px 60px rgba(12, 45, 87, 0.08);
            margin-bottom: 1.2rem;
        }
        .portal-kicker {
            color: #0c2d57;
            font-size: 0.85rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin-bottom: 0.4rem;
        }
        .portal-title {
            color: #0c2d57;
            font-size: 2.2rem;
            font-weight: 800;
            line-height: 1.05;
            margin: 0;
        }
        .portal-subtitle {
            color: #4a6178;
            font-size: 1rem;
            margin: 0.9rem 0 1.1rem 0;
            max-width: 900px;
        }
        .hero-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
            gap: 0.8rem;
            margin-top: 1rem;
        }
        .hero-pill {
            background: rgba(255, 255, 255, 0.9);
            border: 1px solid rgba(12, 45, 87, 0.08);
            border-radius: 18px;
            padding: 0.9rem 1rem;
        }
        .hero-pill-label {
            font-size: 0.8rem;
            color: #6a7e92;
            margin-bottom: 0.2rem;
        }
        .hero-pill-value {
            font-size: 1.25rem;
            font-weight: 800;
            color: #0c2d57;
        }
        .section-card {
            background: #ffffff;
            border: 1px solid rgba(12, 45, 87, 0.08);
            border-radius: 22px;
            padding: 1.1rem 1.2rem;
            box-shadow: 0 10px 28px rgba(12, 45, 87, 0.05);
            margin-bottom: 1rem;
        }
        .section-title {
            color: #0c2d57;
            font-size: 1.05rem;
            font-weight: 800;
            margin-bottom: 0.2rem;
        }
        .section-copy {
            color: #607385;
            font-size: 0.92rem;
            margin-bottom: 0.9rem;
        }
        .mini-card {
            background: linear-gradient(180deg, #ffffff 0%, #f7fbff 100%);
            border: 1px solid rgba(12, 45, 87, 0.08);
            border-radius: 18px;
            padding: 1rem;
            min-height: 120px;
        }
        .mini-label {
            font-size: 0.78rem;
            color: #6b7f92;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            margin-bottom: 0.35rem;
        }
        .mini-value {
            font-size: 1.65rem;
            font-weight: 800;
            color: #0c2d57;
            line-height: 1;
            margin-bottom: 0.4rem;
        }
        .mini-copy {
            font-size: 0.9rem;
            color: #5c7083;
        }
        .badge-row {
            display: flex;
            flex-wrap: wrap;
            gap: 0.45rem;
            margin-top: 0.8rem;
        }
        .badge {
            display: inline-flex;
            align-items: center;
            border-radius: 999px;
            padding: 0.38rem 0.72rem;
            font-size: 0.8rem;
            font-weight: 700;
        }
        .badge-info {
            background: #e6f2ff;
            color: #145ea8;
        }
        .badge-alerta {
            background: #fff4d6;
            color: #8a5a00;
        }
        .badge-critico {
            background: #ffe3df;
            color: #a1260d;
        }
        .badge-ok {
            background: #e6f8ec;
            color: #196b2e;
        }
        .badge-neutral {
            background: #eef2f7;
            color: #52606d;
        }
        .rule-card {
            background: #0c2d57;
            color: #ffffff;
            border-radius: 20px;
            padding: 1rem 1.1rem;
            min-height: 132px;
        }
        .rule-card h4 {
            margin: 0 0 0.4rem 0;
            font-size: 1rem;
        }
        .rule-card p {
            margin: 0;
            opacity: 0.92;
            font-size: 0.92rem;
            line-height: 1.35;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 0.4rem;
        }
        .stTabs [data-baseweb="tab"] {
            border-radius: 999px;
            padding: 0.55rem 1rem;
            background: #eef3f9;
            color: #244360;
            font-weight: 700;
        }
        .stTabs [aria-selected="true"] {
            background: #0c2d57;
            color: #ffffff;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def normalizar_texto(texto) -> str:
    if pd.isna(texto):
        return ""
    return (
        str(texto)
        .strip()
        .upper()
        .replace(".", "")
        .replace(",", "")
        .replace("-", "")
        .replace(" ", "")
    )


def coerce_datetime(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    if not pd.api.types.is_datetime64_any_dtype(parsed):
        return pd.Series(pd.NaT, index=series.index)
    try:
        if getattr(parsed.dt, "tz", None) is None:
            return parsed.dt.tz_localize(COLOMBIA_TZ)
        return parsed.dt.tz_convert(COLOMBIA_TZ)
    except (TypeError, AttributeError, ValueError):
        return pd.Series(pd.NaT, index=series.index)


def coerce_numeric(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.replace(" ", "", regex=False)
        .replace({"nan": None, "None": None, "": None})
    )
    return pd.to_numeric(cleaned, errors="coerce").fillna(0)


def format_money(value: float) -> str:
    return f"${value:,.0f}"


def load_target_suppliers() -> tuple[pd.DataFrame, list[str]]:
    archivo_proveedores = "PROVEDORES_CORREO.xlsx"
    if not os.path.exists(archivo_proveedores):
        st.error(f"No se encontro el archivo '{archivo_proveedores}' en la raiz del proyecto.")
        st.stop()

    df_objetivo = pd.read_excel(archivo_proveedores)
    col_proveedor = next(
        (col for col in df_objetivo.columns if "proveedor" in str(col).lower() or "nombre" in str(col).lower()),
        None,
    )
    if not col_proveedor:
        st.error("PROVEDORES_CORREO.xlsx debe incluir una columna identificable como proveedor.")
        st.stop()

    proveedores_norm = [
        normalizar_texto(valor)
        for valor in df_objetivo[col_proveedor].dropna().astype(str).unique().tolist()
    ]
    return df_objetivo, proveedores_norm


def load_master_source() -> tuple[pd.DataFrame, str]:
    master_df = st.session_state.get("master_df", pd.DataFrame()).copy()
    if not master_df.empty:
        return master_df, "Sesion activa"

    gs_client = connect_to_google_sheets()
    gsheet_df = load_data_from_gsheet(gs_client)
    if not gsheet_df.empty:
        return gsheet_df.copy(), "Google Sheets"

    return pd.DataFrame(), "Sin fuente"


def classify_priority(tipo_alerta: str, dias_conciliacion: float) -> str:
    if pd.isna(dias_conciliacion):
        return "Sin fecha"

    if tipo_alerta == SOLO_CORREO:
        if dias_conciliacion >= CRITICO_SOLO_CORREO_DIAS:
            return "Critico"
        if dias_conciliacion >= ALERTA_SOLO_CORREO_DIAS:
            return "Alerta"
        return "Seguimiento"

    if tipo_alerta == SOLO_ERP:
        if dias_conciliacion >= CRITICO_SOLO_ERP_DIAS:
            return "Critico"
        if dias_conciliacion >= ALERTA_SOLO_ERP_DIAS:
            return "Alerta"
        return "Seguimiento"

    return "Revision"


def build_recommendation(tipo_alerta: str, prioridad: str) -> str:
    if tipo_alerta == SOLO_CORREO:
        if prioridad == "Critico":
            return "Escalar hoy: factura recibida pero aun no conciliada en ERP."
        if prioridad == "Alerta":
            return "Enviar alerta al proveedor y validar radicacion interna."
        return "Monitorear hasta completar la ventana operativa de conciliacion."

    if tipo_alerta == SOLO_ERP:
        if prioridad == "Critico":
            return "Solicitar XML y PDF con urgencia y confirmar envio inmediato."
        if prioridad == "Alerta":
            return "Enviar solicitud formal de documento electronico faltante."
        return "Esperar ventana minima y mantener seguimiento."

    return "Revisar diferencia de valores antes de escalar o enviar correo."


def build_case_frame(df_base: pd.DataFrame, tipo_alerta: str, fecha_col: str, valor_col: str) -> pd.DataFrame:
    subset = df_base.copy()
    if subset.empty:
        return pd.DataFrame()

    fecha_base = coerce_datetime(subset[fecha_col]) if fecha_col in subset.columns else pd.Series(pd.NaT, index=subset.index)
    valor_base = coerce_numeric(subset[valor_col]) if valor_col in subset.columns else pd.Series(0, index=subset.index)
    today = pd.Timestamp.now(tz=COLOMBIA_TZ).normalize()
    dias_conciliacion = (today - fecha_base.dt.normalize()).dt.days

    cases = pd.DataFrame(
        {
            "proveedor": subset["nombre_proveedor"].fillna("Proveedor sin nombre").astype(str),
            "proveedor_norm": subset["nombre_proveedor"].apply(normalizar_texto),
            "num_factura": subset.get("num_factura", pd.Series("", index=subset.index)).astype(str).str.strip(),
            "valor": valor_base,
            "fecha_base": fecha_base,
            "dias_conciliacion": dias_conciliacion,
            "tipo_alerta": tipo_alerta,
            "estado_conciliacion": subset.get("estado_conciliacion", pd.Series("", index=subset.index)).astype(str),
        }
    )
    cases["prioridad"] = cases.apply(
        lambda row: classify_priority(row["tipo_alerta"], row["dias_conciliacion"]), axis=1
    )
    cases["accion_sugerida"] = cases.apply(
        lambda row: build_recommendation(row["tipo_alerta"], row["prioridad"]), axis=1
    )
    cases["listo_para_correo"] = (
        ((cases["tipo_alerta"] == SOLO_CORREO) & (cases["dias_conciliacion"] >= ALERTA_SOLO_CORREO_DIAS))
        | ((cases["tipo_alerta"] == SOLO_ERP) & (cases["dias_conciliacion"] >= ALERTA_SOLO_ERP_DIAS))
    )
    cases["fecha_base_txt"] = cases["fecha_base"].dt.strftime("%Y-%m-%d").fillna("Sin fecha")
    cases["dias_conciliacion"] = cases["dias_conciliacion"].fillna(-1)
    cases["caso_id"] = (
        cases["proveedor_norm"]
        + "|"
        + cases["tipo_alerta"]
        + "|"
        + cases["num_factura"]
    )
    return cases


def build_cases_from_master(master_df: pd.DataFrame, proveedores_objetivo_norm: list[str]) -> pd.DataFrame:
    if master_df.empty:
        return pd.DataFrame()

    df = master_df.copy()
    if "nombre_proveedor" not in df.columns:
        if "nombre_proveedor_erp" in df.columns or "nombre_proveedor_correo" in df.columns:
            df["nombre_proveedor"] = df.get("nombre_proveedor_erp", pd.Series(index=df.index)).fillna(
                df.get("nombre_proveedor_correo", pd.Series(index=df.index))
            )
        else:
            return pd.DataFrame()

    if "estado_conciliacion" not in df.columns:
        return pd.DataFrame()

    df["proveedor_norm"] = df["nombre_proveedor"].apply(normalizar_texto)
    df = df[df["proveedor_norm"].isin(proveedores_objetivo_norm)].copy()
    if df.empty:
        return pd.DataFrame()

    solo_correo = build_case_frame(
        df[df["estado_conciliacion"] == "📧 Solo en Correo"], SOLO_CORREO, "fecha_emision_correo", "valor_total_correo"
    )
    solo_erp = build_case_frame(
        df[df["estado_conciliacion"] == "📬 Pendiente de Correo"], SOLO_ERP, "fecha_emision_erp", "valor_total_erp"
    )
    discrepancias = build_case_frame(
        df[df["estado_conciliacion"] == "⚠️ Discrepancia de Valor"], DISCREPANCIA, "fecha_emision_erp", "valor_total_erp"
    )

    return pd.concat([solo_correo, solo_erp, discrepancias], ignore_index=True)


def build_cases_from_raw_session(proveedores_objetivo_norm: list[str]) -> pd.DataFrame:
    email_df = st.session_state.get("email_df", pd.DataFrame()).copy()
    erp_df = st.session_state.get("erp_df", pd.DataFrame()).copy()
    if email_df.empty and erp_df.empty:
        return pd.DataFrame()

    frames = []

    if not email_df.empty and "nombre_proveedor_correo" in email_df.columns and "num_factura" in email_df.columns:
        email_df["nombre_proveedor"] = email_df["nombre_proveedor_correo"]
        email_df["proveedor_norm"] = email_df["nombre_proveedor"].apply(normalizar_texto)
        email_df = email_df[email_df["proveedor_norm"].isin(proveedores_objetivo_norm)].copy()
        email_df["num_factura"] = email_df["num_factura"].astype(str).str.strip()
        erp_facturas = set(erp_df.get("num_factura", pd.Series(dtype=str)).astype(str).str.strip().tolist())
        frames.append(
            build_case_frame(
                email_df[~email_df["num_factura"].isin(erp_facturas)],
                SOLO_CORREO,
                "fecha_emision_correo",
                "valor_total_correo",
            )
        )

    if not erp_df.empty and "num_factura" in erp_df.columns:
        if "nombre_proveedor" not in erp_df.columns:
            erp_df["nombre_proveedor"] = erp_df.get("nombre_proveedor_erp", pd.Series("", index=erp_df.index))
        erp_df["proveedor_norm"] = erp_df["nombre_proveedor"].apply(normalizar_texto)
        erp_df = erp_df[erp_df["proveedor_norm"].isin(proveedores_objetivo_norm)].copy()
        erp_df["num_factura"] = erp_df["num_factura"].astype(str).str.strip()
        email_facturas = set(email_df.get("num_factura", pd.Series(dtype=str)).astype(str).str.strip().tolist())
        frames.append(
            build_case_frame(
                erp_df[~erp_df["num_factura"].isin(email_facturas)],
                SOLO_ERP,
                "fecha_emision_erp",
                "valor_total_erp",
            )
        )

    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def compute_summary(cases_df: pd.DataFrame) -> dict:
    actionable = cases_df[cases_df["listo_para_correo"]].copy()
    criticos = actionable[actionable["prioridad"] == "Critico"]
    alertas = actionable[actionable["prioridad"] == "Alerta"]
    return {
        "total_casos": int(len(cases_df)),
        "total_accionables": int(len(actionable)),
        "total_criticos": int(len(criticos)),
        "proveedores_alerta": int(actionable["proveedor"].nunique()),
        "valor_accionable": float(actionable["valor"].sum()),
        "total_alertas": int(len(alertas)),
    }


def priority_rank(prioridad: str) -> int:
    ranking = {
        "Critico": 0,
        "Alerta": 1,
        "Revision": 2,
        "Seguimiento": 3,
        "Sin fecha": 4,
    }
    return ranking.get(prioridad, 9)


def metric_card(label: str, value: str, copy: str) -> str:
    return f"""
    <div class="mini-card">
        <div class="mini-label">{escape(label)}</div>
        <div class="mini-value">{escape(value)}</div>
        <div class="mini-copy">{escape(copy)}</div>
    </div>
    """


def badge_markup(label: str, css_class: str) -> str:
    return f'<span class="badge {css_class}">{escape(label)}</span>'


def split_emails(raw_value: str) -> list[str]:
    if not raw_value:
        return []
    candidates = re.split(r"[;,\s]+", raw_value.strip())
    valid = [mail for mail in candidates if mail and re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", mail)]
    return valid


def generate_email_content(proveedor: str, casos_df: pd.DataFrame, tono: str) -> tuple[str, str, str]:
    casos_df = casos_df.sort_values(["prioridad", "dias_conciliacion"], ascending=[True, False]).copy()
    total_valor = format_money(casos_df["valor"].sum())
    max_dias = int(casos_df["dias_conciliacion"].max()) if not casos_df.empty else 0

    intro_map = {
        "Operativo": "Durante la revision diaria de conciliacion detectamos documentos que requieren atencion para cerrar el cruce ERP vs correo.",
        "Formal": "En el proceso de conciliacion documental de Ferreinox identificamos novedades pendientes de regularizacion.",
        "Urgente": "Detectamos novedades criticas en la conciliacion documental y requerimos su apoyo prioritario para regularizarlas hoy.",
    }
    cierre_map = {
        "Operativo": "Agradecemos su confirmacion por este mismo medio una vez los documentos hayan sido enviados o aclarados.",
        "Formal": "Agradecemos su pronta gestion y la confirmacion correspondiente para mantener la conciliacion al dia.",
        "Urgente": "Solicitamos respuesta prioritaria hoy para evitar retrasos operativos en el proceso de pago y conciliacion.",
    }

    resumen_tipo = ", ".join(sorted(casos_df["tipo_alerta"].unique().tolist()))
    subject = f"Ferreinox | Alerta de conciliacion {proveedor} | {len(casos_df)} documentos"

    lineas = []
    for _, row in casos_df.iterrows():
        lineas.append(
            f"- Factura {row['num_factura']} | {row['tipo_alerta']} | {row['fecha_base_txt']} | {int(row['dias_conciliacion']) if row['dias_conciliacion'] >= 0 else 'Sin fecha'} dias | {format_money(row['valor'])}"
        )

    plain_text = (
        f"Estimado proveedor {proveedor},\n\n"
        f"{intro_map[tono]}\n\n"
        f"Resumen: {len(casos_df)} documentos, valor estimado {total_valor}, antiguedad maxima {max_dias} dias.\n"
        f"Tipo de novedad: {resumen_tipo}.\n\n"
        + "\n".join(lineas)
        + "\n\n"
        + cierre_map[tono]
        + "\n\nTesoreria Ferreinox S.A.S. BIC"
    )

    filas_html = "".join(
        [
            "<tr>"
            f"<td style='padding:8px;border-bottom:1px solid #e6edf5'>{escape(str(row['num_factura']))}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #e6edf5'>{escape(row['tipo_alerta'])}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #e6edf5'>{escape(row['fecha_base_txt'])}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #e6edf5;text-align:center'>{'Sin fecha' if row['dias_conciliacion'] < 0 else int(row['dias_conciliacion'])}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #e6edf5;text-align:right'>{escape(format_money(row['valor']))}</td>"
            "</tr>"
            for _, row in casos_df.iterrows()
        ]
    )

    html_body = f"""
    <div style="font-family:Segoe UI, Arial, sans-serif;color:#17324d;max-width:760px">
        <div style="background:linear-gradient(135deg,#0c2d57 0%,#1a5b92 100%);padding:20px 24px;border-radius:18px;color:#ffffff">
            <div style="font-size:12px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;opacity:.85">Portal Tesoreria Ferreinox</div>
            <h2 style="margin:8px 0 6px 0;font-size:28px;line-height:1.05">Alerta de conciliacion documental</h2>
            <p style="margin:0;font-size:14px;opacity:.9">{escape(proveedor)} | {len(casos_df)} documentos | {escape(total_valor)}</p>
        </div>
        <p style="margin:22px 0 10px 0">Estimado proveedor {escape(proveedor)},</p>
        <p style="margin:0 0 16px 0">{escape(intro_map[tono])}</p>
        <div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:18px">
            <div style="background:#f3f8fd;border:1px solid #d8e6f5;border-radius:16px;padding:12px 14px;min-width:150px">
                <div style="font-size:12px;color:#5c7388;text-transform:uppercase;font-weight:700">Documentos</div>
                <div style="font-size:24px;font-weight:800;color:#0c2d57">{len(casos_df)}</div>
            </div>
            <div style="background:#f3f8fd;border:1px solid #d8e6f5;border-radius:16px;padding:12px 14px;min-width:150px">
                <div style="font-size:12px;color:#5c7388;text-transform:uppercase;font-weight:700">Valor</div>
                <div style="font-size:24px;font-weight:800;color:#0c2d57">{escape(total_valor)}</div>
            </div>
            <div style="background:#f3f8fd;border:1px solid #d8e6f5;border-radius:16px;padding:12px 14px;min-width:150px">
                <div style="font-size:12px;color:#5c7388;text-transform:uppercase;font-weight:700">Antiguedad maxima</div>
                <div style="font-size:24px;font-weight:800;color:#0c2d57">{max_dias} dias</div>
            </div>
        </div>
        <table style="width:100%;border-collapse:collapse;background:#ffffff;border:1px solid #e6edf5;border-radius:14px;overflow:hidden">
            <thead>
                <tr style="background:#f7fbff;color:#34506a;text-align:left">
                    <th style='padding:10px'>Factura</th>
                    <th style='padding:10px'>Novedad</th>
                    <th style='padding:10px'>Fecha base</th>
                    <th style='padding:10px;text-align:center'>Dias</th>
                    <th style='padding:10px;text-align:right'>Valor</th>
                </tr>
            </thead>
            <tbody>{filas_html}</tbody>
        </table>
        <p style="margin:18px 0 0 0">{escape(cierre_map[tono])}</p>
        <p style="margin:16px 0 0 0;font-weight:700">Tesoreria Ferreinox S.A.S. BIC</p>
    </div>
    """
    return subject, plain_text, html_body


def send_email_alert(subject: str, plain_text: str, html_body: str, to_list: list[str], cc_list: list[str]) -> None:
    sender = st.secrets.email["address"]
    password = st.secrets.email["password"]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(to_list)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)

    msg.attach(MIMEText(plain_text, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    recipients = to_list + cc_list
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender, password)
        server.sendmail(sender, recipients, msg.as_string())


require_authentication()
inject_styles()

if "tesoreria_contactos" not in st.session_state:
    st.session_state["tesoreria_contactos"] = {}
if "tesoreria_envios_log" not in st.session_state:
    st.session_state["tesoreria_envios_log"] = []

_, proveedores_objetivo_norm = load_target_suppliers()
master_df, source_label = load_master_source()
cases_df = build_cases_from_master(master_df, proveedores_objetivo_norm)

if cases_df.empty:
    cases_df = build_cases_from_raw_session(proveedores_objetivo_norm)
    if not cases_df.empty:
        source_label = "Sesion email/ERP"

st.image("https://www.ferreinox.co/wp-content/uploads/2022/09/logo-ferreinox.png", width=210)

if cases_df.empty:
    st.markdown(
        """
        <div class="portal-shell">
            <div class="portal-kicker">Portal Tesoreria</div>
            <h1 class="portal-title">No hay alertas activas para mostrar</h1>
            <p class="portal-subtitle">
                El portal necesita datos conciliados desde Dashboard General o desde el reporte consolidado para construir
                el radar operativo de alertas y envios.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.warning("Realiza la sincronizacion en Dashboard General para habilitar el centro operativo de alertas.")
    st.stop()

today = pd.Timestamp.now(tz=COLOMBIA_TZ).normalize()
fecha_inicio_default = (today - pd.Timedelta(days=VENTANA_OPERATIVA_DIAS)).date()
fecha_fin_default = today.date()

cases_df["priority_order"] = cases_df["prioridad"].apply(priority_rank)
cases_df = cases_df.sort_values(["priority_order", "dias_conciliacion", "valor"], ascending=[True, False, False]).reset_index(drop=True)

st.markdown(
    f"""
    <div class="portal-shell">
        <div class="portal-kicker">Portal Tesoreria Ferreinox</div>
        <h1 class="portal-title">Alertas claras de conciliacion</h1>
        <p class="portal-subtitle">
            Solo muestra la ventana operativa util para decidir hoy que conciliar y que correo enviar.
        </p>
        <div class="badge-row">
            {badge_markup(f'Fuente: {source_label}', 'badge-info')}
            {badge_markup(f'Ventana sugerida: ultimos {VENTANA_OPERATIVA_DIAS} dias', 'badge-ok')}
            {badge_markup('Mas claro, menos ruido', 'badge-neutral')}
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown("<div class='section-card'>", unsafe_allow_html=True)
st.markdown("<div class='section-title'>Filtros</div>", unsafe_allow_html=True)
st.markdown(
    "<div class='section-copy'>La pagina arranca por defecto en los ultimos 40 dias para evitar basura historica del ERP.</div>",
    unsafe_allow_html=True,
)

filter_cols = st.columns([1.3, 1.1, 1.2, 1.4, 1.4])
tipo_opts = ["Todos"] + sorted(cases_df["tipo_alerta"].dropna().unique().tolist())
prio_opts = sorted(cases_df["prioridad"].dropna().unique().tolist())
prov_opts = ["Todos"] + sorted(cases_df["proveedor"].dropna().unique().tolist())

tipo_sel = filter_cols[0].selectbox("Tipo de alerta", tipo_opts)
prio_sel = filter_cols[1].multiselect("Prioridad", prio_opts, default=prio_opts)
solo_listos = filter_cols[2].checkbox("Solo correo listo", value=False)
proveedor_sel = filter_cols[3].selectbox("Proveedor", prov_opts)
fecha_sel = filter_cols[4].date_input(
    "Ventana fecha base",
    value=(fecha_inicio_default, fecha_fin_default),
    min_value=(today - pd.Timedelta(days=365 * 3)).date(),
    max_value=fecha_fin_default,
)

filtered_df = cases_df.copy()
if isinstance(fecha_sel, tuple) and len(fecha_sel) == 2:
    fecha_inicio_sel, fecha_fin_sel = fecha_sel
else:
    fecha_inicio_sel = fecha_fin_sel = fecha_sel

mask_fecha = filtered_df["fecha_base"].dt.date.between(fecha_inicio_sel, fecha_fin_sel, inclusive="both")
filtered_df = filtered_df[mask_fecha.fillna(False)].copy()
if tipo_sel != "Todos":
    filtered_df = filtered_df[filtered_df["tipo_alerta"] == tipo_sel]
if prio_sel:
    filtered_df = filtered_df[filtered_df["prioridad"].isin(prio_sel)]
if solo_listos:
    filtered_df = filtered_df[filtered_df["listo_para_correo"]]
if proveedor_sel != "Todos":
    filtered_df = filtered_df[filtered_df["proveedor"] == proveedor_sel]

filtered_df = filtered_df.sort_values(["priority_order", "dias_conciliacion", "valor"], ascending=[True, False, False]).reset_index(drop=True)
summary = compute_summary(filtered_df)

st.markdown("</div>", unsafe_allow_html=True)

metric_cols = st.columns(4)
metric_payload = [
    ("Casos", str(summary["total_casos"]), "Lo que si importa en la ventana elegida."),
    ("Para correo", str(summary["total_accionables"]), "Ya deberian gestionarse."),
    ("Criticos", str(summary["total_criticos"]), "Requieren atencion inmediata."),
    ("Valor", format_money(summary["valor_accionable"]), "Monto hoy en seguimiento."),
]
for col, payload in zip(metric_cols, metric_payload):
    with col:
        st.markdown(metric_card(*payload), unsafe_allow_html=True)

if filtered_df.empty:
    st.info("No hay alertas en la ventana seleccionada. Si necesitas revisar historia, amplia el filtro de fechas.")

tab_radar, tab_envio, tab_bitacora = st.tabs(["Radar", "Enviar", "Bitacora"])

with tab_radar:
    radar_cols = st.columns([1.1, 0.9])
    with radar_cols[0]:
        resumen_tipo = (
            filtered_df.groupby(["tipo_alerta", "prioridad"], dropna=False)
            .size()
            .unstack(fill_value=0)
            .sort_index()
        )
        st.markdown("### Donde esta el problema")
        if resumen_tipo.empty:
            st.info("Los filtros actuales no devuelven casos.")
        else:
            st.bar_chart(resumen_tipo)

    with radar_cols[1]:
        top_proveedores = (
            filtered_df.groupby("proveedor", dropna=False)
            .agg(documentos=("caso_id", "count"), valor=("valor", "sum"))
            .sort_values(["documentos", "valor"], ascending=[False, False])
            .head(8)
            .reset_index()
        )
        st.markdown("### Proveedores a tocar")
        st.dataframe(
            top_proveedores,
            use_container_width=True,
            hide_index=True,
            column_config={
                "proveedor": "Proveedor",
                "documentos": "Casos",
                "valor": st.column_config.NumberColumn("Valor", format="$ %d"),
            },
        )

    st.markdown("### Bandeja clara")
    radar_view = filtered_df[
        [
            "prioridad",
            "tipo_alerta",
            "proveedor",
            "num_factura",
            "fecha_base_txt",
            "dias_conciliacion",
            "valor",
            "accion_sugerida",
            "listo_para_correo",
        ]
    ].copy()
    st.dataframe(
        radar_view,
        use_container_width=True,
        hide_index=True,
        column_config={
            "prioridad": "Prioridad",
            "tipo_alerta": "Novedad",
            "proveedor": "Proveedor",
            "num_factura": "Factura",
            "fecha_base_txt": "Fecha base",
            "dias_conciliacion": st.column_config.NumberColumn("Dias", format="%d"),
            "valor": st.column_config.NumberColumn("Valor", format="$ %d"),
            "accion_sugerida": "Que hacer",
            "listo_para_correo": st.column_config.CheckboxColumn("Listo correo"),
        },
    )

    csv_data = filtered_df.drop(columns=["fecha_base"], errors="ignore").to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        "Descargar corte operativo CSV",
        data=csv_data,
        file_name="portal_tesoreria_alertas.csv",
        mime="text/csv",
        use_container_width=True,
    )

with tab_envio:
    actionable_df = filtered_df[filtered_df["listo_para_correo"]].copy()
    st.markdown("### Correo por proveedor")
    st.caption("Captura el destinatario, elige las facturas y envia.")

    if actionable_df.empty:
        st.info("No hay casos listos para correo con los filtros actuales.")
    else:
        proveedores_envio = sorted(actionable_df["proveedor"].unique().tolist())
        proveedor_envio = st.selectbox("Proveedor a gestionar", proveedores_envio)
        provider_cases = actionable_df[actionable_df["proveedor"] == proveedor_envio].copy()

        tipos_proveedor = ["Todas las alertas"] + sorted(provider_cases["tipo_alerta"].unique().tolist())
        tipo_envio_sel = st.radio("Cobertura del correo", tipos_proveedor, horizontal=True)
        if tipo_envio_sel != "Todas las alertas":
            provider_cases = provider_cases[provider_cases["tipo_alerta"] == tipo_envio_sel].copy()

        provider_cases = provider_cases.sort_values(["prioridad", "dias_conciliacion"], ascending=[True, False]).copy()
        provider_cases["seleccionar"] = True

        editor_df = st.data_editor(
            provider_cases[
                [
                    "seleccionar",
                    "prioridad",
                    "tipo_alerta",
                    "num_factura",
                    "fecha_base_txt",
                    "dias_conciliacion",
                    "valor",
                    "accion_sugerida",
                ]
            ],
            use_container_width=True,
            hide_index=True,
            key=f"envio_editor_{normalizar_texto(proveedor_envio)}_{tipo_envio_sel}",
            column_config={
                "seleccionar": st.column_config.CheckboxColumn("Enviar"),
                "prioridad": "Prioridad",
                "tipo_alerta": "Tipo",
                "num_factura": "Factura",
                "fecha_base_txt": "Fecha base",
                "dias_conciliacion": st.column_config.NumberColumn("Dias", format="%d"),
                "valor": st.column_config.NumberColumn("Valor", format="$ %d"),
                "accion_sugerida": "Accion",
            },
            disabled=["prioridad", "tipo_alerta", "num_factura", "fecha_base_txt", "dias_conciliacion", "valor", "accion_sugerida"],
        )

        selected_invoices = editor_df.loc[editor_df["seleccionar"], "num_factura"].astype(str).tolist()
        selected_cases = provider_cases[provider_cases["num_factura"].astype(str).isin(selected_invoices)].copy()

        contacto_key = normalizar_texto(proveedor_envio)
        contacto_guardado = st.session_state["tesoreria_contactos"].get(contacto_key, "")

        form_cols = st.columns([1.4, 1.0, 1.0])
        destinatarios = form_cols[0].text_input(
            "Destinatarios",
            value=contacto_guardado,
            placeholder="proveedor@empresa.com; cuentas@empresa.com",
        )
        cc = form_cols[1].text_input("CC", placeholder="tesoreria@ferreinox.co")
        tono = form_cols[2].selectbox("Tono del mensaje", ["Operativo", "Formal", "Urgente"])

        guardar_contacto = st.button("Guardar destinatario en sesion", use_container_width=True)
        if guardar_contacto:
            st.session_state["tesoreria_contactos"][contacto_key] = destinatarios.strip()
            st.success("Contacto guardado para esta sesion.")

        if selected_cases.empty:
            st.warning("Selecciona al menos una factura para construir el correo.")
        else:
            subject, plain_text, html_body = generate_email_content(proveedor_envio, selected_cases, tono)
            preview_cols = st.columns([1.0, 1.0])
            with preview_cols[0]:
                st.markdown("#### Resumen del envio")
                st.write(f"Documentos: {len(selected_cases)}")
                st.write(f"Valor total: {format_money(selected_cases['valor'].sum())}")
                st.write(f"Dias maximos: {int(selected_cases['dias_conciliacion'].max())}")
                st.write(f"Asunto: {subject}")
            with preview_cols[1]:
                st.markdown("#### Vista previa")
                st.text_area("Borrador", plain_text, height=260, label_visibility="collapsed")

            to_list = split_emails(destinatarios)
            cc_list = split_emails(cc)
            send_disabled = not to_list
            if send_disabled:
                st.info("Ingresa al menos un correo valido para habilitar el envio.")

            if st.button("Enviar correo de alerta", type="primary", disabled=send_disabled, use_container_width=True):
                try:
                    send_email_alert(subject, plain_text, html_body, to_list, cc_list)
                    st.session_state["tesoreria_envios_log"].append(
                        {
                            "fecha": pd.Timestamp.now(tz=COLOMBIA_TZ).strftime("%Y-%m-%d %H:%M"),
                            "proveedor": proveedor_envio,
                            "destinatarios": "; ".join(to_list),
                            "copias": "; ".join(cc_list),
                            "documentos": len(selected_cases),
                            "tipo_alerta": ", ".join(sorted(selected_cases["tipo_alerta"].unique().tolist())),
                            "valor": float(selected_cases["valor"].sum()),
                        }
                    )
                    st.success("Correo enviado correctamente desde el Portal Tesoreria.")
                except Exception as exc:
                    st.error(f"Error al enviar el correo: {exc}")

with tab_bitacora:
    st.markdown("### Bitacora de envios de esta sesion")
    if not st.session_state["tesoreria_envios_log"]:
        st.info("Todavia no se han enviado correos desde esta sesion.")
    else:
        log_df = pd.DataFrame(st.session_state["tesoreria_envios_log"])
        st.dataframe(
            log_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "fecha": "Fecha",
                "proveedor": "Proveedor",
                "destinatarios": "Para",
                "copias": "CC",
                "documentos": "Docs",
                "tipo_alerta": "Tipo",
                "valor": st.column_config.NumberColumn("Valor", format="$ %d"),
            },
        )