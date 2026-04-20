# -*- coding: utf-8 -*-
"""Seguimiento profesional del rebate de Pintuco."""

import email
import imaplib
import io
import re
import xml.etree.ElementTree as ET
import zipfile
from datetime import date, datetime, timedelta
from email.header import decode_header
from email.utils import parsedate_to_datetime
from typing import Any

import dropbox
import gspread
import pandas as pd
import pytz
import streamlit as st
from google.oauth2.service_account import Credentials
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils.dataframe import dataframe_to_rows


if "password_correct" not in st.session_state:
	st.session_state["password_correct"] = False

if not st.session_state["password_correct"]:
	st.error("🔒 Debes iniciar sesión para acceder a esta página.")
	st.info("Por favor, ve a la página principal 'Dashboard General' para ingresar la contraseña.")
	st.stop()


st.set_page_config(layout="wide", page_title="Seguimiento Rebate | Pintuco", page_icon="📈")

PINTUCO_ALIASES = ["PINTUCO", "COMPANIA GLOBAL DE PINTURAS"]
PINTUCO_PROVIDER_NAME_ERP = "PINTUCO COLOMBIA S.A.S"
COLOMBIA_TZ = pytz.timezone("America/Bogota")
CURRENT_CYCLE_NAME = "Ciclo vigente desde el 1 de abril de 2026"
CURRENT_CYCLE_START = date(2026, 4, 1)
EXCLUDED_PURCHASE_PERCENT = 0.12
APPLICABLE_PURCHASE_FACTOR = 1 - EXCLUDED_PURCHASE_PERCENT

IMAP_SERVER = "imap.gmail.com"
EMAIL_FOLDER = "TFHKA/Recepcion/Descargados"
DROPBOX_FILE_PATH = "/data/Proveedores.csv"
PINTUCO_WORKSHEET_NAME = "Rebate_Pintuco"

MONTHLY_BUDGETS = [
	{"Mes": "Abril", "Mes_Num": 4, "Trimestre": "Q2", "Escala 1": 1456867389.0, "Escala 2": 1544279432.0},
	{"Mes": "Mayo", "Mes_Num": 5, "Trimestre": "Q2", "Escala 1": 2094162232.0, "Escala 2": 2219811966.0},
	{"Mes": "Junio", "Mes_Num": 6, "Trimestre": "Q2", "Escala 1": 2237825409.0, "Escala 2": 2372094934.0},
	{"Mes": "Julio", "Mes_Num": 7, "Trimestre": "Q3", "Escala 1": 2000572886.0, "Escala 2": 2120607260.0},
	{"Mes": "Agosto", "Mes_Num": 8, "Trimestre": "Q3", "Escala 1": 2271787723.0, "Escala 2": 2408094987.0},
	{"Mes": "Septiembre", "Mes_Num": 9, "Trimestre": "Q3", "Escala 1": 2232381407.0, "Escala 2": 2366324292.0},
	{"Mes": "Octubre", "Mes_Num": 10, "Trimestre": "Q4", "Escala 1": 1605138135.0, "Escala 2": 1701446423.0},
	{"Mes": "Noviembre", "Mes_Num": 11, "Trimestre": "Q4", "Escala 1": 1147826895.0, "Escala 2": 1216696508.0},
	{"Mes": "Diciembre", "Mes_Num": 12, "Trimestre": "Q4", "Escala 1": 1555236709.0, "Escala 2": 1648550912.0},
]

MONTHLY_REBATE_RATES = {"Escala 1": 0.01, "Escala 2": 0.015, "Sin escala": 0.0}
QUARTERLY_REBATE_RATES = {"Escala 1": 0.01, "Escala 2": 0.025, "Sin escala": 0.0}
SEASONALITY_TARGET_FACTOR = 0.90
SEASONALITY_RATE = 0.01
CYCLE_RECOMPOSITION_FACTOR = 0.85

INVOICE_COLUMNS = [
	"Fecha_Factura",
	"Numero_Factura",
	"Valor_Neto",
	"Proveedor_Correo",
	"Fecha_Recepcion_Correo",
	"Remitente_Correo",
	"Asunto_Correo",
	"Nombre_Adjunto",
	"Message_ID",
	"Estado_Pago",
]

st.markdown(
	f"""
	<style>
		[data-testid="stSidebar"] {{
			background: linear-gradient(180deg, #0d1c30 0%, #133251 55%, #1c4e80 100%);
			border-right: 1px solid rgba(255,255,255,0.08);
		}}
		[data-testid="stSidebar"] * {{
			color: #f3f7fb;
		}}
		.main .block-container {{
			padding-top: 1rem;
			padding-bottom: 2.4rem;
		}}
		.pintuco-banner {{
			background: radial-gradient(circle at top right, rgba(255,255,255,0.18), transparent 30%), linear-gradient(135deg, #0b2440 0%, #145374 48%, #f0ad1f 100%);
			border-radius: 24px;
			padding: 28px 32px;
			color: #ffffff;
			margin-bottom: 18px;
			box-shadow: 0 18px 42px rgba(11, 36, 64, 0.22);
		}}
		.pintuco-banner h1 {{
			margin: 0;
			font-size: 2.2rem;
		}}
		.pintuco-banner p {{
			margin: 10px 0 0 0;
			font-size: 1rem;
			opacity: 0.95;
		}}
		.info-card {{
			background: linear-gradient(180deg, #f9fbfd 0%, #eef3f8 100%);
			border: 1px solid rgba(12, 45, 87, 0.10);
			border-radius: 18px;
			padding: 16px 18px;
			margin-bottom: 16px;
			box-shadow: 0 10px 24px rgba(12, 45, 87, 0.06);
		}}
		.note-card {{
			background: #fff8e7;
			border: 1px solid rgba(240, 173, 31, 0.28);
			border-radius: 18px;
			padding: 14px 16px;
			margin-bottom: 16px;
		}}
		.kpi-grid {{
			display: grid;
			grid-template-columns: repeat(4, minmax(0, 1fr));
			gap: 12px;
			margin: 12px 0 14px 0;
		}}
		.kpi-card {{
			background: #ffffff;
			border-radius: 20px;
			padding: 16px 18px;
			border: 1px solid rgba(12, 45, 87, 0.08);
			box-shadow: 0 12px 26px rgba(12, 45, 87, 0.06);
		}}
		.kpi-card.navy {{ border-top: 4px solid #0c2d57; }}
		.kpi-card.gold {{ border-top: 4px solid #f0ad1f; }}
		.kpi-card.green {{ border-top: 4px solid #119c63; }}
		.kpi-card.red {{ border-top: 4px solid #d94a4a; }}
		.kpi-label {{
			font-size: 0.72rem;
			text-transform: uppercase;
			letter-spacing: 0.08em;
			color: #5d6c7d;
			margin-bottom: 0.2rem;
		}}
		.kpi-value {{
			font-size: 1.55rem;
			font-weight: 800;
			color: #0c2d57;
			line-height: 1.05;
		}}
		.kpi-sub {{
			font-size: 0.80rem;
			color: #6a7b8f;
			margin-top: 0.28rem;
			line-height: 1.4;
		}}
		.section-card {{
			background: #ffffff;
			border-radius: 22px;
			border: 1px solid rgba(12, 45, 87, 0.08);
			padding: 18px 20px;
			box-shadow: 0 12px 28px rgba(12, 45, 87, 0.06);
			margin-bottom: 16px;
		}}
		.pill {{
			display: inline-block;
			padding: 4px 10px;
			border-radius: 999px;
			font-size: 0.78rem;
			font-weight: 700;
		}}
		.pill.green {{ background: rgba(17, 156, 99, 0.14); color: #0a774a; }}
		.pill.gold {{ background: rgba(240, 173, 31, 0.16); color: #8b6509; }}
		.pill.red {{ background: rgba(217, 74, 74, 0.14); color: #a92f2f; }}
		.pill.navy {{ background: rgba(12, 45, 87, 0.10); color: #0c2d57; }}
		@media (max-width: 1100px) {{
			.kpi-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
		}}
		@media (max-width: 680px) {{
			.kpi-grid {{ grid-template-columns: 1fr; }}
		}}
	</style>
	""",
	unsafe_allow_html=True,
)


def format_currency(value: float) -> str:
	return f"${value:,.0f}"


def format_percent(value: float, decimals: int = 1) -> str:
	return f"{value * 100:.{decimals}f}%"


def safe_divide(numerator: float, denominator: float) -> float:
	if not denominator:
		return 0.0
	return float(numerator) / float(denominator)


def kpi_card_html(label: str, value: str, subtext: str = "", tone: str = "navy") -> str:
	sub_html = f'<div class="kpi-sub">{subtext}</div>' if subtext else ""
	return f'<div class="kpi-card {tone}"><div class="kpi-label">{label}</div><div class="kpi-value">{value}</div>{sub_html}</div>'


def render_kpi_grid(cards: list[str]) -> None:
	st.markdown(f'<div class="kpi-grid">{"".join(cards)}</div>', unsafe_allow_html=True)


def pill_html(text: str, tone: str = "navy") -> str:
	return f'<span class="pill {tone}">{text}</span>'


def normalize_invoice_number(inv_num: Any) -> str:
	if not isinstance(inv_num, str):
		inv_num = str(inv_num)
	return re.sub(r"[^A-Z0-9]", "", inv_num.upper()).strip()


def clean_and_convert_numeric(value: Any) -> float:
	if pd.isna(value) or value is None:
		return 0.0
	cleaned_str = str(value).strip().replace("$", "").replace(",", "")
	try:
		return float(cleaned_str)
	except (ValueError, TypeError):
		return 0.0


def decode_mime_text(value: Any) -> str:
	if not value:
		return ""
	decoded_parts = []
	for part, encoding in decode_header(value):
		if isinstance(part, bytes):
			decoded_parts.append(part.decode(encoding or "utf-8", errors="ignore"))
		else:
			decoded_parts.append(part)
	return "".join(decoded_parts).strip()


def parse_email_datetime(value: str) -> pd.Timestamp:
	if not value:
		return pd.NaT
	try:
		dt = parsedate_to_datetime(value)
		if dt.tzinfo is None:
			dt = COLOMBIA_TZ.localize(dt)
		else:
			dt = dt.astimezone(COLOMBIA_TZ)
		return pd.Timestamp(dt)
	except Exception:
		return pd.NaT


def normalize_local_datetime(value: Any) -> pd.Timestamp:
	if value is None or value == "" or pd.isna(value):
		return pd.NaT

	try:
		timestamp = pd.Timestamp(value)
	except Exception:
		return pd.NaT

	if pd.isna(timestamp):
		return pd.NaT

	try:
		if timestamp.tzinfo is None:
			timestamp = timestamp.tz_localize(COLOMBIA_TZ)
		else:
			timestamp = timestamp.tz_convert(COLOMBIA_TZ)
	except (TypeError, ValueError, AttributeError):
		return pd.NaT

	return timestamp.tz_localize(None)


def normalize_datetime_series(series: pd.Series) -> pd.Series:
	normalized = series.apply(normalize_local_datetime)
	return pd.to_datetime(normalized, errors="coerce")


def sort_invoice_dataframe(df: pd.DataFrame, by: list[str], ascending: bool | list[bool] = True) -> pd.DataFrame:
	if df.empty:
		return df

	sorted_df = df.copy()
	for column in ["Fecha_Factura", "Fecha_Recepcion_Correo"]:
		if column in sorted_df.columns:
			sorted_df[column] = normalize_datetime_series(sorted_df[column])

	return sorted_df.sort_values(by=by, ascending=ascending, na_position="last")


def get_third_sunday(month_start: pd.Timestamp) -> date:
	first_day = month_start.date().replace(day=1)
	days_until_sunday = (6 - first_day.weekday()) % 7
	first_sunday = first_day + timedelta(days=days_until_sunday)
	return first_sunday + timedelta(days=14)


def build_budget_frame() -> pd.DataFrame:
	budget_df = pd.DataFrame(MONTHLY_BUDGETS).copy()
	budget_df["Mes_Inicio"] = pd.to_datetime({"year": CURRENT_CYCLE_START.year, "month": budget_df["Mes_Num"], "day": 1})
	budget_df["Mes_Clave"] = budget_df["Mes_Inicio"].dt.strftime("%Y-%m")
	budget_df["Corte_Estacionalidad"] = budget_df["Mes_Inicio"].apply(lambda value: pd.Timestamp(get_third_sunday(value)))
	return budget_df


def get_rebate_configuration() -> dict:
	budget_df = build_budget_frame()
	with st.sidebar:
		st.header("Motor comercial")
		st.caption("Ferreinox vs Pintuco. Todo el rebate y la estacionalidad se calculan sobre el 88% aplicable después del 12% excluido.")
		monthly_rate_e1 = st.number_input("Rebate mensual Escala 1 (%)", min_value=0.0, max_value=100.0, value=MONTHLY_REBATE_RATES["Escala 1"] * 100, step=0.1, format="%.2f") / 100
		monthly_rate_e2 = st.number_input("Rebate mensual Escala 2 (%)", min_value=0.0, max_value=100.0, value=MONTHLY_REBATE_RATES["Escala 2"] * 100, step=0.1, format="%.2f") / 100
		quarterly_rate_e1 = st.number_input("Rebate trimestral Escala 1 (%)", min_value=0.0, max_value=100.0, value=QUARTERLY_REBATE_RATES["Escala 1"] * 100, step=0.1, format="%.2f") / 100
		quarterly_rate_e2 = st.number_input("Rebate trimestral Escala 2 (%)", min_value=0.0, max_value=100.0, value=QUARTERLY_REBATE_RATES["Escala 2"] * 100, step=0.1, format="%.2f") / 100
		seasonality_target = st.number_input("Meta estacionalidad (% de Escala 2)", min_value=0.0, max_value=100.0, value=SEASONALITY_TARGET_FACTOR * 100, step=1.0, format="%.0f") / 100
		seasonality_rate = st.number_input("Bono estacionalidad (%)", min_value=0.0, max_value=100.0, value=SEASONALITY_RATE * 100, step=0.1, format="%.2f") / 100
		recomposition_rate = st.number_input("Recuperación 9 meses (%)", min_value=0.0, max_value=100.0, value=CYCLE_RECOMPOSITION_FACTOR * 100, step=1.0, format="%.0f") / 100

		st.markdown("---")
		edit_budget = st.checkbox("Editar presupuesto mensual en esta sesión", value=False)
		if edit_budget:
			edited_budget = st.data_editor(
				budget_df[["Mes", "Trimestre", "Escala 1", "Escala 2"]],
				hide_index=True,
				use_container_width=True,
				disabled=["Mes", "Trimestre"],
			)
			budget_df[["Escala 1", "Escala 2"]] = edited_budget[["Escala 1", "Escala 2"]].apply(pd.to_numeric, errors="coerce").fillna(0.0)

	return {
		"budget_df": budget_df,
		"monthly_rates": {"Escala 1": monthly_rate_e1, "Escala 2": monthly_rate_e2, "Sin escala": 0.0},
		"quarterly_rates": {"Escala 1": quarterly_rate_e1, "Escala 2": quarterly_rate_e2, "Sin escala": 0.0},
		"seasonality_target_factor": seasonality_target,
		"seasonality_rate": seasonality_rate,
		"cycle_recomposition_factor": recomposition_rate,
	}


@st.cache_resource(show_spinner="Conectando a Google Sheets...")
def connect_to_google_sheets():
	try:
		scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
		creds = Credentials.from_service_account_info(st.secrets["google_credentials"], scopes=scopes)
		return gspread.authorize(creds)
	except Exception as exc:
		st.error(f"❌ Error crítico al autenticar con Google: {exc}")
		return None


def get_worksheet(client: gspread.Client, sheet_key: str, worksheet_name: str):
	try:
		spreadsheet = client.open_by_key(sheet_key)
	except gspread.exceptions.APIError:
		st.error("❌ No se pudo abrir el libro de Google Sheets. Verifica el ID y el acceso del correo de servicio.")
		st.info(f"Correo de servicio: {st.secrets.google_credentials['client_email']}")
		st.stop()
		return None
	try:
		return spreadsheet.worksheet(worksheet_name)
	except gspread.WorksheetNotFound:
		st.warning(f"La pestaña '{worksheet_name}' no existe. Se creará automáticamente.")
		return spreadsheet.add_worksheet(title=worksheet_name, rows="2000", cols="50")


def update_gsheet_from_df(worksheet: gspread.Worksheet, df: pd.DataFrame) -> bool:
	try:
		df_to_upload = df.copy()
		for column in ["Fecha_Factura", "Fecha_Recepcion_Correo"]:
			if column in df_to_upload.columns:
				df_to_upload[column] = pd.to_datetime(df_to_upload[column], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

		df_to_upload = df_to_upload.astype(str).replace({"nan": "", "NaT": "", "None": ""})
		worksheet.clear()
		worksheet.update([df_to_upload.columns.tolist()] + df_to_upload.values.tolist(), "A1")
		return True
	except Exception as exc:
		st.error(f"❌ Error al actualizar la hoja '{worksheet.title}': {exc}")
		return False


@st.cache_data(ttl=600, show_spinner="Descargando cartera vigente de Dropbox...")
def load_pending_documents_from_dropbox() -> set:
	try:
		dbx = dropbox.Dropbox(
			oauth2_refresh_token=st.secrets.dropbox["refresh_token"],
			app_key=st.secrets.dropbox["app_key"],
			app_secret=st.secrets.dropbox["app_secret"],
		)
		_, response = dbx.files_download(DROPBOX_FILE_PATH)
		df = pd.read_csv(
			io.StringIO(response.content.decode("latin1")),
			sep="{",
			header=None,
			engine="python",
			names=[
				"nombre_proveedor_erp",
				"serie",
				"num_entrada",
				"num_factura",
				"doc_erp",
				"fecha_emision_erp",
				"fecha_vencimiento_erp",
				"valor_total_erp",
			],
		)
		pintuco_df = df[df["nombre_proveedor_erp"] == PINTUCO_PROVIDER_NAME_ERP].copy()
		pintuco_df["valor_total_erp"] = pintuco_df["valor_total_erp"].apply(clean_and_convert_numeric)

		credit_note_mask = (pintuco_df["valor_total_erp"] < 0) & (
			pintuco_df["num_factura"].isna() | (pintuco_df["num_factura"].astype(str).str.strip() == "")
		)
		if credit_note_mask.any():
			pintuco_df.loc[credit_note_mask, "num_factura"] = (
				"NC-"
				+ pintuco_df.loc[credit_note_mask, "doc_erp"].astype(str).str.strip()
				+ "-"
				+ pintuco_df.loc[credit_note_mask, "valor_total_erp"].abs().astype(int).astype(str)
			)

		pintuco_df.dropna(subset=["num_factura"], inplace=True)
		return set(pintuco_df["num_factura"].apply(normalize_invoice_number))
	except Exception as exc:
		st.error(f"❌ Error cargando cartera de Dropbox: {exc}")
		return set()


def parse_invoice_xml(xml_content: str) -> dict | None:
	try:
		namespaces = {
			"cac": "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2",
			"cbc": "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2",
		}

		xml_content = re.sub(r"^[^<]+", "", xml_content.strip())
		root = ET.fromstring(xml_content.encode("utf-8"))

		description_node = root.find(".//cac:Attachment/cac:ExternalReference/cbc:Description", namespaces)
		if description_node is not None and description_node.text and "<Invoice" in description_node.text:
			nested_xml = re.sub(r"^[^<]+", "", description_node.text.strip())
			invoice_root = ET.fromstring(nested_xml.encode("utf-8"))
		else:
			invoice_root = root

		supplier_name_node = invoice_root.find(
			".//cac:AccountingSupplierParty/cac:Party/cac:PartyLegalEntity/cbc:RegistrationName",
			namespaces,
		)
		if supplier_name_node is None or not supplier_name_node.text:
			return None

		supplier_name = supplier_name_node.text.strip()
		if not any(alias in supplier_name.upper() for alias in PINTUCO_ALIASES):
			return None

		invoice_number_node = invoice_root.find("./cbc:ID", namespaces)
		issue_date_node = invoice_root.find("./cbc:IssueDate", namespaces)
		net_value_node = invoice_root.find(".//cac:LegalMonetaryTotal/cbc:TaxExclusiveAmount", namespaces)

		if invoice_number_node is None or issue_date_node is None or net_value_node is None:
			return None

		return {
			"Fecha_Factura": issue_date_node.text.strip(),
			"Numero_Factura": normalize_invoice_number(invoice_number_node.text.strip()),
			"Valor_Neto": clean_and_convert_numeric(net_value_node.text.strip()),
			"Proveedor_Correo": supplier_name,
		}
	except Exception:
		return None


def extract_invoice_records_from_message(message_obj) -> tuple[list[dict], dict]:
	records = []
	stats = {"attachments_scanned": 0, "xml_files_scanned": 0, "invoice_rows_detected": 0}

	email_subject = decode_mime_text(message_obj.get("Subject", ""))
	email_sender = decode_mime_text(message_obj.get("From", ""))
	email_message_id = decode_mime_text(message_obj.get("Message-ID", ""))
	email_received_at = parse_email_datetime(message_obj.get("Date", ""))

	for part in message_obj.walk():
		if part.get_content_maintype() == "multipart":
			continue

		filename = decode_mime_text(part.get_filename() or "")
		content_type = (part.get_content_type() or "").lower()
		payload = part.get_payload(decode=True)
		if payload is None:
			continue

		is_zip = filename.lower().endswith(".zip") or content_type in {"application/zip", "application/x-zip-compressed"}
		is_xml = filename.lower().endswith(".xml") or content_type in {"application/xml", "text/xml"}
		if not is_zip and not is_xml:
			continue

		stats["attachments_scanned"] += 1

		if is_zip:
			try:
				with zipfile.ZipFile(io.BytesIO(payload)) as zip_file:
					for internal_name in zip_file.namelist():
						if not internal_name.lower().endswith(".xml"):
							continue
						stats["xml_files_scanned"] += 1
						xml_content = zip_file.read(internal_name).decode("utf-8", "ignore")
						details = parse_invoice_xml(xml_content)
						if details:
							details.update(
								{
									"Fecha_Recepcion_Correo": email_received_at,
									"Remitente_Correo": email_sender,
									"Asunto_Correo": email_subject,
									"Nombre_Adjunto": internal_name,
									"Message_ID": email_message_id,
								}
							)
							records.append(details)
							stats["invoice_rows_detected"] += 1
			except Exception:
				continue

		if is_xml:
			stats["xml_files_scanned"] += 1
			xml_content = payload.decode("utf-8", "ignore")
			details = parse_invoice_xml(xml_content)
			if details:
				details.update(
					{
						"Fecha_Recepcion_Correo": email_received_at,
						"Remitente_Correo": email_sender,
						"Asunto_Correo": email_subject,
						"Nombre_Adjunto": filename or "adjunto_xml",
						"Message_ID": email_message_id,
					}
				)
				records.append(details)
				stats["invoice_rows_detected"] += 1

	return records, stats


def fetch_pintuco_invoices_from_email(start_date: date) -> tuple[pd.DataFrame, dict]:
	invoices_data = []
	stats = {
		"emails_found": 0,
		"emails_processed": 0,
		"attachments_scanned": 0,
		"xml_files_scanned": 0,
		"invoice_rows_detected": 0,
		"started_from": start_date.strftime("%Y-%m-%d"),
	}

	try:
		mail = imaplib.IMAP4_SSL(IMAP_SERVER)
		mail.login(st.secrets.email["address"], st.secrets.email["password"])
		mail.select(f'"{EMAIL_FOLDER}"')

		search_query = f'(SINCE "{start_date.strftime("%d-%b-%Y")}")'
		_, messages = mail.search(None, search_query)
		message_ids = messages[0].split()
		stats["emails_found"] = len(message_ids)

		if not message_ids:
			mail.logout()
			return pd.DataFrame(), stats

		progress_text = f"Procesando {len(message_ids)} correos del buzón de Pintuco..."
		progress_bar = st.progress(0, text=progress_text)

		for index, message_id in enumerate(message_ids, start=1):
			_, data = mail.fetch(message_id, "(RFC822)")
			message_obj = email.message_from_bytes(data[0][1])
			records, email_stats = extract_invoice_records_from_message(message_obj)
			invoices_data.extend(records)

			stats["emails_processed"] += 1
			stats["attachments_scanned"] += email_stats["attachments_scanned"]
			stats["xml_files_scanned"] += email_stats["xml_files_scanned"]
			stats["invoice_rows_detected"] += email_stats["invoice_rows_detected"]

			progress_bar.progress(index / len(message_ids), text=f"{progress_text} ({index}/{len(message_ids)})")

		mail.logout()
		return pd.DataFrame(invoices_data), stats
	except Exception as exc:
		st.error(f"❌ Error procesando correos de Pintuco: {exc}")
		return pd.DataFrame(), stats


def ensure_invoice_columns(df: pd.DataFrame) -> pd.DataFrame:
	normalized_df = df.copy()
	for column in INVOICE_COLUMNS:
		if column not in normalized_df.columns:
			normalized_df[column] = ""
	return normalized_df.reindex(columns=INVOICE_COLUMNS)


def prepare_invoice_dataframe(df: pd.DataFrame) -> pd.DataFrame:
	if df.empty:
		return ensure_invoice_columns(df)

	prepared = ensure_invoice_columns(df)
	prepared["Fecha_Factura"] = normalize_datetime_series(prepared["Fecha_Factura"])
	prepared["Fecha_Recepcion_Correo"] = normalize_datetime_series(prepared["Fecha_Recepcion_Correo"])
	prepared["Valor_Neto"] = pd.to_numeric(prepared["Valor_Neto"], errors="coerce").fillna(0.0)
	prepared["Numero_Factura"] = prepared["Numero_Factura"].apply(normalize_invoice_number)
	prepared["Compra_Excluida_12"] = prepared["Valor_Neto"] * EXCLUDED_PURCHASE_PERCENT
	prepared["Compra_Aplicable_Rebate"] = prepared["Valor_Neto"] * APPLICABLE_PURCHASE_FACTOR
	return prepared


def build_tracking_alerts(df: pd.DataFrame) -> list[str]:
	if df.empty:
		return ["No hay facturas cargadas para evaluar alertas del ciclo."]

	alerts = []
	missing_invoice_date = int(df["Fecha_Factura"].isna().sum())
	missing_email_date = int(df["Fecha_Recepcion_Correo"].isna().sum())
	missing_message_id = int(df["Message_ID"].fillna("").astype(str).str.strip().eq("").sum())
	duplicates = int(df["Numero_Factura"].fillna("").duplicated(keep=False).sum())
	pending_count = int((df["Estado_Pago"] == "Pendiente").sum())

	if missing_invoice_date:
		alerts.append(f"Hay {missing_invoice_date} facturas sin fecha de emisión válida.")
	if missing_email_date:
		alerts.append(f"Hay {missing_email_date} registros sin fecha de recepción del correo.")
	if missing_message_id:
		alerts.append(f"Hay {missing_message_id} registros sin Message-ID; la auditoría del correo queda incompleta.")
	if duplicates:
		alerts.append(f"Se detectaron {duplicates} filas con número de factura repetido en la vista actual.")
	if pending_count:
		alerts.append(f"Siguen pendientes de pago {pending_count} facturas del rango filtrado.")
	if not alerts:
		alerts.append("La trazabilidad está completa para el rango filtrado y no se detectan alertas operativas.")

	return alerts


def run_pintuco_sync():
	with st.spinner("Sincronizando facturas y trazabilidad de Pintuco..."):
		pending_docs_set = load_pending_documents_from_dropbox()

		gs_client = connect_to_google_sheets()
		if not gs_client:
			st.error("Sincronización cancelada. No fue posible conectar con Google Sheets.")
			st.stop()

		worksheet = get_worksheet(gs_client, st.secrets["google_sheet_id"], PINTUCO_WORKSHEET_NAME)

		historical_df = pd.DataFrame(columns=INVOICE_COLUMNS)
		start_date = CURRENT_CYCLE_START

		try:
			records = worksheet.get_all_records()
			if records:
				historical_df = prepare_invoice_dataframe(pd.DataFrame(records))
				historical_df = historical_df[historical_df["Fecha_Factura"].dt.date >= CURRENT_CYCLE_START].copy()
				if not historical_df.empty:
					last_sync_date = historical_df["Fecha_Factura"].max().date()
					start_date = max(CURRENT_CYCLE_START, last_sync_date - timedelta(days=3))
		except Exception as exc:
			st.warning(f"No se pudo leer el histórico de Google Sheets. Se sincronizará desde el inicio del ciclo. Detalle: {exc}")

		st.info(f"Buscando facturas desde {start_date.strftime('%Y-%m-%d')} en la carpeta de correo {EMAIL_FOLDER}.")
		new_invoices_df, sync_stats = fetch_pintuco_invoices_from_email(start_date)

		combined_df = prepare_invoice_dataframe(historical_df.copy())
		if not new_invoices_df.empty:
			new_invoices_df = prepare_invoice_dataframe(new_invoices_df)
			combined_df = prepare_invoice_dataframe(pd.concat([historical_df, new_invoices_df], ignore_index=True))
			combined_df = sort_invoice_dataframe(combined_df, by=["Fecha_Factura", "Fecha_Recepcion_Correo"])
			combined_df.drop_duplicates(subset=["Numero_Factura"], keep="last", inplace=True)
			st.success(f"Se consolidaron {len(new_invoices_df)} registros de factura detectados desde el correo.")
		else:
			st.info("No se detectaron facturas nuevas de Pintuco en el correo para el rango consultado.")

		if not combined_df.empty:
			combined_df["Estado_Pago"] = combined_df["Numero_Factura"].apply(
				lambda number: "Pendiente" if normalize_invoice_number(number) in pending_docs_set else "Pagada"
			)
			combined_df = ensure_invoice_columns(combined_df)

			if update_gsheet_from_df(worksheet, sort_invoice_dataframe(combined_df, by=["Fecha_Factura", "Numero_Factura"])):
				st.success("✅ Base de datos de Pintuco actualizada correctamente.")
			else:
				st.error("❌ La actualización de Google Sheets falló.")
		else:
			st.warning("No hay facturas de Pintuco para guardar en la hoja.")

		st.session_state["last_pintuco_sync"] = datetime.now(COLOMBIA_TZ).strftime("%Y-%m-%d %H:%M:%S")
		st.session_state["last_pintuco_sync_stats"] = sync_stats


@st.cache_data(ttl=300)
def load_pintuco_data_from_gsheet() -> pd.DataFrame:
	try:
		gs_client = connect_to_google_sheets()
		worksheet = get_worksheet(gs_client, st.secrets["google_sheet_id"], PINTUCO_WORKSHEET_NAME)
		records = worksheet.get_all_records()
		if not records:
			return pd.DataFrame()

		df = prepare_invoice_dataframe(pd.DataFrame(records))
		df = df[df["Fecha_Factura"].dt.date >= CURRENT_CYCLE_START].copy()
		return sort_invoice_dataframe(df, by=["Fecha_Factura", "Numero_Factura"])
	except Exception as exc:
		st.error(f"❌ Error al cargar datos desde Google Sheets: {exc}")
		return pd.DataFrame()


def build_cycle_summary(df: pd.DataFrame) -> pd.DataFrame:
	if df.empty:
		return pd.DataFrame()

	return df[
		[
			"Mes",
			"Trimestre",
			"Compra_Neta",
			"Exclusion_12",
			"Compra_Aplicable",
			"Presupuesto_Escala_1",
			"Presupuesto_Escala_2",
			"Cumplimiento_E1",
			"Cumplimiento_E2",
			"Escala_Lograda",
			"Rebate_Mensual_Ganado",
			"Bono_Estacionalidad",
			"Pendiente_Cartera",
		]
	].copy()


def determine_scale(actual_purchase: float, scale_1_target: float, scale_2_target: float) -> str:
	if scale_2_target > 0 and actual_purchase >= scale_2_target:
		return "Escala 2"
	if scale_1_target > 0 and actual_purchase >= scale_1_target:
		return "Escala 1"
	return "Sin escala"


def get_scale_tone(scale_name: str) -> str:
	if scale_name == "Escala 2":
		return "green"
	if scale_name == "Escala 1":
		return "gold"
	if any(token in scale_name for token in ["No", "Riesgo", "Bloqueada"]):
		return "red"
	return "navy"


def build_monthly_rebate_table(df: pd.DataFrame, budget_df: pd.DataFrame, config: dict, snapshot_date: date) -> pd.DataFrame:
	working_df = df.copy()
	if not working_df.empty:
		working_df["Mes_Inicio"] = working_df["Fecha_Factura"].dt.to_period("M").dt.to_timestamp()

	rows = []
	for _, budget_row in budget_df.iterrows():
		month_start = budget_row["Mes_Inicio"]
		month_end = (month_start + pd.offsets.MonthEnd(0)).date()
		cutoff = pd.Timestamp(budget_row["Corte_Estacionalidad"])
		month_df = working_df[working_df["Mes_Inicio"] == month_start].copy() if not working_df.empty else pd.DataFrame(columns=df.columns)

		purchase_net = float(month_df["Valor_Neto"].sum()) if not month_df.empty else 0.0
		excluded_12 = float(month_df["Compra_Excluida_12"].sum()) if not month_df.empty else 0.0
		purchase_applicable = float(month_df["Compra_Aplicable_Rebate"].sum()) if not month_df.empty else 0.0
		pending_value = float(month_df.loc[month_df["Estado_Pago"] == "Pendiente", "Valor_Neto"].sum()) if not month_df.empty else 0.0
		cutoff_purchase = float(month_df.loc[month_df["Fecha_Factura"] <= cutoff, "Compra_Aplicable_Rebate"].sum()) if not month_df.empty else 0.0
		invoice_count = int(month_df["Numero_Factura"].nunique()) if not month_df.empty else 0
		pending_count = int((month_df["Estado_Pago"] == "Pendiente").sum()) if not month_df.empty else 0
		paid_count = int((month_df["Estado_Pago"] == "Pagada").sum()) if not month_df.empty else 0

		scale_1_target = float(budget_row["Escala 1"])
		scale_2_target = float(budget_row["Escala 2"])
		monthly_scale = determine_scale(purchase_applicable, scale_1_target, scale_2_target)
		monthly_rate = config["monthly_rates"].get(monthly_scale, 0.0)
		monthly_rebate = purchase_applicable * monthly_rate

		seasonality_target = scale_2_target * config["seasonality_target_factor"]
		seasonality_progress = safe_divide(cutoff_purchase, seasonality_target)
		seasonality_met = seasonality_target > 0 and cutoff_purchase >= seasonality_target
		current_month = snapshot_date.year == month_start.year and snapshot_date.month == month_start.month
		if snapshot_date < month_start.date():
			month_status = "Futuro"
			seasonality_status = "Futuro"
		elif monthly_scale != "Sin escala":
			month_status = "Cumplida"
			seasonality_status = "Cumplida" if seasonality_met else ("En ventana" if current_month and snapshot_date <= cutoff.date() else "No cumplida")
		elif snapshot_date <= month_end:
			month_status = "Abierta"
			seasonality_status = "En ventana" if snapshot_date <= cutoff.date() else "No cumplida"
		else:
			month_status = "No cumplida"
			seasonality_status = "No cumplida"

		rows.append(
			{
				"Mes": budget_row["Mes"],
				"Mes_Clave": budget_row["Mes_Clave"],
				"Mes_Inicio": month_start,
				"Trimestre": budget_row["Trimestre"],
				"Facturas": invoice_count,
				"Pagadas": paid_count,
				"Pendientes": pending_count,
				"Compra_Neta": purchase_net,
				"Exclusion_12": excluded_12,
				"Compra_Aplicable": purchase_applicable,
				"Presupuesto_Escala_1": scale_1_target,
				"Presupuesto_Escala_2": scale_2_target,
				"Cumplimiento_E1": safe_divide(purchase_applicable, scale_1_target),
				"Cumplimiento_E2": safe_divide(purchase_applicable, scale_2_target),
				"Faltante_E1": max(scale_1_target - purchase_applicable, 0.0),
				"Faltante_E2": max(scale_2_target - purchase_applicable, 0.0),
				"Escala_Lograda": monthly_scale,
				"Estado_Mes": month_status,
				"Rebate_Mensual_Pct": monthly_rate,
				"Rebate_Mensual_Ganado": monthly_rebate,
				"Corte_Estacionalidad": cutoff,
				"Compra_Hasta_Corte": cutoff_purchase,
				"Meta_Estacionalidad": seasonality_target,
				"Avance_Estacionalidad": seasonality_progress,
				"Estado_Estacionalidad": seasonality_status,
				"Bono_Estacionalidad": purchase_applicable * config["seasonality_rate"] if seasonality_met else 0.0,
				"Pendiente_Cartera": pending_value,
				"Cartera_Riesgo": pending_value > 0,
				"Mes_Cerrado": snapshot_date > month_end,
			}
		)

	return pd.DataFrame(rows)


def build_quarterly_rebate_table(monthly_df: pd.DataFrame, config: dict) -> pd.DataFrame:
	rows = []
	for quarter_name, quarter_df in monthly_df.groupby("Trimestre", sort=False):
		purchase_applicable = float(quarter_df["Compra_Aplicable"].sum())
		target_e1 = float(quarter_df["Presupuesto_Escala_1"].sum())
		target_e2 = float(quarter_df["Presupuesto_Escala_2"].sum())
		quarter_scale = determine_scale(purchase_applicable, target_e1, target_e2)
		quarter_rate = config["quarterly_rates"].get(quarter_scale, 0.0)
		quarter_rebate = purchase_applicable * quarter_rate

		recomposition_eligible = 0.0
		recomposition_blocked = 0.0
		quarter_month_rate = config["monthly_rates"].get(quarter_scale, 0.0)
		if quarter_scale != "Sin escala":
			for _, month_row in quarter_df.iterrows():
				rate_gap = max(quarter_month_rate - float(month_row["Rebate_Mensual_Pct"]), 0.0)
				recoverable_value = float(month_row["Compra_Aplicable"]) * rate_gap
				if month_row["Cartera_Riesgo"]:
					recomposition_blocked += recoverable_value
				else:
					recomposition_eligible += recoverable_value

		rows.append(
			{
				"Trimestre": quarter_name,
				"Compra_Aplicable": purchase_applicable,
				"Presupuesto_Escala_1": target_e1,
				"Presupuesto_Escala_2": target_e2,
				"Cumplimiento_E1": safe_divide(purchase_applicable, target_e1),
				"Cumplimiento_E2": safe_divide(purchase_applicable, target_e2),
				"Faltante_E1": max(target_e1 - purchase_applicable, 0.0),
				"Faltante_E2": max(target_e2 - purchase_applicable, 0.0),
				"Escala_Lograda": quarter_scale,
				"Rebate_Trimestral_Pct": quarter_rate,
				"Rebate_Trimestral_Ganado": quarter_rebate,
				"Recomposicion_Trimestral_Proyectada": recomposition_eligible,
				"Recomposicion_Cartera_Bloqueada": recomposition_blocked,
				"Meses_Cubiertos": ", ".join(quarter_df["Mes"].tolist()),
			}
		)

	return pd.DataFrame(rows)


def build_cycle_projection(monthly_df: pd.DataFrame, budget_df: pd.DataFrame, config: dict, snapshot_date: date) -> dict:
	current_month_start = pd.Timestamp(snapshot_date.replace(day=1))
	elapsed_monthly = monthly_df[monthly_df["Mes_Inicio"] <= current_month_start].copy()
	elapsed_budget = budget_df[budget_df["Mes_Inicio"] <= current_month_start].copy()

	applicable_purchase = float(elapsed_monthly["Compra_Aplicable"].sum()) if not elapsed_monthly.empty else 0.0
	target_elapsed_e1 = float(elapsed_budget["Escala 1"].sum()) if not elapsed_budget.empty else 0.0
	target_elapsed_e2 = float(elapsed_budget["Escala 2"].sum()) if not elapsed_budget.empty else 0.0
	target_total_e1 = float(budget_df["Escala 1"].sum())
	target_total_e2 = float(budget_df["Escala 2"].sum())
	cycle_scale = determine_scale(applicable_purchase, target_elapsed_e1, target_elapsed_e2)
	cycle_month_rate = config["monthly_rates"].get(cycle_scale, 0.0)

	recoverable_pool = 0.0
	blocked_pool = 0.0
	if cycle_scale != "Sin escala":
		for _, month_row in elapsed_monthly.iterrows():
			rate_gap = max(cycle_month_rate - float(month_row["Rebate_Mensual_Pct"]), 0.0)
			recoverable_value = float(month_row["Compra_Aplicable"]) * rate_gap
			if month_row["Cartera_Riesgo"]:
				blocked_pool += recoverable_value
			else:
				recoverable_pool += recoverable_value

	return {
		"Compra_Aplicable_Acumulada": applicable_purchase,
		"Meta_Acumulada_E1": target_elapsed_e1,
		"Meta_Acumulada_E2": target_elapsed_e2,
		"Meta_Ciclo_E1": target_total_e1,
		"Meta_Ciclo_E2": target_total_e2,
		"Cumplimiento_Acumulado_E1": safe_divide(applicable_purchase, target_elapsed_e1),
		"Cumplimiento_Acumulado_E2": safe_divide(applicable_purchase, target_elapsed_e2),
		"Escala_Ciclo": cycle_scale,
		"Faltante_E1_Actual": max(target_elapsed_e1 - applicable_purchase, 0.0),
		"Faltante_E2_Actual": max(target_elapsed_e2 - applicable_purchase, 0.0),
		"Faltante_E1_Ciclo": max(target_total_e1 - applicable_purchase, 0.0),
		"Faltante_E2_Ciclo": max(target_total_e2 - applicable_purchase, 0.0),
		"Recomposicion_9M_Proyectada": recoverable_pool * config["cycle_recomposition_factor"],
		"Recomposicion_9M_Bloqueada": blocked_pool,
	}


def generate_excel_report(executive_df: pd.DataFrame, monthly_df: pd.DataFrame, quarterly_df: pd.DataFrame, invoices_df: pd.DataFrame) -> io.BytesIO:
	output = io.BytesIO()
	workbook = Workbook()

	sheets_to_write = [
		("Resumen_Ejecutivo", executive_df if not executive_df.empty else pd.DataFrame([{"Mensaje": "Sin información disponible"}])),
		("Mes_a_Mes", monthly_df if not monthly_df.empty else pd.DataFrame([{"Mensaje": "Sin información disponible"}])),
		("Trimestres", quarterly_df if not quarterly_df.empty else pd.DataFrame([{"Mensaje": "Sin información disponible"}])),
		("Facturas_Pintuco", invoices_df if not invoices_df.empty else pd.DataFrame([{"Mensaje": "Sin información disponible"}])),
	]

	header_font = Font(bold=True, color="FFFFFF")
	header_fill = PatternFill(start_color="0B3C5D", end_color="0B3C5D", fill_type="solid")
	center_alignment = Alignment(horizontal="center", vertical="center")
	currency_format = '_($* #,##0_);_($* (#,##0);_($* "-"??_);_(@_)'
	percent_format = "0.0%"

	for index, (sheet_name, dataframe) in enumerate(sheets_to_write):
		worksheet = workbook.active if index == 0 else workbook.create_sheet(title=sheet_name)
		worksheet.title = sheet_name
		worksheet.freeze_panes = "A2"

		prepared_df = dataframe.copy()
		for column in prepared_df.columns:
			if pd.api.types.is_datetime64_any_dtype(prepared_df[column]):
				prepared_df[column] = prepared_df[column].dt.strftime("%Y-%m-%d %H:%M:%S")

		rows = dataframe_to_rows(prepared_df, index=False, header=True)
		for row_index, row in enumerate(rows, start=1):
			for column_index, value in enumerate(row, start=1):
				cell = worksheet.cell(row=row_index, column=column_index, value=value)
				if row_index == 1:
					cell.font = header_font
					cell.fill = header_fill
					cell.alignment = center_alignment
				elif isinstance(value, (int, float)):
					column_name = prepared_df.columns[column_index - 1]
					if any(token in column_name for token in ["Compra", "Meta", "Faltante", "Rebate", "Exclusion", "Presupuesto", "Bono", "Pendiente", "Recomposicion"]):
						cell.number_format = currency_format
					if any(token in column_name for token in ["Cumplimiento", "Avance", "Pct"]):
						cell.number_format = percent_format

		for column_cells in worksheet.columns:
			max_length = max(len(str(cell.value)) if cell.value is not None else 0 for cell in column_cells)
			worksheet.column_dimensions[column_cells[0].column_letter].width = max_length + 2

	workbook.save(output)
	output.seek(0)
	return output


st.markdown(
	f"""
	<div class="pintuco-banner">
		<h1>Dashboard Ejecutivo de Rebate Pintuco</h1>
		<p>{CURRENT_CYCLE_NAME}. Seguimiento comercial, financiero y operativo del presupuesto Ferreinox con lectura directa de facturas, estacionalidad, escalas, trimestre y recomposición.</p>
	</div>
	""",
	unsafe_allow_html=True,
)

st.markdown(
	f"""
	<div class="info-card">
		<strong>Reglas comerciales activas</strong><br>
		1. La base del cálculo es la compra neta menos el <strong>12% excluido</strong>; el rebate corre sobre el <strong>88% aplicable</strong>.<br>
		2. Cada mes compara contra <strong>Escala 1</strong> y <strong>Escala 2</strong>; Escala 2 paga más que Escala 1.<br>
		3. El pago mensual y trimestral se calcula sobre <strong>toda la compra aplicable del periodo</strong>, sin techo.<br>
		4. Estacionalidad: si al cierre del <strong>tercer domingo</strong> del mes se alcanza al menos el <strong>90% de Escala 2</strong>, se gana un <strong>1% adicional</strong> sobre la compra aplicable del mes.<br>
		5. La recomposición separa el saldo <strong>elegible</strong> del saldo <strong>bloqueado por cartera</strong> para no mezclar comercial con cobranza.
	</div>
	""",
	unsafe_allow_html=True,
)

config = get_rebate_configuration()
budget_df = config["budget_df"]
snapshot_date = datetime.now(COLOMBIA_TZ).date()

sync_col, sync_info_col = st.columns([1, 2])
with sync_col:
	if st.button("🔄 Sincronizar facturas de Pintuco", type="primary", use_container_width=True):
		run_pintuco_sync()
		st.cache_data.clear()
		st.rerun()
with sync_info_col:
	if "last_pintuco_sync" in st.session_state:
		st.success(f"Última foto guardada: {st.session_state['last_pintuco_sync']}")
	else:
		st.info("Aún no hay una foto guardada en esta sesión. Usa 'Sincronizar facturas de Pintuco' para crear la primera foto del ciclo.")

if "last_pintuco_sync_stats" in st.session_state:
	sync_stats = st.session_state["last_pintuco_sync_stats"]
	st.caption(
		" | ".join(
			[
				f"Correos encontrados: {sync_stats.get('emails_found', 0)}",
				f"Correos procesados: {sync_stats.get('emails_processed', 0)}",
				f"Adjuntos revisados: {sync_stats.get('attachments_scanned', 0)}",
				f"XML leídos: {sync_stats.get('xml_files_scanned', 0)}",
				f"Facturas detectadas: {sync_stats.get('invoice_rows_detected', 0)}",
			]
		)
	)

pintuco_df = load_pintuco_data_from_gsheet()
if pintuco_df.empty:
	st.warning("No hay datos de Pintuco para el ciclo vigente. Ejecuta la sincronización inicial.")
	st.stop()

monthly_df = build_monthly_rebate_table(pintuco_df, budget_df, config, snapshot_date)
quarterly_df = build_quarterly_rebate_table(monthly_df, config)
cycle_outlook = build_cycle_projection(monthly_df, budget_df, config, snapshot_date)
summary_df = build_cycle_summary(monthly_df)
tracking_alerts = build_tracking_alerts(pintuco_df)

current_month_start = pd.Timestamp(snapshot_date.replace(day=1))
current_month_df = monthly_df[monthly_df["Mes_Inicio"] == current_month_start]
if current_month_df.empty:
	current_month_df = monthly_df.iloc[[0]]
current_month = current_month_df.iloc[0]
current_quarter = str(current_month["Trimestre"])
current_quarter_df = quarterly_df[quarterly_df["Trimestre"] == current_quarter]
if current_quarter_df.empty:
	current_quarter_df = quarterly_df.iloc[[0]]
current_quarter_row = current_quarter_df.iloc[0]

rebate_total_ganado = float(monthly_df["Rebate_Mensual_Ganado"].sum() + monthly_df["Bono_Estacionalidad"].sum() + quarterly_df["Rebate_Trimestral_Ganado"].sum())

render_kpi_grid(
	[
		kpi_card_html("Compra aplicable acumulada", format_currency(cycle_outlook["Compra_Aplicable_Acumulada"]), f"Cumplimiento E2 acumulado: {format_percent(cycle_outlook['Cumplimiento_Acumulado_E2'])}", "navy"),
		kpi_card_html("Escala actual del ciclo", cycle_outlook["Escala_Ciclo"], f"Faltante acumulado a E2: {format_currency(cycle_outlook['Faltante_E2_Actual'])}", get_scale_tone(cycle_outlook["Escala_Ciclo"])),
		kpi_card_html("Rebate ganado a hoy", format_currency(rebate_total_ganado), "Mensual + trimestral + estacionalidad", "green"),
		kpi_card_html("Bolsa 9M proyectada", format_currency(cycle_outlook["Recomposicion_9M_Proyectada"]), f"Bloqueada por cartera: {format_currency(cycle_outlook['Recomposicion_9M_Bloqueada'])}", "gold"),
	]
)

render_kpi_grid(
	[
		kpi_card_html("Mes actual", format_currency(float(current_month["Compra_Aplicable"])), f"{current_month['Mes']} · escala {current_month['Escala_Lograda']}", get_scale_tone(str(current_month["Escala_Lograda"]))),
		kpi_card_html("Estacionalidad", str(current_month["Estado_Estacionalidad"]), f"Corte: {pd.Timestamp(current_month['Corte_Estacionalidad']).strftime('%Y-%m-%d')} · bono: {format_currency(float(current_month['Bono_Estacionalidad']))}", get_scale_tone(str(current_month["Estado_Estacionalidad"]))),
		kpi_card_html("Trimestre actual", str(current_quarter_row["Escala_Lograda"]), f"{current_quarter} · faltante E2: {format_currency(float(current_quarter_row['Faltante_E2']))}", get_scale_tone(str(current_quarter_row["Escala_Lograda"]))),
		kpi_card_html("Recomposición trimestral", format_currency(float(current_quarter_row["Recomposicion_Trimestral_Proyectada"])), f"Bloqueada cartera: {format_currency(float(current_quarter_row['Recomposicion_Cartera_Bloqueada']))}", "gold"),
	]
)

st.markdown(
	"""
	<div class="note-card">
		<strong>Lectura ejecutiva</strong><br>
		El tablero separa lo ya ganado del saldo recuperable. Cuando un mes o trimestre todavía tiene facturas pendientes, ese valor queda marcado como <strong>bloqueado por cartera</strong> y no se suma a la recomposición automática hasta que el riesgo operativo desaparezca.
	</div>
	""",
	unsafe_allow_html=True,
)

overview_tab, monthly_tab, quarter_tab, invoices_tab, diagnostics_tab = st.tabs(
	["📊 Dirección", "🗓️ Mes a mes", "📦 Trimestre y recomposición", "📑 Facturas y fuente", "🛰️ Diagnóstico"]
)

with overview_tab:
	st.subheader("Ritmo del ciclo")
	chart_df = monthly_df.set_index("Mes")[["Compra_Aplicable", "Presupuesto_Escala_1", "Presupuesto_Escala_2"]]
	st.line_chart(chart_df, use_container_width=True)

	overview_col1, overview_col2 = st.columns([1.2, 1])
	with overview_col1:
		st.markdown(
			f"""
			<div class="section-card">
				<strong>Mes actual: {current_month['Mes']}</strong><br><br>
				Base aplicable: <strong>{format_currency(float(current_month['Compra_Aplicable']))}</strong><br>
				Presupuesto Escala 1: <strong>{format_currency(float(current_month['Presupuesto_Escala_1']))}</strong><br>
				Presupuesto Escala 2: <strong>{format_currency(float(current_month['Presupuesto_Escala_2']))}</strong><br>
				Faltante a E1: <strong>{format_currency(float(current_month['Faltante_E1']))}</strong><br>
				Faltante a E2: <strong>{format_currency(float(current_month['Faltante_E2']))}</strong><br>
				Escala lograda: {pill_html(str(current_month['Escala_Lograda']), get_scale_tone(str(current_month['Escala_Lograda'])))}
			</div>
			""",
			unsafe_allow_html=True,
		)
	with overview_col2:
		st.markdown(
			f"""
			<div class="section-card">
				<strong>{current_quarter}</strong><br><br>
				Compra aplicable: <strong>{format_currency(float(current_quarter_row['Compra_Aplicable']))}</strong><br>
				Rebate trimestral ganado: <strong>{format_currency(float(current_quarter_row['Rebate_Trimestral_Ganado']))}</strong><br>
				Recomposición trimestral elegible: <strong>{format_currency(float(current_quarter_row['Recomposicion_Trimestral_Proyectada']))}</strong><br>
				Recomposición bloqueada por cartera: <strong>{format_currency(float(current_quarter_row['Recomposicion_Cartera_Bloqueada']))}</strong><br>
				Escala lograda: {pill_html(str(current_quarter_row['Escala_Lograda']), get_scale_tone(str(current_quarter_row['Escala_Lograda'])))}
			</div>
			""",
			unsafe_allow_html=True,
		)

	st.subheader("Resumen ejecutivo del ciclo")
	st.dataframe(
		summary_df,
		use_container_width=True,
		hide_index=True,
		column_config={
			"Compra_Neta": st.column_config.NumberColumn("Compra neta", format="$ %,.0f"),
			"Exclusion_12": st.column_config.NumberColumn("12% excluido", format="$ %,.0f"),
			"Compra_Aplicable": st.column_config.NumberColumn("88% aplicable", format="$ %,.0f"),
			"Presupuesto_Escala_1": st.column_config.NumberColumn("Escala 1", format="$ %,.0f"),
			"Presupuesto_Escala_2": st.column_config.NumberColumn("Escala 2", format="$ %,.0f"),
			"Cumplimiento_E1": st.column_config.ProgressColumn("Cumpl. E1", min_value=0.0, max_value=1.2, format="%.0f%%"),
			"Cumplimiento_E2": st.column_config.ProgressColumn("Cumpl. E2", min_value=0.0, max_value=1.2, format="%.0f%%"),
			"Rebate_Mensual_Ganado": st.column_config.NumberColumn("Rebate mensual", format="$ %,.0f"),
			"Bono_Estacionalidad": st.column_config.NumberColumn("Bono estacionalidad", format="$ %,.0f"),
			"Pendiente_Cartera": st.column_config.NumberColumn("Pendiente cartera", format="$ %,.0f"),
		},
	)

with monthly_tab:
	st.subheader("Seguimiento mensual")
	monthly_display_df = monthly_df[
		[
			"Mes",
			"Trimestre",
			"Compra_Aplicable",
			"Presupuesto_Escala_1",
			"Presupuesto_Escala_2",
			"Cumplimiento_E1",
			"Cumplimiento_E2",
			"Faltante_E1",
			"Faltante_E2",
			"Escala_Lograda",
			"Estado_Mes",
			"Compra_Hasta_Corte",
			"Meta_Estacionalidad",
			"Avance_Estacionalidad",
			"Estado_Estacionalidad",
			"Rebate_Mensual_Ganado",
			"Bono_Estacionalidad",
			"Pendiente_Cartera",
		]
	]
	st.dataframe(
		monthly_display_df,
		use_container_width=True,
		hide_index=True,
		column_config={
			"Compra_Aplicable": st.column_config.NumberColumn("Compra aplicable", format="$ %,.0f"),
			"Presupuesto_Escala_1": st.column_config.NumberColumn("Meta E1", format="$ %,.0f"),
			"Presupuesto_Escala_2": st.column_config.NumberColumn("Meta E2", format="$ %,.0f"),
			"Cumplimiento_E1": st.column_config.ProgressColumn("Cumpl. E1", min_value=0.0, max_value=1.2, format="%.0f%%"),
			"Cumplimiento_E2": st.column_config.ProgressColumn("Cumpl. E2", min_value=0.0, max_value=1.2, format="%.0f%%"),
			"Faltante_E1": st.column_config.NumberColumn("Faltante E1", format="$ %,.0f"),
			"Faltante_E2": st.column_config.NumberColumn("Faltante E2", format="$ %,.0f"),
			"Compra_Hasta_Corte": st.column_config.NumberColumn("Compra al corte", format="$ %,.0f"),
			"Meta_Estacionalidad": st.column_config.NumberColumn("Meta estacionalidad", format="$ %,.0f"),
			"Avance_Estacionalidad": st.column_config.ProgressColumn("Avance estacionalidad", min_value=0.0, max_value=1.2, format="%.0f%%"),
			"Rebate_Mensual_Ganado": st.column_config.NumberColumn("Rebate mensual", format="$ %,.0f"),
			"Bono_Estacionalidad": st.column_config.NumberColumn("Bono estacionalidad", format="$ %,.0f"),
			"Pendiente_Cartera": st.column_config.NumberColumn("Pendiente cartera", format="$ %,.0f"),
		},
	)
	st.bar_chart(monthly_df.set_index("Mes")[["Rebate_Mensual_Ganado", "Bono_Estacionalidad"]], use_container_width=True)

with quarter_tab:
	st.subheader("Cumplimiento trimestral y recuperación")
	st.dataframe(
		quarterly_df,
		use_container_width=True,
		hide_index=True,
		column_config={
			"Compra_Aplicable": st.column_config.NumberColumn("Compra aplicable", format="$ %,.0f"),
			"Presupuesto_Escala_1": st.column_config.NumberColumn("Meta E1", format="$ %,.0f"),
			"Presupuesto_Escala_2": st.column_config.NumberColumn("Meta E2", format="$ %,.0f"),
			"Cumplimiento_E1": st.column_config.ProgressColumn("Cumpl. E1", min_value=0.0, max_value=1.2, format="%.0f%%"),
			"Cumplimiento_E2": st.column_config.ProgressColumn("Cumpl. E2", min_value=0.0, max_value=1.2, format="%.0f%%"),
			"Faltante_E1": st.column_config.NumberColumn("Faltante E1", format="$ %,.0f"),
			"Faltante_E2": st.column_config.NumberColumn("Faltante E2", format="$ %,.0f"),
			"Rebate_Trimestral_Ganado": st.column_config.NumberColumn("Rebate trimestral", format="$ %,.0f"),
			"Recomposicion_Trimestral_Proyectada": st.column_config.NumberColumn("Recuperable", format="$ %,.0f"),
			"Recomposicion_Cartera_Bloqueada": st.column_config.NumberColumn("Bloqueado cartera", format="$ %,.0f"),
		},
	)
	st.bar_chart(quarterly_df.set_index("Trimestre")[["Rebate_Trimestral_Ganado", "Recomposicion_Trimestral_Proyectada", "Recomposicion_Cartera_Bloqueada"]], use_container_width=True)

	st.markdown(
		f"""
		<div class="section-card">
			<strong>Recomposición 9 meses</strong><br><br>
			Escala acumulada al ritmo actual: {pill_html(cycle_outlook['Escala_Ciclo'], get_scale_tone(cycle_outlook['Escala_Ciclo']))}<br><br>
			Recuperable proyectado: <strong>{format_currency(cycle_outlook['Recomposicion_9M_Proyectada'])}</strong><br>
			Bloqueado por cartera: <strong>{format_currency(cycle_outlook['Recomposicion_9M_Bloqueada'])}</strong><br>
			Faltante ciclo a E2: <strong>{format_currency(cycle_outlook['Faltante_E2_Ciclo'])}</strong>
		</div>
		""",
		unsafe_allow_html=True,
	)

with invoices_tab:
	st.subheader("Base factura por factura")
	filter_col1, filter_col2, filter_col3, filter_col4 = st.columns([1, 1, 1, 1.4])
	with filter_col1:
		filter_start = st.date_input("Desde", value=CURRENT_CYCLE_START, min_value=CURRENT_CYCLE_START, max_value=snapshot_date, key="rebate_from")
	with filter_col2:
		filter_end = st.date_input("Hasta", value=snapshot_date, min_value=CURRENT_CYCLE_START, max_value=snapshot_date, key="rebate_to")
	with filter_col3:
		estado_options = ["Pendiente", "Pagada"]
		estado_filter = st.multiselect("Estado de pago", options=estado_options, default=estado_options, key="rebate_state")
	with filter_col4:
		search_term = st.text_input("Buscar factura o correo", placeholder="Factura, remitente, asunto o adjunto", key="rebate_search")

	if filter_end < filter_start:
		st.error("La fecha final no puede ser menor que la fecha inicial.")
		st.stop()

	filtered_df = pintuco_df[
		(pintuco_df["Fecha_Factura"].dt.date >= filter_start) & (pintuco_df["Fecha_Factura"].dt.date <= filter_end)
	].copy()

	if estado_filter:
		filtered_df = filtered_df[filtered_df["Estado_Pago"].isin(estado_filter)].copy()

	if search_term.strip():
		search_value = search_term.strip().upper()
		search_columns = ["Numero_Factura", "Proveedor_Correo", "Remitente_Correo", "Asunto_Correo", "Nombre_Adjunto", "Message_ID"]
		search_mask = pd.Series(False, index=filtered_df.index)
		for column in search_columns:
			search_mask = search_mask | filtered_df[column].fillna("").astype(str).str.upper().str.contains(search_value, regex=False)
		filtered_df = filtered_df[search_mask].copy()

	if filtered_df.empty:
		st.warning("No hay facturas en el rango seleccionado.")
	else:
		display_df = sort_invoice_dataframe(filtered_df, by=["Fecha_Factura", "Fecha_Recepcion_Correo"], ascending=[False, False])
		st.dataframe(
			display_df,
			use_container_width=True,
			hide_index=True,
			column_config={
				"Fecha_Factura": st.column_config.DateColumn("Fecha factura", format="YYYY-MM-DD"),
				"Valor_Neto": st.column_config.NumberColumn("Compra neta", format="$ %,.0f"),
				"Compra_Excluida_12": st.column_config.NumberColumn("12% excluido", format="$ %,.0f"),
				"Compra_Aplicable_Rebate": st.column_config.NumberColumn("88% aplicable", format="$ %,.0f"),
				"Fecha_Recepcion_Correo": st.column_config.DatetimeColumn("Fecha correo", format="YYYY-MM-DD HH:mm"),
				"Estado_Pago": st.column_config.TextColumn("Estado de pago"),
			},
		)

with diagnostics_tab:
	st.subheader("Diagnóstico operativo y descarga")
	unique_senders = pintuco_df["Remitente_Correo"].replace("", pd.NA).dropna().nunique()
	unique_subjects = pintuco_df["Asunto_Correo"].replace("", pd.NA).dropna().nunique()
	duplicate_invoices = pintuco_df["Numero_Factura"].duplicated().sum()
	trazabilidad_completa = (
		pintuco_df["Fecha_Recepcion_Correo"].notna()
		& pintuco_df["Remitente_Correo"].fillna("").astype(str).str.strip().ne("")
		& pintuco_df["Message_ID"].fillna("").astype(str).str.strip().ne("")
	).mean()

	render_kpi_grid(
		[
			kpi_card_html("Cobertura de trazabilidad", format_percent(float(trazabilidad_completa), 0), "Correo, fecha y Message-ID", "navy"),
			kpi_card_html("Remitentes identificados", f"{unique_senders:,}", "Origen de correo detectado", "green"),
			kpi_card_html("Asuntos distintos", f"{unique_subjects:,}", "Control de buzón", "gold"),
			kpi_card_html("Duplicados visibles", f"{duplicate_invoices:,}", "Facturas repetidas en la base", "red"),
		]
	)

	st.markdown("Alertas del seguimiento")
	for alert in tracking_alerts:
		if "no se detectan alertas" in alert.lower():
			st.success(alert)
		else:
			st.warning(alert)

	executive_export_df = pd.DataFrame(
		[
			{
				"Foto": datetime.now(COLOMBIA_TZ).strftime("%Y-%m-%d %H:%M:%S"),
				"Compra_Aplicable_Acumulada": cycle_outlook["Compra_Aplicable_Acumulada"],
				"Meta_Acumulada_E1": cycle_outlook["Meta_Acumulada_E1"],
				"Meta_Acumulada_E2": cycle_outlook["Meta_Acumulada_E2"],
				"Escala_Ciclo": cycle_outlook["Escala_Ciclo"],
				"Cumplimiento_Acumulado_E1": cycle_outlook["Cumplimiento_Acumulado_E1"],
				"Cumplimiento_Acumulado_E2": cycle_outlook["Cumplimiento_Acumulado_E2"],
				"Rebate_Ganado_A_Hoy": rebate_total_ganado,
				"Recomposicion_9M_Proyectada": cycle_outlook["Recomposicion_9M_Proyectada"],
				"Recomposicion_9M_Bloqueada": cycle_outlook["Recomposicion_9M_Bloqueada"],
			}
		]
	)
	excel_data = generate_excel_report(executive_export_df, monthly_df, quarterly_df, pintuco_df)

	st.download_button(
		label="⬇️ Descargar consolidado ejecutivo del rebate",
		data=excel_data,
		file_name=f"Rebate_Pintuco_Ejecutivo_{snapshot_date}.xlsx",
		mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		use_container_width=True,
	)
