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

CYCLE_TARGETS = {
	"Meta acumulada del ciclo": {
		"Escala 1": None,
		"Rebate 1": None,
		"Escala 2": None,
		"Rebate 2": None,
	}
}

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
		.pintuco-banner {{
			background: linear-gradient(120deg, #0b3c5d 0%, #145374 45%, #f3a712 100%);
			border-radius: 18px;
			padding: 24px 28px;
			color: #ffffff;
			margin-bottom: 18px;
			box-shadow: 0 12px 32px rgba(11, 60, 93, 0.18);
		}}
		.pintuco-banner h1 {{
			margin: 0;
			font-size: 2rem;
		}}
		.pintuco-banner p {{
			margin: 8px 0 0 0;
			font-size: 1rem;
			opacity: 0.95;
		}}
		.info-card {{
			background: #f7fafc;
			border: 1px solid #d9e2ec;
			border-radius: 14px;
			padding: 16px 18px;
			margin-bottom: 16px;
		}}
	</style>
	""",
	unsafe_allow_html=True,
)


def format_currency(value: float) -> str:
	return f"${value:,.0f}"


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


def get_target_defaults() -> dict:
	configured_target = CYCLE_TARGETS["Meta acumulada del ciclo"]
	return {
		"Escala 1": float(configured_target["Escala 1"] or 0),
		"Rebate 1": float(configured_target["Rebate 1"] or 0),
		"Escala 2": float(configured_target["Escala 2"] or 0),
		"Rebate 2": float(configured_target["Rebate 2"] or 0),
	}


def get_active_targets() -> dict | None:
	defaults = get_target_defaults()
	with st.sidebar:
		st.header("Configuración del ciclo")
		st.caption("El módulo ya está alineado al ciclo iniciado el 2026-04-01. Las metas definitivas pueden cargarse cuando las recibas.")
		use_manual_targets = st.checkbox(
			"Configurar metas temporales en esta sesión",
			value=any(defaults.values()),
			help="Actívalo si quieres proyectar el avance antes de cargar las metas oficiales.",
		)

		if not use_manual_targets:
			return None

		meta_e1 = st.number_input("Meta ciclo Escala 1", min_value=0.0, value=defaults["Escala 1"], step=1000000.0, format="%.0f")
		rebate_e1 = st.number_input("Rebate Escala 1 (%)", min_value=0.0, max_value=100.0, value=defaults["Rebate 1"] * 100, step=0.1, format="%.2f")
		meta_e2 = st.number_input("Meta ciclo Escala 2", min_value=0.0, value=defaults["Escala 2"], step=1000000.0, format="%.0f")
		rebate_e2 = st.number_input("Rebate Escala 2 (%)", min_value=0.0, max_value=100.0, value=defaults["Rebate 2"] * 100, step=0.1, format="%.2f")

	if meta_e1 <= 0 and meta_e2 <= 0:
		return None

	return {
		"Escala 1": meta_e1,
		"Rebate 1": rebate_e1 / 100,
		"Escala 2": meta_e2,
		"Rebate 2": rebate_e2 / 100,
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

	summary = (
		df.assign(Mes=df["Fecha_Factura"].dt.to_period("M").dt.to_timestamp())
		.groupby("Mes", as_index=False)
		.agg(
			Facturas=("Numero_Factura", "nunique"),
			Compra_Neta=("Valor_Neto", "sum"),
			Exclusion_12=("Compra_Excluida_12", "sum"),
			Compra_Aplicable=("Compra_Aplicable_Rebate", "sum"),
			Pendientes=("Estado_Pago", lambda values: (pd.Series(values) == "Pendiente").sum()),
		)
	)
	summary["Pagadas"] = summary["Facturas"] - summary["Pendientes"]
	summary["Mes"] = summary["Mes"].dt.strftime("%Y-%m")
	return summary


def build_target_projection(total_applicable_purchase: float, active_targets: dict | None) -> pd.DataFrame:
	if not active_targets:
		return pd.DataFrame()

	rows = []
	for scale_name, rebate_name in (("Escala 1", "Rebate 1"), ("Escala 2", "Rebate 2")):
		meta_value = active_targets.get(scale_name, 0) or 0
		rebate_value = active_targets.get(rebate_name, 0) or 0
		if meta_value <= 0:
			continue

		fulfilled = total_applicable_purchase >= meta_value
		rows.append(
			{
				"Meta": scale_name,
				"Compra_Aplicable_Actual": total_applicable_purchase,
				"Objetivo": meta_value,
				"Faltante": max(meta_value - total_applicable_purchase, 0),
				"Cumplimiento": min(total_applicable_purchase / meta_value, 1.0),
				"Rebate_%": rebate_value,
				"Rebate_Proyectado": meta_value * rebate_value if fulfilled else total_applicable_purchase * rebate_value,
				"Estado": "Cumplida" if fulfilled else "Pendiente",
			}
		)

	return pd.DataFrame(rows)


def generate_excel_report(summary_df: pd.DataFrame, invoices_df: pd.DataFrame, target_df: pd.DataFrame) -> io.BytesIO:
	output = io.BytesIO()
	workbook = Workbook()

	sheets_to_write = [
		("Resumen_Ciclo", summary_df),
		("Facturas_Pintuco", invoices_df),
		("Metas", target_df if not target_df.empty else pd.DataFrame([{"Estado": "Pendiente por definir metas oficiales"}])),
	]

	header_font = Font(bold=True, color="FFFFFF")
	header_fill = PatternFill(start_color="0B3C5D", end_color="0B3C5D", fill_type="solid")
	center_alignment = Alignment(horizontal="center", vertical="center")
	currency_format = '_($* #,##0_);_($* (#,##0);_($* "-"??_);_(@_)'
	percent_format = "0.0%"

	for index, (sheet_name, dataframe) in enumerate(sheets_to_write):
		worksheet = workbook.active if index == 0 else workbook.create_sheet(title=sheet_name)
		worksheet.title = sheet_name

		if dataframe.empty:
			dataframe = pd.DataFrame([{"Mensaje": "Sin información disponible"}])

		rows = dataframe_to_rows(dataframe, index=False, header=True)
		for row_index, row in enumerate(rows, start=1):
			for column_index, value in enumerate(row, start=1):
				cell = worksheet.cell(row=row_index, column=column_index, value=value)
				if row_index == 1:
					cell.font = header_font
					cell.fill = header_fill
					cell.alignment = center_alignment
				elif isinstance(value, (int, float)):
					column_name = dataframe.columns[column_index - 1]
					if any(token in column_name for token in ["Compra", "Objetivo", "Faltante", "Rebate", "Exclusion"]):
						cell.number_format = currency_format
					if column_name == "Cumplimiento":
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
		<h1>Portal de Rebate Pintuco</h1>
		<p>{CURRENT_CYCLE_NAME}. El tablero consolida facturas leídas desde el correo, separa el 12% excluido y calcula el 88% que sí aplica a metas.</p>
	</div>
	""",
	unsafe_allow_html=True,
)

st.markdown(
	f"""
	<div class="info-card">
		<strong>Reglas activas del análisis</strong><br>
		1. El ciclo actual arranca el <strong>{CURRENT_CYCLE_START.strftime('%Y-%m-%d')}</strong>.<br>
		2. Toda compra se divide entre <strong>12% excluido</strong> y <strong>88% aplicable</strong>.<br>
		3. La lectura de facturas se toma desde la carpeta de correo <strong>{EMAIL_FOLDER}</strong> y se deja trazabilidad del remitente, asunto, adjunto y fecha de recepción.
	</div>
	""",
	unsafe_allow_html=True,
)

active_targets = get_active_targets()

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
		st.info("Aún no hay una foto guardada en esta sesión. Usa 'Actualizar ahora' para refrescar la base del rebate.")

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

filter_col1, filter_col2, filter_col3, filter_col4 = st.columns([1, 1, 1, 1.4])
with filter_col1:
	filter_start = st.date_input("Desde", value=CURRENT_CYCLE_START, min_value=CURRENT_CYCLE_START, max_value=date.today())
with filter_col2:
	filter_end = st.date_input("Hasta", value=date.today(), min_value=CURRENT_CYCLE_START, max_value=date.today())
with filter_col3:
	estado_options = ["Pendiente", "Pagada"]
	estado_filter = st.multiselect("Estado de pago", options=estado_options, default=estado_options)
with filter_col4:
	search_term = st.text_input("Buscar factura o correo", placeholder="Factura, remitente, asunto o adjunto")

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
	st.stop()

total_neto = filtered_df["Valor_Neto"].sum()
total_excluido = filtered_df["Compra_Excluida_12"].sum()
total_aplicable = filtered_df["Compra_Aplicable_Rebate"].sum()
total_facturas = filtered_df["Numero_Factura"].nunique()
facturas_pendientes = (filtered_df["Estado_Pago"] == "Pendiente").sum()
facturas_pagadas = (filtered_df["Estado_Pago"] == "Pagada").sum()
ultima_factura = filtered_df["Fecha_Factura"].max()
ultimo_correo = filtered_df["Fecha_Recepcion_Correo"].max()
valor_pendiente = filtered_df.loc[filtered_df["Estado_Pago"] == "Pendiente", "Valor_Neto"].sum()
trazabilidad_completa = (
	filtered_df["Fecha_Recepcion_Correo"].notna()
	& filtered_df["Remitente_Correo"].fillna("").astype(str).str.strip().ne("")
	& filtered_df["Message_ID"].fillna("").astype(str).str.strip().ne("")
).mean()
tracking_alerts = build_tracking_alerts(filtered_df)

st.markdown(
	f"""
	<div class="info-card">
		<strong>Foto operativa del rebate</strong><br>
		Ventana analizada: <strong>{filter_start.strftime('%Y-%m-%d')}</strong> a <strong>{filter_end.strftime('%Y-%m-%d')}</strong>.<br>
		Base filtrada: <strong>{len(filtered_df):,}</strong> registros con <strong>{trazabilidad_completa:.0%}</strong> de trazabilidad completa.<br>
		Valor aún pendiente de pago: <strong>{format_currency(valor_pendiente)}</strong>.
	</div>
	""",
	unsafe_allow_html=True,
)

metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
metric_col1.metric("Compra neta leída", format_currency(total_neto))
metric_col2.metric("12% excluido", format_currency(total_excluido))
metric_col3.metric("88% aplicable a meta", format_currency(total_aplicable))
metric_col4.metric("Facturas del periodo", f"{total_facturas:,}")

metric_col5, metric_col6, metric_col7, metric_col8 = st.columns(4)
metric_col5.metric("Pendientes de pago", f"{facturas_pendientes:,}")
metric_col6.metric("Pagadas", f"{facturas_pagadas:,}")
metric_col7.metric("Última factura", ultima_factura.strftime("%Y-%m-%d") if pd.notna(ultima_factura) else "N/D")
metric_col8.metric("Último correo leído", ultimo_correo.strftime("%Y-%m-%d %H:%M") if pd.notna(ultimo_correo) else "N/D")

metric_col9, metric_col10, metric_col11, metric_col12 = st.columns(4)
metric_col9.metric("Valor pendiente", format_currency(valor_pendiente))
metric_col10.metric("Cobertura de trazabilidad", f"{trazabilidad_completa:.0%}")
metric_col11.metric("Filtro de estados", ", ".join(estado_filter) if estado_filter else "Todos")
metric_col12.metric("Búsqueda activa", search_term.strip() or "Sin filtro")

summary_df = build_cycle_summary(filtered_df)
target_df = build_target_projection(total_aplicable, active_targets)
excel_data = generate_excel_report(summary_df, filtered_df, target_df)

overview_tab, invoices_tab, targets_tab, diagnostics_tab = st.tabs(
	["📊 Resumen del ciclo", "📑 Facturas y correo", "🎯 Metas y avance", "🛰️ Diagnóstico"]
)

with overview_tab:
	st.subheader("Compras acumuladas del ciclo")
	st.markdown("La siguiente tabla resume mes a mes lo comprado, lo excluido por regla del 12% y lo que realmente suma para rebate.")
	st.dataframe(
		summary_df,
		use_container_width=True,
		hide_index=True,
		column_config={
			"Compra_Neta": st.column_config.NumberColumn("Compra neta", format="$ %d"),
			"Exclusion_12": st.column_config.NumberColumn("12% excluido", format="$ %d"),
			"Compra_Aplicable": st.column_config.NumberColumn("88% aplicable", format="$ %d"),
			"Pendientes": st.column_config.NumberColumn("Pendientes", format="%d"),
			"Pagadas": st.column_config.NumberColumn("Pagadas", format="%d"),
		},
	)
	st.bar_chart(summary_df.set_index("Mes")[["Compra_Neta", "Compra_Aplicable"]], use_container_width=True)

with invoices_tab:
	st.subheader("Factura por factura con trazabilidad de correo")
	st.markdown("Cada fila conserva la relación entre XML leído, remitente, asunto del correo y estado de pago identificado contra la cartera vigente.")
	display_df = sort_invoice_dataframe(filtered_df, by=["Fecha_Factura", "Fecha_Recepcion_Correo"], ascending=[False, False])
	st.dataframe(
		display_df,
		use_container_width=True,
		hide_index=True,
		column_config={
			"Fecha_Factura": st.column_config.DateColumn("Fecha factura", format="YYYY-MM-DD"),
			"Valor_Neto": st.column_config.NumberColumn("Compra neta", format="$ %d"),
			"Compra_Excluida_12": st.column_config.NumberColumn("12% excluido", format="$ %d"),
			"Compra_Aplicable_Rebate": st.column_config.NumberColumn("88% aplicable", format="$ %d"),
			"Fecha_Recepcion_Correo": st.column_config.DatetimeColumn("Fecha correo", format="YYYY-MM-DD HH:mm"),
			"Remitente_Correo": "Remitente",
			"Asunto_Correo": "Asunto",
			"Nombre_Adjunto": "Adjunto XML/ZIP",
			"Message_ID": "Message-ID",
			"Estado_Pago": st.column_config.TextColumn("Estado de pago"),
		},
	)

with targets_tab:
	st.subheader("Metas del nuevo ciclo")
	if target_df.empty:
		st.info("Las metas oficiales del nuevo ciclo aún no están cargadas. Ya quedó lista la base de compras y el cálculo del 12% excluido para que mañana solo actualices los objetivos.")
		st.markdown(
			f"""
			<div class="info-card">
				Compra neta acumulada: <strong>{format_currency(total_neto)}</strong><br>
				Compra excluida (12%): <strong>{format_currency(total_excluido)}</strong><br>
				Compra aplicable actual: <strong>{format_currency(total_aplicable)}</strong>
			</div>
			""",
			unsafe_allow_html=True,
		)
	else:
		st.dataframe(
			target_df,
			use_container_width=True,
			hide_index=True,
			column_config={
				"Compra_Aplicable_Actual": st.column_config.NumberColumn("Compra aplicable actual", format="$ %d"),
				"Objetivo": st.column_config.NumberColumn("Meta", format="$ %d"),
				"Faltante": st.column_config.NumberColumn("Faltante", format="$ %d"),
				"Cumplimiento": st.column_config.ProgressColumn("Cumplimiento", format="%.1f%%", min_value=0, max_value=1),
				"Rebate_%": st.column_config.NumberColumn("Rebate", format="%.2f"),
				"Rebate_Proyectado": st.column_config.NumberColumn("Rebate proyectado", format="$ %d"),
			},
		)

with diagnostics_tab:
	st.subheader("Diagnóstico operativo")
	unique_senders = filtered_df["Remitente_Correo"].replace("", pd.NA).dropna().nunique()
	unique_subjects = filtered_df["Asunto_Correo"].replace("", pd.NA).dropna().nunique()
	duplicate_invoices = filtered_df["Numero_Factura"].duplicated().sum()

	diag_col1, diag_col2, diag_col3 = st.columns(3)
	diag_col1.metric("Remitentes identificados", f"{unique_senders:,}")
	diag_col2.metric("Asuntos de correo distintos", f"{unique_subjects:,}")
	diag_col3.metric("Duplicados en pantalla", f"{duplicate_invoices:,}")

	st.markdown("Alertas de seguimiento")
	for alert in tracking_alerts:
		if "no se detectan alertas" in alert.lower():
			st.success(alert)
		else:
			st.warning(alert)

	st.markdown("Descarga el consolidado para auditoría o trabajo fuera del portal.")
	st.download_button(
		label="⬇️ Descargar consolidado del ciclo",
		data=excel_data,
		file_name=f"Rebate_Pintuco_Ciclo_2026_{date.today()}.xlsx",
		mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		use_container_width=True,
	)
