# -*- coding: utf-8 -*-
"""
Página de Creación y Actualización de Proveedores para FERREINOX.

Este script crea una página dedicada en la aplicación de Streamlit para que los
proveedores puedan diligenciar su información. La página presenta un formulario
profesional y limpio que captura todos los datos necesarios para el proceso de
vinculación.

Funcionalidades clave:
- Formulario detallado dentro de un st.form para una mejor experiencia de usuario.
- Captura de datos generales, tributarios, de contacto y bancarios.
- Presentación de políticas de la empresa y solicitud de aceptación.
- Checklist de los documentos requeridos.
- Generación y descarga de un archivo PDF con la información diligenciada.
- Generación y descarga de un archivo PDF en blanco para llenado manual.
- Generación y descarga de un archivo Excel con todos los datos capturados.
- Uso de widgets interactivos de Streamlit para una experiencia de usuario fluida.

Dependencias adicionales (añadir a requirements.txt):
fpdf2
openpyxl
"""

# ======================================================================================
# --- 0. IMPORTACIÓN DE LIBRERÍAS ---
# ======================================================================================
import streamlit as st
import pandas as pd
from fpdf import FPDF
from datetime import datetime
import io

# ======================================================================================
# --- 1. CONFIGURACIÓN DE LA PÁGINA Y ESTILOS ---
# ======================================================================================

st.set_page_config(
    page_title="Creación de Proveedores | FERREINOX",
    page_icon="👥",
    layout="wide"
)

def load_css():
    """Carga estilos CSS personalizados para mejorar la apariencia."""
    st.markdown("""
        <style>
            .main .block-container {
                padding-top: 2rem;
                padding-left: 2rem;
                padding-right: 2rem;
            }
            .st-bx {
                border-radius: 0.5rem;
                padding: 1.5rem;
                background-color: #F8F9FA;
                border: 1px solid #E0E0E0;
            }
            h1, h2, h3 {
                color: #0C2D57;
            }
            .stButton>button {
                border-radius: 0.5rem;
                border: 1px solid #0C2D57;
                background-color: #0C2D57;
                color: white;
                transition: all 0.2s ease-in-out;
                width: 100%;
            }
            .stButton>button:hover {
                background-color: white;
                color: #0C2D57;
                border: 1px solid #0C2D57;
            }
            .stDownloadButton>button {
                background-color: #28a745;
                color: white;
                border: 1px solid #28a745;
                width: 100%;
            }
            .stDownloadButton>button:hover {
                background-color: white;
                color: #28a745;
            }
        </style>
    """, unsafe_allow_html=True)

# ======================================================================================
# --- 2. CLASE Y FUNCIONES PARA GENERACIÓN DE PDF ---
# ======================================================================================

class PDF(FPDF):
    """Clase extendida de FPDF para crear encabezados y pies de página personalizados."""
    def header(self):
        # self.image('logo.png', 10, 8, 33) # Descomentar si tienes un logo
        self.set_font('Arial', 'B', 14)
        self.cell(0, 10, 'FORMATO DE CREACIÓN Y ACTUALIZACIÓN DE PROVEEDORES', 0, 1, 'C')
        self.set_font('Arial', '', 10)
        self.cell(0, 8, 'FERREINOX S.A.S. BIC', 0, 1, 'C')
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Página {self.page_no()}', 0, 0, 'C')

    def chapter_title(self, title):
        self.set_font('Arial', 'B', 12)
        self.set_fill_color(220, 220, 220)
        self.cell(0, 8, title, 0, 1, 'L', fill=True)
        self.ln(4)

    def form_field(self, label, value):
        self.set_font('Arial', 'B', 10)
        self.cell(60, 8, f'{label}:', 0, 0)
        self.set_font('Arial', '', 10)
        self.multi_cell(w=0, h=8, text=str(value), border=0, align='L')
        self.ln(2)

    def blank_form_field(self, label, value="__________________________________________________"):
        """Crea un campo de formulario con una línea para ser llenado manualmente."""
        self.set_font('Arial', 'B', 10)
        self.cell(60, 8, f'{label}:', 0, 0)
        self.set_font('Arial', '', 10)
        self.multi_cell(w=0, h=8, text=value, border=0, align='L')
        self.ln(2)

def generate_pdf(data: dict) -> bytes:
    """Genera un archivo PDF a partir de los datos del formulario."""
    pdf = PDF()
    pdf.add_page()

    # --- DATOS GENERALES ---
    pdf.chapter_title('1. DATOS GENERALES DE LA EMPRESA')
    pdf.form_field('Fecha de Diligenciamiento', data['fecha_diligenciamiento'])
    pdf.form_field('Razón Social', data['razon_social'])
    pdf.form_field('NIT', f"{data['nit']}-{data['dv']}")
    pdf.form_field('Dirección Principal', data['direccion'])
    pdf.form_field('Ciudad / Departamento', data['ciudad_depto'])
    pdf.form_field('País', 'Colombia')
    pdf.form_field('Teléfono Fijo', data['tel_fijo'])
    pdf.form_field('Teléfono Celular', data['tel_celular'])
    pdf.form_field('Correo Electrónico', data['email_general'])
    pdf.form_field('Página Web', data['website'])
    pdf.ln(5)

    # --- INFORMACIÓN TRIBUTARIA ---
    pdf.chapter_title('2. INFORMACIÓN TRIBUTARIA Y FISCAL')
    pdf.form_field('Tipo de Persona', data['tipo_persona'])
    regimen = f"{data['regimen']} ({data['otro_regimen']})" if data['regimen'] == 'Otro' else data['regimen']
    pdf.form_field('Régimen Tributario', regimen)
    pdf.form_field('Actividad Económica (CIIU)', data['ciiu'])
    pdf.ln(5)

    # --- CONTACTOS ---
    pdf.chapter_title('3. INFORMACIÓN DE CONTACTOS')
    pdf.set_font('Arial', 'I', 11)
    pdf.cell(0, 8, 'Contacto Comercial', 0, 1)
    pdf.form_field('Nombre', data['comercial_nombre'])
    pdf.form_field('Cargo', data['comercial_cargo'])
    pdf.form_field('Correo Electrónico', data['comercial_email'])
    pdf.form_field('Teléfono / Celular', data['comercial_tel'])
    pdf.ln(4)

    pdf.set_font('Arial', 'I', 11)
    pdf.cell(0, 8, 'Contacto para Pagos y Facturación', 0, 1)
    pdf.form_field('Nombre', data['pagos_nombre'])
    pdf.form_field('Cargo', data['pagos_cargo'])
    pdf.form_field('Correo para Factura Electrónica', data['pagos_email'])
    pdf.form_field('Teléfono / Celular', data['pagos_tel'])
    pdf.ln(5)

    # --- INFORMACIÓN BANCARIA ---
    pdf.chapter_title('4. INFORMACIÓN BANCARIA PARA PAGOS')
    pdf.form_field('Nombre del Banco', data['banco_nombre'])
    pdf.form_field('Titular de la Cuenta', data['banco_titular'])
    pdf.form_field('NIT / C.C. del Titular', data['banco_nit_cc'])
    pdf.form_field('Tipo de Cuenta', data['banco_tipo_cuenta'])
    pdf.form_field('Número de la Cuenta', data['banco_numero_cuenta'])
    pdf.ln(5)

    # --- DOCUMENTOS Y FIRMA ---
    pdf.chapter_title('6. DOCUMENTOS REQUERIDOS')
    pdf.set_font('Arial', '', 10)
    pdf.cell(0, 8, f"[ X ] RUT actualizado." if data['doc_rut'] else "[   ] RUT actualizado.", 0, 1)
    pdf.cell(0, 8, f"[ X ] Cámara de Comercio." if data['doc_camara'] else "[   ] Cámara de Comercio.", 0, 1)
    pdf.cell(0, 8, f"[ X ] Certificación Bancaria." if data['doc_bancaria'] else "[   ] Certificación Bancaria.", 0, 1)
    pdf.cell(0, 8, f"[ X ] Fotocopia C.C. Representante Legal." if data['doc_cc_rl'] else "[   ] Fotocopia C.C. Representante Legal.", 0, 1)
    pdf.ln(10)

    pdf.chapter_title('7. FIRMA Y ACEPTACIÓN')
    pdf.set_font('Arial', '', 10)
    pdf.multi_cell(
        w=0, h=6,
        text="Con la firma de este documento, el representante legal o persona autorizada certifica la veracidad de la información suministrada y acepta las políticas establecidas por FERREINOX S.A.S. BIC.",
        border=0, ln=1, align='L'
    )
    pdf.ln(5)
    pdf.form_field('Nombre del Representante Legal', data['rl_nombre'])
    pdf.form_field('C.C. No.', data['rl_cc'])
    pdf.ln(20)
    pdf.cell(80, 8, '_________________________________', 0, 1)
    pdf.cell(80, 8, 'Firma', 0, 0, 'C')

    # --- CORRECCIÓN CLAVE ---
    # Se asegura que la salida sea en bytes, codificando la salida de string.
    # Esto soluciona el error StreamlitAPIException.
    return pdf.output(dest='S').encode('latin-1')

def generate_blank_pdf() -> bytes:
    """Genera un archivo PDF en blanco del formulario para ser diligenciado manualmente."""
    pdf = PDF()
    pdf.add_page()

    # --- DATOS GENERALES ---
    pdf.chapter_title('1. DATOS GENERALES DE LA EMPRESA')
    pdf.blank_form_field('Fecha de Diligenciamiento', '____ / ____ / ________')
    pdf.blank_form_field('Razón Social')
    pdf.blank_form_field('NIT')
    pdf.blank_form_field('Dirección Principal')
    pdf.blank_form_field('Ciudad / Departamento')
    pdf.blank_form_field('País', 'Colombia')
    pdf.blank_form_field('Teléfono Fijo')
    pdf.blank_form_field('Teléfono Celular')
    pdf.blank_form_field('Correo Electrónico')
    pdf.blank_form_field('Página Web')
    pdf.ln(5)

    # --- INFORMACIÓN TRIBUTARIA ---
    pdf.chapter_title('2. INFORMACIÓN TRIBUTARIA Y FISCAL')
    pdf.set_font('Arial', 'B', 10)
    pdf.cell(60, 8, 'Tipo de Persona:', 0, 0)
    pdf.set_font('Arial', '', 10)
    pdf.cell(40, 8, '[   ] Persona Jurídica', 0, 0)
    pdf.cell(0, 8, '[   ] Persona Natural', 0, 1)
    pdf.ln(2)
    pdf.set_font('Arial', 'B', 10)
    pdf.cell(60, 8, 'Régimen Tributario:', 0, 1)
    pdf.set_font('Arial', '', 10)
    pdf.cell(0, 8, '[   ] Régimen Común / Responsable de IVA', 0, 1)
    pdf.cell(0, 8, '[   ] Régimen Simplificado / No Responsable de IVA', 0, 1)
    pdf.cell(0, 8, '[   ] Gran Contribuyente', 0, 1)
    pdf.cell(0, 8, '[   ] Autorretenedor de Renta', 0, 1)
    pdf.cell(0, 8, '[   ] Otro: _________________________________', 0, 1)
    pdf.ln(2)
    pdf.blank_form_field('Actividad Económica (CIIU)')
    pdf.ln(5)

    # --- CONTACTOS ---
    pdf.chapter_title('3. INFORMACIÓN DE CONTACTOS')
    pdf.set_font('Arial', 'I', 11)
    pdf.cell(0, 8, 'Contacto Comercial', 0, 1)
    pdf.blank_form_field('Nombre')
    pdf.blank_form_field('Cargo')
    pdf.blank_form_field('Correo Electrónico')
    pdf.blank_form_field('Teléfono / Celular')
    pdf.ln(4)

    pdf.set_font('Arial', 'I', 11)
    pdf.cell(0, 8, 'Contacto para Pagos y Facturación', 0, 1)
    pdf.blank_form_field('Nombre')
    pdf.blank_form_field('Cargo')
    pdf.blank_form_field('Correo para Factura Electrónica')
    pdf.blank_form_field('Teléfono / Celular')
    pdf.ln(5)

    # --- INFORMACIÓN BANCARIA ---
    pdf.chapter_title('4. INFORMACIÓN BANCARIA PARA PAGOS')
    pdf.blank_form_field('Nombre del Banco')
    pdf.blank_form_field('Titular de la Cuenta')
    pdf.blank_form_field('NIT / C.C. del Titular')
    pdf.set_font('Arial', 'B', 10)
    pdf.cell(60, 8, 'Tipo de Cuenta:', 0, 0)
    pdf.set_font('Arial', '', 10)
    pdf.cell(40, 8, '[   ] Ahorros', 0, 0)
    pdf.cell(0, 8, '[   ] Corriente', 0, 1)
    pdf.ln(2)
    pdf.blank_form_field('Número de la Cuenta')
    pdf.ln(5)

    if pdf.get_y() > 180:
        pdf.add_page()

    # --- POLÍTICAS ---
    pdf.chapter_title('5. POLÍTICAS Y ACEPTACIÓN DEL PROVEEDOR')
    pdf.set_font('Arial', '', 10)
    politicas_texto = (
        "Le agradecemos leer y aceptar nuestras políticas básicas para una relación comercial transparente y efectiva.\n\n"
        "- Protección de Datos: El proveedor autoriza a FERREINOX S.A.S. BIC a tratar sus datos personales y "
        "comerciales con el fin de gestionar la relación contractual, realizar pagos y enviar comunicaciones, de "
        "acuerdo con la Ley 1581 de 2012 y nuestras políticas de tratamiento de datos.\n"
        "- Calidad y Cumplimiento: El proveedor se compromete a entregar los productos y/o servicios bajo las "
        "condiciones de calidad, tiempo y forma acordadas en cada orden de compra o contrato.\n"
        "- Facturación: Toda factura debe ser emitida a nombre de FERREINOX S.A.S. BIC con NIT 900.205.211-8 "
        "y enviada al correo electrónico designado para facturación. La factura deberá hacer referencia a una orden "
        "de compra o contrato válido para su gestión.\n"
        "- Ética y Transparencia: El proveedor declara que sus recursos no provienen de actividades ilícitas y se "
        "compromete a actuar con ética, honestidad y transparencia en todas sus interacciones comerciales con nuestra "
        "empresa, rechazando cualquier práctica de soborno, corrupción o fraude."
    )
    pdf.multi_cell(w=0, h=6, text=politicas_texto, align='L')
    pdf.ln(5)

    # --- DOCUMENTOS Y FIRMA ---
    pdf.chapter_title('6. DOCUMENTOS REQUERIDOS')
    pdf.set_font('Arial', '', 10)
    pdf.cell(0, 8, "[   ] RUT (Registro Único Tributario) actualizado.", 0, 1)
    pdf.cell(0, 8, "[   ] Cámara de Comercio con fecha de expedición no mayor a 30 días.", 0, 1)
    pdf.cell(0, 8, "[   ] Certificación Bancaria con fecha de expedición no mayor a 30 días.", 0, 1)
    pdf.cell(0, 8, "[   ] Fotocopia de la Cédula de Ciudadanía del Representante Legal.", 0, 1)
    pdf.ln(10)

    pdf.chapter_title('7. FIRMA Y ACEPTACIÓN')
    pdf.set_font('Arial', '', 10)
    pdf.multi_cell(
        w=0, h=6,
        text="Con la firma de este documento, el representante legal o persona autorizada certifica la veracidad de la información suministrada y acepta las políticas establecidas por FERREINOX S.A.S. BIC.",
        border=0, ln=1, align='L'
    )
    pdf.ln(5)
    pdf.blank_form_field('Nombre del Representante Legal')
    pdf.blank_form_field('C.C. No.')
    pdf.ln(20)
    pdf.cell(80, 8, '_________________________________', 0, 1)
    pdf.cell(80, 8, 'Firma', 0, 0, 'C')

    # --- CORRECCIÓN CLAVE ---
    return pdf.output(dest='S').encode('latin-1')


# ======================================================================================
# --- 3. FUNCIÓN PARA GENERACIÓN DE EXCEL ---
# ======================================================================================

def generate_excel(data: dict) -> bytes:
    """Genera un archivo Excel a partir de los datos del formulario."""
    # Prepara el diccionario para el DataFrame, excluyendo los booleanos de los documentos
    excel_data = {key: value for key, value in data.items() if not key.startswith('doc_')}
    
    df_data = {
        'Campo': list(excel_data.keys()),
        'Información Suministrada': list(excel_data.values())
    }
    df = pd.DataFrame(df_data)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='DatosProveedor')
        worksheet = writer.sheets['DatosProveedor']
        worksheet.column_dimensions['A'].width = 35
        worksheet.column_dimensions['B'].width = 60

    processed_data = output.getvalue()
    return processed_data


# ======================================================================================
# --- 4. INTERFAZ DE USUARIO PRINCIPAL (STREAMLIT) ---
# ======================================================================================

load_css()

# st.image("logo.png", width=300) # Descomentar si tienes la imagen
st.title("Formato de Creación y Actualización de Proveedores")
st.markdown("---")
st.markdown("""
Estimado proveedor, para dar inicio a nuestro proceso de vinculación comercial y garantizar una gestión eficiente, 
le solicitamos amablemente diligenciar la siguiente información y adjuntar los documentos requeridos.
""")

# --- Botón de descarga para el formato en blanco (siempre visible) ---
st.header("Opción 1: Descargar Formulario en Blanco")
st.markdown("Descargue esta versión si prefiere diligenciar el formato manualmente.")
blank_pdf_bytes = generate_blank_pdf()
st.download_button(
    label="📄 Descargar Formato en Blanco (PDF)",
    data=blank_pdf_bytes,
    file_name="Formato_Proveedor_FERREINOX_en_Blanco.pdf",
    mime="application/pdf"
)
st.markdown("---")


# --- Formulario para diligenciar en línea ---
st.header("Opción 2: Diligenciar Formulario en Línea")
st.markdown("Complete los siguientes campos para generar automáticamente los documentos.")

form_data = {}

with st.form(key="provider_form"):
    
    # --- 1. DATOS GENERALES ---
    with st.container():
        st.markdown("<div class='st-bx'>", unsafe_allow_html=True)
        st.subheader("1. Datos Generales de la Empresa")
        
        form_data['fecha_diligenciamiento'] = st.date_input(
            "Fecha de Diligenciamiento:",
            datetime.now(),
            help="Fecha en la que se está llenando este formulario."
        ).strftime('%Y-%m-%d')
        
        form_data['razon_social'] = st.text_input("Razón Social (Nombre legal completo):", key="razon_social")
        
        col_nit, col_dv = st.columns([4, 1])
        with col_nit:
            form_data['nit'] = st.text_input("NIT (Número de Identificación Tributaria):", key="nit", help="Ingrese el número sin el dígito de verificación.")
        with col_dv:
            form_data['dv'] = st.text_input("DV:", key="dv", max_chars=1, help="Dígito de Verificación.")
            
        form_data['direccion'] = st.text_input("Dirección Principal:", key="direccion")

        col1, col2 = st.columns(2)
        with col1:
            form_data['ciudad_depto'] = st.text_input("Ciudad / Departamento:", key="ciudad")
            form_data['tel_fijo'] = st.text_input("Teléfono Fijo:", key="tel_fijo")
            form_data['email_general'] = st.text_input("Correo Electrónico General:", key="email_general")
        with col2:
            st.text_input("País:", "Colombia", disabled=True, key="pais")
            form_data['tel_celular'] = st.text_input("Teléfono Celular:", key="tel_celular")
            form_data['website'] = st.text_input("Página Web (Opcional):", placeholder="https://www.suempresa.com", key="website")
        st.markdown("</div>", unsafe_allow_html=True)


    # --- 2. INFORMACIÓN TRIBUTARIA Y FISCAL ---
    with st.container():
        st.markdown("<div class='st-bx' style='margin-top: 20px;'>", unsafe_allow_html=True)
        st.subheader("2. Información Tributaria y Fiscal")
        col1, col2 = st.columns(2)
        with col1:
            form_data['tipo_persona'] = st.radio("Tipo de Persona:", ('Persona Jurídica', 'Persona Natural'), key="tipo_persona")
            form_data['ciiu'] = st.text_input("Actividad Económica (Código CIIU):", help="Encuentre este código en su RUT.", key="ciiu")
        with col2:
            form_data['regimen'] = st.radio(
                "Régimen Tributario:",
                ('Régimen Común / Responsable de IVA',
                 'Régimen Simplificado / No Responsable de IVA',
                 'Gran Contribuyente',
                 'Autorretenedor de Renta',
                 'Otro'),
                key="regimen"
            )
            if form_data['regimen'] == 'Otro':
                form_data['otro_regimen'] = st.text_input("Especifique otro régimen:", key="otro_regimen")
            else:
                form_data['otro_regimen'] = ""
        st.markdown("</div>", unsafe_allow_html=True)


    # --- 3. INFORMACIÓN DE CONTACTOS ---
    with st.container():
        st.markdown("<div class='st-bx' style='margin-top: 20px;'>", unsafe_allow_html=True)
        st.subheader("3. Información de Contactos")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("<h6>Contacto Comercial</h6>", unsafe_allow_html=True)
            st.markdown("_(Para órdenes de compra y cotizaciones)_")
            form_data['comercial_nombre'] = st.text_input("Nombre (Comercial):", key="com_nombre")
            form_data['comercial_cargo'] = st.text_input("Cargo (Comercial):", key="com_cargo")
            form_data['comercial_email'] = st.text_input("Correo Electrónico (Comercial):", key="com_email")
            form_data['comercial_tel'] = st.text_input("Teléfono / Celular (Comercial):", key="com_tel")
        with col2:
            st.markdown("<h6>Contacto para Pagos y Facturación</h6>", unsafe_allow_html=True)
            st.markdown("_(Tesorería / Cartera)_")
            form_data['pagos_nombre'] = st.text_input("Nombre (Pagos):", key="pag_nombre")
            form_data['pagos_cargo'] = st.text_input("Cargo (Pagos):", key="pag_cargo")
            form_data['pagos_email'] = st.text_input("Correo para Radicación de Factura Electrónica:", key="pag_email")
            form_data['pagos_tel'] = st.text_input("Teléfono / Celular (Pagos):", key="pag_tel")
        st.markdown("</div>", unsafe_allow_html=True)


    # --- 4. INFORMACIÓN BANCARIA ---
    with st.container():
        st.markdown("<div class='st-bx' style='margin-top: 20px;'>", unsafe_allow_html=True)
        st.subheader("4. Información Bancaria para Pagos")
        st.warning("La información suministrada debe coincidir exactamente con la certificación bancaria adjunta.")
        col1, col2 = st.columns(2)
        with col1:
            form_data['banco_nombre'] = st.text_input("Nombre del Banco:", key="banco_nombre")
            form_data['banco_titular'] = st.text_input("Titular de la Cuenta:", key="banco_titular")
            form_data['banco_nit_cc'] = st.text_input("NIT / C.C. del Titular:", key="banco_nit")
        with col2:
            form_data['banco_tipo_cuenta'] = st.radio("Tipo de Cuenta:", ('Ahorros', 'Corriente'), key="banco_tipo")
            form_data['banco_numero_cuenta'] = st.text_input("Número de la Cuenta:", key="banco_num")
        st.markdown("</div>", unsafe_allow_html=True)

    # --- 5. POLÍTICAS ---
    with st.expander("5. Políticas y Aceptación del Proveedor (Haga clic para leer)", expanded=False):
        st.markdown("""
        Le agradecemos leer y aceptar nuestras políticas básicas para una relación comercial transparente y efectiva.

        - **Protección de Datos:** El proveedor autoriza a FERREINOX S.A.S. BIC a tratar sus datos personales y comerciales con el fin de gestionar la relación contractual, realizar pagos y enviar comunicaciones, de acuerdo con la Ley 1581 de 2012 y nuestras políticas de tratamiento de datos.
        - **Calidad y Cumplimiento:** El proveedor se compromete a entregar los productos y/o servicios bajo las condiciones de calidad, tiempo y forma acordadas en cada orden de compra o contrato.
        - **Facturación:** Toda factura debe ser emitida a nombre de **FERREINOX S.A.S. BIC** con NIT **900.205.211-8** y enviada al correo electrónico designado para facturación. La factura deberá hacer referencia a una orden de compra o contrato válido para su gestión.
        - **Ética y Transparencia:** El proveedor declara que sus recursos no provienen de actividades ilícitas y se compromete a actuar con ética, honestidad y transparencia en todas sus interacciones comerciales con nuestra empresa, rechazando cualquier práctica de soborno, corrupción o fraude.
        """)

    # --- 6. DOCUMENTOS REQUERIDOS ---
    with st.container():
        st.markdown("<div class='st-bx' style='margin-top: 20px;'>", unsafe_allow_html=True)
        st.subheader("6. Checklist de Documentos Requeridos")
        st.info("Por favor, asegúrese de tener listos los siguientes documentos para enviarlos junto a este formato.")
        form_data['doc_rut'] = st.checkbox("RUT (Registro Único Tributario) actualizado.")
        form_data['doc_camara'] = st.checkbox("Cámara de Comercio con fecha de expedición no mayor a 30 días.")
        form_data['doc_bancaria'] = st.checkbox("Certificación Bancaria con fecha de expedición no mayor a 30 días.")
        form_data['doc_cc_rl'] = st.checkbox("Fotocopia de la Cédula de Ciudadanía del Representante Legal.")
        st.markdown("</div>", unsafe_allow_html=True)


    # --- 7. FIRMA Y ACEPTACIÓN ---
    with st.container():
        st.markdown("<div class='st-bx' style='margin-top: 20px;'>", unsafe_allow_html=True)
        st.subheader("7. Firma y Aceptación")
        st.success("Al diligenciar los siguientes campos, usted certifica la veracidad de la información y acepta las políticas de la empresa.")
        form_data['rl_nombre'] = st.text_input("Nombre Completo del Representante Legal:", key="rl_nombre")
        form_data['rl_cc'] = st.text_input("Cédula de Ciudadanía del Representante Legal:", key="rl_cc")
        st.markdown("</div>", unsafe_allow_html=True)
    
    st.markdown("---")
    
    # --- BOTÓN DE ENVÍO DEL FORMULARIO ---
    submitted = st.form_submit_button("✅ Generar Documentos Diligenciados")

# --- LÓGICA DE PROCESAMIENTO POST-ENVÍO ---
if submitted:
    # Validar que los campos mínimos requeridos estén llenos
    if all([form_data['razon_social'], form_data['nit'], form_data['dv'], form_data['rl_nombre']]):
        st.success("¡Formulario validado! Ya puede descargar sus documentos.")
        
        col1, col2 = st.columns(2)

        # Generar PDF con datos
        pdf_bytes = generate_pdf(form_data)
        pdf_filename = f"Formato_Proveedor_{form_data['razon_social'].replace(' ', '_')}.pdf"
        with col1:
            st.download_button(
                label="📄 Descargar como PDF",
                data=pdf_bytes,
                file_name=pdf_filename,
                mime="application/pdf",
                help="Descarga el formulario completo en formato PDF."
            )

        # Generar Excel con datos
        excel_bytes = generate_excel(form_data)
        excel_filename = f"Datos_Proveedor_{form_data['razon_social'].replace(' ', '_')}.xlsx"
        with col2:
            st.download_button(
                label="📊 Descargar como Excel",
                data=excel_bytes,
                file_name=excel_filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Descarga los datos en una hoja de cálculo para fácil procesamiento."
            )
    else:
        st.error("Por favor, diligencie como mínimo la Razón Social, el NIT (con DV) y el Nombre del Representante Legal para poder generar los documentos.")
