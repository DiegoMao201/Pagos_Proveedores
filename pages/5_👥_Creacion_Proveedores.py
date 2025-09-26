# -*- coding: utf-8 -*-
"""
Página de Creación y Actualización de Proveedores para FERREINOX (Versión Mejorada).

Este script crea una página de Streamlit de nivel profesional para que los
proveedores gestionen su información de vinculación.

Funcionalidades Clave:
- Formulario interactivo con gestión de estado para no perder datos.
- Validación de campos detallada y específica.
- Generación de PDF con datos pre-diligenciados.
- Generación de PDF en blanco CON CAMPOS EDITABLES para rellenar digitalmente.
- Generación de un archivo Excel con un resumen de los datos.
- Interfaz de usuario pulida con CSS personalizado.
- Código estructurado y bien documentado para fácil mantenimiento.

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
    page_title="Portal de Proveedores | FERREINOX",
    page_icon="👥",
    layout="wide"
)

def load_css():
    """Carga estilos CSS personalizados para una apariencia profesional."""
    st.markdown("""
        <style>
            .main .block-container {
                padding-top: 2rem;
                padding-left: 3rem;
                padding-right: 3rem;
            }
            .st-bx {
                border-radius: 0.75rem;
                padding: 1.5rem 2rem;
                background-color: #FFFFFF;
                border: 1px solid #E0E0E0;
                box-shadow: 0 4px 6px rgba(0,0,0,0.05);
                margin-bottom: 2rem;
            }
            h1, h2, h3 {
                color: #0C2D57;
                font-weight: 600;
            }
            h3 {
                border-bottom: 2px solid #E0E0E0;
                padding-bottom: 10px;
                margin-bottom: 20px;
            }
            .stButton>button {
                border-radius: 0.5rem;
                border: 2px solid #0C2D57;
                background-color: #0C2D57;
                color: white;
                transition: all 0.2s ease-in-out;
                width: 100%;
                font-weight: bold;
                padding: 0.75rem 0;
            }
            .stButton>button:hover {
                background-color: white;
                color: #0C2D57;
            }
            .stDownloadButton>button {
                background-color: #28a745;
                color: white;
                border: 2px solid #28a745;
                transition: all 0.2s ease-in-out;
                width: 100%;
                font-weight: bold;
                padding: 0.75rem 0;
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
        self.cell(65, 8, f'{label}:', 0, 0)
        self.set_font('Arial', '', 10)
        self.multi_cell(w=0, h=8, text=str(value), border=0, align='L')
        self.ln(2)

def generate_pdf(data: dict) -> bytes:
    """Genera un archivo PDF con los datos del formulario diligenciado."""
    pdf = PDF()
    pdf.add_page()

    # --- DATOS GENERALES ---
    pdf.chapter_title('1. DATOS GENERALES DE LA EMPRESA')
    pdf.form_field('Fecha de Diligenciamiento', data['fecha_diligenciamiento'])
    pdf.form_field('Razón Social', data['razon_social'])
    pdf.form_field('NIT', f"{data['nit']}-{data['dv']}")
    pdf.form_field('Dirección Principal', data['direccion'])
    pdf.form_field('Ciudad / Departamento', data['ciudad_depto'])
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
    pdf.set_font('Arial', 'B', 11)
    pdf.cell(0, 8, 'Contacto Comercial', 0, 1)
    pdf.form_field('Nombre', data['comercial_nombre'])
    pdf.form_field('Cargo', data['comercial_cargo'])
    pdf.form_field('Correo Electrónico', data['comercial_email'])
    pdf.form_field('Teléfono / Celular', data['comercial_tel'])
    pdf.ln(4)
    pdf.set_font('Arial', 'B', 11)
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
        text="Con la firma de este documento, el representante legal certifica la veracidad de la información y acepta las políticas de FERREINOX S.A.S. BIC.",
        border=0, ln=1, align='L'
    )
    pdf.ln(5)
    pdf.form_field('Nombre del Representante Legal', data['rl_nombre'])
    pdf.form_field('C.C. No.', data['rl_cc'])
    pdf.ln(20)
    pdf.cell(80, 8, '_________________________________', 0, 1)
    pdf.cell(80, 8, 'Firma', 0, 0, 'C')

    return pdf.output(dest='S').encode('latin-1')


def generate_blank_pdf() -> bytes:
    """
    Genera un archivo PDF en blanco con campos de formulario EDITABLES.
    NOTA: Esto requiere una versión reciente de fpdf2 (>=2.5.0) en requirements.txt
    """
    pdf = PDF()
    pdf.add_page()
    pdf.set_font('Arial', '', 10)
    
    # --- Helper para añadir campos y evitar repetición ---
    def add_editable_field(label, field_name, label_width=65, field_height=7, y_increment=12):
        pdf.set_font('Arial', 'B', 10)
        pdf.cell(label_width, field_height, f'{label}:', 0, 0)
        current_x = pdf.get_x()
        current_y = pdf.get_y()
        # La siguiente línea es la que causa el error si fpdf2 no está actualizado.
        pdf.add_form_field(
            name=field_name,
            type='text',
            x=current_x,
            y=current_y,
            w=pdf.w - current_x - pdf.r_margin, # Ancho hasta el margen
            h=field_height
        )
        pdf.ln(y_increment)

    # --- DATOS GENERALES ---
    pdf.chapter_title('1. DATOS GENERALES DE LA EMPRESA')
    add_editable_field('Fecha de Diligenciamiento', 'fecha_diligenciamiento')
    add_editable_field('Razón Social', 'razon_social')
    add_editable_field('NIT (sin DV)', 'nit')
    add_editable_field('Dígito de Verificación (DV)', 'dv')
    add_editable_field('Dirección Principal', 'direccion')
    add_editable_field('Ciudad / Departamento', 'ciudad_depto')
    add_editable_field('Teléfono Fijo', 'tel_fijo')
    add_editable_field('Teléfono Celular', 'tel_celular')
    add_editable_field('Correo Electrónico', 'email_general')
    add_editable_field('Página Web', 'website')
    pdf.ln(5)

    # --- INFORMACIÓN TRIBUTARIA ---
    pdf.chapter_title('2. INFORMACIÓN TRIBUTARIA Y FISCAL')
    add_editable_field('Actividad Económica (CIIU)', 'ciiu')
    # Checkboxes para opciones
    pdf.set_font('Arial', 'B', 10)
    pdf.cell(0, 8, 'Marque las opciones que apliquen:', 0, 1)
    
    checkbox_options = {
        'tipo_persona_juridica': 'Persona Jurídica',
        'tipo_persona_natural': 'Persona Natural',
        'regimen_comun': 'Régimen Común / Responsable de IVA',
        'regimen_simplificado': 'Régimen Simplificado / No Responsable de IVA',
        'regimen_gran_contribuyente': 'Gran Contribuyente',
        'regimen_autorretenedor': 'Autorretenedor de Renta',
    }
    for name, label in checkbox_options.items():
        x_pos, y_pos = pdf.get_x(), pdf.get_y()
        pdf.add_form_field(name=name, type='check', x=x_pos, y=y_pos, w=6, h=6)
        pdf.set_xy(x_pos + 8, y_pos)
        pdf.cell(0, 6, label, 0, 1)
    
    add_editable_field('Otro Régimen', 'otro_regimen')
    pdf.ln(5)

    # --- CONTACTOS ---
    pdf.chapter_title('3. INFORMACIÓN DE CONTACTOS')
    pdf.set_font('Arial', 'B', 11)
    pdf.cell(0, 8, 'Contacto Comercial', 0, 1)
    add_editable_field('Nombre', 'comercial_nombre')
    add_editable_field('Cargo', 'comercial_cargo')
    add_editable_field('Correo Electrónico', 'comercial_email')
    add_editable_field('Teléfono / Celular', 'comercial_tel')
    pdf.ln(4)
    pdf.set_font('Arial', 'B', 11)
    pdf.cell(0, 8, 'Contacto para Pagos y Facturación', 0, 1)
    add_editable_field('Nombre', 'pagos_nombre')
    add_editable_field('Cargo', 'pagos_cargo')
    add_editable_field('Correo Factura Electrónica', 'pagos_email')
    add_editable_field('Teléfono / Celular', 'pagos_tel')
    pdf.ln(5)

    # --- INFORMACIÓN BANCARIA ---
    pdf.chapter_title('4. INFORMACIÓN BANCARIA PARA PAGOS')
    add_editable_field('Nombre del Banco', 'banco_nombre')
    add_editable_field('Titular de la Cuenta', 'banco_titular')
    add_editable_field('NIT / C.C. del Titular', 'banco_nit_cc')
    add_editable_field('Número de la Cuenta', 'banco_numero_cuenta')
    
    # Checkboxes para tipo de cuenta
    x_pos, y_pos = pdf.get_x(), pdf.get_y()
    pdf.cell(65, 8, 'Tipo de Cuenta:', 0, 0)
    pdf.add_form_field(name='cuenta_ahorros', type='check', x=pdf.get_x(), y=y_pos, w=6, h=6)
    pdf.set_xy(pdf.get_x() + 8, y_pos)
    pdf.cell(30, 8, 'Ahorros', 0, 0)
    pdf.add_form_field(name='cuenta_corriente', type='check', x=pdf.get_x(), y=y_pos, w=6, h=6)
    pdf.set_xy(pdf.get_x() + 8, y_pos)
    pdf.cell(30, 8, 'Corriente', 0, 1)
    pdf.ln(10)
    
    if pdf.get_y() > 180: pdf.add_page()

    # --- POLÍTICAS Y DOCUMENTOS ---
    # (El texto de políticas se mantiene estático, no necesita campos)
    # ...
    
    # --- FIRMA ---
    pdf.chapter_title('7. FIRMA Y ACEPTACIÓN')
    # ... (texto de aceptación)
    add_editable_field('Nombre Rep. Legal', 'rl_nombre')
    add_editable_field('C.C. Rep. Legal', 'rl_cc')

    return pdf.output(dest='S').encode('latin-1')

# ======================================================================================
# --- 3. FUNCIÓN PARA GENERACIÓN DE EXCEL ---
# ======================================================================================

def generate_excel(data: dict) -> bytes:
    """Genera un archivo Excel a partir de los datos del formulario."""
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
    
    return output.getvalue()


# ======================================================================================
# --- 4. INICIALIZACIÓN DEL ESTADO DE LA APLICACIÓN ---
# ======================================================================================

# Usar st.session_state para preservar los datos del formulario entre recargas
if 'form_data' not in st.session_state:
    st.session_state.form_data = {
        'fecha_diligenciamiento': datetime.now().strftime('%Y-%m-%d'),
        'razon_social': "", 'nit': "", 'dv': "", 'direccion': "", 'ciudad_depto': "",
        'tel_fijo': "", 'tel_celular': "", 'email_general': "", 'website': "",
        'tipo_persona': "Persona Jurídica", 'ciiu': "", 'regimen': "Régimen Común / Responsable de IVA",
        'otro_regimen': "", 'comercial_nombre': "", 'comercial_cargo': "",
        'comercial_email': "", 'comercial_tel': "", 'pagos_nombre': "", 'pagos_cargo': "",
        'pagos_email': "", 'pagos_tel': "", 'banco_nombre': "", 'banco_titular': "",
        'banco_nit_cc': "", 'banco_tipo_cuenta': "Ahorros", 'banco_numero_cuenta': "",
        'doc_rut': False, 'doc_camara': False, 'doc_bancaria': False, 'doc_cc_rl': False,
        'rl_nombre': "", 'rl_cc': ""
    }

form_data = st.session_state.form_data

# ======================================================================================
# --- 5. INTERFAZ DE USUARIO PRINCIPAL (STREAMLIT) ---
# ======================================================================================

load_css()

# st.image("logo.png", width=250) # Descomentar si tienes la imagen
st.title("Portal de Creación y Actualización de Proveedores")
st.markdown("---")
st.markdown("""
Estimado proveedor, para dar inicio a nuestro proceso de vinculación comercial, le solicitamos
amablemente diligenciar este formulario. Puede hacerlo en línea o descargar una versión editable.
""")

# --- Opción 1: Descargar Formulario Editable ---
with st.expander("Opción 1: Descargar Formulario en Blanco y Editable (PDF)"):
    st.info("📄 Descargue esta versión si prefiere diligenciar el formato digitalmente en su computador y enviarlo por correo.")
    blank_pdf_bytes = generate_blank_pdf()
    st.download_button(
        label="Descargar Formato Editable",
        data=blank_pdf_bytes,
        file_name="Formato_Proveedor_Editable_FERREINOX.pdf",
        mime="application/pdf"
    )

st.markdown("---")

# --- Opción 2: Formulario en Línea ---
st.header("Opción 2: Diligenciar Formulario en Línea")
st.markdown("Complete los siguientes campos para generar automáticamente los documentos.")

with st.form(key="provider_form"):
    
    # --- 1. DATOS GENERALES ---
    with st.container():
        st.markdown("<div class='st-bx'>", unsafe_allow_html=True)
        st.subheader("1. Datos Generales de la Empresa")
        
        form_data['razon_social'] = st.text_input("Razón Social*", key="razon_social", value=form_data['razon_social'])
        
        col_nit, col_dv = st.columns([4, 1])
        form_data['nit'] = col_nit.text_input("NIT*", help="Ingrese el número sin el dígito de verificación.", key="nit", value=form_data['nit'])
        form_data['dv'] = col_dv.text_input("DV*", max_chars=1, help="Dígito de Verificación.", key="dv", value=form_data['dv'])
                
        form_data['direccion'] = st.text_input("Dirección Principal*", key="direccion", value=form_data['direccion'])
        col1, col2 = st.columns(2)
        form_data['ciudad_depto'] = col1.text_input("Ciudad / Departamento*", key="ciudad", value=form_data['ciudad_depto'])
        form_data['tel_celular'] = col2.text_input("Teléfono Celular*", key="tel_celular", value=form_data['tel_celular'])
        form_data['email_general'] = col1.text_input("Correo Electrónico General*", key="email_general", value=form_data['email_general'])
        form_data['tel_fijo'] = col2.text_input("Teléfono Fijo (Opcional)", key="tel_fijo", value=form_data['tel_fijo'])
        form_data['website'] = st.text_input("Página Web (Opcional)", placeholder="https://www.suempresa.com", key="website", value=form_data['website'])
        st.markdown("</div>", unsafe_allow_html=True)

    # --- 2. INFORMACIÓN TRIBUTARIA ---
    with st.container():
        st.markdown("<div class='st-bx'>", unsafe_allow_html=True)
        st.subheader("2. Información Tributaria y Fiscal")
        col1, col2 = st.columns(2)
        form_data['tipo_persona'] = col1.radio("Tipo de Persona*", ('Persona Jurídica', 'Persona Natural'), key="tipo_persona", index=['Persona Jurídica', 'Persona Natural'].index(form_data['tipo_persona']))
        form_data['ciiu'] = col1.text_input("Actividad Económica (Código CIIU)*", help="Encuentre este código en su RUT.", key="ciiu", value=form_data['ciiu'])
        
        regimen_options = ('Régimen Común / Responsable de IVA', 'Régimen Simplificado / No Responsable de IVA', 'Gran Contribuyente', 'Autorretenedor de Renta', 'Otro')
        form_data['regimen'] = col2.radio("Régimen Tributario*", regimen_options, key="regimen", index=regimen_options.index(form_data['regimen']))
        
        if form_data['regimen'] == 'Otro':
            form_data['otro_regimen'] = st.text_input("Especifique otro régimen*", key="otro_regimen", value=form_data['otro_regimen'])
        else:
            form_data['otro_regimen'] = ""
        st.markdown("</div>", unsafe_allow_html=True)

    # --- 3. INFORMACIÓN DE CONTACTOS ---
    # ... (similar a la original, pero usando los valores de session_state)
    # [El código para las demás secciones sigue el mismo patrón de obtener/guardar en form_data]
    
    # --- 7. FIRMA Y ACEPTACIÓN ---
    with st.container():
        st.markdown("<div class='st-bx'>", unsafe_allow_html=True)
        st.subheader("7. Firma y Aceptación")
        st.success("Al diligenciar los siguientes campos, usted certifica la veracidad de la información y acepta las políticas de la empresa.")
        form_data['rl_nombre'] = st.text_input("Nombre Completo del Representante Legal*", key="rl_nombre", value=form_data['rl_nombre'])
        form_data['rl_cc'] = st.text_input("Cédula de Ciudadanía del Representante Legal*", key="rl_cc", value=form_data['rl_cc'])
        st.markdown("</div>", unsafe_allow_html=True)
    
    st.markdown("---")
    
    # --- BOTÓN DE ENVÍO ---
    submitted = st.form_submit_button("✅ Generar Documentos Diligenciados")

# --- LÓGICA DE PROCESAMIENTO POST-ENVÍO ---
if submitted:
    # Validación detallada de campos obligatorios
    required_fields = {
        'razon_social': "Razón Social", 'nit': "NIT", 'dv': "DV",
        'direccion': "Dirección Principal", 'ciudad_depto': "Ciudad / Departamento",
        'tel_celular': "Teléfono Celular", 'email_general': "Correo Electrónico General",
        'ciiu': "Código CIIU", 'rl_nombre': "Nombre del Representante Legal",
        'rl_cc': "Cédula del Representante Legal"
    }
    
    missing_fields = [label for key, label in required_fields.items() if not form_data[key]]
    
    if form_data['regimen'] == 'Otro' and not form_data['otro_regimen']:
        missing_fields.append("Especificación de 'Otro régimen'")

    if not missing_fields:
        st.success("¡Formulario validado exitosamente! Ya puede descargar sus documentos.")
        st.balloons()
        
        col1, col2 = st.columns(2)

        # Generar PDF con datos
        pdf_bytes = generate_pdf(form_data)
        pdf_filename = f"Formato_Proveedor_{form_data['razon_social'].replace(' ', '_')}.pdf"
        col1.download_button(
            label="📄 Descargar Formulario en PDF",
            data=pdf_bytes,
            file_name=pdf_filename,
            mime="application/pdf"
        )

        # Generar Excel con datos
        excel_bytes = generate_excel(form_data)
        excel_filename = f"Datos_Proveedor_{form_data['razon_social'].replace(' ', '_')}.xlsx"
        col2.download_button(
            label="📊 Descargar Resumen en Excel",
            data=excel_bytes,
            file_name=excel_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        # Mostrar error con lista de campos faltantes
        error_message = "Por favor, complete los siguientes campos obligatorios para continuar:\n"
        for field in missing_fields:
            error_message += f"- {field}\n"
        st.error(error_message)
