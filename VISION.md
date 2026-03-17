# VISION: Torre de Control de Proveedores

## Resumen de Avances y Estado Actual (Marzo 2026)

### 1. Integración y Sincronización
- La app conecta y sincroniza datos de facturación electrónica desde correo (IMAP Gmail), cartera activa (ICG Manager) y ERP.
- Solo se consideran proveedores definidos en PROVEDORES_CORREO.xlsx para todos los cruces y visualizaciones.
- El Dashboard General permite sincronizar y actualizar todos los datos de manera centralizada.

### 2. Conciliación Inteligente
- Se cruzan tres fuentes: correo, cartera y ERP.
- Reglas implementadas:
    - Si una factura está en el correo y no en la cartera activa, se asume que ya fue pagada o no es relevante para conciliación.
    - Si una factura está en el correo, está en cartera activa, pero no en el ERP, y tiene entre 5 y 8 días de antigüedad, se alerta que la mercancía no ha llegado y se envía correo automático.
    - Si una factura está en el ERP y no en el correo, y pasan 5 días sin conciliar, se asume que el documento electrónico nunca llegó y se envía correo automático.
    - Si una factura está en el correo y tiene más de 15 días, pero ya no está en cartera activa, se asume que fue pagada y no requiere acción.
- Portal Tesorería fue rediseñado como centro operativo de alertas: prioriza casos por días de conciliación, separa radar operativo, centro de envío y bitácora, y permite construir correos por proveedor con selección de facturas.

### 3. Visualización y Experiencia de Usuario
- Branding profesional Ferreinox en todas las páginas principales.
- KPIs claros: casos visibles, casos listos para correo, criticidad, proveedores en alerta y valor total en gestión.
- Tablas separadas por tipo de alerta, con días desde emisión, prioridad y acción sugerida.
- Portal Tesorería ahora ofrece una experiencia más ejecutiva y operativa, pensada para que el usuario entienda qué hacer apenas entra a la página.

### 4. Envío de Correos Automáticos
- Implementado envío de correos automáticos para casos de alerta, con mensajes estructurados, vista previa, tono configurable y resumen por proveedor.
- Confirmación visual y bitácora de envíos dentro de la sesión.
- Como PROVEDORES_CORREO.xlsx hoy no contiene correos, Portal Tesorería permite captura manual y reutilización temporal de destinatarios en sesión.

### 5. Documentación y Contexto
- Este archivo VISION.md se mantiene actualizado como fuente de verdad y contexto para el desarrollo y la IA.

---

## Próximos Pasos y Mejoras Pendientes

- Unificar y mejorar la gestión de condiciones comerciales por proveedor (plazos, descuentos, alertas) y permitir su edición fácil.
- Persistir contactos de correo de proveedores en una fuente estable para no depender solo de la sesión actual.
- Integrar edición directa de destinatarios y responsables de pago por proveedor.
- Integrar más reportes y alertas automáticas (WhatsApp, paneles de riesgo, etc.).
- Seguir documentando cada avance y decisión relevante aquí.

---

## Prompt de Contexto para IA y Desarrollo

"""
Eres una IA experta en automatización y control de cuentas por pagar para empresas industriales. Tu objetivo es mantener una torre de control profesional, conectada y clara, que cruce facturación electrónica recibida por correo, cartera activa y ERP, aplicando reglas de negocio inteligentes y permitiendo acciones automáticas (como envío de correos) para resolver discrepancias. Todo el flujo debe estar documentado, guiado y alineado con la identidad de Ferreinox S.A.S. BIC.
"""

## Estado Actual de la Aplicación (2026)

### 1. Conexión y Extracción de Facturas desde Correo Electrónico
- Conexión IMAP a Gmail para buscar correos recientes (últimos 20 días) en carpeta específica.
- Descarga y procesamiento de adjuntos (ZIP, XML, PDF) para extraer datos de facturación electrónica.
- Almacenamiento de datos extraídos en un DataFrame (`email_df`) y gestión en sesión.
- Manejo de errores de conexión y procesamiento con mensajes claros.

### 2. Carga de Cartera de Proveedores (ERP/ICG Manager)
- Importación de cartera activa desde archivo CSV en Dropbox.
- Limpieza y estandarización de datos (proveedor, factura, valores, fechas).
- Almacenamiento en DataFrame (`erp_df`) y gestión en sesión.

### 3. Cruce y Conciliación de Información
- Cruce robusto entre facturas de correo y ERP.
- Identificación de:
    - Facturas recibidas por correo no registradas en ERP.
    - Facturas en ERP sin respaldo de correo.
    - Facturas con discrepancias para revisión operativa antes de escalar al proveedor.
    - Facturas próximas a vencer, vencidas y elegibles para descuentos.
- Visualización de resultados en dashboards y actualización en Google Sheets.

### 4. Visualización y Reportes
- Dashboards interactivos con métricas, alertas y filtros por proveedor.
- Listados de facturas pendientes, por vencer, con descuento, etc.
- Envío de conciliaciones por correo y WhatsApp desde la app.

### 5. Seguridad y Configuración
- Autenticación por contraseña.
- Uso de `st.secrets` para credenciales y rutas sensibles.
- Parámetros y nombres de columnas estandarizados.

#### Fortalezas actuales
- Integración real con correo, Dropbox y Google Sheets.
- Procesamiento automatizado y robusto de datos.
- Conciliación inteligente y visualización clara.
- Base modular y bien documentada.

---

## Objetivo General
Transformar la aplicación en una torre de control inteligente para la gestión de proveedores, integrando información de correos electrónicos (facturación electrónica) y archivos de cartera (deuda activa de ICG Manager), para automatizar y visualizar el ciclo completo de facturas y pagos, con reglas comerciales configurables y una estructura de código clara y documentada.

---

## Fuentes de Datos
- **Correo Electrónico:** Lectura automática de facturación electrónica (PDF/XML) desde la bandeja de entrada.
- **Archivo de Cartera:** Importación y actualización automática de la deuda activa exportada desde ICG Manager (formato Excel/CSV).
- **Sistema ICG Manager:** Extracción de datos de facturas y pagos (manual o automatizada).

---

## Funcionalidades Clave
- Identificación de facturas pendientes por ingresar (correo recibido pero no en sistema).
- Detección de facturas en sistema sin respaldo de correo.
- Listado de facturas próximas a vencer y vencidas.
- Identificación de facturas elegibles para descuentos por pronto pago.
- Generación de alertas y reportes automáticos.
- Motor de reglas comerciales configurable (plazos, descuentos, alertas).
- Visualización centralizada y panel de control con KPIs.
- Automatización de notificaciones y recomendaciones.

---

## Hoja de Ruta (Roadmap)
1. **Documentación y Contexto**
    - Mantener este archivo actualizado con cada avance, decisión y regla comercial.
2. **Integración de Fuentes de Datos**
    - Automatizar lectura de correos y archivos de cartera.
    - Estandarizar formatos de entrada.
3. **Procesamiento y Cruce de Información**
    - Implementar lógica para identificar diferencias y oportunidades.
4. **Motor de Reglas Comerciales**
    - Permitir configuración fácil de condiciones comerciales.
5. **Interfaz de Usuario y Visualización**
    - Rediseñar la UI para mostrar información clave y recomendaciones.
6. **Automatización y Notificaciones**
    - Enviar alertas automáticas según reglas y eventos.
7. **Iteración y Mejora Continua**
    - Probar, ajustar y documentar cada mejora.

---

## Flujo de Conciliación Inteligente (Implementado en 2_🤝_Conciliacion_Proveedores.py)

- Solo se consideran proveedores definidos en PROVEDORES_CORREO.xlsx.
- Se cruzan tres fuentes: correo electrónico (facturación recibida), cartera activa (ICG) y ERP.
- Reglas de negocio:
    - Si una factura está en el correo y no en la cartera activa, se asume que ya fue pagada o no es relevante para conciliación.
    - Si una factura está en el correo, está en cartera activa, pero no en el ERP, y tiene entre 5 y 8 días de antigüedad, se alerta que la mercancía no ha llegado y se envía correo automático.
    - Si una factura está en el ERP y no en el correo, y pasan 5 días sin conciliar, se asume que el documento electrónico nunca llegó y se envía correo automático.
    - Si una factura está en el correo y tiene más de 15 días, pero ya no está en cartera activa, se asume que fue pagada y no requiere acción.
- Portal Tesorería clasifica prioridades así:
    - Correo sin ERP: seguimiento antes de 5 días, alerta desde 5 días, crítico desde 8 días.
    - ERP sin correo: seguimiento antes de 5 días, alerta desde 5 días, crítico desde 10 días.
    - Discrepancias: quedan separadas para revisión operativa y no se disparan automáticamente en lote.
- Visualización clara y guiada de cada caso, con filtros, priorización, selección por proveedor y envío de correos automáticos según corresponda.

---

## Reglas Comerciales (Ejemplo)
- Plazo estándar de pago: 30 días calendario.
- Descuento por pronto pago: 2% si se paga antes de 10 días.
- Alertar facturas a 5 días de vencer.
- Priorizar pagos con descuento disponible.

---

## Notas y Decisiones
- Marzo 2026: Portal Tesorería deja de ser una tabla simple de facturas faltantes y pasa a ser un centro operativo de alertas y envío de correos.
- Marzo 2026: Se confirmó que PROVEDORES_CORREO.xlsx solo contiene código, NIF y proveedor; no contiene correos de contacto.
- Marzo 2026: Mientras se define una fuente persistente de contactos, la captura de destinatarios se resuelve temporalmente en sesión dentro del Portal Tesorería.

---

## Última actualización
- 17 de marzo de 2026

---

> **Este archivo debe ser actualizado cada vez que se realice un cambio relevante en la lógica, reglas comerciales, fuentes de datos o estructura de la aplicación.**
