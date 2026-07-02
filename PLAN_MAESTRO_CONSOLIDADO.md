# Plan Maestro Consolidado — Super App Pagos Proveedores Ferreinox

> **Este documento reemplaza y consolida** las fases anteriores del Plan_Migracion, ADDENDUM y prompts previos. Es la hoja de ruta operativa completa desde el estado actual (post Tarea 6b) hasta app en producción completa.
>
> Fecha: 2026-07-01. Reglas operativas 1-13 de prompts anteriores siguen vigentes en todas las iteraciones.

---

## A. Objetivos definitivos de la app (confirmados por Diego)

**Objetivo 1** — Conciliación bidireccional inteligente correos ↔ ERP.
**Objetivo 2** — Motor completo de análisis financiero: descuentos por pronto pago, alertas, beneficios capturados vs perdidos.
**Objetivo 3** — Armado de paquetes de pago + exportación a formatos de Bancolombia y Davivienda + notificación a proveedores vía SendGrid.
**Objetivo 4** — Módulo Rebate gerencial ejecutivo para Pintuco, Abracol, Goya con simulador en vivo.
**Filosofía transversal** — Total manejo operativo desde la UI: registro y edición de proveedores, retenciones, descuentos por pronto pago, condiciones comerciales, correos de reporte. Todo con audit trail. Todo a un clic o dos. Nada oculto en código.

---

## B. Features innovadores confirmados (9 de 10)

| # | Feature | Notas |
|---|---|---|
| 1 | Command+K búsqueda global | ⌘K desde cualquier pantalla, busca proveedor / factura / valor / message_id / lote |
| 2 | Detección de facturas duplicadas | Alerta amarilla, opción "es la misma / son distintas" |
| 3 | ~~Anomalías en valores~~ | Configurable por proveedor. Pintuco Colombia excluido por default. Otros proveedores: activo. Toggle en panel de gestión del proveedor |
| 4 | Payment Runway | Efectivo requerido próximos 7/15/30 días en dashboard ejecutivo |
| 5 | Semáforo de salud por proveedor | Score compuesto: puntualidad, captura descuentos, calidad datos, volumen |
| 6 | Simulador de rebate en vivo | Gap al siguiente escalón + ROI marginal + viabilidad estimada |
| 7 | Templates de correo con branding Ferreinox | Nunito + paleta corporativa en el correo al proveedor |
| 8 | Timeline visual por factura | Correo → ERP → conciliación → lote → pago → notificación |
| 9 | Bulk actions con undo 60s | Toast con "Deshacer" para operaciones en masa |
| 10 | Cambio de banco en un clic | Re-exportación del paquete al formato del banco alternativo sin rearmarlo |

---

## C. Iteraciones — ritmo y contenido

Cada iteración es autocontenida y se despliega en producción antes de la siguiente. Diego valida visualmente y aprueba el paso.

### Iteración 1 — Fundamento visual + Conciliación (en curso)
**Deliverables**:
- Shell de la app con paleta Ferreinox y tipografía Nunito.
- Login refinado.
- Dashboard ejecutivo con KPIs base (facturas ingestadas, cartera pendiente/saldada, descuentos capturables).
- Signature element: `PaymentCalendarHeatmap` + `AgingSwatch`.
- Vista de facturas ingestadas (3,531 filas) con filtros y búsqueda.
- Vista de cartera pendiente (116 filas) con matching contra correo.
- Vista de cartera saldada (1,530 filas) con matching contra correo.
- Motor de conciliación con las 4 tabs (conciliadas / correo sin ERP / ERP sin correo / alertas de descuento).
- Vista de proveedores en modo lectura.

**Checkpoints**: 3a → 3f según prompt anterior (`PROMPT_FASE_3_FRONTEND.md`).

**Diego valida contra**: su ERP visualmente. Los 6 conteos de la validación empírica (D.2 del prompt anterior) son la primera pista.

### Iteración 2 — Módulo Rebate ejecutivo (prioridad alta por uso diario de gerencia)

**Contexto crítico**: gerencia revisa esta pantalla a diario. Debe ser espectacular, limpia, muy clara, con análisis a un clic. Es el corazón de la vista gerencial.

**Deliverables por proveedor (Pintuco, Abracol, Goya)**:
- **Página dedicada** para cada uno: `/rebate/pintuco`, `/rebate/abracol`, `/rebate/goya`.
- Cada página con la misma anatomía visual, adaptada al ciclo del proveedor (mensual+trimestral Pintuco, bimestral Abracol, semestral Goya).

**Cálculos** (portados de la lógica actual de Streamlit `Rebate_Pintuco.py`, movidos a SQL/vistas):
- Escala lograda por periodo (con corte del 88% de valor neto para Pintuco por bono estacionalidad).
- Recomposición de 9 meses (Pintuco).
- Cálculo por tramos de crecimiento 20/30/40/50% (Goya).
- Bono de estacionalidad con fecha de corte configurable.

**Presupuestos editables**: pantalla admin/comercial `/rebate/pintuco/presupuestos` (misma para Abracol/Goya). Edita las metas por periodo con versionado (valid_from / valid_to). No más presupuestos hardcodeados en Python.

**Vista gerencia**: mismo layout pero sin edición de presupuestos y sin el simulador (solo consulta). Puede navegar entre Pintuco/Abracol/Goya con tabs superiores.

**Diego valida contra**: comparativo mental con la Streamlit actual de rebate que ya conoce. Debe verse mejor y más rápido.

### Iteración 3 — Gestión completa de proveedores

**Objetivo**: eliminar las limitaciones de Streamlit — inscribir proveedores, editar descuentos, retenciones, condiciones, correos, todo desde la UI.

**Deliverables**:

1. **`/proveedores/nuevo`** — Formulario de inscripción de proveedor nuevo (nombre normalizado, NIF, emails, contactos, condiciones comerciales, alias de conciliación, toggle de detección de anomalías).
2. **`/proveedores/[id]`** — Perfil completo: header con semáforo de salud, KPIs (facturado 12m, días de pago, descuentos capturados/perdidos, concentración), timeline de facturas, tabs (facturas, descuentos, retenciones, condiciones, correos/contactos, historial de cambios).
3. **Semáforo de salud (feature #5)**: score compuesto — puntualidad (30), captura de descuentos (30), calidad de datos (20), volumen (20). Labels: Excelente (85+), Bueno (70-84), Aceptable (50-69), Requiere atención (<50).
4. **DDL nuevo**: `providers.discount_rule`, `providers.retention_rule`, `providers.provider_alias`, columnas nuevas en `providers.provider` (plazo_pago_dias, forma_pago, limite_credito, dia_corte_pagos, anomaly_detection), `audit.provider_history`.

Semilla inicial: portar `DISCOUNT_PROVIDERS` de `treasury_core.py` a `providers.discount_rule` con `valid_from = '2026-01-01'`. Retenciones se cargan manualmente por Diego con contabilidad después del deploy.

### Iteración 4 — Motor de análisis inteligente + búsqueda global

**Deliverables**:
1. Command+K global (feature #1) con búsqueda difusa e índices GIN.
2. Detección de duplicados (feature #2): trigger + banner de resolución.
3. Detección de anomalías (feature #3): job del worker, media+2σ por proveedor, Pintuco excluido.
4. Payment Runway (feature #4): efectivo requerido 7/15/30 días.
5. Timeline visual por factura (feature #8): recibida → ERP → conciliada → lote → pagada → notificada.
6. Motor de alertas de descuento con CTA "Incluir en lote".

### Iteración 5 — Armado de pagos + exportación bancaria + notificaciones

**Núcleo operativo. Requiere formatos exactos de Bancolombia y Davivienda antes de iniciar.**

**Deliverables**:
1. `/planificador`: armado de lotes, bulk actions con undo 60s, panel "lote en construcción".
2. `/lotes`: historial de lotes por estado.
3. Cambio de banco en un clic (feature #10): re-exportación sin rearmar.
4. Exportación a bancos: `lib/bank-exports/bancolombia.ts` y `davivienda.ts`.
5. Notificación a proveedores vía SendGrid (feature #7) con branding Ferreinox, registro en `treasury.email_log`.
6. DDL nuevo: `treasury.bank_export_log`.

### Iteración 6 — PWA + Web Push + Pulido final

**Deliverables**:
1. PWA instalable (manifest, service worker, cache offline mínimo).
2. Web Push (tabla `auth.push_subscription` ya existe): pago de lote, anomalía detectada, cambio de escalón de rebate, worker caído.
3. Centro de notificaciones in-app (campana en topbar).
4. Pulido final: accesibilidad AA, responsive 375px, eliminar `/api/debug/postgrest-test`, backups automatizados a DO Spaces.

---

## D. Criterios transversales de calidad (todas las iteraciones)

- Look/feel según `GUIA_DISENO_VISUAL.md` — paleta Ferreinox, Nunito, Inter tabular, signature de muestra de pintura.
- Todos los estados diseñados (happy, loading skeleton, empty invitacional, error accionable).
- Formato colombiano: `$ 1.234.567,00`, `15 jul 2026`.
- Responsive hasta mobile 375px sin romperse.
- Accesibilidad AA: contraste, focus visible, keyboard navigation.
- RLS activa: cada request lleva el JWT del usuario, PostgREST ejecuta bajo la identidad correcta.
- Verificación empírica post-escritura (regla 12): el log del código no cuenta como verificación.
- Un click al valor: información importante accesible en máximo 2 clicks.

---

## E. Ritmo de trabajo estimado

- Iteración 1 (fundamento + conciliación): ~1-2 días.
- Iteración 2 (rebate): ~1-2 días.
- Iteración 3 (proveedores completo): ~2-3 días.
- Iteración 4 (análisis + Command+K + timeline + payment runway): ~2-3 días.
- Iteración 5 (armado + exportación + notificaciones): ~2-3 días (más los formatos de bancos que Diego debe proveer).
- Iteración 6 (PWA + push + pulido): ~1-2 días.

Total estimado: 10-15 días de trabajo activo. Depende de aprobación de checkpoints a ritmo, formatos bancarios claros para Iteración 5, y ausencia de bugs de fondo que requieran refactorización.

---

## F. Bloqueantes de progreso — pedirle a Diego cuando toque

- **Iteración 2**: confirmar presupuestos vigentes de Pintuco/Abracol/Goya para semilla inicial.
- **Iteración 3**: reglas actuales de retención por proveedor (Diego + contabilidad).
- **Iteración 4**: definir umbral de "descuento por perder" (¿5 días? ¿3 días?).
- **Iteración 5** (Bancos): especificaciones de archivo de Bancolombia y Davivienda, ejemplos reales si existen.
- **Iteración 5** (SendGrid): remitente autorizado, template base si existe, dominio verificado.
- **Iteración 6** (PWA): iconos oficiales Ferreinox (192px, 512px, 180px iOS).

---

## G. Estado actual y avance

**Estado post Tarea 6b (verificado empíricamente)**:
- Postgres cluster: base `pagos_proveedores` con schema completo, RLS activa, migraciones 001-013 aplicadas (incluye invoice_key real en erp_pending/erp_paid y vistas de conciliación).
- PostgREST dedicado desplegado.
- Worker Python en producción, ingestando IMAP + Dropbox cada 5 min.
- 3,531 facturas ingestadas, 116 pendientes ERP, 1,530 saldadas ERP, 1,363 conciliadas.
- Next.js shell con login funcional.

**Checkpoint 3a — aprobado por Diego** (2026-07-02): shell visual, login rediseñado, dashboard vacío con estructura de la sección 7 de la guía, desplegado en `proveedores.ferreinox.co`.

**Próximo paso concreto**: Checkpoint 3b — conectar los 4 KPIs y el `PaymentCalendarHeatmap` a datos reales vía PostgREST.

**Referencias operativas para Claude Code**:
- `GUIA_DISENO_VISUAL.md` (repo `proveedores_pagos`) es contrato visual — no negociable.
- Reglas operativas 1-13 vigentes en todas las iteraciones.
- Regla 12 (verificación post-escritura) crítica en iteraciones 3 y 5 donde hay muchas escrituras.

---

**Fin del plan maestro.** Este documento es la fuente de verdad para el resto del proyecto. Cualquier feature no listado aquí que se proponga a mitad del camino se agrega como fast-follow post-MVP, no se cuela en la iteración en curso.
