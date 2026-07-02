# Plan de Migración: Streamlit → Next.js + PostgREST

> **Estado:** Documento de planeación. No se ha tocado ni desplegado nada todavío.
> La app Streamlit actual sigue siendo la fuente de verdad y queda 100% intacta.
> Este plan es la hoja de ruta para construir la versión Next.js/PostgREST en paralelo.
>
> **Actualización 2026-07-01:** se agregó la **Parte 2 (Addendum)** al final de este documento, con revisiones técnicas y features MVP adicionales. Donde la Parte 2 contradice a la Parte 1 (roles, infraestructura de Postgres/PostgREST, DDL), **la Parte 2 manda** — así lo indica ella misma. Léelas ambas antes de tocar código.
>
> **Identidad visual:** el brief de diseño completo (colores, tipografía, componentes, signature element, copy) vive en `GUIA_DISENO_VISUAL.md` dentro del repo `proveedores_pagos`. Es contrato visual obligatorio para toda la Fase 3/4 — cualquier desviación debe justificarse contra un principio de ese documento, no por gusto propio.

Fecha de investigación: 2026-07-01.

---

## 1. Resumen ejecutivo

- La app actual (`Pagos_Proveedores`, Streamlit) tiene 3 dominios funcionales: **Tesorería/Conciliación** (`app.py`, `Portal_Tesoreria`, `Planificador_de_Pagos`), **Rebate de proveedores estratégicos** (`Rebate_Pintuco`, que cubre Pintuco/Abracol/Goya) y **Calidad de datos de proveedores** (`Creacion_Proveedores`).
- Hoy el almacenamiento es **Google Sheets** (8 hojas vía `gspread`), la ingesta es **IMAP Gmail** + **Dropbox CSV**, y el envío de correo usa **SendGrid**.
- Investigué el servidor de Coolify (`panel.datovatenexuspro.com`, droplet DigitalOcean `servidor-nexuspro`, `nyc1`) con la API y por SSH (`coolify-server`). Es un **droplet compartido de 2 vCPU / 8GB RAM** que ya hospeda ~14 proyectos (Cotizador Ferreinox, Bigotes y Paticas, Begranda, Optiferre MVP, etc.).
- **Hallazgo crítico:** el servidor está bajo presión severa de memoria — swap al 100% (4.0GB/4.0GB), solo ~180MB de RAM libre, load average 6.7 en 2 CPUs. La causa principal identificada es un contenedor ajeno a este proyecto (`begranda-web`, del proyecto "Agente Pedidos Begranda") consumiendo 2.4GB de RAM y 92% de CPU. **No toqué ese contenedor** (no es parte de este proyecto), pero es un riesgo que condiciona dónde desplegar la nueva app.
- Ya existe en ese mismo servidor un proyecto (`Optiferre / Servicio al cliente Ferreinox`) corriendo exactamente el patrón que buscamos: **Next.js (frontend) + backend Node + PostgREST + Postgres**, todo vía Coolify. Es la prueba de que el stack objetivo es viable ahí — el problema no es de compatibilidad, es de capacidad disponible en este momento.
- Recomendación de arquitectura (detalle en la sección 4): **Next.js 15 (App Router) + Postgres + PostgREST**, manteniendo la lógica de ingesta de correo/Dropbox como un **worker Python en contenedor separado** (cron), reutilizando el parsing UBL/XML que ya está probado en producción, en vez de reescribirlo desde cero en Node. Esto reduce drásticamente el riesgo de la migración.
- Espacio en disco: sí hay (35GB libres en `/`, 44GB libres en `/mnt/docker_data`). El limitante real es **RAM/CPU**, no disco.

---

## 2. Infraestructura actual (hallazgos de la investigación)

### 2.1 Servidor Coolify

| Recurso | Valor |
|---|---|
| Host | `servidor-nexuspro` (droplet DigitalOcean, id `541027375`, región `nyc1`) |
| CPU | 2 vCPU (`DO-Premium-AMD`) |
| RAM | 7.8 GiB total — **7.2 GiB usados, ~180MB libres** |
| Swap | 4 GiB — **100% usado** (síntoma de sobrecarga sostenida) |
| Load average | 6.76 / 7.42 / 7.28 (para 2 CPUs — muy por encima de lo saludable) |
| Disco `/` | 48G, 14G usado, **35G libres** (28%) |
| Disco `/mnt/docker_data` | 100G, 51G usado, **44G libres** (54%) |
| Coolify | v4.1.2, un solo servidor registrado (`localhost` = el mismo droplet) |

### 2.2 Qué corre ahí ahora mismo (relevante)

- ~14 proyectos Coolify: Cotizador Ferreinox, Servinet HR IA, Alsum, Actualizar Datos Ferreinox, CRM Ferreinox, MVP PostgreSQL, Optiferre Frontend MVP, Binance Trading, Bigotes y Paticas, Agente Pedidos Begranda, entre otros.
- **Ya hay un stack de referencia probado**: proyecto `h0ks004ss84g4oc8cs4go4k0` ("Servicio al cliente Ferreinox") corre `frontend` (Next.js/Node, 267MB RAM), `backend` (Node, 10MB RAM), `postgrest` (7.8MB RAM) y `db` (Postgres 15, 17MB RAM). Confirma que Coolify soporta este patrón exacto de forma nativa y con huella de memoria muy baja.
- **Causa raíz de la presión de memoria:** `begranda-web` usa 2.4 GiB de RAM y 92% de CPU sostenido — esto no es de este proyecto y no lo toqué, pero es la explicación de por qué el servidor está al límite. Vale la pena que se revise aparte (posible memory leak).
- Dominios ya activos bajo `*.datovatenexuspro.com` vía Traefik (proxy de Coolify) con TLS automático — se puede usar el mismo patrón para el nuevo dominio, ej. `proveedores.datovatenexuspro.com`.

### 2.3 Conclusión de capacidad

- **Sí hay espacio en disco** de sobra para Postgres + PostgREST + Next.js + worker (footprint esperado total: ~300–500MB de RAM según el stack gemelo que ya corre ahí).
- **No hay margen de RAM disponible hoy** para añadir nada de forma segura sin: (a) resolver el consumo de `begranda-web`, o (b) desplegar esta app en un **droplet nuevo y dedicado**.
- Dado que esta app maneja pagos a proveedores y datos financieros, mi recomendación inicial era un droplet nuevo dedicado. **Decisión confirmada (sección 10): se reutiliza el servidor actual.** Esto es viable, pero exige primero acotar el consumo de `begranda-web` y desplegar los contenedores nuevos con límites explícitos de memoria/CPU — ver sección 10.1 para el detalle del prerrequisito.

---

## 3. Inventario funcional actual (lo que hay que preservar)

### 3.1 `app.py` — Centro ejecutivo / Dashboard general
- Autenticación por contraseña compartida (`st.session_state`).
- Sincronización maestra (`sync_treasury_data`): botón "Actualizar ahora" + opción "Reconstruir foto completa".
- KPIs: pendiente por pagar, pendiente sin correo, ahorro capturable, proveedores monitoreados, alertas 48h.
- Salud de fuentes (conteos por fuente: Dropbox pendiente, histórico correo, maestro facturas, trazabilidad).
- Foco operativo (top proveedores por riesgo/ahorro) y trazabilidad reciente (lotes y correos).
- Vista maestra en 4 pestañas: Qué pagar ya / Correo sin reflejo ERP / No conciliado / Conciliado.

### 3.2 `common/treasury_core.py` — el motor (2611 líneas, el corazón del sistema)
- **Ingesta de correo (IMAP Gmail):** conecta a `imap.gmail.com`, carpeta `TFHKA/Recepcion/Descargados`, descarga adjuntos ZIP/XML, parsea UBL (facturas y notas crédito/débito) extrayendo proveedor, número, fechas, valores base/IVA/total, referencias a documentos relacionados.
- **Ingesta de cartera ERP (Dropbox):** dos CSVs (`/data/Proveedores.csv` = pendiente, `/data/cartera_saldada.csv` = saldada), parseados con separador `{`.
- **Catálogo de proveedores:** `PROVEDORES_CORREO.xlsx` (código, NIF, proveedor) + alias de normalización (`SUPPLIER_ALIASES`) para unificar nombres entre correo/ERP.
- **Conciliación inteligente:** cruce por `invoice_key` (proveedor normalizado + número de documento normalizado, con candidatos y variantes), aplicando las reglas de negocio documentadas en `VISION.md` (correo sin ERP, ERP sin correo, ventanas de días, etc.).
- **Motor de descuentos por pronto pago:** reglas por proveedor (`DISCOUNT_PROVIDERS`) con tramos de días/tasa (ej. Pintuco 15d→3%, 30d→2%).
- **Conciliación manual:** registro de resoluciones manuales cuando el cruce automático no basta.
- **Exclusión de facturas:** registro/activación de exclusiones de facturas específicas (usado también por Rebate).
- **Plan de pagos y lotes:** arma propuesta de pago, agrupa en lotes con responsable/fecha, registra en Sheets.
- **Envío de correo (SendGrid):** arma HTML de correo de pago por proveedor y registra bitácora de envío.
- **Persistencia:** todo hoy vive en 8 hojas de Google Sheets (`Maestro_Proveedores`, `Historial_Correo_Proveedores`, `Maestro_Facturas`, `Propuesta_Pagos`, `Lotes_Pago`, `Historial_Correos`, `Conciliacion_Manual`, `Facturas_Excluidas`).
- **Exportación a Excel** con formato corporativo (openpyxl).

### 3.3 `pages/3_💰_Portal_Tesoreria.py` (1139 líneas)
- Centro operativo de alertas: pestañas Resumen / Pagar / Correo / Nota crédito / No conciliado / Conciliación / Aging / Proveedores / Trazabilidad.
- Buckets de antigüedad (aging), selección de facturas por proveedor, armado y envío de correos de conciliación.
- Gestión de notas crédito en todas las pestañas de pago (funcionalidad agregada recientemente).

### 3.4 `pages/3_💵_Planificador_de_Pagos.py` (828 líneas)
- Pestañas: Crítico / Financiero / Neto / Notas crédito / Programación / Conciliación / Descuentos.
- Selección de facturas para armar lotes de pago, aplicando descuentos por pronto pago y filtrando notas crédito.

### 3.5 `pages/3_📈_Rebate_Pintuco.py` (2287 líneas — trabajado en esta misma sesión)
- 3 proveedores de rebate (Pintuco, Abracol, Goya), cada uno con:
  - Sincronización de facturas por correo (mismo patrón IMAP, alias propios).
  - Presupuestos por periodo (mensual+trimestral para Pintuco, bimestral para Abracol, semestral para Goya) — hoy hardcodeados en Python (`MONTHLY_BUDGETS`, `ABRACOL_BIMESTER_BUDGETS`, `GOYA_SEMESTER_BUDGETS`).
  - Cálculo de escalas (Escala 1/2 o tramos de crecimiento 20/30/40/50%), bono de estacionalidad con fecha de corte configurable, recomposición de 9 meses.
  - Tarjetas ejecutivas por periodo (recién rediseñadas) + tablas detalladas + exclusión de facturas.
  - 3 hojas de Sheets propias: `Rebate_Pintuco`, `Rebate_Abracol`, `Rebate_Goya`.

### 3.6 `pages/5_👥_Creacion_Proveedores.py` (134 líneas)
- Brechas de calidad del maestro de proveedores + edición operativa (correos de pago/alertas, contactos, condiciones comerciales).

### 3.7 Integraciones externas a reemplazar/conservar
| Integración | Uso actual | En la migración |
|---|---|---|
| Google Sheets (`gspread`) | Toda la persistencia | **Se reemplaza por Postgres + PostgREST** |
| Gmail IMAP | Lectura de facturas XML/ZIP | Se conserva igual, pero como worker aparte |
| Dropbox API | CSV de cartera ERP | Se conserva igual, worker aparte |
| SendGrid | Envío de correos de pago | Se conserva igual, llamado desde Next.js o el worker |
| `PROVEDORES_CORREO.xlsx` | Catálogo base de proveedores | Se carga una vez a Postgres como semilla, luego editable desde la UI |

---

## 4. Arquitectura objetivo

### 4.1 Principio guía: *strangler pattern*, no reescritura de todo a la vez

La parte más riesgosa de todo el sistema es el **parsing de XML UBL de facturación electrónica colombiana** (namespaces `cac`/`cbc`, notas crédito, adjuntos ZIP anidados) y la lectura IMAP. Esa lógica ya está probada en producción. Reescribirla en Node/TypeScript agrega riesgo sin beneficio real.

**Recomendación:** dividir en dos servicios independientes:

1. **Ingestion Worker (Python, se conserva casi tal cual)** — un contenedor con cron interno (o `schedule`/APScheduler) que cada N minutos:
   - Lee IMAP y descarga/parsea XML igual que hoy (reutilizando ~80% del código de `treasury_core.py` y `Rebate_Pintuco.py`).
   - Descarga los CSV de Dropbox y los sincroniza.
   - En vez de escribir a Google Sheets, escribe directo a Postgres (via `psycopg2`/SQLAlchemy) o hace `POST`/`PATCH` a PostgREST.
   - Expone opcionalmente un endpoint HTTP simple de salud/estado (para verlo desde Coolify).

2. **Web App (Next.js 15, App Router, TypeScript)** — reemplaza 100% del front Streamlit:
   - Server Components para las vistas de solo lectura (dashboards, tablas).
   - Route Handlers (`/app/api/...`) como capa fina que llama a PostgREST con la service key en el servidor (nunca se expone PostgREST directo al navegador).
   - Client Components solo donde hay interactividad real (filtros, formularios de edición, botón de sincronizar, selección de facturas para lotes).

3. **PostgREST** — expone Postgres como API REST automática. Las reglas de negocio "pesadas" (determinar escala de rebate, aging buckets, motor de descuentos, cálculo de plan de pagos) se implementan como **funciones/vistas SQL** (`CREATE FUNCTION ... RETURNS TABLE`, expuestas como `rpc/`) para que PostgREST las sirva sin necesitar un backend intermedio. Esto es exactamente el patrón que ya usa el proyecto Optiferre en el mismo servidor.

4. **Postgres 16** — una sola base, con schemas separados: `treasury` (tesorería/conciliación/pagos), `rebate` (Pintuco/Abracol/Goya), `providers` (maestro proveedores), `audit` (logs, exclusiones, bitácora).

```
┌─────────────┐      ┌──────────────────┐      ┌────────────┐      ┌──────────────┐
│   Next.js    │ ───▶ │  Route Handlers   │ ───▶ │  PostgREST │ ───▶ │  Postgres 16  │
│ (Coolify app)│      │ (server-only fetch)│     │ (Coolify)  │      │  (Coolify DB) │
└─────────────┘      └──────────────────┘      └────────────┘      └──────┬───────┘
                                                                            │
┌───────────────────────────────────────────────────────────────────────┐│
│  Ingestion Worker (Python, cron/APScheduler)                          ││
│  IMAP Gmail → parseo UBL/XML  |  Dropbox CSV → cartera ERP            ││
│  escribe directo a Postgres ──────────────────────────────────────────┘│
└───────────────────────────────────────────────────────────────────────┘
```

### 4.2 Stack de frontend

| Pieza | Elección | Por qué |
|---|---|---|
| Framework | Next.js 15 (App Router) + TypeScript | Estándar, ya probado en este mismo servidor |
| UI kit | Tailwind CSS + shadcn/ui | Rápido de montar, permite recrear el look corporativo (navy/gold/rojo Ferreinox) con temas |
| Tablas | TanStack Table | Reemplaza `st.dataframe` con filtros/orden/paginación reales, mucho más rápido que Streamlit con datasets grandes |
| Gráficas | Recharts o Tremor | Reemplaza `st.line_chart`/`st.bar_chart` |
| Formularios | React Hook Form + Zod | Para edición de proveedores, exclusiones, registro de lotes |
| Data fetching | Server Components + `fetch` con caché de Next.js (`revalidate`), TanStack Query solo donde hay refresco en vivo | Permite carga inicial instantánea (SSR) — la mejora de velocidad más grande frente a Streamlit |
| Auth | NextAuth (Credentials) contra tabla `users` en Postgres, con roles (admin/tesorería/lectura) | Reemplaza la contraseña única compartida por login real, sin perder simplicidad |

### 4.3 Por qué esto es más rápido que Streamlit

- Streamlit re-ejecuta el script completo en cada interacción y recalcula todo en memoria del server Python en cada carga — de ahí la lentitud actual, sobre todo en Rebate (2287 líneas se ejecutan de nuevo por cada clic).
- Next.js con Server Components solo pide a PostgREST los datos que la vista necesita; PostgREST traduce a SQL directo (sin ORM intermedio) — la latencia por página baja de "segundos" a "decenas de milisegundos" en la mayoría de vistas.
- Las agregaciones (sumas por trimestre, cumplimiento de metas, aging) se calculan **en Postgres** (motor optimizado para esto) en vez de en pandas en cada rerun.
- Assets estáticos y HTML se sirven cacheados/paginados en vez de reconstruir todo el DOM en cada rerun de Streamlit.

---

## 5. Modelo de datos Postgres (propuesta, basada en las columnas reales actuales)

### 5.1 Schema `providers`

```sql
CREATE TABLE providers.provider (
    id                    bigserial PRIMARY KEY,
    codigo_proveedor      text,
    nif                   text,
    nombre                text NOT NULL,
    nombre_normalizado    text NOT NULL UNIQUE,
    activo                boolean NOT NULL DEFAULT true,
    email_pago            text,
    email_cc              text,
    email_alertas         text,
    contacto_pagos        text,
    contacto_tesoreria    text,
    telefono              text,
    condiciones_comerciales text,
    observaciones         text,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now()
);
```
Reemplaza `Maestro_Proveedores` + `PROVEDORES_CORREO.xlsx` (semilla inicial).

### 5.2 Schema `treasury`

- `treasury.email_invoice` — reemplaza `Historial_Correo_Proveedores` (columnas de `EMAIL_COLUMNS`: invoice_key, num_factura, proveedor_correo/norm, tipo_documento, valores base/iva/total, fechas, remitente, message_id, referencias, origen_soporte).
- `treasury.erp_pending` / `treasury.erp_paid` — reemplaza los dos CSV de Dropbox una vez normalizados (columnas de `PENDING_COLUMNS`/`PAID_COLUMNS`).
- `treasury.master_invoice` — la tabla más importante, reemplaza `Maestro_Facturas`: todas las columnas de `MASTER_OPTIONAL_DEFAULTS` (invoice_key PK, estados de conciliación/ERP/vencimiento, valores, descuentos, flags de riesgo, lote asociado).
- `treasury.payment_plan` (vista o tabla materializada calculada) — reemplaza `Propuesta_Pagos`.
- `treasury.payment_lot` — reemplaza `Lotes_Pago` (`PAYMENT_LOT_COLUMNS`).
- `treasury.email_log` — reemplaza `Historial_Correos` (`EMAIL_LOG_COLUMNS`).
- `treasury.manual_reconciliation` — reemplaza `Conciliacion_Manual` (`MANUAL_RECONCILIATION_COLUMNS`).
- `audit.invoice_exclusion` — reemplaza `Facturas_Excluidas` (`INVOICE_EXCLUSION_COLUMNS`), **compartida entre Tesorería y Rebate** igual que hoy.

### 5.3 Schema `rebate`

- `rebate.provider_budget` — presupuestos por periodo y proveedor (hoy hardcodeados en Python: `MONTHLY_BUDGETS`, `ABRACOL_BIMESTER_BUDGETS`, `GOYA_SEMESTER_BUDGETS`). Pasan a ser **filas editables en Postgres** en vez de requerir un deploy de código cada vez que cambia el presupuesto (mejora directa sobre el dolor que ya vivimos hoy: cada ajuste de meta requeria una edición manual del `.py`).
- `rebate.invoice` — una tabla por proveedor o una tabla única con columna `provider_key` (pintuco/abracol/goya) — reemplaza las 3 hojas `Rebate_Pintuco/Abracol/Goya`.
- Vistas SQL: `rebate.v_monthly_summary`, `rebate.v_quarterly_summary`, `rebate.v_period_cards` — mueven a SQL los cálculos que hoy hace `build_monthly_rebate_table`/`build_quarterly_rebate_table`/`build_cycle_projection` en pandas.

### 5.4 Seguridad a nivel de fila (RLS)
PostgREST + Postgres RLS permite, por ejemplo, que un rol "lectura" solo vea `treasury.master_invoice` sin poder hacer `UPDATE`, y que solo el rol "tesorería" pueda registrar lotes/exclusiones — algo que hoy Streamlit no controla en absoluto (toda la app comparte una sola contraseña con permisos totales).

---

## 6. Autenticación y roles

- Hoy: una sola contraseña compartida (`st.secrets["password"]`), sin usuarios ni roles.
- Propuesto: tabla `providers.app_user` (o schema `auth`) con `email`, `password_hash`, `role`. NextAuth Credentials Provider + `bcrypt`. JWT con claim `role` que Postgres usa vía `current_setting('request.jwt.claims')` para RLS.
- **Confirmado (sección 10): se implementan los 4 roles desde el lanzamiento**, no un admin único temporal: `admin` (todo), `tesoreria` (tesorería + pagos), `comercial` (solo rebate), `lectura` (solo dashboards, sin edición). Esto implica definir desde la Fase 0 las políticas RLS por schema/tabla y la pantalla de gestión de usuarios antes de dar por cerrada la Fase 3.

---

## 7. Plan de despliegue en Coolify

1. **Prerrequisito bloqueante:** sanear memoria del servidor actual (ver 10.1) antes de crear cualquier recurso — verificar `free -h` y `docker stats` de nuevo el día del despliegue; si sigue con swap alto y <500MB libres, pausar.
2. Crear proyecto Coolify nuevo: **"Pagos Proveedores Next.js"**.
3. Recurso 1 — **Postgres 16** (recurso nativo de Coolify, con backup automático habilitado desde el día uno, `mem_limit` explícito).
4. Recurso 2 — **PostgREST** (Docker Compose custom, igual al patrón que ya corre en el proyecto Optiferre) apuntando a la base anterior, expuesto solo en red interna de Docker (no público), `mem_limit` explícito.
5. Recurso 3 — **Next.js app**, desplegado desde el repo nuevo `pagos-proveedores-web` (GitHub), build con Nixpacks/Dockerfile, dominio `proveedores.datovatenexuspro.com` vía Traefik/Coolify, TLS automático.
6. Recurso 4 — **Ingestion Worker** (Docker Compose custom, imagen Python liviana con el cron de IMAP+Dropbox), `mem_limit` explícito.
7. Variables de entorno (las mismas credenciales de Gmail/Dropbox/SendGrid que ya existen en `secrets.toml`, migradas a "Environment Variables" de Coolify — nunca commiteadas al repo).
8. Monitoreo: Coolify Sentinel (ya activo en el servidor) + alertas de uso de memoria/CPU específicas para este proyecto, dado que el servidor es compartido.

---

## 8. Fases de migración (sin tocar Streamlit)

**Fase 0 — Preparación (0 riesgo para producción)**
- Provisionar servidor/droplet + Postgres + PostgREST vacíos.
- Diseñar y crear el esquema completo (secciones 5.1–5.3) con migraciones versionadas (`sqlx`/`Atlas`/`golang-migrate`, o simplemente `.sql` numerados).

**Fase 1 — Migración de datos (una sola vez, en paralelo, sin apagar Sheets)**
- Script de un solo uso: lee las 8 hojas de Google Sheets + las 3 hojas de Rebate + `PROVEDORES_CORREO.xlsx`, las vuelca a Postgres. Streamlit sigue escribiendo en Sheets mientras tanto.

**Fase 2 — Ingestion Worker en paralelo (modo sombra)**
- El worker Python corre en paralelo al Streamlit actual, escribiendo a Postgres, **sin que nadie lo use todavía**. Se valida días/semanas comparando cifras Postgres vs. Sheets hasta que coincidan al 100%.

**Fase 3 — Next.js en modo lectura**
- Se construyen las vistas de solo lectura primero (dashboards ejecutivos, Rebate, Portal Tesorería en modo consulta) contra PostgREST. Usuarios internos empiezan a mirarla en paralelo a Streamlit, sin decisiones operativas todavía basadas en ella.

**Fase 4 — Next.js en modo escritura**
- Se activan las acciones (registrar lotes, enviar correos, marcar exclusiones, editar proveedores) en Next.js. Se congela la escritura equivalente en Streamlit (o se deja como solo-lectura de respaldo).

**Fase 5 — Corte**
- Streamlit pasa a modo "solo por si acaso" (se deja desplegado, sin uso operativo). Next.js es la app de producción.
- Google Sheets se conserva como respaldo de solo lectura por un tiempo prudente (ej. 90 días) antes de dar de baja definitivamente esa integración.

Cada fase es reversible: mientras Streamlit no se toque, cualquier problema en la fase nueva no afecta la operación diaria actual.

---

## 9. Riesgos y mitigaciones

| Riesgo | Mitigación |
|---|---|
| Servidor compartido sin RAM libre | Droplet nuevo dedicado (recomendado) o resolver `begranda-web` antes de desplegar aquí |
| Reescribir parsing UBL/XML en Node introduce bugs nuevos | Conservar el worker de ingesta en Python, solo cambia el destino (Postgres en vez de Sheets) |
| Reglas de negocio complejas (rebate, descuentos, aging) mal migradas a SQL | Migrar función por función con tests que comparen contra el resultado actual de pandas antes de dar por buena cada una |
· Pérdida de auditoría/trazabilidad durante el corte | Mantener Sheets como espejo de solo lectura durante todo el periodo de validación (fases 2–4) |
| Presupuestos de rebate hardcodeados hoy en `.py` (como los que ajustamos hoy mismo) | Pasan a ser filas editables en `rebate.provider_budget`, evitando el próximo deploy manual cada vez que cambie una meta |
| Autenticación única compartida | Se sustituye por login con roles, sin bloquear el lanzamiento (se puede arrancar con 1 solo usuario admin) |

---

## 10. Decisiones confirmadas (2026-07-01)

1. **Infraestructura:** se reutiliza el servidor Coolify actual (no se provisiona droplet nuevo).
2. **Fuentes de datos:** se mantienen Gmail/IMAP y Dropbox exactamente igual; solo cambia el destino de guardado (Postgres en vez de Sheets).
3. **Usuarios:** se implementan roles completos (admin/tesorería/comercial/lectura) desde el lanzamiento, no un solo admin temporal.
4. **Repositorio:** código nuevo en un repositorio separado (`pagos-proveedores-web`), independiente de este repo Streamlit.

### 10.1 Salvedad importante sobre el punto 1 (reusar el servidor actual)

Esta decisión implicaba un **prerrequisito de infraestructura antes de desplegar cualquier contenedor nuevo**: el servidor tenía el swap al 100% y ~180MB de RAM libre por culpa de `begranda-web`.

**Actualización 2026-07-01 (tarde) — Fase 0.a resuelta como efecto colateral de un incidente de seguridad no relacionado:** `begranda-web` resultó estar comprometido (cryptominer + reverse shell vía RCE de Next.js 15.0.0), lo cual explica el consumo de 2.4GB/92% CPU. El incidente se contuvo y remedió (contenedor reconstruido, Next.js parchado a 15.3.3, límites reales de CPU/RAM aplicados, firewall endurecido — ver detalle en la conversación de esa fecha). Verificación re-hecha después del arreglo:

| Métrica | Antes del incidente | Ahora (verificado) |
| --- | --- | --- |
| RAM disponible | ~180MB | **~2.9GB** |
| Load average (2 CPU) | 6.76 | **~2.0–2.9** |
| Swap usado | 100% (4.0/4.0GB) | 100% aún, pero sin presión activa (buff/cache sano: 3.4GB) |

**Pendiente menor (no bloqueante):** el swap sigue mostrando 100% usado — Linux no libera páginas swapeadas hasta que se reclaman, así que esto no indica sobrecarga activa (load average y RAM disponible ya son saludables), pero antes de desplegar la Fase 0 conviene un **reinicio programado del droplet** (fuera de horario operativo) para limpiar el swap por completo y arrancar con margen total. No es bloqueante para empezar a construir el schema/repo, sí para el despliegue final en Coolify.

- **Fase 0.b — Límites de recursos explícitos** sigue vigente como práctica: todos los contenedores nuevos de este proyecto (Postgres, PostgREST, Next.js, worker) se despliegan con `mem_limit`/`cpus` explícitos — ver docker-compose de referencia en la sección 11.

**Con esto, la Fase 0 ya está desbloqueada.** El resto de este documento (secciones 11 en adelante) completa el detalle técnico concreto: DDL completo de Postgres, políticas RLS, estructura de Next.js, script de migración de datos, y docker-compose de despliegue — listo para empezar a construir.

---

## 11. DDL completo de Postgres (Fase 0 — listo para ejecutar)

Todas las tablas usan `timestamptz` para fechas con hora, `date` para fechas puras, `numeric(18,2)` para dinero (nunca `float`), y texto normalizado en minúsculas/sin tildes para las claves de cruce (`*_norm`), igual que hace hoy `normalize_supplier_key`/`normalize_invoice_number` en Python.

### 11.1 Schema `auth` — usuarios y roles

```sql
CREATE SCHEMA IF NOT EXISTS auth;

CREATE TABLE auth.app_user (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    email           text NOT NULL UNIQUE,
    password_hash   text NOT NULL,
    full_name       text,
    role            text NOT NULL CHECK (role IN ('admin','tesoreria','comercial','lectura')),
    active          boolean NOT NULL DEFAULT true,
    last_login_at   timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);
```

### 11.2 Schema `providers`

```sql
CREATE SCHEMA IF NOT EXISTS providers;

CREATE TABLE providers.provider (
    id                      bigserial PRIMARY KEY,
    codigo_proveedor        text,
    nif                     text,
    nombre                  text NOT NULL,
    nombre_normalizado      text NOT NULL UNIQUE,
    activo                  boolean NOT NULL DEFAULT true,
    email_pago              text,
    email_cc                text,
    email_alertas           text,
    contacto_pagos          text,
    contacto_tesoreria      text,
    telefono                text,
    condiciones_comerciales text,
    observaciones           text,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);
-- Reemplaza Maestro_Proveedores + PROVEDORES_CORREO.xlsx (semilla inicial vía Fase 1).
```

### 11.3 Schema `treasury`

```sql
CREATE SCHEMA IF NOT EXISTS treasury;

-- Reemplaza Historial_Correo_Proveedores
CREATE TABLE treasury.email_invoice (
    invoice_key                 text PRIMARY KEY,
    num_factura                 text NOT NULL,
    proveedor_correo            text,
    proveedor_norm              text,
    tipo_documento_correo       text DEFAULT 'FACTURA',
    documento_relacionado_correo text,
    descripcion_nota_correo     text,
    fecha_emision_correo        date,
    fecha_vencimiento_correo    date,
    valor_total_correo          numeric(18,2) DEFAULT 0,
    valor_base_correo           numeric(18,2) DEFAULT 0,
    valor_iva_correo            numeric(18,2) DEFAULT 0,
    fecha_recepcion_correo      timestamptz,
    remitente_correo            text,
    asunto_correo               text,
    nombre_adjunto              text,
    message_id                  text,
    referencias_correo          text,
    valor_detectado_correo      numeric(18,2),
    origen_soporte               text,
    created_at                  timestamptz NOT NULL DEFAULT now(),
    updated_at                  timestamptz NOT NULL DEFAULT now()
);

-- Reemplaza el CSV /data/Proveedores.csv (cartera pendiente Dropbox)
CREATE TABLE treasury.erp_pending (
    id                    bigserial PRIMARY KEY,
    nombre_proveedor_erp  text NOT NULL,
    serie                 text,
    num_entrada           text,
    num_factura           text NOT NULL,
    doc_erp               text,
    fecha_emision_erp     date,
    fecha_vencimiento_erp date,
    valor_total_erp       numeric(18,2) NOT NULL DEFAULT 0,
    synced_at             timestamptz NOT NULL DEFAULT now(),
    UNIQUE (nombre_proveedor_erp, num_factura)
);

-- Reemplaza el CSV /data/cartera_saldada.csv
CREATE TABLE treasury.erp_paid (
    id                    bigserial PRIMARY KEY,
    nombre_proveedor_erp  text NOT NULL,
    serie                 text,
    num_entrada           text,
    num_factura           text NOT NULL,
    estado_documento      text,
    fecha_emision_erp     date,
    fecha_vencimiento_erp date,
    valor_total_erp       numeric(18,2) NOT NULL DEFAULT 0,
    synced_at             timestamptz NOT NULL DEFAULT now(),
    UNIQUE (nombre_proveedor_erp, num_factura)
);

-- Reemplaza Maestro_Facturas — la tabla central de todo el motor de conciliación
CREATE TABLE treasury.master_invoice (
    invoice_key              text PRIMARY KEY,
    proveedor                text,
    proveedor_norm           text,
    proveedor_erp            text,
    proveedor_correo         text,
    num_factura              text,
    tipo_documento_correo    text DEFAULT 'FACTURA',
    documento_relacionado_correo text,
    descripcion_nota_correo  text,
    factura_compensada_correo text,
    manual_resolution_id     text,
    manual_resolution_type   text,
    manual_resolution_notes  text,
    manual_resolution_target text,
    exclusion_id             text,
    motivo_exclusion         text,
    estado_erp               text NOT NULL DEFAULT 'No ERP',
    estado_conciliacion      text NOT NULL DEFAULT 'Sin clasificar',
    estado_vencimiento       text NOT NULL DEFAULT 'No aplica',
    valor_erp                numeric(18,2) NOT NULL DEFAULT 0,
    valor_total_correo       numeric(18,2) NOT NULL DEFAULT 0,
    valor_base_correo        numeric(18,2) NOT NULL DEFAULT 0,
    valor_iva_correo         numeric(18,2) NOT NULL DEFAULT 0,
    valor_base_descuento     numeric(18,2) NOT NULL DEFAULT 0,
    diferencia_valor         numeric(18,2) NOT NULL DEFAULT 0,
    detalle_valor            text,
    detalle_conciliacion     text,
    valor_descuento          numeric(18,2) NOT NULL DEFAULT 0,
    valor_a_pagar            numeric(18,2) NOT NULL DEFAULT 0,
    descuento_pct            numeric(6,4) NOT NULL DEFAULT 0,
    fecha_limite_descuento   date,
    fecha_vencimiento_erp    date,
    fecha_emision_erp        date,
    fecha_emision_correo     date,
    fecha_vencimiento_correo date,
    fecha_recepcion_correo   timestamptz,
    fecha_programada_pago    date,
    estado_descuento         text NOT NULL DEFAULT 'No aplica',
    registrada_para_pago     boolean NOT NULL DEFAULT false,
    excluir_de_calculos      boolean NOT NULL DEFAULT false,
    riesgo_mora_48h          boolean NOT NULL DEFAULT false,
    dias_para_vencer         integer NOT NULL DEFAULT 0,
    remitente_correo         text,
    asunto_correo            text,
    nombre_adjunto           text,
    message_id               text,
    motivo_base              text,
    lote_id                  text,
    estado_lote              text,
    -- columnas de proveedor "aplanadas" para lectura rápida sin join (se llenan por trigger/función)
    email_pago               text,
    email_cc                 text,
    email_alertas            text,
    contacto_pagos           text,
    condiciones_comerciales  text,
    activo                   boolean NOT NULL DEFAULT true,
    created_at               timestamptz NOT NULL DEFAULT now(),
    updated_at               timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_master_invoice_proveedor_norm ON treasury.master_invoice (proveedor_norm);
CREATE INDEX idx_master_invoice_estado_erp ON treasury.master_invoice (estado_erp);
CREATE INDEX idx_master_invoice_estado_conciliacion ON treasury.master_invoice (estado_conciliacion);

-- Reemplaza Lotes_Pago
CREATE TABLE treasury.payment_lot (
    lote_id               text NOT NULL,
    invoice_key           text NOT NULL REFERENCES treasury.master_invoice(invoice_key),
    fecha_registro        timestamptz NOT NULL DEFAULT now(),
    fecha_programada_pago date,
    responsable           text,
    proveedor             text,
    num_factura           text,
    valor_factura         numeric(18,2) NOT NULL DEFAULT 0,
    valor_descuento       numeric(18,2) NOT NULL DEFAULT 0,
    valor_a_pagar         numeric(18,2) NOT NULL DEFAULT 0,
    estado_lote           text NOT NULL DEFAULT 'Programado',
    motivo_pago           text,
    email_destino         text,
    PRIMARY KEY (lote_id, invoice_key)
);

-- Reemplaza Historial_Correos
CREATE TABLE treasury.email_log (
    envio_id       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    fecha_envio    timestamptz NOT NULL DEFAULT now(),
    lote_id        text,
    proveedor      text,
    email_destino  text,
    email_cc       text,
    asunto         text,
    facturas       text,
    ahorro_total   numeric(18,2) DEFAULT 0,
    estado_envio   text NOT NULL,
    detalle_envio  text
);

-- Reemplaza Conciliacion_Manual
CREATE TABLE treasury.manual_reconciliation (
    resolution_id       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at           timestamptz NOT NULL DEFAULT now(),
    resolution_type      text NOT NULL,
    invoice_key_source   text,
    invoice_key_target   text,
    proveedor_norm       text,
    source_num_factura   text,
    target_num_factura   text,
    status               text NOT NULL DEFAULT 'ACTIVO',
    notes                text
);
```

### 11.4 Schema `audit` (compartido entre Tesorería y Rebate)

```sql
CREATE SCHEMA IF NOT EXISTS audit;

-- Reemplaza Facturas_Excluidas
CREATE TABLE audit.invoice_exclusion (
    exclusion_id    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at      timestamptz NOT NULL DEFAULT now(),
    invoice_key     text NOT NULL,
    proveedor_norm  text NOT NULL,
    num_factura     text NOT NULL,
    status          text NOT NULL DEFAULT 'ACTIVO',
    reason          text,
    source          text NOT NULL,
    created_by      uuid REFERENCES auth.app_user(id)
);
CREATE INDEX idx_invoice_exclusion_key ON audit.invoice_exclusion (invoice_key);
```

### 11.5 Schema `rebate`

```sql
CREATE SCHEMA IF NOT EXISTS rebate;

-- Reemplaza MONTHLY_BUDGETS (hoy hardcodeado en Rebate_Pintuco.py) — ahora editable desde la UI
CREATE TABLE rebate.pintuco_budget (
    mes                   text NOT NULL,
    mes_num               integer NOT NULL,
    trimestre             text NOT NULL,
    escala_1              numeric(18,2) NOT NULL,
    escala_2              numeric(18,2) NOT NULL,
    corte_estacionalidad  date,
    updated_at            timestamptz NOT NULL DEFAULT now(),
    updated_by            uuid REFERENCES auth.app_user(id),
    PRIMARY KEY (mes_num)
);

-- Reemplaza ABRACOL_BIMESTER_BUDGETS
CREATE TABLE rebate.abracol_budget (
    periodo       text PRIMARY KEY,
    inicio        date NOT NULL,
    fin           date NOT NULL,
    ventas_2025   numeric(18,2) NOT NULL,
    meta_2026     numeric(18,2) NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now(),
    updated_by    uuid REFERENCES auth.app_user(id)
);

-- Reemplaza GOYA_SEMESTER_BUDGETS
CREATE TABLE rebate.goya_budget (
    periodo       text PRIMARY KEY,
    inicio        date NOT NULL,
    fin           date NOT NULL,
    ventas_2024   numeric(18,2) NOT NULL,
    base_2025     numeric(18,2) NOT NULL,
    meta_20       numeric(18,2) NOT NULL,
    meta_30       numeric(18,2) NOT NULL,
    meta_40       numeric(18,2) NOT NULL,
    meta_50       numeric(18,2) NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now(),
    updated_by    uuid REFERENCES auth.app_user(id)
);

-- Reemplaza las 3 hojas Rebate_Pintuco / Rebate_Abracol / Rebate_Goya
CREATE TABLE rebate.invoice (
    provider_key           text NOT NULL CHECK (provider_key IN ('pintuco','abracol','goya')),
    numero_factura         text NOT NULL,
    fecha_factura          date,
    valor_neto             numeric(18,2) NOT NULL DEFAULT 0,
    proveedor_correo       text,
    fecha_recepcion_correo timestamptz,
    remitente_correo       text,
    asunto_correo          text,
    nombre_adjunto         text,
    message_id             text,
    estado_pago            text NOT NULL DEFAULT 'Pendiente',
    created_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (provider_key, numero_factura)
);
```

### 11.6 Vistas SQL (mueven los cálculos de pandas a SQL)

```sql
-- Reemplaza determine_scale() + build_monthly_rebate_table() de Rebate_Pintuco.py
CREATE OR REPLACE VIEW rebate.v_pintuco_monthly AS
SELECT
    b.mes, b.mes_num, b.trimestre, b.escala_1, b.escala_2, b.corte_estacionalidad,
    COALESCE(SUM(i.valor_neto) FILTER (WHERE date_trunc('month', i.fecha_factura) = make_date(2026, b.mes_num, 1)), 0) AS compra_neta,
    CASE
        WHEN COALESCE(SUM(i.valor_neto) FILTER (WHERE date_trunc('month', i.fecha_factura) = make_date(2026, b.mes_num, 1)), 0) * 0.88 >= b.escala_2 THEN 'Escala 2'
        WHEN COALESCE(SUM(i.valor_neto) FILTER (WHERE date_trunc('month', i.fecha_factura) = make_date(2026, b.mes_num, 1)), 0) * 0.88 >= b.escala_1 THEN 'Escala 1'
        ELSE 'Sin escala'
    END AS escala_lograda
FROM rebate.pintuco_budget b
LEFT JOIN rebate.invoice i ON i.provider_key = 'pintuco'
GROUP BY b.mes, b.mes_num, b.trimestre, b.escala_1, b.escala_2, b.corte_estacionalidad;
```
*(Vista de ejemplo — se completa 1:1 contra `build_monthly_rebate_table`/`build_quarterly_rebate_table`/`build_cycle_projection` durante la Fase 0, con tests que comparen el resultado contra el pandas actual antes de dar cada vista por buena — ver Riesgos, sección 9).*

---

## 12. Políticas de seguridad a nivel de fila (RLS) — modelo concreto

PostgREST usa un solo rol de conexión (`authenticator`) que cambia de "personalidad" en cada request según el JWT que llega desde Next.js. La policía de acceso vive en Postgres, no en el código:

```sql
-- Rol técnico que PostgREST usa para conectarse (no es un usuario de negocio)
CREATE ROLE authenticator NOINHERIT LOGIN PASSWORD '...rotar-en-Fase-0...';
CREATE ROLE web_anon NOLOGIN;         -- sin sesión (solo /login)
CREATE ROLE web_authenticated NOLOGIN; -- cualquier usuario logueado
GRANT web_anon, web_authenticated TO authenticator;

-- Función que PostgREST llama para resolver el rol de Postgres según el JWT
CREATE OR REPLACE FUNCTION auth.jwt_role() RETURNS text AS $$
  SELECT COALESCE(current_setting('request.jwt.claims', true)::json->>'role', 'lectura');
$$ LANGUAGE sql STABLE;

ALTER TABLE treasury.master_invoice ENABLE ROW LEVEL SECURITY;

CREATE POLICY master_invoice_select ON treasury.master_invoice
  FOR SELECT TO web_authenticated
  USING (auth.jwt_role() IN ('admin','tesoreria','lectura'));

CREATE POLICY master_invoice_write ON treasury.master_invoice
  FOR INSERT TO web_authenticated WITH CHECK (auth.jwt_role() IN ('admin','tesoreria'));

CREATE POLICY master_invoice_update ON treasury.master_invoice
  FOR UPDATE TO web_authenticated
  USING (auth.jwt_role() IN ('admin','tesoreria'))
  WITH CHECK (auth.jwt_role() IN ('admin','tesoreria'));

-- Mismo patrón para treasury.payment_lot, treasury.email_log, audit.invoice_exclusion
-- (solo admin/tesoreria escriben) y para rebate.* (solo admin/comercial escriben, lectura de todos).
```

Matriz de permisos por rol (referencia para replicar en cada tabla):

| Schema/tabla | admin | tesoreria | comercial | lectura |
| --- | --- | --- | --- | --- |
| `treasury.*` (todas) | RW | RW | R | R |
| `rebate.*` (todas) | RW | R | RW | R |
| `providers.provider` | RW | RW | R | R |
| `audit.invoice_exclusion` | RW | RW | RW | R |
| `auth.app_user` | RW | - | - | - |

---

## 13. Estructura del repo Next.js (`pagos-proveedores-web`)

Mapeo 1:1 con las páginas actuales de Streamlit, para que nadie se pierda buscando "dónde quedó X":

```text
pagos-proveedores-web/
  app/
    login/page.tsx                       ← reemplaza check_password() de app.py
    (dashboard)/
      layout.tsx                         ← sidebar + shell (equivalente a display_sidebar)
      page.tsx                           ← Dashboard general (app.py: hero + KPIs + salud de fuentes)
      tesoreria/
        page.tsx                         ← Portal Tesorería: resumen
        pagar/page.tsx
        correo/page.tsx
        notas-credito/page.tsx
        no-conciliado/page.tsx
        conciliacion/page.tsx
        aging/page.tsx
        proveedores/page.tsx
        trazabilidad/page.tsx
      planificador/
        page.tsx                         ← Planificador de Pagos (tabs: crítico/financiero/neto/notas/programación/conciliación/descuentos)
      rebate/
        pintuco/page.tsx
        abracol/page.tsx
        goya/page.tsx
      proveedores/
        page.tsx                         ← Creación/Calidad de Proveedores
      usuarios/
        page.tsx                         ← nuevo: gestión de usuarios/roles (no existía en Streamlit)
    api/
      auth/[...nextauth]/route.ts
      sync/route.ts                      ← dispara/lee estado del ingestion worker
  lib/
    postgrest.ts                         ← fetch wrapper server-only con el JWT firmado
    auth.ts                              ← config de NextAuth + roles
  components/
    kpi-card.tsx
    period-card.tsx                      ← mismo diseño de tarjetas por periodo ya construido en Rebate
    data-table.tsx                       ← TanStack Table genérica
  drizzle/ (o prisma/)
    schema.ts                            ← reflejo tipado del DDL de la sección 11, solo para tipos, las escrituras van vía PostgREST
```

---

## 14. Script de migración de datos (Fase 1) — outline concreto

Un solo script Python (`migrate_sheets_to_postgres.py`), ejecutado una vez, que **reutiliza las funciones de lectura de `treasury_core.py`** en vez de reescribirlas:

```python
from common.treasury_core import (
    connect_to_google_sheets, load_sheet_df,
    SHEET_PROVIDER_MASTER, SHEET_EMAIL_HISTORY, SHEET_MASTER_INVOICES,
    SHEET_PAYMENT_LOTS, SHEET_EMAIL_LOG, SHEET_MANUAL_RECONCILIATION,
    SHEET_INVOICE_EXCLUSIONS,
)
import psycopg2, os

PG_DSN = os.environ["MIGRATION_PG_DSN"]  # nunca hardcodeado

SHEET_TO_TABLE = {
    SHEET_PROVIDER_MASTER: "providers.provider",
    SHEET_EMAIL_HISTORY: "treasury.email_invoice",
    SHEET_MASTER_INVOICES: "treasury.master_invoice",
    SHEET_PAYMENT_LOTS: "treasury.payment_lot",
    SHEET_EMAIL_LOG: "treasury.email_log",
    SHEET_MANUAL_RECONCILIATION: "treasury.manual_reconciliation",
    SHEET_INVOICE_EXCLUSIONS: "audit.invoice_exclusion",
}

def migrate():
    client = connect_to_google_sheets()
    conn = psycopg2.connect(PG_DSN)
    for sheet_name, table_name in SHEET_TO_TABLE.items():
        df = load_sheet_df(client, sheet_name)
        # TRUNCATE + COPY: seguro porque Sheets sigue siendo la fuente de verdad en Fase 1-2
        bulk_upsert(conn, table_name, df)
    # + 3 hojas de Rebate (Rebate_Pintuco/Abracol/Goya) → rebate.invoice con provider_key
    # + PROVEDORES_CORREO.xlsx → providers.provider (solo si el proveedor no existe ya)
    conn.commit()
```

Se corre cuantas veces sea necesario durante la Fase 1-2 (es idempotente: trunca y recarga), hasta que las cifras en Postgres cuadren al 100% contra Sheets antes de pasar a Fase 3.

---

## 15. Docker Compose de referencia para PostgREST + Ingestion Worker

Postgres se crea como recurso nativo de Coolify (no en este compose). Esto sí va como "Docker Compose" custom, con límites explícitos (Fase 0.b):

```yaml
services:
  postgrest:
    image: postgrest/postgrest:v12.2.8
    environment:
      PGRST_DB_URI: postgres://authenticator:${PGRST_AUTHENTICATOR_PASSWORD}@${PG_HOST}:5432/pagos_proveedores
      PGRST_DB_SCHEMAS: providers,treasury,rebate,audit,auth
      PGRST_DB_ANON_ROLE: web_anon
      PGRST_JWT_SECRET: ${JWT_SECRET}
      PGRST_DB_MAX_ROWS: "5000"
    mem_limit: 128m
    cpus: 0.5
    security_opt: ["no-new-privileges:true"]
    restart: unless-stopped
    networks: [pagos_proveedores_internal]   # sin puerto público

  ingestion-worker:
    build: ./worker
    environment:
      DATABASE_URL: postgres://ingestion_svc:${WORKER_DB_PASSWORD}@${PG_HOST}:5432/pagos_proveedores
      GMAIL_ADDRESS: ${GMAIL_ADDRESS}
      GMAIL_APP_PASSWORD: ${GMAIL_APP_PASSWORD}
      DROPBOX_APP_KEY: ${DROPBOX_APP_KEY}
      DROPBOX_APP_SECRET: ${DROPBOX_APP_SECRET}
      DROPBOX_REFRESH_TOKEN: ${DROPBOX_REFRESH_TOKEN}
      SENDGRID_API_KEY: ${SENDGRID_API_KEY}
    mem_limit: 256m
    cpus: 0.5
    security_opt: ["no-new-privileges:true"]
    read_only: false   # necesita /tmp para descomprimir ZIP de correo
    tmpfs:
      - /tmp:noexec,nosuid,size=128m
    restart: unless-stopped
    networks: [pagos_proveedores_internal]

networks:
  pagos_proveedores_internal:
    driver: bridge
```

La app Next.js se despliega como recurso "Application" nativo de Coolify (no en este compose), con `mem_limit`/`cpus` configurados en su propia pestaña de recursos del panel.

---

## 16. Decisiones finales (confirmadas 2026-07-01)

1. **Dominio:** `proveedores.datovatenexuspro.com` — confirmado.
2. **Reinicio del droplet:** no se hace por ahora — los números actuales (RAM disponible ~2.9GB, load average ~2.0–2.9) ya son saludables; se reevalúa más adelante si vuelve a haber presión de memoria.
3. **Usuario admin:** se migra la contraseña actual de la app a un usuario `admin` real en `auth.app_user`; los usuarios de tesorería/comercial/lectura se crean después desde la nueva pantalla de gestión de usuarios (no se siembran de entrada).

**Plan completo y cerrado.** Con las 4 decisiones de la sección 10 más estas 3, no quedan puntos abiertos que bloqueen empezar. El siguiente paso concreto es la **Fase 0**: crear el proyecto Coolify, provisionar Postgres, correr el DDL de la sección 11, y arrancar el repo `pagos-proveedores-web` con la estructura de la sección 13.

---
---

# Parte 2 — Addendum: Decisiones finales, fixes técnicos y features MVP

> **Cómo usar este documento:** este addendum **complementa y modifica** la Parte 1 anterior. Donde este addendum contradice a la Parte 1, **este addendum manda**. Léelo entero antes de tocar código.
>
> Fecha: 2026-07-01. Autor: revisión externa sobre el plan (sesión de VS Code), con decisiones confirmadas por Diego en la misma fecha.
>
> **Verificación previa (hecha antes de incorporar este addendum):** se confirmó por SSH que el cluster Postgres existente (proyecto Optiferre) corre **PostgreSQL 15.17** — cumple el mínimo ≥15 exigido en el punto G.1/I.3 de este addendum — y que el acceso `postgres` tiene `rolsuper=true`, así que crear la base y roles nuevos ahí (decisión B.1) es viable sin bloqueos.

## A. Alcance de la revisión

Objetivos de esta revisión:

1. **Fixes técnicos** al plan tal como estaba: identifiqué 8 puntos donde el plan tenía brechas reales (auditoría, JWT, backup, concurrencia en lotes, etc.) que hay que resolver antes de la Fase 0.
2. **Features MVP innovadores** que justifican reescribir la app en vez de solo migrarla. El plan original era un rewrite fiel de Streamlit; este addendum define qué features convierten la app en algo genuinamente superior desde el día uno.
3. **Consolidación de decisiones abiertas** en 8 puntos cerrados para arrancar Fase 0 sin ambigüedad.

---

## B. Decisiones finales confirmadas (todas cerradas)

1. **Base de datos**: se crea una **nueva database `pagos_proveedores` dentro del cluster Postgres ya existente en Coolify**. No se provisiona un cluster nuevo. Aislamiento por database + roles Postgres separados por proyecto (los roles de este proyecto no tienen `CONNECT` sobre otras databases del cluster). Esto ahorra los ~200MB de RAM que costaría duplicar el motor y mantiene aislamiento total a nivel de datos, permisos y backups.

2. **PostgREST**: **servicio dedicado a este proyecto** (nuevo contenedor en el mismo servidor). No se reutiliza el PostgREST del proyecto Optiferre. Motivos: JWT secret propio, schemas expuestos propios (`providers,treasury,rebate,audit,auth`), rol authenticator propio con permisos limitados a la nueva database. Mezclar PostgREST entre proyectos abre la puerta a leaks entre ellos y complica la rotación de secretos.

3. **Fuentes de datos**: Gmail/IMAP y Dropbox se mantienen **exactamente como hoy**. El ERP (Begranda) **no expone API** y no se toca — la ingesta de cartera sigue vía CSVs de Dropbox, igual que en la Streamlit actual. El worker Python es responsable de ambas fuentes.

4. **Usuarios y roles (4 personas totales, sin más)** — **reemplaza el modelo de roles de la Parte 1 (admin/tesorería/comercial/lectura)**:
   - `admin` — solo Diego. Acceso total.
   - `tesoreria` — 1 persona. **Autonomía total operativa**: arma lotes y ejecuta pagos sin autorización de gerencia. Envía correos a proveedores. Aplica descuentos por pronto pago.
   - `contabilidad` — 1 persona. **Misma operativa que tesorería** + auditoría transversal: revisa descuentos aplicados, resuelve discrepancias de descuentos, revisa retenciones, gestiona exclusiones de facturas, audita el historial de cambios.
   - `gerencia` — 1 persona. **Solo lectura de vistas resumen**: qué se pagó, qué hay por pagar, cómo van los rebates. No aprueba nada, no edita nada, no ve detalle operativo fino.

5. **LLM/Copilot NL2SQL**: **fuera del MVP**. Se planea para v2 una vez la app esté en producción y estable.

6. **WhatsApp Business**: **descartado**. No se integra en ninguna versión.

7. **Notificaciones push web (PWA)**: **incluidas en el MVP**, en modalidad **informativa no bloqueante**. Destinatarios: gerencia, contabilidad, admin. Ejemplo: "Tesorería pagó lote L-2026-07-03: 12 facturas, $18.4M, ahorro $340K por pronto pago". Tesorería no recibe notificaciones (es quien genera los eventos). Cero costo externo, cero dependencia de Meta/Google.

8. **Dominio**: `proveedores.datovatenexuspro.com` — confirmado (igual que en Parte 1).

---

## C. Matriz de permisos por rol (reemplaza la sección 12 de la Parte 1)

> **Actualización 2026-07-01 (durante ejecución de Fase 0, Tarea 3): modelo simplificado.** Diego confirmó que no hay problema en que gerencia tenga los mismos permisos de escritura que tesorería/contabilidad (armar lotes, editar facturas) — la restricción original de "gerencia solo lectura vía vistas resumen" era más granular de lo que el negocio realmente necesita con solo 4 personas de confianza. **La matriz de abajo ya refleja el modelo simplificado vigente** (reemplaza la versión anterior de esta sección).

Esta matriz es la fuente de verdad para las políticas RLS de Postgres. Cada tabla mencionada en el DDL (sección 11) debe tener sus policies alineadas exactamente con esta matriz.

| Schema / tabla | admin | tesoreria | contabilidad | gerencia |
| --- | --- | --- | --- | --- |
| `treasury.*` (todas: master_invoice, payment_lot, email_log, manual_reconciliation, email_invoice, erp_pending, erp_paid) | RW | RW | RW | RW |
| `rebate.*` (todas: invoice, pintuco_budget, abracol_budget, goya_budget) | RW | RW | RW | RW |
| `audit.invoice_exclusion` | RW | RW | RW | RW |
| `audit.master_invoice_history` (nueva, ver D.2) | R | R | R | R |
| `providers.provider` | RW | RW | RW | RW |
| `auth.app_user` | RW | - | - | - |

**Notas clave**:
- Los 4 roles humanos comparten el mismo Postgres role técnico `web_authenticated` — la diferenciación ya no es granular por tabla (excepto `auth.app_user`, que sigue siendo admin-only por higiene básica de gestión de usuarios). Todos pueden armar lotes, editar facturas, gestionar exclusiones y presupuestos de rebate.
- `audit.master_invoice_history` sigue siendo de **solo lectura para todos** (incluido admin) — nadie edita el historial de auditoría directamente, solo el trigger de la sección D.2 escribe ahí.
- El **worker de ingesta** corre con un rol técnico `ingestion_svc` distinto de los 4 roles de usuario, con `BYPASSRLS` y permisos de INSERT/UPDATE sobre `treasury.email_invoice`, `treasury.erp_pending`, `treasury.erp_paid`, `rebate.invoice` y nada más.
- **Corrección 2026-07-01 (durante Tarea 5, revisión de Diego):** la implementación inicial de `auth.app_user` (migración `008_rls.sql`) agregó, sin autorización, policies de auto-acceso (`app_user_self_select`/`self_update`) que permitían a cualquier usuario ver/editar su propia fila. Eso desviaba de esta matriz (que dice "-" para no-admin, sin excepciones) y nada en la app dependía de ello. Se revirtió en `009_remove_app_user_self_access.sql` y se verificó empíricamente que gerencia vuelve a obtener 0 filas (no su propia fila) al consultar `auth.app_user`. Si en el futuro se necesita una pantalla de "mi perfil", se diseña y documenta explícitamente en ese momento, no se reintroduce silenciosamente.

---

## D. Fixes técnicos obligatorios al plan original

Cada uno de estos puntos debe estar resuelto **antes de terminar la Fase 0**. No son opcionales.

### D.1 Capa FastAPI para lógica de negocio pesada

El plan original pone toda la lógica en funciones PL/pgSQL expuestas como `rpc/` de PostgREST. Para agregaciones simples funciona; para el motor de rebate (recomposición de 9 meses, bono de estacionalidad, escalas con corte por fecha) y el armado de lotes optimizados, las funciones SQL se vuelven ilegibles e imposibles de testear.

**Decisión**: se agrega un servicio **FastAPI ligero** (Python 3.12) como contenedor separado, con los siguientes endpoints:

- `POST /rebate/recompute` — recalcula rebate para un proveedor y periodo dado.
- `POST /payments/optimize` — optimizador de descuentos (ver E.1).
- `POST /payments/build-lot` — arma un lote con validación transaccional.
- `GET /forecast/cashflow` — cash flow proyectado (ver E.4).
- `POST /anomaly/score-invoice` — score de anomalía sobre una factura (ver E.3).

Next.js llama a FastAPI vía sus Route Handlers server-side (nunca desde el navegador). PostgREST sigue siendo el 80% del tráfico (CRUD y lecturas). FastAPI comparte pool de conexiones con Postgres pero corre en su propio contenedor con `mem_limit: 256m`.

**Ventaja**: los tests unitarios de reglas de negocio se escriben en `pytest` (Diego ya tiene músculo de bot Deriv y Bigotes), no en SQL.

### D.2 Audit trail en `treasury.master_invoice`

Tabla adicional obligatoria:

```sql
CREATE TABLE audit.master_invoice_history (
    history_id       bigserial PRIMARY KEY,
    invoice_key      text NOT NULL,
    changed_at       timestamptz NOT NULL DEFAULT now(),
    changed_by       uuid REFERENCES auth.app_user(id),
    operation        text NOT NULL CHECK (operation IN ('INSERT','UPDATE','DELETE')),
    old_row          jsonb,
    new_row          jsonb,
    changed_fields   text[]
);
CREATE INDEX idx_master_invoice_history_key ON audit.master_invoice_history (invoice_key, changed_at DESC);
```

Trigger `AFTER INSERT OR UPDATE OR DELETE` sobre `treasury.master_invoice` que popula esta tabla. Contabilidad tiene una vista dedicada `audit.v_master_invoice_timeline` que reconstruye la línea de tiempo de cualquier factura. **Sin esto no se puede auditar por qué una factura pasó a exclusión o cambió de estado — inaceptable para datos financieros.**

Mismo patrón (más ligero) para `treasury.payment_lot`, `audit.invoice_exclusion` y `rebate.*_budget`.

### D.3 Materialized view en vez de columnas denormalizadas con triggers

El plan original mete `email_pago`, `email_cc`, `contacto_pagos`, etc. como columnas en `treasury.master_invoice` sincronizadas por trigger desde `providers.provider`. Esto genera problemas de consistencia (¿qué pasa si el trigger falla? ¿si se cambia el proveedor?).

**Alternativa correcta**: `CREATE MATERIALIZED VIEW treasury.mv_master_invoice_enriched AS SELECT m.*, p.email_pago, p.email_cc, ... FROM treasury.master_invoice m LEFT JOIN providers.provider p ON m.proveedor_norm = p.nombre_normalizado`. Refresh incremental con `REFRESH MATERIALIZED VIEW CONCURRENTLY` disparado por el worker cada N minutos o después de cambios sobre `providers.provider`. Sin locks de lectura, consistencia natural, cero magia de triggers.

Ajusta la sección 11.3 de la Parte 1: **quitar** las columnas `email_pago`, `email_cc`, `email_alertas`, `contacto_pagos`, `condiciones_comerciales`, `activo` de la tabla `treasury.master_invoice`. Dejarlas solo en la MV.

### D.4 JWT con refresh tokens y revocación server-side

El plan original menciona NextAuth con JWT pero no menciona rotación, refresh, ni revocación. Para datos financieros con 4 usuarios reales esto es crítico.

**Requerimientos**:
- Access token con expiración corta (15 minutos).
- Refresh token con expiración larga (24 horas) almacenado como cookie httpOnly + secure + SameSite=Strict.
- Tabla `auth.revoked_token` para revocación server-side. Logout inserta el `jti` del refresh token ahí.
- Middleware de Next.js valida contra la tabla en cada request de refresh (query indexado, latencia despreciable).
- Rotación automática de refresh token en cada uso (mitiga replay).

### D.5 Observability desde el día uno

Ausente en el plan original. Sin esto, cuando algo falle en producción el equipo estará ciego.

**Stack mínimo**:
- **Sentry self-hosted** (ya tienes el patrón operativo). Errores de Next.js, del worker Python y de FastAPI. Alertas por email a `admin`.
- **Logs estructurados** (JSON) en los 3 servicios. Formato: `{timestamp, service, level, user_id, request_id, event, ...}`.
- **Métricas básicas de negocio** expuestas como endpoints `/metrics` (Prometheus format): latencia p50/p95/p99 de PostgREST y FastAPI, facturas procesadas por hora por el worker, correos enviados/fallidos por SendGrid, tamaño de la cola de exclusiones pendientes.
- **Health checks** en los 3 servicios (`/health` — retorna 200 con `{db: ok, imap: ok, dropbox: ok}`). Coolify Sentinel los consume y alerta.

### D.6 Backup externo a DO Spaces

El backup nativo de Coolify vive en el mismo droplet. Si el droplet falla (o alguien lo compromete, como pasó con Begranda), no sirve. Para datos financieros esto es obligatorio.

**Configuración**:
- Cron diario a las 03:00 hora local: `pg_dump` de la database `pagos_proveedores` → comprimido → subido a bucket `pagos-proveedores-backups.nyc3` en DO Spaces (mismo patrón que usas para catálogo de Bigotes).
- Retención de 30 días, con lifecycle rule del bucket para borrar automáticamente.
- **Verificación mensual**: primer día del mes, cron descarga el backup más reciente, lo restaura en una database `pagos_proveedores_verify` en el mismo cluster (baja carga a esa hora), corre `SELECT COUNT(*) FROM ... ` sobre las 5 tablas críticas, borra la database de verificación, envía email de "backup verificado OK" al admin. Si falla, alerta ruidosa.
- **WAL archiving semanal** a Spaces para permitir point-in-time recovery si algún día se necesita.

### D.7 IMAP IDLE en vez de cron

El plan original hereda el patrón cron de la Streamlit actual. Con IMAP IDLE (soportado por Gmail), la ingesta de facturas nuevas pasa de "cada N minutos" a "casi tiempo real" (<30 segundos desde que llega el correo hasta que la factura aparece en la app).

**Implementación**: el worker Python mantiene una conexión IDLE persistente a Gmail. Cuando IDLE notifica un correo nuevo, procesa. Supervisor externo (systemd o el propio Docker healthcheck) reinicia el proceso si la conexión se cae. Fallback: cron de respaldo cada 15 minutos por si IDLE queda colgado silenciosamente.

Cambia la sección 4.1 de la Parte 1: el worker ya no es solo "cron cada N minutos", es un daemon con conexión persistente + cron de respaldo.

### D.8 Concurrencia en armado de lotes

El plan original no aborda qué pasa si tesorería y contabilidad (que tienen los mismos permisos operativos) intentan meter la misma factura en dos lotes distintos simultáneamente.

**Solución**: el endpoint `POST /payments/build-lot` de FastAPI abre una transacción con `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE`, hace `SELECT ... FOR UPDATE` sobre las facturas que se van a incluir, verifica que ninguna tenga `lote_id IS NOT NULL`, y solo entonces crea el lote. Si falla la serialización, retorna 409 Conflict con detalle de cuáles facturas ya fueron tomadas por otro proceso. El frontend muestra un mensaje humano y refresca.

---

## E. Features MVP innovadores (obligatorios para Fase 3/4)

Estos son los features que convierten esto en "app de próxima generación" y no un rewrite fiel. Todos son técnicamente accesibles con el stack propuesto y sin LLM.

### E.1 Optimizador de descuentos con presupuesto de caja

**Problema hoy**: tesorería selecciona a mano qué facturas pagar. Deja plata sobre la mesa por descuentos no capturados.

**Solución**: endpoint FastAPI `POST /payments/optimize` que recibe `{cash_available: 50000000, horizon_days: 15, priority: "max_discount"}` y retorna el subset óptimo de facturas a pagar. Es un **problema de mochila 0/1 con restricción de fecha** (cada factura tiene un descuento vigente hasta X fecha, un valor a pagar, y un ahorro potencial). Se resuelve con programación dinámica clásica o `scipy.optimize.linprog` — para el volumen de facturas de Ferreinox (cientos, no miles), resuelve en milisegundos.

**UI**: botón "Sugerir plan óptimo" en el Planificador de Pagos. Tesorería revisa, ajusta manualmente si quiere, confirma. Se muestra "ahorro capturado: $X vs ahorro máximo teórico: $Y" para dar contexto de qué tanto se aprovechó.

### E.2 Predictor de rebate en tiempo real

**Problema hoy**: los cálculos de rebate son reactivos — se ve cuánto se lleva del trimestre, no cuánto falta para el siguiente escalón ni si es viable.

**Solución**: en cada tarjeta de periodo (mensual/trimestral/bimestral/semestral según proveedor), agregar:

- **Gap al siguiente escalón**: "faltan $12.4M para pasar de Escala 1 a Escala 2".
- **Días restantes en el periodo**: "quedan 18 días de 30".
- **Ritmo requerido**: "necesitas $688K/día promedio, ritmo actual: $420K/día".
- **Viabilidad estimada**: "viable / justo / no alcanzable" — basado en ritmo histórico de los últimos 3 periodos comparables.
- **ROI marginal del escalón**: "capturar Escala 2 = +$3.2M adicionales de rebate".

Todo esto se calcula con SQL puro sobre las tablas de `rebate.*`. Sin LLM. Convierte el módulo de Rebate de reporte a herramienta de decisión comercial.

### E.3 Anomaly detection heurístico sobre facturas nuevas

**Problema hoy**: facturas con valores fuera de patrón (posible error del proveedor o cambio no comunicado) se procesan sin alerta.

**Solución sin LLM**: cada factura que entra por el worker se evalúa contra el histórico del proveedor:

- **Valor**: ¿está a más de 2 desviaciones estándar del valor típico del proveedor en los últimos 12 meses?
- **Frecuencia**: ¿este proveedor factura típicamente semanal/mensual? ¿esta factura llega muy cerca de la anterior?
- **IVA calculado**: ¿el `valor_iva / valor_base` es coherente con el porcentaje esperado (19%, 5%, 0%)?
- **Numeración**: ¿el número de factura salta bruscamente respecto de la última recibida del mismo proveedor?

Cada regla genera un score 0-1. Score agregado > 0.6 marca la factura con flag `anomaly_flagged=true` y motivo. Aparece en un panel dedicado de contabilidad ("facturas para revisar") y en la vista de detalle con badge visible.

Todo con SQL + Python en el worker. Sin LLM en MVP.

### E.4 Cash flow forecast + calendar heatmap

**Problema hoy**: gerencia pregunta "¿cuánto necesitamos el 15?" y hay que armar el cálculo a mano.

**Solución**: vista `treasury.v_cashflow_forecast` que proyecta por día las próximas 8 semanas:
- Salida de caja esperada por día (facturas con fecha programada de pago o vencimiento).
- Ventana de descuento por pronto pago activa por día (oportunidades de captura).
- Riesgo por día (facturas críticas cerca de vencer).

**UI**: heatmap tipo calendario GitHub, coloreado por criticidad. Un click sobre un día expande la lista de facturas de ese día. Gerencia lo ve en su dashboard resumen; tesorería lo ve en el Planificador. Es una de las 3 vistas del dashboard de gerencia.

### E.5 Presupuestos versionados (no editables in-place)

La Parte 1 propone `rebate.pintuco_budget` como tabla editable. Problema: si cambia una meta a mitad de año, se pierde el histórico de qué meta estaba vigente cuando se calculó un rebate del trimestre anterior — no se puede auditar retroactivamente.

**Fix**: agregar columnas `valid_from date NOT NULL` y `valid_to date` (nullable, `NULL` = vigente). Las ediciones no hacen `UPDATE`, hacen `UPDATE ... SET valid_to = CURRENT_DATE WHERE valid_to IS NULL AND mes_num = ?` seguido de `INSERT` de la nueva versión. Las vistas de rebate filtran por `date_valued BETWEEN valid_from AND COALESCE(valid_to, 'infinity')`.

Aplica igual a `abracol_budget` y `goya_budget`.

### E.6 Provider intelligence dashboard

**Problema hoy**: el "Maestro de Proveedores" es un catálogo pasivo. No cuenta la historia comercial.

**Solución**: perfil por proveedor con:
- **Comportamiento de pago histórico**: cuánto tardamos en promedio en pagarles, cuántas veces capturamos descuento, cuántas veces lo perdimos.
- **Oportunidades no capturadas**: "$X en descuentos perdidos en los últimos 12 meses". Este número solo es visible y va a ser incómodo — que sea incómodo es el punto.
- **Concentración**: % de nuestro volumen de compras que representa este proveedor.
- **Calidad de datos**: score 0-100 basado en completitud del maestro (¿tiene email de pago? ¿NIF? ¿contacto?).
- **Timeline de últimos 12 meses**: gráfico de facturación mes a mes.

Todo con vistas SQL. Convierte el módulo de Creación de Proveedores en herramienta comercial real.

### E.7 PWA con notificaciones push informativas

Ya definido en la sección B punto 7. Detalle técnico:

- **PWA estándar**: `manifest.json`, service worker, instalable en iOS/Android/desktop.
- **Web Push API** (nativa del navegador, sin Firebase Cloud Messaging obligatorio en el navegador — Chrome/Edge usan FCM por debajo pero es transparente).
- **Server-side**: librería `web-push` en Node dentro de un microservicio o dentro del propio Next.js. Cada evento operativo relevante (lote pagado, factura excluida, descuento aplicado, rebate cambia de escalón) dispara push a los suscriptores según su rol.
- **Tabla `auth.push_subscription`**: guarda el endpoint y las keys por usuario, se popula cuando el usuario autoriza notificaciones en el navegador.
- **Eventos que generan push** (v1):
  - Tesorería paga un lote → push a gerencia, contabilidad, admin.
  - Contabilidad marca una factura como excluida → push a tesorería, admin.
  - Un proveedor de rebate pasa de escalón → push a gerencia, contabilidad, admin.
  - Anomalía detectada en factura nueva → push a contabilidad, admin.
  - Backup diario falla → push a admin.
- **No hay push que requiera acción bloqueante en v1**. Son todas informativas.

---

## F. Features v2 (fuera del MVP, planeados)

Se documentan aquí para no perderlos, pero **no se construyen en Fase 3/4**.

### F.1 Copilot NL2SQL con Claude Haiku 4.5

Chat sobre los datos en lenguaje natural. "¿Cuánto le debo a Pintuco vencido más de 30 días?" → LLM genera SQL validado contra whitelist de tablas/vistas → ejecuta → renderiza resultado. Costo estimado con Haiku: ~$5-15 USD/mes para el volumen de 4 usuarios. Se agrega cuando la app esté estable en producción y haya feedback real de qué preguntas hacen los usuarios (para calibrar prompt).

### F.2 Drafts de correo generados por IA

El correo de pago a proveedores hoy usa template rígido. Con LLM se puede personalizar por historial de relación, destacar el descuento capturado, ajustar tono. Tesorería revisa y envía. Bloqueado hasta v2 por dependencia con F.1 (misma infra de LLM).

---

## G. Ajustes al despliegue Coolify (reemplaza la sección 7 de la Parte 1)

Orden de creación en el nuevo proyecto Coolify "Pagos Proveedores":

1. **Verificar el cluster Postgres existente** (el de Optiferre). Confirmar que su versión es Postgres 15 o superior. Si es 14 o menor, planificar upgrade antes o crear cluster nuevo dedicado (excepción a la decisión B.1). *(✅ Verificado: PostgreSQL 15.17, con acceso superusuario — ver nota al inicio de esta Parte 2)*.

2. **Crear database dentro del cluster**:
   ```sql
   CREATE DATABASE pagos_proveedores OWNER pagos_proveedores_owner;
   REVOKE ALL ON DATABASE pagos_proveedores FROM PUBLIC;
   ```
   Roles: `pagos_proveedores_owner` (dueño, para migraciones), `authenticator` (PostgREST), `ingestion_svc` (worker), `fastapi_svc` (FastAPI), `web_anon`, `web_authenticated`. Cada rol con permisos mínimos.

3. **Recurso Coolify — PostgREST dedicado** (contenedor nuevo, no compartido con Optiferre). Docker Compose personalizado con `mem_limit: 128m`, `cpus: 0.5`, red interna, sin puerto público. Apunta a la nueva database vía `PGRST_DB_URI`.

4. **Recurso Coolify — FastAPI**. Contenedor Python 3.12, `mem_limit: 256m`, `cpus: 0.5`. Red interna.

5. **Recurso Coolify — Ingestion Worker**. Contenedor Python 3.12 con conexión IMAP IDLE persistente + cron respaldo. `mem_limit: 256m`, `cpus: 0.5`. Necesita `/tmp` para descomprimir ZIP.

6. **Recurso Coolify — Push Notification Service** (opcional: puede ir dentro de Next.js). `mem_limit: 64m`.

7. **Recurso Coolify — Next.js app**. Desde repo `pagos-proveedores-web`. `mem_limit: 512m`, `cpus: 1.0`. Dominio `proveedores.datovatenexuspro.com` con TLS automático.

8. **Recurso Coolify — Backup service**. Contenedor con cron que ejecuta `pg_dump` diario a Spaces (ver D.6). `mem_limit: 128m`.

**Consumo total estimado**: ~1.5 GB RAM, ~3 vCPU en límites (no reservas — el servidor tiene 2 vCPU físicos; los `cpus:` de Docker son techos de ráfaga, no reservas, igual que ya opera el resto de los ~14 proyectos del servidor). Con el servidor ya saneado (2.9GB libres) hay margen suficiente en RAM. **Bloqueante persistente**: si en el momento del despliegue el servidor vuelve a tener <500MB libres o load average >4, pausar y diagnosticar antes de continuar.

---

## H. Fases de migración (ajuste a la Parte 1)

Las fases 0, 1, 2 y 5 quedan como en la Parte 1. **Fase 3 y 4 se rediseñan** para incorporar los features MVP innovadores:

**Fase 3 — Next.js modo lectura + PWA base + features de análisis**
- Dashboards ejecutivos (gerencia, tesorería, contabilidad).
- Vistas de rebate con predictor en tiempo real (E.2).
- Cash flow forecast + calendar heatmap (E.4).
- Provider intelligence dashboard (E.6).
- PWA instalable, notificaciones push implementadas pero no activadas todavía (se prueban con usuario admin únicamente).
- Audit trail visible para contabilidad (D.2).

**Fase 4 — Next.js modo escritura + optimizadores + activación de notificaciones**
- Optimizador de descuentos (E.1).
- Anomaly detection en worker + panel de revisión para contabilidad (E.3).
- Edición de presupuestos versionada (E.5).
- Armado de lotes con `SELECT FOR UPDATE` (D.8).
- Activación de push notifications para gerencia y contabilidad.
- Congelamiento de escritura en Streamlit.

**Fase 5 — Corte** (igual a la Parte 1).

---

## I. Checklist de arranque de Fase 0 (lo que se hace primero, en este orden)

1. Crear el repo `pagos-proveedores-web` (Next.js 15 + TypeScript, App Router).
2. Crear el repo `pagos-proveedores-worker` (Python 3.12, worker + FastAPI en el mismo repo con dos entrypoints).
3. Verificar versión Postgres del cluster existente. Si <15, escalar antes. *(✅ Hecho: 15.17)*.
4. Crear database `pagos_proveedores` y los roles listados en G.2.
5. Ejecutar el DDL completo (sección 11 de la Parte 1 + tabla `audit.master_invoice_history` de D.2 + campos `valid_from`/`valid_to` en tablas de rebate de E.5 + tabla `auth.push_subscription` + tabla `auth.revoked_token` de D.4 + quitar columnas denormalizadas según D.3 + crear la materialized view).
6. Cargar policies RLS según la matriz de la sección C.
7. Provisionar los 6 servicios Coolify (sección G).
8. Configurar backup automático a Spaces (D.6).
9. Configurar Sentry self-hosted + health checks (D.5).
10. Health checks de los 3 servicios verdes en Coolify Sentinel.
11. Cerrar Fase 0. Pasar a Fase 1 (migración de datos desde Sheets + Dropbox).

---

## J. Puntos que quedan explícitamente fuera de alcance del MVP

Para evitar scope creep durante la construcción:

- Integración con Begranda (ERP no expone API).
- WhatsApp Business (descartado).
- LLM/Copilot NL2SQL (v2).
- Drafts de correo generados por IA (v2).
- Multi-tenant / multi-empresa (proyecto single-tenant, solo Ferreinox).
- Firma digital / certificados sobre lotes (tesorería tiene autonomía, no hay flujo de aprobación).
- Mobile app nativa (PWA es suficiente).
- Reportes descargables en PDF con branding (Excel es suficiente para MVP, PDF va a v2).

---

**Fin del addendum.** Con este documento + la Parte 1, no quedan decisiones abiertas para arrancar Fase 0. Cualquier ambigüedad futura debe resolverse verificando: (1) qué dice este addendum, (2) qué dice la Parte 1, (3) si ninguno lo cubre, se documenta la decisión antes de codificar.

---
---

# Parte 3 — Prompt operativo de ejecución: Fase 0 núcleo

> **Nota de consolidación (2026-07-01):** el prompt original hace referencia a dos archivos separados, `Plan_Migracion_Streamlit_a_Next.md` y `ADDENDUM_PLAN_MIGRACION.md`. **Ambos ya están consolidados en este único archivo** (`MIGRACION_NEXTJS_POSTGREST.md` — Parte 1 = plan original, Parte 2 = addendum, Parte 3 = este prompt operativo). Cualquier sesión de Claude Code que reciba este prompt debe leer `MIGRACION_NEXTJS_POSTGREST.md` completo, no buscar los dos archivos por separado.
>
> **Nota de repos (2026-07-01):** los nombres de repo reales creados por Diego son distintos a los usados como placeholder en el prompt original (`pagos-proveedores-web` / `pagos-proveedores-worker`). Los reales son:
> - `https://github.com/DiegoMao201/proveedores_pagos.git` → **repo web** (Next.js, antes referenciado como `pagos-proveedores-web`)
> - `https://github.com/DiegoMao201/proovedores_work.git` → **repo worker** (Python worker + FastAPI stub, antes referenciado como `pagos-proveedores-worker`)
>
> Este mapeo es una inferencia por nombre (ambos repos están vacíos, sin commits, así que no se pudo verificar por contenido) — **pendiente de confirmación explícita de Diego** antes de empezar a hacer push de código. Todas las referencias de este prompt a `pagos-proveedores-web`/`pagos-proveedores-worker` deben leerse como los nombres reales de arriba.

> **Cómo usar este prompt**: pegar completo a Claude Code en VS Code como primer mensaje de la sesión de trabajo del proyecto Pagos Proveedores Next.js. Este prompt asume que Claude Code ya tiene acceso al repo `Pagos_Proveedores` (Streamlit actual), a los dos repos nuevos `proveedores_pagos` y `proovedores_work`, al servidor Coolify vía SSH, y a la API de Coolify.

## Contexto obligatorio antes de leer este prompt

Existen **dos documentos previos** que son la fuente de verdad para este proyecto y **debes leerlos completos antes de proponer cualquier cambio** (en la práctica: son las Partes 1 y 2 de este mismo archivo):

1. Plan original — arquitectura, DDL, RLS y fases (Parte 1 de este archivo).
2. Addendum — revisión externa con decisiones finales, fixes técnicos (D.1-D.8) y features MVP (E.1-E.7) (Parte 2 de este archivo).

Este prompt **complementa esos dos documentos con las decisiones más recientes tomadas después del addendum**. Donde haya conflicto: este prompt manda sobre el addendum (Parte 2), y el addendum manda sobre el plan original (Parte 1).

Si no tienes acceso a los dos documentos anteriores, detente y pídelos antes de continuar.

## Decisiones finales actualizadas (post-addendum)

Estas decisiones cierran los últimos puntos abiertos:

### 1. PostgREST dedicado, no compartido con Optiferre

Se despliega un contenedor PostgREST nuevo, exclusivo para este proyecto. NO se reutiliza el PostgREST del proyecto Optiferre/Servicio al Cliente Ferreinox.

**Razones técnicas** (no negociables):
- Aislamiento de JWT secret: cada proyecto rota su llave independientemente. Si Optiferre necesita rotar por cualquier motivo, no debe caer Pagos Proveedores.
- Aislamiento de `PGRST_DB_SCHEMAS`: el PostgREST de Pagos Proveedores solo expone los schemas de esta app (`providers,treasury,rebate,audit,auth`). No debe poder tocar ninguna otra base ni schema del cluster.
- Aislamiento operativo: reinicios, actualizaciones y errores de un proyecto no impactan al otro.
- Costo: ~20MB de RAM adicionales. Despreciable frente al margen actual del servidor (~2.9GB libres).

### 2. Backups automatizados a DO Spaces — POSTPUESTOS

Los backups automatizados a DigitalOcean Spaces (sección D.6 del addendum) **quedan postpuestos**. Se implementan **~30 días después de que la app esté en producción operativa y estable**. Durante ese periodo, Google Sheets sigue siendo la fuente de verdad paralela (Fase 2-4 del plan original), lo cual sirve como respaldo natural.

**No implementar en Fase 0 ni fast-follow**. Cuando llegue el momento, se documenta como una tarea separada.

### 3. GlitchTip, no Sentry self-hosted

Cuando llegue la fase de observabilidad (fast-follow, no Fase 0 núcleo), se usa **GlitchTip** en vez de Sentry self-hosted. Motivo: Sentry self-hosted requiere 4+ GB de RAM (Postgres + Redis + Clickhouse + Kafka + workers). GlitchTip es SDK-compatible con Sentry y corre en ~250-350MB con un solo contenedor + su propio Postgres chico.

**No implementar en Fase 0 núcleo**. Solo dejar los logs de los 3 servicios (Next.js, worker, futuro FastAPI) escribiendo en **formato JSON estructurado a stdout** desde el día uno, para que cuando llegue GlitchTip ya sean parseables sin cambiar código.

### 4. IMAP simple con cron en Fase 0, IDLE en fast-follow

El worker de Fase 0 usa cron simple (cada 5-10 minutos revisa Gmail). El upgrade a IMAP IDLE (D.7 del addendum) queda en fast-follow. Motivo: reducir superficie de bugs en Fase 0.

### 5. FastAPI fuera de Fase 0 núcleo

Fase 0 solo tiene PostgREST + Next.js + worker. FastAPI (para lógica de negocio pesada, D.1 del addendum) llega en Fase 3/4 junto con los features que la requieren (optimizador de descuentos, cash flow forecast, etc.).

### 6. PWA + Web Push fuera de Fase 0 núcleo

Van en fast-follow con el resto de features MVP innovadores. En Fase 0 el frontend es una Next.js estándar, no PWA todavía.

### 7. Los 4 fixes que SÍ van en el DDL de Fase 0 núcleo

Estos son gratis hacerlos bien de entrada y carísimos migrar después. Van desde el primer commit del schema:

- **D.2 Audit trail**: tabla `audit.master_invoice_history` + trigger. Sin excepciones.
- **D.3 Materialized view**: `treasury.mv_master_invoice_enriched` para el join con proveedores. La tabla `treasury.master_invoice` NO lleva las columnas denormalizadas `email_pago`, `email_cc`, `email_alertas`, `contacto_pagos`, `condiciones_comerciales`, `activo` que sí aparecen en la Parte 1 sección 11.3. Quitarlas.
- **D.4 JWT con refresh + revocación**: tabla `auth.revoked_token`. NextAuth con access token de 15 minutos y refresh token de 24 horas (cookie httpOnly + secure + SameSite=Strict). Logout revoca en la tabla, no solo borra cookie.
- **E.5 Presupuestos versionados**: las 3 tablas `rebate.pintuco_budget`, `abracol_budget`, `goya_budget` llevan columnas `valid_from date NOT NULL` y `valid_to date` (nullable, NULL = vigente). Las vistas de rebate filtran por rango.

## Tareas Fase 0 núcleo — ejecutar en este orden

### Tarea 0 — Verificación previa (ANTES DE TOCAR NADA)

Ejecutar y reportar los resultados exactos con timestamp:

```bash
# Salud del servidor
free -h
uptime
df -h /
df -h /mnt/docker_data
docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}'

# Cluster Postgres existente (buscar el contenedor de la DB de Optiferre)
docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}' | grep -i postgres
# Luego ejecutar SELECT version() dentro del contenedor identificado
docker exec <nombre-contenedor-postgres> psql -U postgres -c 'SELECT version();'
docker exec <nombre-contenedor-postgres> psql -U postgres -c '\l'
```

**Criterios para continuar**:
- RAM disponible > 500MB.
- Load average < 4 en las últimas 5 minutos.
- Postgres 15 o superior. Si es 14 o menor, detener y consultar a Diego antes de proceder.
- Disco `/mnt/docker_data` con >20GB libres.

Si algún criterio falla: **detente y reporta a Diego con evidencia empírica antes de continuar**. No intentes resolver problemas del servidor por tu cuenta.

### Tarea 1 — Aprovisionamiento Postgres

Dentro del cluster Postgres existente, crear:

```sql
-- Database aislada
CREATE DATABASE pagos_proveedores 
  ENCODING 'UTF8' 
  LC_COLLATE 'en_US.UTF-8' 
  LC_CTYPE 'en_US.UTF-8';

REVOKE ALL ON DATABASE pagos_proveedores FROM PUBLIC;

-- Roles técnicos (contraseñas se generan con openssl rand y se guardan en Coolify env vars, JAMÁS en el repo)
CREATE ROLE pagos_proveedores_owner LOGIN PASSWORD '<generar>';   -- para migraciones DDL
CREATE ROLE authenticator LOGIN PASSWORD '<generar>' NOINHERIT;   -- para PostgREST
CREATE ROLE ingestion_svc LOGIN PASSWORD '<generar>';             -- para el worker Python
CREATE ROLE web_anon NOLOGIN;                                      -- sin sesión
CREATE ROLE web_authenticated NOLOGIN;                             -- cualquier usuario logueado
GRANT web_anon, web_authenticated TO authenticator;

-- El rol owner es dueño de todos los objetos que se creen
GRANT ALL ON DATABASE pagos_proveedores TO pagos_proveedores_owner;
```

**Ninguno de estos roles debe tener CONNECT sobre otras databases del cluster**. Verificar con `\du+` después de crearlos.

Guardar las 3 contraseñas generadas en el gestor de secretos de Coolify (variables de entorno del proyecto nuevo). Reportar a Diego que fueron generadas, sin incluir los valores en ningún log o commit.

### Tarea 2 — DDL completo (con los 4 fixes incorporados)

Crear el schema completo siguiendo:
- Sección 11 de la Parte 1 (schemas providers, treasury, rebate, audit, auth).
- Modificaciones de la Parte 2/addendum:
  - Quitar columnas denormalizadas de `treasury.master_invoice` (D.3).
  - Crear `treasury.mv_master_invoice_enriched` como materialized view (D.3).
  - Crear `audit.master_invoice_history` con su trigger AFTER INSERT/UPDATE/DELETE (D.2).
  - Agregar `valid_from`/`valid_to` a las 3 tablas de budget de rebate (E.5).
  - Crear `auth.revoked_token` para revocación de JWT (D.4).
  - Crear `auth.push_subscription` (aunque push sea fast-follow, la tabla es barata y evita migración futura).

Formato: migraciones SQL numeradas (`001_schemas.sql`, `002_providers.sql`, `003_treasury.sql`, etc.) en el repo `proovedores_work` (bajo `db/migrations/`), aplicables con un script simple. NO usar ORM ni herramientas complejas de migración en esta fase.

**Cada migración debe ser idempotente** (`CREATE ... IF NOT EXISTS`, `DO $$ BEGIN ... END $$` para verificar antes de alterar).

Después de aplicar el DDL, verificar con:
```sql
\dn                                    -- lista de schemas creados
\dt providers.*                        -- tablas por schema
\dt treasury.*
\dt rebate.*
\dt audit.*
\dt auth.*
SELECT count(*) FROM pg_policies;      -- policies RLS activas
SELECT count(*) FROM pg_trigger WHERE tgname LIKE '%history%';  -- triggers de audit
```

Reportar los conteos como evidencia empírica.

### Tarea 3 — Policies RLS según la matriz de la sección C del addendum (Parte 2)

Para cada tabla mencionada en la matriz, crear las policies exactas. La matriz es la fuente de verdad.

**Test obligatorio antes de dar Tarea 3 por terminada**:

```sql
-- Crear un usuario de prueba de cada rol
-- Simular sesión con cada uno
SET LOCAL request.jwt.claims TO '{"role":"lectura","user_id":"..."}';
SELECT count(*) FROM treasury.master_invoice;  -- debe fallar o retornar 0
-- (esta tabla no debe ser accesible directo por gerencia, solo vías vistas resumen)

SET LOCAL request.jwt.claims TO '{"role":"gerencia","user_id":"..."}';
INSERT INTO treasury.payment_lot ...;  -- debe fallar
```

Reportar los resultados de los tests.

### Tarea 4 — PostgREST dedicado en Coolify

Crear proyecto Coolify **"Pagos Proveedores"** (nuevo, separado de Optiferre).

Recurso Docker Compose para PostgREST:

```yaml
services:
  postgrest:
    image: postgrest/postgrest:v12.2.8
    environment:
      PGRST_DB_URI: postgres://authenticator:${PGRST_AUTHENTICATOR_PASSWORD}@${PG_HOST}:5432/pagos_proveedores
      PGRST_DB_SCHEMAS: providers,treasury,rebate,audit,auth
      PGRST_DB_ANON_ROLE: web_anon
      PGRST_JWT_SECRET: ${JWT_SECRET}    # openssl rand -base64 48
      PGRST_JWT_SECRET_IS_BASE64: "true"
      PGRST_DB_MAX_ROWS: "5000"
      PGRST_LOG_LEVEL: "info"
    mem_limit: 128m
    cpus: 0.5
    security_opt: ["no-new-privileges:true"]
    restart: unless-stopped
    networks: [pagos_proveedores_internal]
    healthcheck:
      test: ["CMD-SHELL", "wget --spider -q http://localhost:3000/ready || exit 1"]
      interval: 30s
      timeout: 5s
      retries: 3

networks:
  pagos_proveedores_internal:
    driver: bridge
```

**Requisitos**:
- Sin puerto público. Solo accesible desde la red interna de Docker.
- JWT secret propio, generado con `openssl rand -base64 48` y guardado en env vars de Coolify.
- Health check activo, consumido por Coolify Sentinel.
- Después del deploy, verificar que responde `GET /` desde otro contenedor de la misma red y NO desde internet.

### Tarea 5 — Next.js shell con login funcional

Repo: `proveedores_pagos`.

Stack:
- Next.js 15 (App Router) + TypeScript
- Tailwind CSS + shadcn/ui
- NextAuth v5 (Auth.js) con Credentials provider
- JWT + refresh tokens (D.4 del addendum)

Estructura mínima según sección 13 de la Parte 2, pero solo lo estrictamente necesario para Fase 0:

```text
proveedores_pagos/
  app/
    login/page.tsx
    (dashboard)/
      layout.tsx
      page.tsx                    ← placeholder por ahora
    api/
      auth/[...nextauth]/route.ts
  lib/
    postgrest.ts                  ← fetch wrapper server-only
    auth.ts                       ← config NextAuth
  middleware.ts                   ← protección de rutas + refresh silent
```

**Funcionalidad mínima Fase 0**:
- Login con email + password contra tabla `auth.app_user`.
- Password hasheado con bcrypt (cost factor 12).
- Access token 15 min, refresh 24h.
- Logout revoca refresh token en `auth.revoked_token`.
- Middleware valida rol y bloquea acceso a rutas no autorizadas.
- Dashboard placeholder solo muestra "Hola {nombre}, tu rol es {rol}".

**Usuarios reales a crear** (4 personas):
- 1 admin: Diego. Contraseña actual de la Streamlit migrada (Diego la cambia después desde la UI en fast-follow).
- 1 tesorería, 1 contabilidad, 1 gerencia: Diego provee los emails y contraseñas iniciales. Si no los tiene aún, dejar los usuarios sin crear y documentar como pendiente para fast-follow inmediato.

**Logs JSON estructurados desde el día uno**:
```typescript
// lib/logger.ts
console.log(JSON.stringify({
  timestamp: new Date().toISOString(),
  service: 'web',
  level: 'info',
  request_id: '...',
  user_id: '...',
  event: 'login_success',
  ...
}));
```

Formato reutilizado idéntico en el worker Python.

### Tarea 6 — Worker Python base

Repo: `proovedores_work`.

Estructura según sección de repos del último mensaje (dos entrypoints preparados, pero en Fase 0 solo el del worker; el de FastAPI queda con un stub por ahora):

```text
proovedores_work/
  common/
    db.py                    ← SQLAlchemy engine con connection pool
    ubl_parser.py            ← extraído de treasury_core.py
    supplier_norm.py         ← extraído de treasury_core.py
    settings.py              ← pydantic-settings, lee env vars
    logging_config.py        ← logs JSON estructurados
  worker/
    main.py                  ← entrypoint (schedule cada 5 min)
    imap_ingest.py           ← IMAP simple sin IDLE
    dropbox_sync.py
  api/
    main.py                  ← stub FastAPI (solo /health por ahora)
  db/
    migrations/              ← SQL numerados de Tarea 2
    apply_migrations.py      ← script simple para aplicarlas
  Dockerfile.worker
  Dockerfile.api
  pyproject.toml
```

**Funcionalidad Fase 0**:
- Conecta a Gmail vía IMAP (mismas credenciales que Streamlit actual, migradas a env vars de Coolify).
- Descarga adjuntos ZIP/XML de la carpeta `TFHKA/Recepcion/Descargados`.
- Parsea UBL reutilizando la lógica de `treasury_core.py` (no reescribir el parser).
- Escribe a `treasury.email_invoice` en Postgres vía SQLAlchemy usando el rol `ingestion_svc`.
- Descarga CSVs de Dropbox y sincroniza a `treasury.erp_pending` / `treasury.erp_paid`.
- Cron interno con `schedule` cada 5 minutos.
- Logs JSON estructurados a stdout.
- Health endpoint HTTP simple (puerto interno) que retorna estado de IMAP/Dropbox/DB.

**NO implementar en Fase 0**: IMAP IDLE, envío de correos SendGrid (queda para Fase 4), anomaly detection, actualización de `master_invoice` (esa lógica llega en Fase 1 con el script de migración de Sheets).

Recurso Coolify para el worker:
```yaml
services:
  ingestion-worker:
    build:
      context: .
      dockerfile: Dockerfile.worker
    environment:
      DATABASE_URL: postgres://ingestion_svc:${WORKER_DB_PASSWORD}@${PG_HOST}:5432/pagos_proveedores
      GMAIL_ADDRESS: ${GMAIL_ADDRESS}
      GMAIL_APP_PASSWORD: ${GMAIL_APP_PASSWORD}
      DROPBOX_APP_KEY: ${DROPBOX_APP_KEY}
      DROPBOX_APP_SECRET: ${DROPBOX_APP_SECRET}
      DROPBOX_REFRESH_TOKEN: ${DROPBOX_REFRESH_TOKEN}
    mem_limit: 256m
    cpus: 0.5
    security_opt: ["no-new-privileges:true"]
    tmpfs:
      - /tmp:noexec,nosuid,size=128m
    restart: unless-stopped
    networks: [pagos_proveedores_internal]
    healthcheck:
      test: ["CMD-SHELL", "wget --spider -q http://localhost:8080/health || exit 1"]
      interval: 60s
```

### Tarea 7 — Recurso Next.js en Coolify

Deploy del repo `proveedores_pagos`:
- `mem_limit: 512m`, `cpus: 1.0`.
- Dominio: `proveedores.datovatenexuspro.com` con TLS automático vía Traefik/Coolify.
- Variables de entorno: `DATABASE_URL` (para NextAuth), `NEXTAUTH_SECRET`, `POSTGREST_URL` (interno), `POSTGREST_JWT_SECRET` (mismo que PostgREST).

### Tarea 8 — Verificación final Fase 0 (checklist empírico)

Reportar a Diego evidencia empírica (comandos + outputs con timestamp) de cada uno de estos puntos:

1. `curl https://proveedores.datovatenexuspro.com/login` retorna la página de login.
2. Login con usuario admin funciona. Se recibe access token + refresh token.
3. Access token expira a los 15 minutos. Refresh silencioso funciona.
4. Logout revoca token: intentar reutilizar el refresh token después del logout debe fallar.
5. **(Actualizado, ver matriz simplificada en Parte 2 sección C)** Con JWT de rol `gerencia`, `SELECT * FROM treasury.master_invoice` vía PostgREST retorna filas (gerencia ya tiene RW igual que los demás roles humanos, salvo `auth.app_user`).
6. Con JWT de rol `lectura`/rol inexistente o sin JWT (`web_anon`), `SELECT * FROM treasury.master_invoice` retorna 401 o 0 filas (RLS activa contra acceso no autenticado).
7. El worker corre y escribe. Verificar con: `SELECT count(*), max(created_at) FROM treasury.email_invoice;` — debe haber filas nuevas de las últimas horas.
8. `SELECT count(*) FROM audit.master_invoice_history;` — cuando se haga la migración de datos en Fase 1 empezará a poblarse; por ahora debe existir con 0 filas y su trigger activo.
9. Health checks de los 3 servicios (Postgres, PostgREST, worker, Next.js) verdes en Coolify Sentinel.
10. Ningún puerto público expuesto excepto el 443 de Next.js.
11. `docker stats` muestra que los 4 contenedores nuevos respetan sus `mem_limit`.
12. Load average del servidor después de 24h con todo corriendo: < 4.

**Solo cuando los 12 puntos estén verdes con evidencia empírica, se cierra Fase 0 núcleo.**

## Reglas operativas obligatorias para toda la sesión

Estas reglas vienen de la experiencia previa con este servidor y de las memorias de Diego sobre patrones sistémicos observados. Son de cumplimiento estricto.

1. **Reportes empíricos, no optimistas**: cualquier afirmación sobre comportamiento del sistema debe ir acompañada de comando ejecutado + output con timestamp. Frases como "debería funcionar", "asumo que", "probablemente" no cuentan como verificación. Esta regla es crítica por patrón previo documentado.

2. **Nada de credenciales en commits**: contraseñas, tokens, JWT secrets — todo va a env vars de Coolify. Los repos son privados pero eso no autoriza commitear secretos. Si por accidente algo se commiteó, rotar inmediatamente.

3. **Ninguna acción irreversible sin confirmación**: `DROP DATABASE`, `DROP SCHEMA CASCADE`, borrado de contenedores con volúmenes, eliminación de proyectos Coolify — todo requiere confirmación explícita de Diego en el chat antes de ejecutar.

4. **Verificar impacto en el servidor antes de crear recursos**: cada recurso nuevo debe ir precedido de un `free -h` + `docker stats` inmediatamente antes de crearlo. Si el servidor está bajo presión, pausar.

5. **Un cambio, un commit, un mensaje claro**: no mezclar cambios grandes en un solo commit. Facilita rollback y auditoría.

6. **Aislar es más importante que rápido**: si dudas entre reutilizar algo existente o crear algo nuevo para este proyecto, la respuesta es crear nuevo. Regla especialmente relevante para PostgREST, roles Postgres y credenciales de acceso.

7. **RLS es la línea de defensa principal, no la aplicación**: si una policy RLS podría bloquear un acceso legítimo, el fix va en la policy o en la vista, nunca en el código de la app "bypasseando" con un rol superior.

8. **Las migraciones SQL son idempotentes y versionadas**: cada archivo `.sql` debe poder correrse dos veces sin romper. Nunca modificar una migración ya aplicada — se hace una nueva.

9. **No tomes decisiones de producto sin Diego**: si aparece una ambigüedad de negocio (¿este flujo cómo debe funcionar?), pregunta antes de codificar. Estás implementando, no diseñando.

10. **Al final de cada tarea, checkpoint**: reporte breve de qué se hizo, qué queda, qué necesita decisión de Diego. Sin este checkpoint no avanzas a la siguiente tarea.

## Repos GitHub (ya creados por Diego)

- `https://github.com/DiegoMao201/proveedores_pagos.git` (privado) — repo web
- `https://github.com/DiegoMao201/proovedores_work.git` (privado) — repo worker

Ambos van bajo la cuenta personal, no bajo organización.

## Primer paso concreto

Empezar por **Tarea 0 — Verificación previa**. No hacer ninguna otra cosa antes de reportar los resultados de esa tarea a Diego. En particular:

- Reportar versión exacta de Postgres del cluster de Optiferre.
- Reportar RAM disponible, load average, disco.
- Confirmar acceso a los dos repos nuevos.
- Confirmar acceso al servidor Coolify vía SSH y a la API de Coolify.

**Con eso reportado y aprobado por Diego, avanzar a Tarea 1.**

---

**Fin de la Parte 3.** Las Partes 1, 2 y 3 de este archivo son el contexto operativo completo del proyecto Fase 0 núcleo. No hay más decisiones abiertas.
