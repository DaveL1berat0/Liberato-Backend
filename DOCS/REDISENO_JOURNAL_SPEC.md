# ESPECIFICACIÓN AUTORITATIVA — REDISEÑO DEL JOURNAL

Archivo objetivo: `/Users/macbookpro/Desktop/liberato_quant/Claude projects/The last Liberato Web/dashboard/journal.html` (3494 líneas). Un solo HTML + JS vanilla. `node --check` antes de cada guardado. Deploy = `git push` (GitHub Pages).

---

## 1. VISIÓN

El journal rediseñado convierte fills reales de broker y CSVs en operaciones round-trip verificadas, con un pipeline de ingesta que valida, deduplica y enriquece cada trade antes de que toque una sola estadística. Se ve tan pulido como TradeZella (dashboard, calendario heat-map, reportes por dimensión), pero su diferenciador es que **cada número mostrado es un trade que ocurrió de verdad** (Regla #1), arranca en 0 por estudiante y mide la ejecución contra el plan (adherencia a playbook).

---

## 2. DECISIONES RESUELTAS

### 2.1 Color de P&L → **VERDE / ROJO** (no morado)

Los tres blueprints coinciden y la síntesis lo confirma. Justificación consolidada:

- La regla "morado bajista nunca rojo" pertenece al **dashboard de PRECIO (GEX)**, donde morado = sesgo bajista del mercado (gamma negativa ≠ pérdida) y evita el rojo de alarma. En un **journal de P&L**, rojo tiene otra semántica universal: "perdiste dinero". Son dos dominios distintos; no hay conflicto de marca.
- Dave pidió explícitamente "que se vea como TradeZella". Todo el sector (TradeZella, Tradervue, Edgewonk, brokers) usa verde/rojo para P&L; el estudiante lo lee en 200 ms sin aprender un código nuevo.
- El código **ya** usa `--red:#ef4444` / `--green:#22c55e` con variantes soft en todo el journal (calendario `.sloss/.bloss` L504-505, gauges, KPIs). Cambiar a morado es re-trabajo grande que empeora legibilidad (verde-vs-morado tiene peor contraste que verde-vs-rojo).
- **El morado `#B197FC` se reserva como acento de marca en elementos NO-P&L**: Liberato Score, badges de playbook, tags, línea "planned" vs "actual", headers neutros. Así marca y semántica de dinero no colisionan.

**Refuerzo anti-ambigüedad (barato, premium, obligatorio):** todo número de P&L lleva signo explícito (`+$1.240` / `−$380`); el calendario conserva intensidad por magnitud (`.great/.good/.sloss/.bloss`).

**Tokens (añadir a `:root` L110-120 y a `[data-theme=light]` L196-203):**
```css
--purple:#B197FC; --purple-soft:rgba(177,151,252,0.12);
--amber ya existe (#f59e0b) → severidad "revisar"
```
Verde/rojo/gold/amber ya existen y no cambian. Regla operativa de color:

| Rol | Token | Uso |
|---|---|---|
| Ganancia | `--green` | P&L +, win, curva up, celda great/good |
| Pérdida | `--red` | P&L −, loss, drawdown, celda sloss/bloss |
| Marca / acento / CTA | `--gold #CCA94F` | KPI labels, bordes, botones, estructura |
| Score / tags / playbook / neutro | `--purple #B197FC` | Liberato Score, badge playbook, planned-line |
| Breakeven / scratch | `--text3` | trades be |
| Severidad anomalía | `--amber` (revisar) / `--red` (bloqueante) | banner y chips de calidad |

Fondo base `#06070D` (el CSS actual usa `#06080D`; se mantiene el existente, la diferencia de 1 dígito es imperceptible — no tocar).

### 2.2 Navegación final

Sidebar de **6 items** (hoy 5). Se adopta la estructura del Ángulo C/A con la vista de Ingesta del Ángulo B **integrada dentro del modal de import existente + una sub-vista de cola**, no como 6º item de nav completo (para no saturar la barra móvil). Decisión de conflicto resuelta así:

```
LIBERATO · Journal
├─ Panel        (dashboard)  renderDash    — KPIs, equity, score, calendar-mini, leaderboard
├─ Calendario   (calendar)   renderCal     — heat-map mensual + anual
├─ Operaciones  (trades)     renderTrades  — lista round-trip + drill-down planned/actual
├─ Reportes     (reports)    renderReports ← NUEVA (tabs: Resumen·Día&Hora·Símbolo·Playbook·Calidad)
├─ Playbooks    (playbooks)  renderPlaybooks — hub de rendimiento + adherencia
└─ El Genio     (genius)     renderGenius  — coach + detección de conducta (local)
──────
[CTA] Registrar trade      openForm
[CTA] Importar / Broker    openCsvImport  → abre modal con pestañas: Broker | CSV | COLA DE REVISIÓN
[CTA] AI Coach             openAIChat
[Toggle global $ / % / R]  en header de cada vista de datos
```

- **Cola de revisión (Ángulo B)** vive como **tercera pestaña dentro del modal de importación** (`#csv-modal`, L1704), no como vista de nav. Razón: mantiene 6 items (límite sano de sidebar/bottom-tabs), y la cola es contextual al acto de importar. El banner de salud de datos (`.dq-banner`) sí aparece en Panel y Operaciones cuando hay pendientes.
- **Filtro global de playbook (`#pb-scope`, Ángulo C)**: barra de chips persistente bajo el header en Panel/Calendario/Operaciones/Reportes. Estado `pbScope` (default 'all'). Reutiliza patrón `.fchip` (L546) + listener estilo L3062.
- Routing: extender `go(v)` (L3054) con `case 'reports': renderReports()`. `curView` válidos: `dashboard, calendar, trades, reports, playbooks, genius`.
- Bottom-tabs móvil (`.btabs` L1535): Panel · Calendario · [FAB Registrar] · Operaciones · Reportes. Playbooks/Genio desde Panel.

### 2.3 Arranque en 0 por usuario + demo separado

Tres cambios coordinados:

**(a) `loadTrades()` deja de auto-sembrar DEMO** (hoy L1820-1822 siembra y PERSISTE los 5 demo). Nuevo comportamiento: si no hay data válida → devuelve `[]`. Empty-state premium ("Aún no tienes operaciones. Conecta tu broker o importa un CSV." + 2 CTAs).

**(b) DEMO pasa a modo explícito y efímero.** `var DEMO=[...]` (L1811) se conserva pero solo se carga con el botón "Ver datos de ejemplo": cada trade lleva `__demo:true`, banner permanente "MODO EJEMPLO — no son trades reales", **nunca se persiste** en la store del usuario y se excluye de KPIs reales. Cumple Regla #1.

**(c) Namespace por usuario.** Todas las stores pasan a `<base>::<userId>`. `userId` decodificado del JWT de 'Liberato-Usuarios' (payload base64, sin verificar firma en cliente — la verificación es del backend); fallback `'local'`. Migración one-shot idempotente: si existe clave global vieja sin namespace, moverla a `::local`.

```js
function currentUserId(){
  try{ var t=localStorage.getItem('lbc_jwt');
       if(t) return JSON.parse(atob(t.split('.')[1])).sub || 'local'; }catch(e){}
  return 'local';
}
function userKey(base){ return base+'::'+currentUserId(); }
```

---

## 3. MODELO DE DATOS

### 3.1 Schema final de `trade`

**Campos crudos** (form / import / broker):
```js
{
  id, date:'YYYY-MM-DD', time:'HH:MM',
  asset,                 // 'NQ'|'MNQ'|'ES'|'MES' | 'QQQ C' | 'QQQ P' (opciones)
  instrument,           // 'future' | 'option' | 'equity'   (NUEVO)
  option,               // {underlying,strike,expiry,type} | null   (NUEVO)
  direction,            // 'long'|'short'|'sideways'
  entry, exit, stop,    // precios (medios si multi-fill)
  contracts,            // qty real (ya NO hardcode 1)
  mult,                 // 100 opción / CV[asset] futuro / 1 equity   (NUEVO, override de CV)
  fees,                 // comisión total; 0 o null si no disponible   (NUEVO)
  exitTime,             // ISO cierre (NUEVO, de broker)
  target,               // precio objetivo planeado | null   (NUEVO, planned)
  mae, mfe,             // max adverse/favorable excursion | null   (NUEVO)
  setup, playbookId,    // playbookId es el enlace FUERTE (ver 3.4)
  vwap, gamma, t4h, exitType, checklist:[bool], note,
  // metadata de ingesta:
  source,               // 'manual'|'csv'|'snaptrade'   (NUEVO)
  status,               // 'verified'|'pending'   (NUEVO)
  flags,                // [{code,severity,msg}]   (NUEVO)
  sourceFills,          // [broker_order_id...] trazabilidad   (NUEVO)
  dedupKey,             // clave natural de deduplicación   (NUEVO)
  __demo                // true solo en modo ejemplo, nunca persistido
}
```

**Campos derivados que añade `calcTrade(t)`** (extender L1826-1837):
```js
{
  points,               // pts = long?(exit-entry):(entry-exit), redond. 1 dec
  pnlGross,             // pts * mult * contracts   (mult respeta opciones)
  pnl,                  // = pnlNet = pnlGross - (fees||0)   (el pnl "oficial" es NETO)
  risk,                 // long?(entry-stop):(stop-entry)  en puntos
  riskDollars,          // risk * mult * contracts
  rr,                   // rPlanned = |points|/risk (planeado, 1 dec)   [ya existía como rr]
  rMultiple,            // REALIZADO = pnlNet / riskDollars   (NUEVO; '—' si riskDollars<=0)
  holdMin,              // (exitTime - entry datetime)/60000  | null   (NUEVO)
  result,               // 'win'|'loss'|'be'  (por pnl neto)
  chkAll,               // todos los ítems del checklist marcados
  adhered,              // = chkAll (siguió su plan)   (NUEVO, semántico)
  dow, session          // día semana; 'RTH'|'ON'   (NUEVO, para reportes/validación)
}
```

Cambio crítico en `calcTrade` (L1827): `var m = t.mult || CV[t.asset] || 20;` — respeta multiplicador de opciones (×100) que hoy no existe. `pnl` pasa a ser **neto de comisiones**. `rMultiple` es el R realizado, distinto del `rr` planeado.

### 3.2 Claves localStorage (todas namespaced por `::userId`)

| Base | Cambio |
|---|---|
| `lbc_jrn_v3` | → `lbc_jrn_v3::<uid>`. Ya NO auto-siembra DEMO |
| `lbc_broker_fills` | → `::<uid>`. Ahora SÍ se consume (groupFills) |
| `lbc_import_log` | **NUEVO** `::<uid>`: historial append-only de imports `[{ts,source,read,accepted,pending,dup,rejected}]` |
| `lbc_playbooks_v1` | → `::<uid>`. Schema gana `rules:[{title,desc}]` (checklist per-playbook) |
| `lbc_checklists`, `lbc_checklist_active` | → `::<uid>` (legacy; migrar a rules per-playbook en Fase 3) |
| `lbc_ctx_fields_v1`, `lbc_pbe_custom_v1` | → `::<uid>` |
| `lbc_view_mode` | **NUEVO** `::<uid>`: 'money'|'pct'|'r' (toggle global) |
| `lbc_theme`, `lbc_access`, `lbc_jwt` | globales (no namespaced) |

### 3.3 ALGORITMO de agrupación de fills opciones → trades round-trip

Los fills de `lbc_broker_fills` son ejecuciones individuales de opciones (QQQ 0DTE). Un round-trip = secuencia de fills sobre la **misma clave de instrumento + cuenta** cuya posición neta abre desde 0 y vuelve a 0. Precio medio ponderado por qty; P&L neto con multiplicador 100.

```js
function instrKey(f){
  return f.instrument==='option'
    ? [f.account, f.option.underlying, f.option.strike, f.option.expiry, f.option.type].join('|')
    : [f.account, f.symbol].join('|');
}

function groupFills(fills){
  // 1. dedup de fills crudos por broker_order_id
  var seen={}, clean=[];
  fills.forEach(function(f){ if(!seen[f.broker_order_id]){ seen[f.broker_order_id]=1; clean.push(f); } });

  // 2. agrupar por instrumento+cuenta, ordenar por time ISO ascendente
  var byKey={};
  clean.forEach(function(f){ (byKey[instrKey(f)] = byKey[instrKey(f)]||[]).push(f); });

  var trades=[];
  Object.keys(byKey).forEach(function(k){
    var seq = byKey[k].sort(function(a,b){ return a.time.localeCompare(b.time); });
    var pos=0, legIn=[], legOut=[];
    seq.forEach(function(f){
      var signed = (f.side==='BUY'? 1 : -1) * f.qty;
      // "abrir" si posición está en 0 o el fill va en la misma dirección que la posición actual
      var opening = (pos===0) || (Math.sign(pos)===Math.sign(signed));
      (opening ? legIn : legOut).push(f);
      pos += signed;
      if(pos===0){ trades.push(buildRoundTrip(legIn, legOut, k, false)); legIn=[]; legOut=[]; }
    });
    // posición sobrante (round-trip abierto o pierna huérfana): emitir con flag, NUNCA inventar el otro lado
    if(pos!==0 || legIn.length || legOut.length){
      trades.push(buildRoundTrip(legIn, legOut, k, true));
    }
  });
  return trades;
}

function wavg(legs){ // precio medio ponderado por qty
  var n=0, d=0; legs.forEach(function(f){ n+=f.price*f.qty; d+=f.qty; });
  return d>0 ? n/d : 0;
}

function buildRoundTrip(legIn, legOut, key, open){
  var first = legIn[0] || legOut[0];
  var isOpt = first.instrument==='option';
  var mult  = isOpt ? 100 : (CV[first.symbol]||20);
  var qtyIn = legIn.reduce(function(s,f){return s+f.qty;},0);
  var dir   = legIn.length ? (legIn[0].side==='BUY'?'long':'short') : (legOut[0].side==='SELL'?'long':'short');
  var avgIn = wavg(legIn), avgOut = wavg(legOut);
  var lastOut = legOut.length ? legOut[legOut.length-1] : null;
  var pts = dir==='long' ? (avgOut-avgIn) : (avgIn-avgOut);
  var fees = 0; // SnapTrade fill no trae comisión → 0/null, se marca (ver 5)
  var allIds = legIn.concat(legOut).map(function(f){return f.broker_order_id;}).sort();

  var t = {
    id: 'bt_'+allIds[0],
    source:'snaptrade',
    broker_order_id: allIds[0],
    sourceFills: allIds,
    dedupKey: 'bo|'+allIds.join(','),
    date: first.time.slice(0,10),
    time: first.time.slice(11,16),
    exitTime: lastOut ? lastOut.time : null,
    instrument: first.instrument,
    option: first.option || null,
    asset: isOpt ? (first.option.underlying+' '+first.option.type) : first.symbol,
    direction: dir,
    entry: Math.round(avgIn*100)/100,
    exit:  open ? null : Math.round(avgOut*100)/100,
    stop: 0, target:null,
    contracts: qtyIn,
    mult: mult,
    fees: fees,
    setup:'Importado (broker)',
    playbookId:null,
    checklist:[], vwap:null, gamma:null, t4h:null, exitType:'full', note:'',
    status:'pending',                 // todo import entra a cola
    flags: open ? [{code:'open_position',severity:'amber',msg:'Posición abierta o pierna sin cierre en la ventana'}] : []
  };
  return t; // calcTrade() se aplica después, con mult ya presente
}
```

Notas de corrección (Regla #1): posiciones que no cierran dentro de los 120 días de SnapTrade quedan con `open_position` y **no se les fabrica** el lado faltante; se muestran como "abiertas", no se pierden. Cruces de largo↔corto en un mismo instrumento cierran el lote al pasar por 0 y abren el siguiente.

### 3.4 Enlace trade ↔ playbook

Hoy las stats agrupan por `t.setup` (string, L1850) y DEMO/CSV/broker no traen `playbookId`. Se migra toda agregación a `playbookId`; fallback a `setup` solo legacy. Backfill idempotente al cargar: si `t.setup === playbook.name` y falta `playbookId`, asignarlo. Imports entran con `playbookId:null` (badge "sin plan"); el estudiante los clasifica — **nunca auto-asignar** (inventaría intención).

---

## 4. FEATURES POR FASES

### FASE 1 — IMPRESCINDIBLE (Regla #1, calidad núcleo, arranque en 0)

**F1.1 — Arrancar en 0 + DEMO efímero**
- Qué: `loadTrades()` no siembra; DEMO solo por botón, marcado y no persistido.
- Aceptación: localStorage vacío + primer load ⇒ `T.length===0`, empty-state visible, 0 filas en calendario/stats. Pulsar "Ver ejemplo" muestra 5 trades con banner; recargar ⇒ vuelve a 0.
- Toca: `loadTrades()` L1820-1823, `DEMO` L1811, `var T` L1838, añadir `seedDemoEphemeral()`.

**F1.2 — Namespace por usuario**
- Qué: `userKey()` en todas las stores; migración one-shot.
- Aceptación: cambiar `lbc_jwt` con otro `sub` ⇒ journal distinto/vacío; sin JWT ⇒ `::local` estable; datos viejos globales aparecen bajo `::local` tras migración, sin pérdida.
- Toca: `KEY/PB_KEY/CHK_KEY/...` (L1757-2967), `saveTrades/loadTrades`, `savePlaybooks/loadPlaybooks`, etc.

**F1.3 — Multiplicador de opciones + fees en `calcTrade`**
- Qué: `mult` override, `pnl` neto de fees, `rMultiple`.
- Aceptación: opción QQQ (entry 2.00, exit 3.00, 1 contrato) ⇒ `pnl = (3-2)*100*1 = +$100`; futuro NQ intacto (pts*20).
- Toca: `calcTrade` L1826-1837, `CV` L1754 (documentar que opción usa mult=100 vía `t.mult`).

**F1.4 — Agrupación fills → round-trip (`groupFills`, `buildRoundTrip`, `wavg`, `instrKey`)**
- Qué: motor §3.3.
- Aceptación: dado un `lbc_broker_fills` con BUY 2@2.00 + SELL 2@3.00 mismo contrato ⇒ 1 trade long, entry 2.00, exit 3.00, contracts 2, pnl +$200; fills sin cierre ⇒ 1 trade con flag `open_position`, exit null.
- Toca: bloque nuevo junto a `syncFills` L2464-2477.

**F1.5 — Deduplicación (`tradeHash`, `mergeTrades`)**
- Qué: clave `broker_order_id`/`dedupKey`; hash `date|time|asset|entry|exit` para CSV/manual.
- Aceptación: re-sincronizar el mismo broker ⇒ 0 trades añadidos; re-importar el mismo CSV ⇒ 0 duplicados; contador reportado.
- Toca: nuevo helper; se invoca en `confirmCsvImport` L2613 y en el flujo de broker.

**F1.6 — Estado pending/verified + cola de revisión + banner**
- Qué: imports entran `status:'pending'`; auto-verify si `flags.length===0`; los flagged esperan aprobación en la 3ª pestaña del modal import; KPIs solo cuentan `verified`.
- Aceptación: import con 1 anomalía ⇒ ese trade NO altera Net P&L hasta "Aprobar"; banner muestra "✓N ⚠M ✕D".
- Toca: `renderTrades`/`stats` (filtrar por status), modal `#csv-modal` L1704 (pestaña cola), nuevo `#dq-banner`.

**F1.7 — CSV completo (romper truncado a 41 líneas)**
- Qué: parsear TODO el CSV en cliente; a la IA del backend solo cabecera+5 filas para mapeo de columnas; aplicar mapeo local a todas las filas.
- Aceptación: CSV de 200 filas ⇒ 200 trades candidatos (menos rechazados reportados), no 40.
- Toca: `handleCsvFile` L2514, `analyzeCsvWithAI` L2533 (L2526 muestra), `confirmCsvImport` L2613.

### FASE 2 — ALTO VALOR (paridad TradeZella + decisión)

**F2.1 — Toggle global $/%/R (`fmtVal(pnl,t,mode)`)**: chips estilo `.cal-mode-btn` L180; estado `VIEWMODE` + `lbc_view_mode`; todos los renders lo consultan. `%`/`R` muestran `'—'` si falta balance/riesgo (no inventar). Toca: header de cada vista, `renderDash/renderCal/renderTrades`.

**F2.2 — Vista Reportes con tabs** (Resumen · Día&Hora · Símbolo · Playbook · Calidad): `renderReports()` + `renderReportTab()`; agregaciones `arr.reduce` sobre `T` verificado filtrado por `pbScope`; `getDay()` para día-semana, franjas horarias reales (romper split fijo mañana/tarde L2064); reusar `donutSVG/ringMini`/barras. Toca: nueva vista `#v-reports`, `go()` L3054.

**F2.3 — Calendario anual heat-map (`renderCalYear`)**: toggle MES/AÑO; celdas pequeñas reusando `.great/.good/.sloss/.bloss`. Toca: `renderCal` L2081.

**F2.4 — Rule Adherence Rate + stats por playbook**: KPI `% trades con adhered=true`; leaderboard `#pb-leaderboard`; Adherence-vs-PnL. Toca: `stats` L1841, `renderDash` L1948, `renderPlaybooks` L2770.

**F2.5 — Drill-down Planned vs Actual + MAE/MFE + traza de fills**: en `openTDetail` L2124, bloque R planeado vs real, target/stop, y sub-tabla `.fill-trace` de `sourceFills`. Campos capturados en form o enriquecidos.

**F2.6 — Motor de anomalías completo (`detectFlags`)** con panel de revisión detallado (§5).

### FASE 3 — EXTRA

- **F3.1** Detección de conducta LOCAL/determinista en El Genio (revenge = trade <N min tras loss con tamaño↑; overtrading = nº/día > umbral; tilt = racha losses + tamaño creciente). Reusa `renderPatterns` L2172. **No depende de IA** (el backend IA está roto, ver nota).
- **F3.2** Checklist per-playbook (`rules` en schema playbook, migrar `lbc_checklists`).
- **F3.3** Reporte matriz Playbook × {símbolo|hora|día} heat-map.
- **F3.4** Arreglar AI Coach/Chat/Screenshot: mover de `api.anthropic.com` (sin key, L2675/2705/2754) al backend Railway, mismo patrón que el CSV ya usa (L2561). Ortogonal al rediseño.

---

## 5. HERRAMIENTAS DE CALIDAD DE DATOS (objetivo central de Dave)

### 5.1 Deduplicación
```js
function tradeHash(t){ return [t.date,t.time,t.asset,t.entry,t.exit].join('|'); }
function dedupKeyOf(t){ return t.dedupKey || (t.broker_order_id? 'bo|'+t.broker_order_id : 'h|'+tradeHash(t)); }
function mergeTrades(existing, incoming){
  var seen={}; existing.forEach(function(t){ seen[dedupKeyOf(t)]=1; });
  var added=[], dup=0;
  incoming.forEach(function(t){ var k=dedupKeyOf(t);
    if(seen[k]){ dup++; } else { seen[k]=1; added.push(t); } });
  return { trades: existing.concat(added), added: added.length, dup: dup };
}
```
Nota: `genId()` (L1825) usa `Date.now()+random` y NUNCA colisiona ⇒ hoy es imposible detectar el mismo trade dos veces. `dedupKey` es lo que lo arregla.

### 5.2 Detección de anomalías
```js
function detectFlags(t){
  var f=[];
  if(!/^\d{4}-\d{2}-\d{2}$/.test(t.date)) f.push(F('fecha_invalida','red','Fecha con formato inválido'));
  if(new Date(t.date) > new Date()) f.push(F('fecha_futura','red','Fecha en el futuro'));
  if(t.instrument!=='option' && !CV[t.asset]) f.push(F('simbolo_desconocido','amber','Símbolo no reconocido'));
  if(t.instrument==='option' && t.mult!==100) f.push(F('mult_opcion','amber','Multiplicador de opción sospechoso'));
  if(t.exit!=null && Math.abs(t.pnl) > (t.contracts*t.mult* (t.entry||1) *3)) f.push(F('pnl_improbable','amber','P&L fuera de rango plausible'));
  if(t.stop>0){ var wrong = t.direction==='long'? t.stop>=t.entry : t.stop<=t.entry;
    if(wrong) f.push(F('stop_lado_incorrecto','red','Stop del lado equivocado de la entrada')); }
  var h=parseInt(t.time,10); if(!isNaN(h) && (h<6||h>20)) f.push(F('fuera_de_sesion','amber','Hora fuera de sesión típica'));
  return f;
}
function F(code,sev,msg){ return {code:code,severity:sev,msg:msg}; }
```
Regla de flujo: `flags` con severidad `red` ⇒ trade queda `pending` obligatorio; solo `amber` ⇒ `pending` revisable; sin flags ⇒ auto-`verified`. KPIs cuentan solo `verified`.

### 5.3 Enriquecimiento
- Import asigna `session` (RTH/ON por hora), `dow`, `holdMin` (si `exitTime`), `mult`, `instrument`.
- CSV/broker entran con contexto `null` pero se marcan como "sin contexto" en reporte de Calidad; el estudiante puede completar vwap/gamma/t4h/playbook en el drill-down.
- **Nunca** rellenar contexto inventado; campo ausente = `'—'`.

### 5.4 Reporte de import (romper el `alert` mudo L2646)
Panel post-import: filas leídas / aceptadas / en revisión / duplicadas ignoradas / rechazadas **con motivo por fila** (los flags). Se persiste en `lbc_import_log::uid`. Pestaña "Calidad" de Reportes: % trades con stop, % con contexto, % verificados, comisiones totales conocidas vs desconocidas.

---

## 6. INTEGRACIÓN SNAPTRADE (fills → journal)

Estado actual: `syncFills` (L2464) hace GET `/api/broker/snaptrade/fills?days=120`, lee `d.trades`, guarda en `lbc_broker_fills` (L2472) y solo pinta 4 fills de preview. **`lbc_broker_fills` nunca se lee después** — los fills quedan huérfanos.

Flujo nuevo:
```
connectBroker() → portal OAuth (L2448) → syncFills():
  1. GET fills → guarda crudos en lbc_broker_fills::uid  (conservar la fuente cruda siempre)
  2. importFromBroker():
       raw   = JSON.parse(localStorage['lbc_broker_fills::uid']).fills
       cand  = groupFills(raw)                    // §3.3 round-trips
       cand  = cand.map(calcTrade)                // pnl neto, rMultiple, mult opción
       cand.forEach(t=>{ t.flags = detectFlags(t);
                         t.status = t.flags.some(f=>f.severity==='red')?'pending'
                                    : t.flags.length? 'pending':'verified'; })
       res   = mergeTrades(T, cand)               // dedup por broker_order_id
       T     = res.trades; saveTrades(T)
       logImport('snaptrade', {read:raw.length, accepted:res.added, dup:res.dup,
                               pending: cand.filter(x=>x.status==='pending').length})
  3. Refrescar banner + cola de revisión + vista activa
```

Presentación:
- Trades verificados aparecen en Panel/Calendario/Operaciones como cualquier otro, con **badge de origen "BROKER"** (reusa `.ctx-tag` L529) e icono opción vs futuro.
- Trades pending aparecen SOLO en la cola de revisión (pestaña del modal) hasta aprobar; no tocan KPIs.
- Drill-down muestra `sourceFills` en sub-tabla `.fill-trace` (transparencia: "por qué este trade dice esto").
- `open_position` se muestra como "abierta" (exit `'—'`, P&L `'—'`), nunca con lado inventado.
- **Comisión no disponible en el fill de SnapTrade** ⇒ `fees:0`, P&L mostrado con asterisco "P&L bruto — comisión no disponible" y campo fee `'—'`. Jamás un fee estimado presentado como real (Regla #1).

---

## 7. PLAN DE IMPLEMENTACIÓN (pasos seguros sobre journal.html)

Cada paso: editar → `node --check journal.html` → abrir en navegador y verificar el criterio → `git add/commit`. No avanzar si `node --check` falla. Commits pequeños por paso.

**Paso 0 — Rama y respaldo.** `git checkout -b rediseno-journal` desde `develop`. Confirmar que el archivo carga hoy sin errores en consola.

**Paso 1 — Tokens de color.** Añadir `--purple/--purple-soft` a `:root` (L110-120) y a `[data-theme=light]` (L196-203). Verificar: sin cambio visual aún; `node --check` OK.

**Paso 2 — Namespace de stores (F1.2).** Crear `currentUserId()` y `userKey()`. Reemplazar lecturas/escrituras de `KEY, PB_KEY, CHK_KEY, CHK_ACTIVE_KEY, 'lbc_broker_fills', CTX_KEY, PBE_CUSTOM_KEY` por `userKey(base)`. Añadir `migrateGlobalStores()` idempotente (global→`::local`) llamada una vez en `DOMContentLoaded` (L3071). Verificar: datos existentes de Dave siguen visibles (migrados a `::local`); cambiar JWT de prueba muestra journal vacío.

**Paso 3 — Arranque en 0 + DEMO efímero (F1.1).** Reescribir `loadTrades()` para devolver `[]` si no hay data válida (quitar `DEMO.map+saveTrades`). Añadir `seedDemoEphemeral()` (marca `__demo`, no persiste) y botón "Ver ejemplo" en empty-state. Verificar: localStorage limpio ⇒ 0 trades + empty-state; ejemplo carga y desaparece al recargar.

**Paso 4 — `calcTrade` con mult/fees/derivados (F1.3).** Extender L1826-1837: `mult`, `pnlGross`, `pnl` neto, `rMultiple`, `holdMin`, `dow`, `session`, `adhered`. Verificar: trades futuros existentes dan el MISMO pnl que antes (regresión); test manual de opción da ×100.

**Paso 5 — Dedup + merge (F1.5).** Añadir `tradeHash/dedupKeyOf/mergeTrades`. Integrar en `confirmCsvImport` (L2613). Verificar: importar el mismo CSV dos veces ⇒ 2ª vez 0 añadidos.

**Paso 6 — Motor de fills (F1.4) + integración broker (§6).** Añadir `instrKey/wavg/buildRoundTrip/groupFills` e `importFromBroker()`. Enganchar tras `syncFills` (L2464). Verificar con un `lbc_broker_fills` de prueba (BUY+SELL): 1 round-trip correcto; fills sin cierre ⇒ flag `open_position`.

**Paso 7 — Anomalías + pending/verified + banner (F1.6, F2.6, 5.2/5.4).** Añadir `detectFlags/F/logImport`. `stats()` (L1841) filtra `status==='verified'`. Añadir 3ª pestaña "Revisión" en `#csv-modal` (L1704) con cola + Aprobar/Editar/Descartar, y `.dq-banner` en Panel/Operaciones. Verificar: import con anomalía no mueve KPIs hasta aprobar; banner cuenta bien.

**Paso 8 — CSV completo (F1.7).** Parsear todo el archivo en cliente; IA solo mapea columnas. Verificar: CSV de >41 filas importa todas menos rechazadas reportadas.

**Paso 9 — Enlace playbookId + backfill (F2.4 base).** Migrar `bySetup` (L1850) y stats a `playbookId` con fallback `setup`; backfill idempotente en carga. Verificar: reportes por playbook cuadran con conteo manual.

**Paso 10 — Toggle global $/%/R (F2.1).** `fmtVal()` + estado + `lbc_view_mode::uid`; todos los renders numéricos lo usan; `%`/`R` ⇒ `'—'` si falta base. Verificar: cambiar modo recalcula Panel/Calendario/Operaciones sin recargar.

**Paso 11 — Vista Reportes (F2.2).** Añadir `#v-reports`, `renderReports/renderReportTab`, caso en `go()` (L3054), item sidebar + bottom-tab. Verificar: 5 tabs renderizan; Día&Hora usa `getDay` real.

**Paso 12 — Calendario anual (F2.3) + Leaderboard/Adherencia (F2.4) + drill-down planned/actual (F2.5).** Verificar cada uno contra conteo manual y contra un trade con MAE/MFE de prueba.

**Paso 13 — `#pb-scope` global (Ángulo C).** Barra de chips + `pbScope` + re-render de vista activa. Verificar: filtrar por un playbook recalcula Panel/Calendario/Operaciones/Reportes.

**Paso 14 — Fase 3** (conducta local, checklist per-playbook, matriz, fix IA backend), cada uno en su commit.

**Cierre.** Revisión completa en navegador (desktop + móvil 1100px), `node --check` final, `git push` a la rama, PR a `main` cuando Dave lo autorice (no pushear a main sin permiso; hoy en `develop`).

**Notas de riesgo a vigilar durante la ejecución:**
- El archivo pasará de ~3494 a ~5000+ líneas en un solo HTML vanilla: usar prefijos de dominio (`fill_`, `dq_`, `rep_`, `pb_`) para evitar colisión de nombres globales.
- `groupFills` es el punto de mayor riesgo de P&L incorrecto (scaling in/out, cruces de signo, 0DTE sin cierre): probar con casos límite antes de exponer números.
- El bug de `api.anthropic.com` sin key (L2675/2705/2754) NO se toca hasta F3.4; por eso la detección de conducta (F3.1) debe ser 100% local/determinista.
- `openTradeModal` (L2212) y selectores `#trade-modal` son código huérfano/legacy: no construir sobre él; usar el patrón de drawers actual (`#tdrawer`).