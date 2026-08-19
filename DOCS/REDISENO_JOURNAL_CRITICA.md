A continuación los 8 huecos/riesgos más graves, cada uno con corrección concreta. No hizo falta abrir el archivo: los problemas son de diseño y viven dentro de la propia especificación.

---

**1. `groupFills` calcula mal contracts y P&L en scaling out + re-entrada (el caso más común en 0DTE).**
La regla `opening = (pos===0) || sign(pos)===sign(signed)` acumula en `legIn` cada fill que va en la dirección de la posición. Secuencia real: BUY2 → SELL1 (scale out, pos=+1) → BUY1 (re-add, pos=+2) → SELL2 (pos=0). Resultado: `legIn` suma 3 contratos aunque la posición máxima fue 2. `qtyIn=3` y `pnlGross = pts*mult*3` **inflan el P&L y el riesgo**. `wavg` sobre precios promedio tapa el error pero no lo corrige.
Corrección: no derives P&L de `avg*qtyMax`. Calcula el realizado como flujo de caja firmado sobre todo el ciclo: `pnl = -Σ(signedQty * price) * mult` y `contracts = max(|posición acumulada|)`. Es robusto a scaling in/out y a cruces de signo. Añade un test de aceptación con esa secuencia exacta (esperado: contracts=2, no 3).

**2. Opciones que expiran (ITM/OTM) nunca cierran → pérdidas reales ocultas (violación Regla #1 por omisión).**
`groupFills` solo ve fills BUY/SELL. Una opción 0DTE que expira OTM no genera SELL de cierre: queda `pos≠0` → `open_position` → exit `'—'`, P&L `'—'` **para siempre**. Pero económicamente es una pérdida realizada del 100% de la prima. Mostrar "abierta" una posición que ya no existe es tan falso como inventar el cierre.
Corrección: consumir el evento de expiración/asignación de SnapTrade (activities, no fills). OTM → cerrar a 0 usando el registro real del broker (pérdida = prima). ITM/asignación → flag `expiracion_itm` a cola de revisión (nunca liquidar el subyacente automáticamente). Si SnapTrade no entrega el evento, marcar `expirada_desconocida` y decir "posible pérdida de prima no confirmada", no "abierta".

**3. `date`/`time`/`session` derivados por slice de ISO = zona horaria equivocada.**
`first.time.slice(0,10)` y `slice(11,16)` toman el string ISO crudo. Si SnapTrade entrega UTC (`...Z`), un fill de las 15:30 ET sale con fecha/hora UTC (20:30, y cerca de medianoche **cambia de día**). Esto corrompe la celda del calendario, `dow`, `session` (RTH/ON) y el flag `fuera_de_sesion`. Un trade de tarde puede caer en el día siguiente.
Corrección: convertir a `America/New_York` (con DST) antes de derivar `date/time/session/dow`. Un solo helper `toET(iso)` usado en `buildRoundTrip` y en el parser CSV. Documentar la TZ asumida del origen.

**4. Trades importados con `stop:0` inventan un `rMultiple`.**
`buildRoundTrip` fija `stop:0`. En `calcTrade`, `risk = entry - 0 = entry` (positivo), `riskDollars > 0`, y el guard de `rMultiple` solo devuelve `'—'` si `riskDollars<=0`. Resultado: cada trade de broker muestra un R realizado calculado contra un **stop ficticio de 0** — número inventado presentado como real. Igual con `rr` planeado.
Corrección: `stop` ausente = `null`, no `0`. Tratar `stop==null` como "sin stop" → `risk`, `riskDollars`, `rr`, `rMultiple` todos `'—'`. Añadir a `detectFlags` un aviso `sin_stop` (amber) para que el estudiante lo complete en drill-down.

**5. Sin deduplicación cruzada entre fuentes: broker + CSV del mismo broker = todo duplicado.**
`dedupKeyOf` usa `'bo|'+order_id` para broker y `'h|'+hash` para CSV/manual. El mismo trade importado por SnapTrade y por el CSV del broker genera **dos claves distintas** → `mergeTrades` no los ve como duplicados. El estudiante que conecta broker y además sube el CSV duplica su cuenta entera. Y al revés: `tradeHash` (`date|time|asset|entry|exit`, sin contratos ni cantidad) colapsa **como duplicado dos scalps legítimos** en el mismo minuto/precio, perdiendo un trade real.
Corrección: (a) clave natural normalizada compartida entre fuentes: `date|time|asset|dir|entry|exit|contracts` para el fallback de hash, incluyendo contratos; (b) cuando exista `order_id`, seguir prefiriéndolo, pero calcular también la clave natural y cruzarla, para que un CSV del mismo broker haga match. Reportar colisiones sospechosas a revisión en vez de descartar en silencio.

**6. El namespace por usuario filtra datos entre estudiantes sin JWT y expone los trades reales de Dave.**
`currentUserId()` cae a `'local'` sin JWT. La migración global→`::local` mete los trades reales de Dave en `::local`, que es **exactamente** el espacio que ve cualquier usuario anónimo o recién deslogueado en ese navegador. Dos alumnos en la misma máquina sin login comparten `::local`. Y al hacer logout (borrar `lbc_jwt`) el journal vuelve a mostrar `::local`.
Corrección: no persistir bajo `::local` cuando no hay JWT; usar un id local aleatorio por navegador (`lbc_anon_id` generado una vez) para el fallback, y no migrar los datos globales de Dave a un espacio compartido — migrarlos a su `::<sub>` real la primera vez que aparezca su JWT, o dejarlos en una clave `::owner` que nunca sea el fallback anónimo. Empty-state real para usuarios nuevos.

**7. El filtro `status==='verified'` debe aplicarse a TODAS las agregaciones, no solo `stats()`.**
La spec dice que `stats()` y `renderTrades` filtran por status, pero la equity curve, el calendar heat-map (mini y anual), el leaderboard de playbooks, los Reportes y el `pbScope` agregan sobre `T` directamente. Si no filtran, los trades `pending` (los que aún no deben tocar KPIs) **sí entran** en curva, calendario y reportes → el banner dice "no cuenta" pero el gráfico ya lo pintó.
Corrección: una única función `verifiedTrades()` (o `T.filter(t=>t.status==='verified' && !t.__demo)`) como fuente para todo render de datos; prohibir el uso directo de `T` en renders. Enumerar explícitamente en el plan los ~6 renders que deben migrar, no solo `stats`.

**8. "Parsear todo el CSV en cliente" no especifica el parser → romperá con formatos de broker reales.**
F1.7/Paso 8 asume que aplicar el mapeo de columnas a todas las filas es trivial, pero no define parser CSV. Los exports de broker traen comas dentro de campos entre comillas, saltos de línea en notas, `;` como separador, BOM, y encabezados variables. Un `split(',')` ingenuo desalinea columnas → precios/qty en el campo equivocado → P&L basura marcado como `verified`.
Corrección: especificar un parser RFC-4180 (comillas, separador auto-detectado, BOM) en cliente; la IA del backend devuelve **solo** un JSON de mapeo `{columnaCSV → campoTrade}` sobre 5 filas; el cliente valida que cada fila mapeada produzca tipos válidos (fecha parseable, números finitos) y manda a `detectFlags` las que no. Nada entra `verified` sin pasar validación de tipos.

---

Menores que vale la pena anotar (no en el top 8): MAE/MFE en F2.5 quedarán casi siempre vacíos para imports (no hay feed de precios) — el drill-down "planned vs actual" debe degradar a `'—'` sin parecer roto; la atribución de P&L al calendario usa la fecha del **primer** fill (apertura), no del cierre — decidir y documentar (TradeZella usa fecha de cierre para el realizado), o un trade overnight cae en el día equivocado; `fees:0` de SnapTrade debería intentar primero el endpoint de activities (a menudo sí trae comisión) antes de rendirse al asterisco "bruto".

El punto #1 (P&L por flujo de caja firmado) y el #2 (expiraciones) son los que más urge cerrar antes de exponer un solo número: son los dos que producen cifras falsas silenciosas.