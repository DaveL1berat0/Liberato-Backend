"""Liberato Backend — Trading terminal NQ.
Servicios desacoplados: FlashAlpha (GEX) + Finnhub (calendario macro).
Si uno falla, el otro sigue. Keys solo en variables de entorno.
"""
import os, time, asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

# ── Credenciales (SOLO desde variables de entorno) ──
FLASHALPHA_KEY = os.getenv("FLASHALPHA_KEY", "")
FINNHUB_KEY    = os.getenv("FINNHUB_KEY", "")

FA_BASE = "https://lab.flashalpha.com"
FH_BASE = "https://finnhub.io/api/v1"
NY = ZoneInfo("America/New_York")
PROXIES = {"NQ": "QQQ", "ES": "SPY", "GC": "GLD"}
# Futuros reales por activo (para cuando se conecte el broker de precio en vivo)
FUTURES = {"NQ": "NQ", "ES": "ES", "GC": "GC"}

# ═══════════════════════════════════════════════════════════
#  PRECIO DEL FUTURO EN VIVO — SLOT PARA TRADESTATION / BROKER
#  Cuando conectes TradeStation (o cualquier broker con precio del NQ),
#  implementa fetch_futures_price() aquí. El resto del sistema ya está
#  cableado para usar ese precio y calcular el ratio EXACTO QQQ→NQ.
#
#  La conversión exacta es: ratio = precio_NQ / precio_QQQ
#  (el factor NO es fijo: cambia con el fair value del futuro día a día)
# ═══════════════════════════════════════════════════════════
TRADESTATION_KEY = os.getenv("TRADESTATION_KEY", "")
TRADESTATION_SECRET = os.getenv("TRADESTATION_SECRET", "")

async def fetch_futures_price(asset: str):
    """Devuelve el precio EN VIVO del futuro (NQ/ES/GC) o None si no hay fuente.

    >>> PUNTO ÚNICO DE CONEXIÓN PARA TRADESTATION <<<
    Cuando tengas las credenciales de TradeStation, implementa aquí la llamada
    a su API de quotes (símbolo del futuro continuo, ej '@NQ' o 'NQU26').
    Ejemplo de estructura (ajustar al API real de TradeStation):

        symbol = {"NQ": "@NQ", "ES": "@ES", "GC": "@GC"}.get(asset)
        url = f"https://api.tradestation.com/v3/marketdata/quotes/{symbol}"
        headers = {"Authorization": f"Bearer {token}"}
        async with httpx.AsyncClient(timeout=5, headers=headers) as client:
            r = await client.get(url)
            if r.status_code == 200:
                q = r.json()["Quotes"][0]
                return float(q["Last"])

    Por ahora devuelve None → el frontend usa el ratio fallback hasta conectar.
    """
    if not TRADESTATION_KEY:
        return None
    # TODO: implementar llamada real a TradeStation cuando haya credenciales.
    return None

async def get_nq_ratio(asset: str, qqq_price: float):
    """Calcula el ratio EXACTO futuro/proxy si hay precio del futuro en vivo.
    ratio = precio_NQ / precio_QQQ. Devuelve (nq_price, ratio) o (None, None)."""
    if not qqq_price or qqq_price <= 0:
        return None, None
    nq_price = await fetch_futures_price(asset)
    if nq_price and nq_price > 0:
        return nq_price, round(nq_price / qqq_price, 4)
    return None, None


app = FastAPI(title="Liberato Backend")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Caché en memoria por servicio (desacoplado) ──
cache = {
    "gex":      {},                                    # { "NQ": {...} }
    "calendar": {"data": [], "last_update": None, "status": "offline"},
    "movers":   {"data": [], "last_update": None, "status": "offline"},
    "earnings": {"data": [], "last_update": None, "status": "offline"},
    "company": {},   # { "AAPL": {"data": {...}, "ts": 123456} } — detalle por empresa, cache 24h
    "health":   {"flashalpha": "offline", "finnhub": "offline"},
    # ── Control estricto de llamadas a FlashAlpha ──
    # Estructura: { "YYYY-MM-DD": {"close": bool, "open": bool} }
    # "close" = llamada de las 7:00 PM ET (después del cierre) — niveles del día anterior
    # "open"  = llamada de las 9:10 AM ET (verificación antes de apertura)
    # Si cualquiera es True para la fecha actual, NO SE HACE otra llamada ese día/slot.
    "gex_schedule": {},
}

# ── Persistencia a disco ──
# El cache vive en memoria y se pierde cuando Railway redespliega. Para que la
# data (earnings/gex) sobreviva los reinicios, la guardamos en disco. Así, si al
# arrancar Finnhub/FlashAlpha está rate-limited, servimos la última copia válida
# en vez de quedar vacíos.
import json as _json
_PERSIST_FILE = "/tmp/lbc_cache.json"

def _save_cache():
    try:
        snapshot = {
            "gex": cache["gex"],
            "earnings": cache["earnings"],
            "calendar": cache["calendar"],
            # ── CRÍTICO: guardar qué llamadas programadas ya se hicieron hoy ──
            # Así, si el servidor se reinicia, NO vuelve a llamar FlashAlpha.
            "gex_schedule": cache["gex_schedule"],
        }
        with open(_PERSIST_FILE, "w") as f:
            _json.dump(snapshot, f)
    except Exception as e:
        print(f"[persist] no se pudo guardar: {e}")

def _load_cache():
    try:
        with open(_PERSIST_FILE, "r") as f:
            snap = _json.load(f)
        for k in ("gex", "earnings", "calendar"):
            if k in snap and snap[k]:
                # Solo restaurar si tiene datos reales
                if k == "gex" and snap[k]:
                    cache["gex"] = snap[k]
                elif snap[k].get("data"):
                    cache[k]["data"] = snap[k]["data"]
                    cache[k]["last_update"] = snap[k].get("last_update")
                    cache[k]["status"] = "stale"  # marcar como stale hasta refrescar
        # ── Restaurar registro de llamadas programadas ──
        # Fundamental: si el servidor se reinicia DESPUÉS de la llamada de las 7 PM,
        # no debe volver a consultar FlashAlpha hasta las 9:10 AM del día siguiente.
        if "gex_schedule" in snap and snap["gex_schedule"]:
            cache["gex_schedule"].update(snap["gex_schedule"])
            print(f"[persist] schedule GEX restaurado: {snap['gex_schedule']}")
        ec = len(cache["earnings"]["data"])
        gex_assets = list(cache["gex"].keys())
        print(f"[persist] cache restaurado: {ec} earnings, GEX assets: {gex_assets}")
    except FileNotFoundError:
        print("[persist] sin cache previo en disco (primer arranque)")
    except Exception as e:
        print(f"[persist] no se pudo cargar: {e}")

# ═══════════════════════════════════════════════════════════
#  SERVICIO 1: FlashAlpha GEX (igual que antes, sin tocar)
# ═══════════════════════════════════════════════════════════
async def fetch_flashalpha(asset: str):
    ticker = PROXIES.get(asset, "QQQ")
    headers = {"X-Api-Key": FLASHALPHA_KEY}
    async with httpx.AsyncClient(timeout=15, headers=headers) as client:
        r = await client.get(f"{FA_BASE}/v1/stock/{ticker}/summary")
        if r.status_code == 429:
            raise RuntimeError("FlashAlpha 429 (rate limit) — usando caché")
        if r.status_code == 200:
            d = r.json()
            px = d.get("price", {}) or {}
            ex = d.get("exposure", {}) or {}
            out = {
                "underlying_price": px.get("mid") or px.get("last"),
                "call_wall": ex.get("call_wall"), "put_wall": ex.get("put_wall"),
                "gamma_flip": ex.get("gamma_flip"), "net_gex": ex.get("net_gex"),
                "regime": ex.get("regime"), "ticker": ticker, "source": "summary",
            }
            if out["call_wall"] or out["gamma_flip"] or out["underlying_price"]:
                return out
        today = datetime.now(NY).strftime("%Y-%m-%d")
        r = await client.get(f"{FA_BASE}/v1/exposure/gex/{ticker}", params={"expiration": today})
        if r.status_code == 200:
            d = r.json()
            strikes = d.get("strikes", []) or []
            bc = max((s for s in strikes if (s.get("call_gex") or 0) > 0), key=lambda s: s["call_gex"], default=None)
            bp = min((s for s in strikes if (s.get("put_gex") or 0) < 0), key=lambda s: s["put_gex"], default=None)
            return {
                "underlying_price": d.get("underlying_price"),
                "call_wall": bc["strike"] if bc else None,
                "put_wall": bp["strike"] if bp else None,
                "gamma_flip": d.get("gamma_flip"), "net_gex": d.get("net_gex"),
                "regime": d.get("net_gex_label"), "ticker": ticker, "source": "gex",
            }
        if r.status_code == 429:
            raise RuntimeError("FlashAlpha 429 (rate limit) — usando caché")
        raise RuntimeError(f"FlashAlpha {r.status_code}")

async def refresh_gex(asset="NQ", slot: str = None):
    """Refresca GEX desde FlashAlpha.

    LLAMADA CONTROLADA — solo puede ejecutarse desde los schedulers programados.
    Slots permitidos:
        "close" → 7:00 PM ET  (niveles post-cierre del día anterior)
        "open"  → 9:10 AM ET  (verificación pre-apertura)

    Fuera de esos dos slots, esta función simplemente NO llama FlashAlpha.
    Si el servidor se reinicia, lee el snapshot de disco y no llama la API
    hasta el próximo slot programado.

    Si FlashAlpha falla (429/timeout), MANTIENE los últimos datos válidos
    (stale) — nunca los borra. Regla de tolerancia a fallos.
    """
    today = datetime.now(NY).strftime("%Y-%m-%d")

    # ── GATE: verificar que el slot no se haya ejecutado ya hoy ──
    if slot:
        day_schedule = cache["gex_schedule"].setdefault(today, {"close": False, "open": False})
        if day_schedule.get(slot):
            print(f"[gex] slot '{slot}' ya ejecutado hoy ({today}) — sin llamada a FlashAlpha")
            return
    else:
        # Sin slot = llamada no programada (startup, endpoint, etc.) → BLOQUEADA
        # Solo permitir si NUNCA hemos tenido datos (primer arranque absoluto sin snapshot)
        if cache["gex"].get(asset):
            print(f"[gex] llamada sin slot bloqueada — FlashAlpha solo en slots programados")
            return
        print(f"[gex] primer arranque sin datos en disco — llamada inicial única")

    try:
        data = await fetch_flashalpha(asset)
        data["_ts"] = time.time()
        cache["gex"][asset] = data
        cache["health"]["flashalpha"] = "online"
        # ── Marcar slot como completado ──
        if slot:
            cache["gex_schedule"].setdefault(today, {"close": False, "open": False})[slot] = True
            print(f"[gex] slot '{slot}' completado para {today}: {data.get('source')}")
        _save_cache()  # persistir GEX + schedule para sobrevivir redeploys
        print(f"[gex] {asset} ok: call_wall={data.get('call_wall')} gamma_flip={data.get('gamma_flip')}")
    except Exception as e:
        # Si ya tenemos datos previos, mantenerlos (stale). Solo offline si nunca hubo datos.
        if cache["gex"].get(asset):
            cache["health"]["flashalpha"] = "stale"
            print(f"[gex] {asset} fallo ({e}) — manteniendo último dato válido (stale)")
        else:
            cache["health"]["flashalpha"] = "offline"
            print(f"[gex] error {asset}: {e}")

# ═══════════════════════════════════════════════════════════
#  SERVICIO 2: Finnhub — CALENDARIO MACRO (Fuente 1)
# ═══════════════════════════════════════════════════════════
#  FILTRO DE CALENDARIO — lógica estilo Finviz
#  En vez de lista blanca rígida, usamos:
#   1) BLOCKLIST de ruido (auctions, EIA, reportes agrícolas...)
#   2) SCORING de relevancia para el NQ/QQQ/ES/SPY
#  Así nunca perdemos eventos macro relevantes nuevos.
# ═══════════════════════════════════════════════════════════

# Ruido que NUNCA es relevante para el trader de índices (se descarta siempre)
EVENT_BLOCKLIST = [
    # Subastas de deuda (no mueven índices)
    "bill auction", "bond auction", "note auction", "tips auction", "frn auction",
    "3-month", "6-month", "4-week", "8-week", "6-week", "17-week", "52-week",
    "15-year", "20-year", "5-year", "2-year", "3-year", "7-year", "10-year", "30-year",
    # Energía / commodities (ruido para NQ)
    "nopa crush", "baker hughes", "rig count", "wasde", "grain stocks",
    "eia ", "api crude", "crude oil stock", "natural gas stock",
    "cushing", "distillate", "gasoline production", "gasoline stock",
    "heating oil", "refinery", "crude runs", "crude oil imports",
    # Hipotecas (irrelevante intradía)
    "mba ", "mortgage rate", "mortgage application", "mortgage market",
    "mortgage refinance", "purchase index",
    # Flujos / balances técnicos
    "fed balance sheet", "foreign bond investment", "tic flows",
    "net capital flows", "capital flows", "money supply",
    # Indicadores menores / encuestas privadas de bajo impacto
    "redbook", "lmi logistics", "rcm/tipp", "tipp economic",
    "used car prices", "corporate profits", "current account",
    "stress test",
]

# Eventos de ALTA relevancia para índices (siempre mostrar si aparecen)
HIGH_RELEVANCE = [
    "cpi", "core cpi", "ppi", "core ppi", "pce", "core pce",
    "fomc", "fed interest rate", "interest rate decision", "federal funds",
    "fed minutes", "powell", "fed chair", "fed press conference",
    "rate projection", "economic projection",
    # Discursos de miembros del Fed (mueven mercado)
    "fed speech", "goolsbee", "waller", "williams", "bostic", "kashkari",
    "daly", "barkin", "logan", "bowman", "jefferson", "cook", "barr",
    "fed governor", "fed president", "speech",
    "non farm", "nonfarm", "non-farm", "unemployment rate", "average hourly earnings",
    "gdp", "retail sales", "ism manufacturing", "ism services", "ism non-manufacturing",
    "jolts", "adp", "initial jobless", "continuing jobless", "jobless claims", "unemployment claims",
    "empire state", "philadelphia fed", "philly fed",
    "consumer confidence", "consumer sentiment", "michigan",
    "durable goods",
]

# Eventos de relevancia MEDIA para índices
MEDIUM_RELEVANCE = [
    # Vivienda
    "housing starts", "building permits", "new home sales", "existing home sales",
    "pending home sales", "nahb", "housing market index", "case-shiller", "home price",
    # Comercio exterior
    "import prices", "export prices", "trade balance", "balance of trade",
    "goods trade balance", "exports", "imports",
    # Producción / inventarios
    "factory orders", "industrial production", "manufacturing production",
    "capacity utilization", "business inventories", "wholesale inventories", "retail inventories",
    # Consumo / sentimiento
    "cb leading", "leading index", "inflation expectations", "consumer expectations",
    "current conditions", "consumer inflation", "personal income", "personal spending",
    "real personal spending", "consumer credit", "vehicle sales", "construction spending",
    # Fed regionales (índices manufactureros)
    "chicago pmi", "chicago fed", "dallas fed", "richmond fed", "kansas fed",
    "fed services", "services activity",
    # Empleo secundario
    "challenger", "productivity", "labor costs", "participation rate",
    "manufacturing payrolls", "government payrolls", "nonfarm payrolls private",
    "u-6 unemployment", "average weekly hours",
    # ISM sub-líneas (secundario al PMI principal)
    "ism manufacturing", "ism services",
]

# Feriados US — mercado cerrado (Finviz los muestra; el trader debe saberlo)
US_HOLIDAYS = [
    "independence day", "juneteenth", "memorial day", "labor day",
    "thanksgiving", "christmas", "new year", "martin luther king",
    "washington", "presidents day", "columbus day", "veterans day",
    "bank holiday", "markets closed",
]

def _is_holiday(name: str) -> bool:
    if not name:
        return False
    n = name.lower()
    return any(h in n for h in US_HOLIDAYS)

def _event_allowed(name: str) -> bool:
    """Estilo Finviz: rechaza ruido conocido, acepta cualquier macro relevante."""
    if not name:
        return False
    n = name.lower()
    # Feriados SÍ se muestran (mercado cerrado)
    if _is_holiday(name):
        return True
    # 1) Descartar ruido explícito
    for bad in EVENT_BLOCKLIST:
        if bad in n:
            return False
    # 2) Aceptar si está en alta o media relevancia
    for kw in HIGH_RELEVANCE:
        if kw in n:
            return True
    for kw in MEDIUM_RELEVANCE:
        if kw in n:
            return True
    return False

def _event_relevance(name: str, ff_impact: str) -> str:
    """Determina el impacto final combinando la categoría del evento + el impacto de la fuente.
    Estilo Finviz: ciertos eventos son siempre high aunque la fuente diga medium."""
    n = (name or "").lower()
    # Feriados: impacto especial "holiday" (el frontend lo muestra distinto)
    if _is_holiday(name):
        return "holiday"
    # Eventos que SIEMPRE son alto impacto para índices
    for kw in HIGH_RELEVANCE:
        if kw in n:
            return "high"
    # El resto que pasó el filtro es al menos medium
    for kw in MEDIUM_RELEVANCE:
        if kw in n:
            # Respetar 'high' si la fuente lo marcó así, sino medium
            return "high" if ff_impact == "high" else "medium"
    return ff_impact or "medium"

def _impact_label(impact) -> str:
    # Finnhub usa 1/2/3 o low/medium/high
    s = str(impact).lower()
    if s in ("3", "high"):   return "high"
    if s in ("2", "medium"): return "medium"
    if s in ("1", "low"):    return "low"
    return "medium"

# Forex Factory publica un JSON semanal gratis, SIN API key.
# (Finnhub calendar es premium; FF es la fuente gratuita para macro.)
FF_URLS = [
    "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
    "https://nfs.faireconomy.media/ff_calendar_nextweek.json",
]

def _ff_impact(val) -> str:
    s = str(val).lower()
    if "high" in s:   return "high"
    if "medium" in s: return "medium"
    if "low" in s:    return "low"
    return "medium"

async def fetch_calendar():
    """Calendario macro US desde Forex Factory (sin key). Filtra US + High/Medium + eventos permitidos."""
    out = []
    async with httpx.AsyncClient(timeout=3, headers={"User-Agent": "Mozilla/5.0"}) as client:
        for url in FF_URLS:
            try:
                r = await client.get(url)
                if r.status_code != 200:
                    continue
                rows = r.json()
            except Exception as e:
                print(f"[calendar] FF {url} fallo: {e}")
                continue
            for ev in rows:
                # FF usa "country":"USD" para eventos de Estados Unidos
                country = str(ev.get("country", "")).upper()
                if country not in ("USD", "US", "UNITED STATES"):
                    continue
                name = ev.get("title", "") or ev.get("event", "")
                if not _event_allowed(name):
                    continue
                # Impacto final = relevancia del evento para índices (estilo Finviz)
                ff_imp = _ff_impact(ev.get("impact"))
                impact = _event_relevance(name, ff_imp)
                # Solo mostramos high + medium (descartamos lo que quedó en low)
                if impact == "low":
                    continue
                actual = ev.get("actual", "")
                released = actual is not None and str(actual).strip() != ""
                out.append({
                    "title": name,
                    "source": "Forex Factory",
                    "time": ev.get("date", ""),   # ISO con timezone, ej 2026-06-15T08:30:00-04:00
                    "impact": impact,
                    "actual": actual or None,
                    "forecast": ev.get("forecast") or None,
                    "previous": ev.get("previous") or None,
                    "status": "Released" if released else "Upcoming",
                    "type": "macro",
                })
    if not out:
        raise RuntimeError("Forex Factory sin eventos US (o no respondió)")
    # Dedup: mismo evento+hora (FF a veces repite). Mantener cronológico.
    seen = set()
    deduped = []
    for e in out:
        k = (e["title"].lower().strip(), e["time"][:16])
        if k in seen:
            continue
        seen.add(k)
        deduped.append(e)
    deduped.sort(key=lambda e: e.get("time") or "")
    return deduped

async def refresh_calendar(retry=True):
    """Con retry 2s/5s y stale-data (regla)."""
    delays = [0, 2, 5]
    for i, d in enumerate(delays):
        if d:
            await asyncio.sleep(d)
        try:
            t0 = time.time()
            data = await fetch_calendar()
            cache["calendar"]["data"] = data
            cache["calendar"]["last_update"] = datetime.now(NY).isoformat()
            cache["calendar"]["status"] = "fresh"
            cache["health"]["finnhub"] = "online"
            print(f"[calendar] ok: {len(data)} eventos en {time.time()-t0:.2f}s")
            return
        except Exception as e:
            print(f"[calendar] intento {i+1} falló: {e}")
    # Si falla todo: mantener últimos datos válidos (stale)
    if cache["calendar"]["data"]:
        cache["calendar"]["status"] = "stale"
        cache["health"]["finnhub"] = "stale"
    else:
        cache["calendar"]["status"] = "offline"
        cache["health"]["finnhub"] = "offline"
    print("[calendar] usando datos previos (stale)")

# ═══════════════════════════════════════════════════════════
#  SERVICIO 3: Finnhub — MOVERS ULTRA (noticias, Fuente 2)
# ═══════════════════════════════════════════════════════════
# Keywords que SÍ nos interesan (empresas + temas macro del NQ)
MOVER_KEYWORDS = {
    # Empresas (con su score)
    "nvidia": 95, "nvda": 95,
    "apple": 94, "aapl": 94,
    "tesla": 92, "tsla": 92,
    "microsoft": 90, "msft": 90,
    "amazon": 88, "amzn": 88,
    "meta": 85, "facebook": 85,
    "broadcom": 80, "avgo": 80,
    "amd": 75,
    "oracle": 70, "orcl": 70,
    "intel": 60, "intc": 60,
    "alphabet": 88, "google": 88, "googl": 88,
    "tsmc": 82, "taiwan semiconductor": 82,
    "qualcomm": 72, "qcom": 72,
    "openai": 90,
    "blackrock": 96, "larry fink": 96,
    # Macro / personas / temas (score alto)
    "federal reserve": 98, "fed ": 98, "fomc": 100,
    "powell": 98, "jerome powell": 98,
    "trump": 97, "tariff": 97, "tariffs": 97,
    "elon musk": 85,
    "us treasury": 90, "treasury yield": 88, "bond yield": 88,
    "china": 85, "trade war": 90,
    "opec": 75,
    "inflation": 88, "cpi": 100, "ppi": 100,
    "interest rate": 90, "rate cut": 92, "rate hike": 92,
    "recession": 88, "nasdaq": 85, "s&p 500": 85, "s&p500": 85,
    "semiconductor": 80, "artificial intelligence": 78, " ai ": 75,
    "geopolitics": 78,
}
# Basura que se ignora siempre
MOVER_BLOCKLIST = [
    "penny stock", "otc", "small cap", "crypto", "bitcoin", "ethereum",
    "memecoin", "dogecoin", "shiba", "nft", "sports", "entertainment",
    "celebrity", "gossip", "horoscope", "lottery", "casino", "betting",
    "coupon", "discount code", "giveaway", "sponsored",
]

def _score_headline(title: str):
    """Devuelve (score, symbol) si el titular es relevante, sino (0, None)."""
    if not title:
        return 0, None
    t = " " + title.lower() + " "
    # Bloquear basura
    for bad in MOVER_BLOCKLIST:
        if bad in t:
            return 0, None
    # Buscar la keyword de mayor score presente
    best_score, best_sym = 0, None
    for kw, sc in MOVER_KEYWORDS.items():
        if kw in t and sc > best_score:
            best_score = sc
            best_sym = kw.strip().upper()[:8]
    return best_score, best_sym

async def fetch_movers():
    """Noticias relevantes para el NQ desde Finnhub /news (gratis). Máx 5 por score."""
    if not FINNHUB_KEY:
        raise RuntimeError("FINNHUB_KEY no configurada")
    async with httpx.AsyncClient(timeout=8) as client:  # /news puede tardar
        r = await client.get(f"{FH_BASE}/news",
                             params={"category": "general", "token": FINNHUB_KEY})
        if r.status_code != 200:
            raise RuntimeError(f"Finnhub news {r.status_code}")
        rows = r.json()
    scored = []
    seen_titles = set()
    for n in rows:
        title = n.get("headline", "") or ""
        key = title.lower().strip()[:60]
        if key in seen_titles:
            continue
        score, sym = _score_headline(title)
        if score < 60:
            continue
        seen_titles.add(key)
        scored.append({
            "title": title,
            "source": n.get("source", "Finnhub"),
            "timestamp": n.get("datetime", 0),
            "url": n.get("url", ""),
            "impact": "ultra" if score >= 95 else ("high" if score >= 85 else "medium"),
            "score": score,
            "type": "mover",
            "symbol": sym,
        })
    scored.sort(key=lambda x: x["score"], reverse=True)
    # Dedup por TEMA: máx 1 noticia por símbolo para dar variedad.
    # (evita mostrar 5 noticias del mismo evento, ej. 5x Trump)
    out, used_syms = [], set()
    for m in scored:
        s = m["symbol"]
        if s in used_syms:
            continue
        used_syms.add(s)
        out.append(m)
        if len(out) >= 5:
            break
    # Si no llegamos a 5 con símbolos únicos, rellenar con los siguientes por score
    if len(out) < 5:
        for m in scored:
            if m not in out:
                out.append(m)
                if len(out) >= 5:
                    break
    return out[:5]

async def refresh_movers(retry=True):
    delays = [0, 2, 5]
    for i, d in enumerate(delays):
        if d:
            await asyncio.sleep(d)
        try:
            t0 = time.time()
            data = await fetch_movers()
            cache["movers"]["data"] = data
            cache["movers"]["last_update"] = datetime.now(NY).isoformat()
            cache["movers"]["status"] = "fresh"
            cache["health"]["finnhub"] = "online"
            print(f"[movers] ok: {len(data)} noticias en {time.time()-t0:.2f}s")
            return
        except Exception as e:
            print(f"[movers] intento {i+1} fallo: {e}")
    if cache["movers"]["data"]:
        cache["movers"]["status"] = "stale"
    else:
        cache["movers"]["status"] = "offline"
    print("[movers] usando datos previos (stale)")

# ═══════════════════════════════════════════════════════════
#  SERVICIO 4: Finnhub — EARNINGS CALENDAR
#  Trae earnings de empresas US, etiqueta impacto en el NQ.
# ═══════════════════════════════════════════════════════════

# Magnificent 7 + mega-caps → impacto EXTREMO en el NQ
EARN_EXTREME = {
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "TSLA",
    "AVGO", "NFLX",
}
# Big tech NQ-100 / movers importantes → impacto ALTO
EARN_HIGH = {
    "AMD", "INTC", "QCOM", "MU", "TSM", "ASML", "ORCL", "CRM", "ADBE",
    "CSCO", "TXN", "AMAT", "LRCX", "PANW", "CRWD", "SNOW", "PLTR", "SMCI",
    "MRVL", "ARM", "DELL", "NOW", "INTU", "IBM", "UBER", "SHOP", "COIN",
    "PYPL", "SBUX", "PEP", "COST", "CMCSA", "TMUS", "AMGN", "GILD", "BKNG",
    "ABNB", "MRNA", "REGN", "ADP", "ADI", "KLAC", "MCHP", "WDAY", "FTNT",
    "DDOG", "ZS", "NXPI",
}
# Empresas grandes conocidas (S&P, no NQ-100) → impacto MEDIO
EARN_MEDIUM = {
    "JPM", "BAC", "WFC", "C", "GS", "MS", "V", "MA", "AXP", "BLK",
    "UNH", "JNJ", "LLY", "PFE", "ABBV", "MRK", "TMO", "DHR", "ABT",
    "WMT", "HD", "MCD", "NKE", "DIS", "KO", "PG", "XOM", "CVX", "CAT",
    "BA", "GE", "HON", "RTX", "GM", "F", "T", "VZ", "FDX", "UPS",
    "ACN", "NOW", "ISRG", "TGT", "LOW", "DAL", "UAL", "GIS", "STZ",
    "KMX", "JBL", "LEN", "KBH", "NKE", "MKC", "PAYX", "WBA",
}

def _earn_impact(symbol: str) -> str:
    s = (symbol or "").upper()
    if s in EARN_EXTREME: return "extreme"
    if s in EARN_HIGH:    return "high"
    if s in EARN_MEDIUM:  return "medium"
    return "normal"

def _is_relevant_earning(ev: dict) -> bool:
    """Incluye TODAS las empresas con datos reales (como el 'All' de EarningsHub).
    Solo descarta basura total: símbolos sin ningún dato (shells/SPACs/OTC sin
    EPS ni revenue estimado) o símbolos malformados. El filtro por severidad
    (extreme/high/medium) lo hace el FRONTEND, no el backend."""
    sym = (ev.get("symbol") or "").upper()
    # Símbolos en nuestras listas → siempre incluir
    if _earn_impact(sym) != "normal":
        return True
    # Símbolo válido (letras, 1-5 chars, sin sufijos raros tipo .XX)
    if not sym or not sym.replace(".", "").isalpha() or len(sym) > 6:
        return False
    # Debe tener AL MENOS un dato (EPS o revenue estimado) — señal de cobertura real
    has_eps = ev.get("epsEstimate") is not None
    has_rev = ev.get("revenueEstimate") not in (None, 0)
    return has_eps or has_rev

async def fetch_earnings(days_ahead=45):
    """Trae el calendario de earnings de Finnhub para las próximas semanas."""
    if not FINNHUB_KEY:
        raise RuntimeError("Sin FINNHUB_KEY")
    today = datetime.now(NY).date()
    frm = today.isoformat()
    to = (today + timedelta(days=days_ahead)).isoformat()
    url = f"{FH_BASE}/calendar/earnings?from={frm}&to={to}&token={FINNHUB_KEY}"
    async with httpx.AsyncClient(timeout=8) as client:
        r = await client.get(url)
        if r.status_code != 200:
            raise RuntimeError(f"Finnhub earnings {r.status_code}")
        data = r.json()
    rows = data.get("earningsCalendar", []) if isinstance(data, dict) else []
    out = []
    for ev in rows:
        if not _is_relevant_earning(ev):
            continue
        sym = (ev.get("symbol") or "").upper()
        out.append({
            "symbol": sym,
            "date": ev.get("date"),
            "hour": ev.get("hour") or "",          # bmo / amc / dmh / ""
            "quarter": ev.get("quarter"),
            "year": ev.get("year"),
            "epsEstimate": ev.get("epsEstimate"),
            "epsActual": ev.get("epsActual"),
            "revenueEstimate": ev.get("revenueEstimate"),
            "revenueActual": ev.get("revenueActual"),
            "impact": _earn_impact(sym),
        })
    # Ordenar por fecha y luego por impacto (extremo primero dentro del día)
    impact_rank = {"extreme": 0, "high": 1, "medium": 2, "normal": 3}
    out.sort(key=lambda e: (e.get("date") or "", impact_rank.get(e["impact"], 9), e["symbol"]))
    return out

async def refresh_earnings(retry=True):
    delays = [0, 2, 5]
    for i, d in enumerate(delays):
        if d:
            await asyncio.sleep(d)
        try:
            t0 = time.time()
            data = await fetch_earnings()
            cache["earnings"]["data"] = data
            cache["earnings"]["last_update"] = datetime.now(NY).isoformat()
            cache["earnings"]["status"] = "fresh"
            cache["health"]["finnhub"] = "online"
            _save_cache()  # persistir para sobrevivir redeploys
            print(f"[earnings] ok: {len(data)} empresas en {time.time()-t0:.2f}s")
            return
        except Exception as e:
            print(f"[earnings] intento {i+1} fallo: {e}")
    if cache["earnings"]["data"]:
        cache["earnings"]["status"] = "stale"
    else:
        cache["earnings"]["status"] = "offline"
    print("[earnings] usando datos previos (stale)")

# ═══════════════════════════════════════════════════════════
#  ENDPOINTS
# ═══════════════════════════════════════════════════════════
@app.get("/")
def root():
    return {"status": "ok", "service": "Liberato Backend"}

@app.get("/health")
def health():
    today = datetime.now(NY).strftime("%Y-%m-%d")
    day_sched = cache["gex_schedule"].get(today, {})
    return {
        "flashalpha": cache["health"]["flashalpha"],
        "finnhub": cache["health"]["finnhub"],
        "calendar_status": cache["calendar"]["status"],
        "calendar_count": len(cache["calendar"]["data"]),
        "movers_status": cache["movers"]["status"],
        "movers_count": len(cache["movers"]["data"]),
        "earnings_status": cache["earnings"]["status"],
        "earnings_count": len(cache["earnings"]["data"]),
        "gex_assets_cached": list(cache["gex"].keys()),
        "gex_schedule_today": {
            "date": today,
            "close_done": day_sched.get("close", False),   # 7:00 PM ET
            "open_done":  day_sched.get("open",  False),   # 9:10 AM ET
            "calls_today": sum(1 for v in day_sched.values() if v),
            "max_calls_per_day": 2,
        },
    }

@app.get("/api/gex/schedule")
def gex_schedule_status():
    """Endpoint de diagnóstico — muestra el estado del control de créditos FlashAlpha.
    Permite verificar que no se están consumiendo créditos fuera de los horarios programados."""
    now_et = datetime.now(NY)
    today = now_et.strftime("%Y-%m-%d")
    day_sched = cache["gex_schedule"].get(today, {})
    all_schedules = cache["gex_schedule"]
    total_calls = sum(
        sum(1 for v in slots.values() if v)
        for slots in all_schedules.values()
    )
    return {
        "policy": "FlashAlpha se consulta MÁXIMO 2 veces/día en horarios fijos",
        "slots": {
            "close": {"time": "7:00 PM ET", "purpose": "Niveles post-cierre (fuente principal)", "done_today": day_sched.get("close", False)},
            "open":  {"time": "9:10 AM ET", "purpose": "Verificación pre-apertura", "done_today": day_sched.get("open",  False)},
        },
        "today": today,
        "now_et": now_et.isoformat(),
        "calls_today": sum(1 for v in day_sched.values() if v),
        "total_calls_tracked": total_calls,
        "gex_cached_assets": list(cache["gex"].keys()),
        "flashalpha_status": cache["health"]["flashalpha"],
        "schedule_history": all_schedules,
    }

@app.get("/api/market/gamma-levels/{asset}")
async def gamma_levels(asset: str):
    """Niveles de Gamma Exposure. Lee EXCLUSIVAMENTE del caché.

    ──────────────────────────────────────────────────────────────
    ARQUITECTURA ESTRICTA: FlashAlpha → Scheduler → Caché → Frontend
    Este endpoint NUNCA llama FlashAlpha directamente.
    FlashAlpha solo se consume 2 veces/día desde los schedulers:
        • 7:00 PM ET  → slot "close" (niveles post-cierre)
        • 9:10 AM ET  → slot "open"  (verificación pre-apertura)
    ──────────────────────────────────────────────────────────────
    Si no hay datos en caché (primer arranque sin snapshot de disco),
    devuelve 503 y el frontend muestra "--" hasta el próximo slot.
    """
    asset = asset.upper()
    if asset not in PROXIES:
        raise HTTPException(400, "Activo no soportado")

    cached = cache["gex"].get(asset)
    if not cached:
        # Sin snapshot de disco ni llamada programada completada aún
        raise HTTPException(503, detail={
            "error": "GEX no disponible",
            "message": "Datos de gamma pendientes. Disponibles tras el cierre del mercado (7 PM ET).",
            "next_update": "7:00 PM ET (post-cierre) o 9:10 AM ET (pre-apertura)",
        })

    # Calcular ratio EXACTO si hay precio del futuro en vivo (TradeStation).
    qqq_price = cached.get("underlying_price")
    nq_price, ratio = await get_nq_ratio(asset, qqq_price)

    # Informar qué slot fue el último y cuándo
    today = datetime.now(NY).strftime("%Y-%m-%d")
    day_sched = cache["gex_schedule"].get(today, {})

    return {
        **cached,
        "asset": asset,
        "nq_price": nq_price,
        "ratio": ratio,
        "credits_used": 0,
        "_schedule": {
            "close_done": day_sched.get("close", False),
            "open_done": day_sched.get("open", False),
            "next_slots": ["7:00 PM ET", "9:10 AM ET"],
        },
    }

@app.get("/api/calendar")
async def get_calendar():
    """Calendario macro US. Lee del caché (los usuarios nunca disparan la API)."""
    # Refrescar si lleva más de 5 min sin actualizar
    last = cache["calendar"]["last_update"]
    stale = True
    if last:
        try:
            age = (datetime.now(NY) - datetime.fromisoformat(last)).total_seconds()
            stale = age > 300
        except Exception:
            stale = True
    if stale:
        await refresh_calendar()
    # Próximo evento (el primer Upcoming)
    upcoming = [e for e in cache["calendar"]["data"] if e["status"] == "Upcoming"]
    return {
        "macro_calendar": cache["calendar"]["data"],
        "next_macro_event": upcoming[0] if upcoming else None,
        "last_update": cache["calendar"]["last_update"],
        "status": cache["calendar"]["status"],
        "count": len(cache["calendar"]["data"]),
    }

@app.get("/api/movers")
async def get_movers():
    """Movers Ultra. Refresca al vuelo si está vacío o lleva +2 min."""
    last = cache["movers"]["last_update"]
    need = not cache["movers"]["data"]
    if last and not need:
        try:
            age = (datetime.now(NY) - datetime.fromisoformat(last)).total_seconds()
            need = age > 120
        except Exception:
            need = True
    if need:
        await refresh_movers()
    return {
        "market_movers": cache["movers"]["data"],
        "last_update": cache["movers"]["last_update"],
        "status": cache["movers"]["status"],
        "count": len(cache["movers"]["data"]),
    }

@app.get("/api/earnings")
async def get_earnings():
    """Calendario de earnings (Finnhub). Refresca si está vacío o lleva +6h.
    Los earnings cambian poco durante el día, así que el caché largo basta."""
    last = cache["earnings"]["last_update"]
    need = not cache["earnings"]["data"]
    if last and not need:
        try:
            age = (datetime.now(NY) - datetime.fromisoformat(last)).total_seconds()
            need = age > 6 * 3600
        except Exception:
            need = True
    if need:
        await refresh_earnings()
    return {
        "earnings": cache["earnings"]["data"],
        "last_update": cache["earnings"]["last_update"],
        "status": cache["earnings"]["status"],
        "count": len(cache["earnings"]["data"]),
    }

@app.get("/api/company/{ticker}")
async def get_company(ticker: str):
    """Detalle de UNA empresa: histórico de earnings (últimos 4-5), crecimiento,
    market cap. Se llama solo al tocar la empresa. Cache 24h por ticker."""
    sym = ticker.upper().strip()
    # Servir de caché si es reciente (24h)
    cached = cache["company"].get(sym)
    if cached and (time.time() - cached.get("ts", 0)) < 24 * 3600:
        return cached["data"]
    if not FINNHUB_KEY:
        raise HTTPException(503, "Sin FINNHUB_KEY")

    out = {
        "symbol": sym,
        "name": sym,
        "marketCap": None,
        "epsGrowthYoY": None,
        "revenueGrowthYoY": None,
        "history": [],   # [{period, epsEstimate, epsActual, surprise, surprisePercent}]
        "nextEpsEstimate": None,
        "nextRevenueEstimate": None,
    }
    try:
        async with httpx.AsyncClient(timeout=6) as client:
            # 1) Perfil (nombre, market cap)
            try:
                rp = await client.get(f"{FH_BASE}/stock/profile2",
                                      params={"symbol": sym, "token": FINNHUB_KEY})
                if rp.status_code == 200:
                    p = rp.json() or {}
                    out["name"] = p.get("name") or sym
                    mc = p.get("marketCapitalization")  # en millones USD
                    if mc:
                        out["marketCap"] = (f"${mc/1e6:.2f}T" if mc >= 1e6
                                            else f"${mc/1e3:.1f}B" if mc >= 1e3
                                            else f"${mc:.0f}M")
            except Exception as e:
                print(f"[company] profile {sym} fallo: {e}")

            # 2) Histórico de earnings (EPS estimado vs actual, últimos trimestres)
            try:
                re_ = await client.get(f"{FH_BASE}/stock/earnings",
                                       params={"symbol": sym, "limit": 5, "token": FINNHUB_KEY})
                if re_.status_code == 200:
                    rows = re_.json() or []
                    hist = []
                    for r in rows[:5]:
                        est = r.get("estimate")
                        act = r.get("actual")
                        surprise = r.get("surprise")
                        surprise_pct = r.get("surprisePercent")
                        # period viene como "2026-03-31"
                        period = r.get("period", "")
                        q = r.get("quarter")
                        y = r.get("year")
                        label = f"Q{q} {y}" if q and y else period
                        beat = None
                        if est is not None and act is not None:
                            beat = "beat" if act >= est else "miss"
                        hist.append({
                            "period": label,
                            "date": period,
                            "epsEstimate": est,
                            "epsActual": act,
                            "surprise": surprise,
                            "surprisePercent": surprise_pct,
                            "result": beat,
                        })
                    out["history"] = hist
            except Exception as e:
                print(f"[company] earnings hist {sym} fallo: {e}")

            # 3) Métricas básicas (crecimiento)
            try:
                rm = await client.get(f"{FH_BASE}/stock/metric",
                                      params={"symbol": sym, "metric": "all", "token": FINNHUB_KEY})
                if rm.status_code == 200:
                    m = (rm.json() or {}).get("metric", {}) or {}
                    epsg = m.get("epsGrowthTTMYoy") or m.get("epsGrowthQuarterlyYoy")
                    revg = m.get("revenueGrowthTTMYoy") or m.get("revenueGrowthQuarterlyYoy")
                    if epsg is not None:
                        out["epsGrowthYoY"] = f"{epsg:+.1f}%"
                    if revg is not None:
                        out["revenueGrowthYoY"] = f"{revg:+.1f}%"
            except Exception as e:
                print(f"[company] metric {sym} fallo: {e}")
    except Exception as e:
        print(f"[company] {sym} error general: {e}")

    cache["company"][sym] = {"data": out, "ts": time.time()}
    return out

@app.get("/api/dashboard")
async def get_dashboard():
    """Endpoint ÚNICO que consume el frontend. Junta calendario + movers + health."""
    upcoming = [e for e in cache["calendar"]["data"] if e["status"] == "Upcoming"]
    # Breaking popup: el mover de mayor score si es >= 95
    movers = cache["movers"]["data"]
    breaking = None
    if movers and movers[0].get("score", 0) >= 95:
        breaking = movers[0]
    return {
        "macro_calendar": cache["calendar"]["data"],
        "market_movers": movers,
        "breaking_popup": breaking,
        "next_macro_event": upcoming[0] if upcoming else None,
        "last_update": {
            "calendar": cache["calendar"]["last_update"],
            "movers": cache["movers"]["last_update"],
        },
        "health": {
            "flashalpha": cache["health"]["flashalpha"],
            "finnhub": cache["health"]["finnhub"],
            "calendar": cache["calendar"]["status"],
            "movers": cache["movers"]["status"],
        },
    }

# ═══════════════════════════════════════════════════════════
#  SCHEDULER — FlashAlpha: EXACTAMENTE 2 LLAMADAS/DÍA
# ═══════════════════════════════════════════════════════════
scheduler = AsyncIOScheduler(timezone=NY)

# ── Wrappers para los dos slots permitidos ──

async def gex_slot_close():
    """7:00 PM ET — Niveles post-cierre del mercado (los datos del día).
    Estos son los niveles calculados al cierre (el 17 Jun usa datos del 16 Jun).
    Es la llamada principal y más importante del día."""
    print("[scheduler] ▶ slot CLOSE (7:00 PM ET) — llamando FlashAlpha")
    await refresh_gex("NQ", slot="close")

async def gex_slot_open():
    """9:10 AM ET — Verificación pre-apertura.
    Confirma si FlashAlpha publicó algún ajuste overnight antes del market open.
    Segunda y última llamada del día."""
    print("[scheduler] ▶ slot OPEN (9:10 AM ET) — llamando FlashAlpha")
    await refresh_gex("NQ", slot="open")

# ── Limpieza del registro de slots (medianoche ET) ──
async def reset_daily_schedule():
    """Limpia el registro de slots completados al inicio de cada nuevo día ET.
    Se ejecuta a las 12:00 AM ET — así los slots del nuevo día quedan disponibles."""
    today = datetime.now(NY).strftime("%Y-%m-%d")
    # Solo limpiar días anteriores, no el actual
    days_to_clean = [d for d in list(cache["gex_schedule"].keys()) if d < today]
    for d in days_to_clean:
        del cache["gex_schedule"][d]
    if days_to_clean:
        print(f"[scheduler] schedule limpiado para días anteriores: {days_to_clean}")
    _save_cache()

@app.on_event("startup")
async def startup():
    # ── PASO 1: Restaurar snapshot de disco ──
    # Si hay datos de GEX + schedule guardados, el servidor arranca con datos
    # sin hacer ninguna llamada a FlashAlpha. Este es el comportamiento correcto
    # ante cualquier deploy, reinicio, hot reload o cambio de código.
    _load_cache()

    # ── PASO 2: Registrar scheduler de FlashAlpha (solo 2 slots/día) ──
    #
    #   7:00 PM ET  → slot "close" — niveles post-cierre (fuente de verdad)
    #   9:10 AM ET  → slot "open"  — verificación pre-apertura
    #
    # Sin más. Ningún otro job toca FlashAlpha.
    scheduler.add_job(
        gex_slot_close,
        CronTrigger(hour=19, minute=0, day_of_week="mon-fri"),
        id="gex_close",
        replace_existing=True,
        misfire_grace_time=300,   # 5 min de gracia si el servidor estaba bajando
    )
    scheduler.add_job(
        gex_slot_open,
        CronTrigger(hour=9, minute=10, day_of_week="mon-fri"),
        id="gex_open",
        replace_existing=True,
        misfire_grace_time=300,
    )
    # Limpieza del registro de slots: cada noche a medianoche ET
    scheduler.add_job(
        reset_daily_schedule,
        CronTrigger(hour=0, minute=0),
        id="schedule_reset",
        replace_existing=True,
    )

    # ── PASO 3: Schedulers para Finnhub (sin impacto en FlashAlpha) ──
    # Forex Factory (calendario macro): cada 5 min (sin key, sin límite relevante)
    scheduler.add_job(refresh_calendar, IntervalTrigger(minutes=5), id="calendar_refresh", replace_existing=True)
    # Movers (Finnhub news free tier): cada 60s
    scheduler.add_job(refresh_movers, IntervalTrigger(seconds=60), id="movers_refresh", replace_existing=True)
    # Earnings (Finnhub): cada 6 horas (cambian poco durante el día)
    scheduler.add_job(refresh_earnings, IntervalTrigger(hours=6), id="earnings_refresh", replace_existing=True)

    scheduler.start()

    # ── PASO 4: Primera carga de Finnhub (calendario, movers, earnings) ──
    # Estos son seguros — sin límites críticos. Se cargan al arrancar.
    # FlashAlpha NO se llama aquí. Si hay snapshot de disco, se usa directamente.
    await asyncio.gather(
        refresh_calendar(),
        refresh_movers(),
        refresh_earnings(),
        return_exceptions=True,
    )

    # ── LOG de estado al arrancar ──
    gex_cached = list(cache["gex"].keys())
    today = datetime.now(NY).strftime("%Y-%m-%d")
    day_sched = cache["gex_schedule"].get(today, {})
    fa_status = cache["health"]["flashalpha"]
    print(f"[startup] FlashAlpha: {fa_status} | GEX assets en caché: {gex_cached}")
    print(f"[startup] Schedule hoy ({today}): close={day_sched.get('close', False)}, open={day_sched.get('open', False)}")
    if not gex_cached:
        print(f"[startup] ⚠ Sin datos GEX en caché. Próxima actualización: 7:00 PM ET (slot close) o 9:10 AM ET (slot open)")
    else:
        print(f"[startup] ✓ GEX servido desde caché — FlashAlpha NO llamado al arrancar")
