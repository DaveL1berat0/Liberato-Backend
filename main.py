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

app = FastAPI(title="Liberato Backend")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Caché en memoria por servicio (desacoplado) ──
cache = {
    "gex":      {},                                    # { "NQ": {...} }
    "calendar": {"data": [], "last_update": None, "status": "offline"},
    "movers":   {"data": [], "last_update": None, "status": "offline"},
    "health":   {"flashalpha": "offline", "finnhub": "offline"},
}

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

async def refresh_gex(asset="NQ"):
    """Refresca GEX. Si FlashAlpha falla (429/timeout), MANTIENE los últimos
    datos válidos (stale) — nunca los borra. Regla de tolerancia a fallos."""
    try:
        data = await fetch_flashalpha(asset)
        data["_ts"] = time.time()
        cache["gex"][asset] = data
        cache["health"]["flashalpha"] = "online"
        print(f"[gex] {asset} ok: {data.get('source')}")
    except Exception as e:
        # Si ya tenemos datos previos, mantenerlos (stale). Solo offline si nunca hubo datos.
        if cache["gex"].get(asset):
            cache["health"]["flashalpha"] = "stale"
            print(f"[gex] {asset} fallo ({e}) — manteniendo último dato válido")
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
#  ENDPOINTS
# ═══════════════════════════════════════════════════════════
@app.get("/")
def root():
    return {"status": "ok", "service": "Liberato Backend"}

@app.get("/health")
def health():
    return {
        "flashalpha": cache["health"]["flashalpha"],
        "finnhub": cache["health"]["finnhub"],
        "calendar_status": cache["calendar"]["status"],
        "calendar_count": len(cache["calendar"]["data"]),
        "movers_status": cache["movers"]["status"],
        "movers_count": len(cache["movers"]["data"]),
    }

@app.get("/api/market/gamma-levels/{asset}")
async def gamma_levels(asset: str):
    asset = asset.upper()
    if asset not in PROXIES:
        raise HTTPException(400, "Activo no soportado")
    # Servir SIEMPRE del caché. Solo refrescar si no hay datos, o si son MUY viejos (>12h).
    # Esto minimiza llamadas a FlashAlpha (evita el 429 por exceso de peticiones).
    cached = cache["gex"].get(asset)
    age = time.time() - cached.get("_ts", 0) if cached else 1e9
    if not cached or age > 12 * 3600:
        await refresh_gex(asset)
        cached = cache["gex"].get(asset)
    if not cached:
        # Sin datos aún (FlashAlpha caído y nunca cargó) — error suave, el frontend muestra "--"
        raise HTTPException(503, "GEX temporalmente no disponible")
    return {**cached, "asset": asset, "credits_used": 0}

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
#  SCHEDULER
# ═══════════════════════════════════════════════════════════
scheduler = AsyncIOScheduler(timezone=NY)

async def recover_gex_if_down():
    """Si FlashAlpha está stale/offline, reintenta 1 vez. Recuperación tras 429.
    Solo actúa si hace falta — no gasta llamadas si todo está bien."""
    if cache["health"]["flashalpha"] != "online":
        print("[gex] recuperación: reintentando FlashAlpha...")
        await refresh_gex("NQ")

@app.on_event("startup")
async def startup():
    # GEX: 9:00, 9:30, 9:45 ET lun-vie
    for h, m in [(9, 0), (9, 30), (9, 45)]:
        scheduler.add_job(refresh_gex, CronTrigger(hour=h, minute=m, day_of_week="mon-fri"), args=["NQ"])
    # Calendario: cada 5 min (regla)
    scheduler.add_job(refresh_calendar, IntervalTrigger(minutes=5))
    # Movers: cada 60s (Finnhub free tier — 30s gastaría demasiadas llamadas)
    scheduler.add_job(refresh_movers, IntervalTrigger(seconds=60))
    # Recuperación GEX: cada 30 min, reintenta SOLO si FlashAlpha está caído (tras 429)
    scheduler.add_job(recover_gex_if_down, IntervalTrigger(minutes=30))
    scheduler.start()
    # Primera carga al arrancar. Calendario y movers son seguros (límites altos/gratis).
    # GEX: intento único — si da 429, el scheduler lo reintentará en el próximo cron
    # (9:00/9:30/9:45 ET). Así no insistimos y evitamos agravar el rate limit.
    await asyncio.gather(refresh_calendar(), refresh_movers(), return_exceptions=True)
    # GEX por separado, tolerante a fallo (no rompe el arranque)
    try:
        await refresh_gex("NQ")
    except Exception as e:
        print(f"[startup] GEX inicial falló (reintentará en cron): {e}")
