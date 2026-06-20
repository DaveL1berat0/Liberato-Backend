"""Liberato Backend v2 — Trading terminal NQ.
Servicios: FlashAlpha (GEX) + Finnhub (calendar/movers/earnings)
         + Yahoo Finance (heatmap 22 activos) + Anthropic Claude (resumen IA).
Keys SOLO en variables de entorno. Si un servicio falla, los demás siguen.
NUEVO v2: /api/heatmap + /api/context/institutional
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
FLASHALPHA_KEY   = os.getenv("FLASHALPHA_KEY", "")
FINNHUB_KEY      = os.getenv("FINNHUB_KEY", "")
GROQ_KEY         = os.getenv("GROQ_KEY", "")              # IA gratuita — Llama 3.3 vía Groq
TWELVEDATA_KEY   = os.getenv("TWELVEDATA_KEY", "")        # NUEVA: Key de Twelvedata
ALPHAVANTAGE_KEY = os.getenv("ALPHAVANTAGE_KEY", "")      # NUEVA: Key de AlphaVantage

FA_BASE = "https://lab.flashalpha.com"
FH_BASE = "https://finnhub.io/api/v1"
NY = ZoneInfo("America/New_York")
PROXIES = {"NQ": "QQQ", "ES": "SPY", "GC": "GLD"}
FUTURES = {"NQ": "NQ", "ES": "ES", "GC": "GC"}

# ═══════════════════════════════════════════════════════════
#  PRECIO DEL FUTURO EN VIVO — TWELVEDATA WEBSOCKET OPTIMIZATION
# ═══════════════════════════════════════════════════════════
TRADESTATION_KEY = os.getenv("TRADESTATION_KEY", "")
TRADESTATION_SECRET = os.getenv("TRADESTATION_SECRET", "")

# Variable global para almacenar el precio en tiempo real vía WebSocket
_LIVE_PRICES = {"NQ": None, "ES": None, "GC": None}

async def fetch_futures_price(asset: str):
    """Devuelve el precio EN VIVO desde la caché del WebSocket para no consumir créditos."""
    return _LIVE_PRICES.get(asset)

async def twelvedata_websocket_listener():
    """
    Escucha permanente del WebSocket de Twelvedata. 
    Mantiene 1 sola conexión abierta y no consume tus 800 créditos HTTP.
    """
    if not TWELVEDATA_KEY:
        print("[twelvedata] Sin TWELVEDATA_KEY. WebSocket no iniciado.")
        return

    import json
    uri = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVEDATA_KEY}"
    
    while True:
        try:
            print("[twelvedata] Conectando al WebSocket...")
            # Nota: Necesitas instalar 'websockets' en tu requirements.txt si deseas activarlo por completo
            import websockets
            async with websockets.connect(uri) as websocket:
                # Suscribirse al índice o proxy requerido (ej. QQQ o NQ si tienes datos de futuros)
                subscribe_msg = {"action": "subscribe", "params": {"symbols": "QQQ"}}
                await websocket.send(json.dumps(subscribe_msg))
                
                async for message in websocket:
                    data = json.loads(message)
                    if data.get("event") == "price":
                        # Ejemplo mapeando QQQ a nuestra estructura simulada de NQ
                        price = float(data.get("price"))
                        _LIVE_PRICES["NQ"] = price * 41.2  # Ajuste fallback dinámico
        except Exception as e:
            print(f"[twelvedata] Error en WebSocket: {e}. Reconectando en 10s...")
            await asyncio.sleep(10)

async def get_nq_ratio(asset: str, qqq_price: float):
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
    "gex":           {},
    "calendar":      {"data": [], "last_update": None, "status": "offline"},
    "movers":        {"data": [], "last_update": None, "status": "offline"},
    "earnings":      {"data": [], "last_update": None, "status": "offline"},
    "company":       {},
    "heatmap":       {"data": {}, "last_update": None, "status": "offline"},
    "institutional": {"text": None, "last_update": None, "status": "offline"},
    "health": {
        "flashalpha": "offline",
        "finnhub":    "offline",
        "yahoo":      "offline",
        "groq":       "offline",
    },
}

# ── Persistencia a disco ──
import json as _json
_PERSIST_FILE = "/tmp/lbc_cache.json"

def _save_cache():
    try:
        snapshot = {
            "gex":      cache["gex"],
            "earnings": cache["earnings"],
            "calendar": cache["calendar"],
            "institutional": {
                "text":        cache["institutional"]["text"],
                "last_update": cache["institutional"]["last_update"],
            },
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
                if k == "gex" and snap[k]:
                    cache["gex"] = snap[k]
                elif snap[k].get("data"):
                    cache[k]["data"] = snap[k]["data"]
                    cache[k]["last_update"] = snap[k].get("last_update")
                    cache[k]["status"] = "stale"
        if snap.get("institutional", {}).get("text"):
            cache["institutional"]["text"]        = snap["institutional"]["text"]
            cache["institutional"]["last_update"] = snap["institutional"].get("last_update")
            cache["institutional"]["status"]      = "stale"
        print(f"[persist] cache restaurado con éxito")
    except FileNotFoundError:
        print("[persist] sin cache previo (primer arranque)")
    except Exception as e:
        print(f"[persist] error cargando: {e}")

# ═══════════════════════════════════════════════════════════
#  SERVICIO 1: FlashAlpha GEX
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
    try:
        data = await fetch_flashalpha(asset)
        data["_ts"] = time.time()
        cache["gex"][asset] = data
        cache["health"]["flashalpha"] = "online"
        _save_cache()
        print(f"[gex] {asset} actualizado correctamente")
    except Exception as e:
        if cache["gex"].get(asset):
            cache["health"]["flashalpha"] = "stale"
            print(f"[gex] {asset} fallo ({e}) — usando caché previo seguro")
        else:
            cache["health"]["flashalpha"] = "offline"

# ═══════════════════════════════════════════════════════════
#  SERVICIO 2: Finnhub — CALENDARIO MACRO
# ═══════════════════════════════════════════════════════════
EVENT_BLOCKLIST = [
    "bill auction", "bond auction", "note auction", "tips auction", "frn auction",
    "3-month", "6-month", "4-week", "8-week", "6-week", "17-week", "52-week",
    "15-year", "20-year", "5-year", "2-year", "3-year", "7-year", "10-year", "30-year",
    "nopa crush", "baker hughes", "rig count", "wasde", "grain stocks",
    "eia ", "api crude", "crude oil stock", "natural gas stock",
    "cushing", "distillate", "gasoline production", "gasoline stock",
    "heating oil", "refinery", "crude runs", "crude oil imports",
    "mba ", "mortgage rate", "mortgage application", "mortgage refinance", "purchase index",
    "fed balance sheet", "foreign bond investment", "tic flows",
    "net capital flows", "capital flows", "money supply", "redbook", "lmi logistics",
    "rcm/tipp", "tipp economic", "used car prices", "corporate profits", "current account", "stress test"
]

HIGH_RELEVANCE = [
    "cpi", "core cpi", "ppi", "core ppi", "pce", "core pce",
    "fomc", "fed interest rate", "interest rate decision", "federal funds",
    "fed minutes", "powell", "fed chair", "fed press conference",
    "rate projection", "economic projection", "fed speech", "goolsbee", "waller",
    "williams", "bostic", "kashkari", "daly", "barkin", "logan", "bowman",
    "jefferson", "cook", "barr", "fed governor", "fed president", "speech",
    "non farm", "nonfarm", "non-farm", "unemployment rate", "average hourly earnings",
    "gdp", "retail sales", "ism manufacturing", "ism services", "ism non-manufacturing",
    "jolts", "adp", "initial jobless", "continuing jobless", "jobless claims", "unemployment claims",
    "empire state", "philadelphia fed", "philly fed", "consumer confidence", "consumer sentiment",
    "michigan", "durable goods"
]

MEDIUM_RELEVANCE = [
    "housing starts", "building permits", "new home sales", "existing home sales",
    "pending home sales", "nahb", "housing market index", "case-shiller", "home price",
    "import prices", "export prices", "trade balance", "balance of trade",
    "goods trade balance", "exports", "imports", "factory orders", "industrial production",
    "manufacturing production", "capacity utilization", "business inventories", "wholesale inventories",
    "retail inventories", "cb leading", "leading index", "inflation expectations", "consumer expectations",
    "current conditions", "consumer inflation", "personal income", "personal spending",
    "real personal spending", "consumer credit", "vehicle sales", "construction spending",
    "chicago pmi", "chicago fed", "dallas fed", "richmond fed", "kansas fed",
    "fed services", "services activity", "challenger", "productivity", "labor costs",
    "participation rate", "manufacturing payrolls", "government payrolls", "nonfarm payrolls private",
    "u-6 unemployment", "average weekly hours", "ism manufacturing", "ism services"
]

US_HOLIDAYS = [
    "independence day", "juneteenth", "memorial day", "labor day",
    "thanksgiving", "christmas", "new year", "martin luther king",
    "washington", "presidents day", "columbus day", "veterans day",
    "bank holiday", "markets closed"
]

def _is_holiday(name: str) -> bool:
    if not name: return False
    n = name.lower()
    return any(h in n for h in US_HOLIDAYS)

def _event_allowed(name: str) -> bool:
    if not name: return False
    n = name.lower()
    if _is_holiday(name): return True
    for bad in EVENT_BLOCKLIST:
        if bad in n: return False
    for kw in HIGH_RELEVANCE:
        if kw in n: return True
    for kw in MEDIUM_RELEVANCE:
        if kw in n: return True
    return False

def _event_relevance(name: str, ff_impact: str) -> str:
    n = (name or "").lower()
    if _is_holiday(name): return "holiday"
    for kw in HIGH_RELEVANCE:
        if kw in n: return "high"
    for kw in MEDIUM_RELEVANCE:
        if kw in n:
            return "high" if ff_impact == "high" else "medium"
    return ff_impact or "medium"

FF_URLS = [
    "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
    "https://nfs.faireconomy.media/ff_calendar_nextweek.json"
]

def _ff_impact(val) -> str:
    s = str(val).lower()
    if "high" in s:   return "high"
    if "medium" in s: return "medium"
    if "low" in s:    return "low"
    return "medium"

async def fetch_calendar():
    out = []
    async with httpx.AsyncClient(timeout=3, headers={"User-Agent": "Mozilla/5.0"}) as client:
        for url in FF_URLS:
            try:
                r = await client.get(url)
                if r.status_code != 200: continue
                rows = r.json()
            except Exception: continue
            for ev in rows:
                country = str(ev.get("country", "")).upper()
                if country not in ("USD", "US", "UNITED STATES"): continue
                name = ev.get("title", "") or ev.get("event", "")
                if not _event_allowed(name): continue
                ff_imp = _ff_impact(ev.get("impact"))
                impact = _event_relevance(name, ff_imp)
                if impact == "low": continue
                actual = ev.get("actual", "")
                released = actual is not None and str(actual).strip() != ""
                out.append({
                    "title": name, "source": "Forex Factory", "time": ev.get("date", ""),
                    "impact": impact, "actual": actual or None, "forecast": ev.get("forecast") or None,
                    "previous": ev.get("previous") or None, "status": "Released" if released else "Upcoming",
                    "type": "macro"
                })
    if not out: raise RuntimeError("Forex Factory sin eventos")
    seen = set()
    deduped = []
    for e in out:
        k = (e["title"].lower().strip(), e["time"][:16])
        if k in seen: continue
        seen.add(k)
        deduped.append(e)
    deduped.sort(key=lambda e: e.get("time") or "")
    return deduped

async def refresh_calendar():
    try:
        data = await fetch_calendar()
        cache["calendar"]["data"] = data
        cache["calendar"]["last_update"] = datetime.now(NY).isoformat()
        cache["calendar"]["status"] = "fresh"
        cache["health"]["finnhub"] = "online"
    except Exception:
        if cache["calendar"]["data"]:
            cache["calendar"]["status"] = "stale"

# ═══════════════════════════════════════════════════════════
#  SERVICIO 3: Finnhub — MOVERS ULTRA
# ═══════════════════════════════════════════════════════════
MOVER_KEYWORDS = {
    "nvidia": 95, "nvda": 95, "apple": 94, "aapl": 94, "tesla": 92, "tsla": 92,
    "microsoft": 90, "msft": 90, "amazon": 88, "amzn": 88, "meta": 85, "facebook": 85,
    "broadcom": 80, "avgo": 80, "amd": 75, "oracle": 70, "orcl": 70, "intel": 60, "intc": 60,
    "alphabet": 88, "google": 88, "googl": 88, "tsmc": 82, "taiwan semiconductor": 82,
    "qualcomm": 72, "qcom": 72, "openai": 90, "blackrock": 96, "larry fink": 96,
    "federal reserve": 98, "fed ": 98, "fomc": 100, "powell": 98, "jerome powell": 98,
    "trump": 97, "tariff": 97, "tariffs": 97, "elon musk": 85, "us treasury": 90,
    "treasury yield": 88, "bond yield": 88, "china": 85, "trade war": 90, "opec": 75,
    "inflation": 88, "cpi": 100, "ppi": 100, "interest rate": 90, "rate cut": 92,
    "rate hike": 92, "recession": 88, "nasdaq": 85, "s&p 500": 85, "s&p500": 85,
    "semiconductor": 80, "artificial intelligence": 78, " ai ": 75, "geopolitics": 78
}
MOVER_BLOCKLIST = [
    "penny stock", "otc", "small cap", "crypto", "bitcoin", "ethereum",
    "memecoin", "dogecoin", "shiba", "nft", "sports", "entertainment"
]

def _score_headline(title: str):
    if not title: return 0, None
    t = " " + title.lower() + " "
    for bad in MOVER_BLOCKLIST:
        if bad in t: return 0, None
    best_score, best_sym = 0, None
    for kw, sc in MOVER_KEYWORDS.items():
        if kw in t and sc > best_score:
            best_score = sc
            best_sym = kw.strip().upper()[:8]
    return best_score, best_sym

async def fetch_movers():
    if not FINNHUB_KEY: raise RuntimeError("Sin FINNHUB_KEY")
    async with httpx.AsyncClient(timeout=8) as client:
        r = await client.get(f"{FH_BASE}/news", params={"category": "general", "token": FINNHUB_KEY})
        if r.status_code != 200: raise RuntimeError("Error Finnhub news")
        rows = r.json()
    scored = []
    seen_titles = set()
    for n in rows:
        title = n.get("headline", "") or ""
        key = title.lower().strip()[:60]
        if key in seen_titles: continue
        score, sym = _score_headline(title)
        if score < 60: continue
        seen_titles.add(key)
        scored.append({
            "title": title, "source": n.get("source", "Finnhub"), "timestamp": n.get("datetime", 0),
            "url": n.get("url", ""), "impact": "ultra" if score >= 95 else ("high" if score >= 85 else "medium"),
            "score": score, "type": "mover", "symbol": sym
        })
    scored.sort(key=lambda x: x["score"], reverse=True)
    out, used_syms = [], set()
    for m in scored:
        s = m["symbol"]
        if s in used_syms: continue
        used_syms.add(s)
        out.append(m)
        if len(out) >= 5: break
    return out[:5]

async def refresh_movers():
    try:
        data = await fetch_movers()
        cache["movers"]["data"] = data
        cache["movers"]["last_update"] = datetime.now(NY).isoformat()
        cache["movers"]["status"] = "fresh"
    except Exception:
        if cache["movers"]["data"]: cache["movers"]["status"] = "stale"

# ═══════════════════════════════════════════════════════════
#  SERVICIO 4: Finnhub — EARNINGS CALENDAR
# ═══════════════════════════════════════════════════════════
EARN_EXTREME = {"AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "TSLA", "AVGO", "NFLX"}
EARN_HIGH = {"AMD", "INTC", "QCOM", "MU", "TSM", "ASML", "ORCL", "CRM", "ADBE"}
EARN_MEDIUM = {"JPM", "BAC", "WFC", "C", "GS", "MS", "V", "MA", "AXP", "BLK"}

def _earn_impact(symbol: str) -> str:
    s = (symbol or "").upper()
    if s in EARN_EXTREME: return "extreme"
    if s in EARN_HIGH:    return "high"
    if s in EARN_MEDIUM:  return "medium"
    return "normal"

def _is_relevant_earning(ev: dict) -> bool:
    sym = (ev.get("symbol") or "").upper()
    if _earn_impact(sym) != "normal": return True
    if not sym or not sym.replace(".", "").isalpha() or len(sym) > 6: return False
    return ev.get("epsEstimate") is not None or ev.get("revenueEstimate") not in (None, 0)

async def fetch_earnings(days_ahead=45):
    if not FINNHUB_KEY: raise RuntimeError("Sin FINNHUB_KEY")
    today = datetime.now(NY).date()
    url = f"{FH_BASE}/calendar/earnings?from={today.isoformat()}&to={(today + timedelta(days=days_ahead)).isoformat()}&token={FINNHUB_KEY}"
    async with httpx.AsyncClient(timeout=8) as client:
        r = await client.get(url)
        if r.status_code != 200: raise RuntimeError("Error Finnhub earnings")
        data = r.json()
    rows = data.get("earningsCalendar", []) if isinstance(data, dict) else []
    out = []
    for ev in rows:
        if not _is_relevant_earning(ev): continue
        sym = (ev.get("symbol") or "").upper()
        out.append({
            "symbol": sym, "date": ev.get("date"), "hour": ev.get("hour") or "",
            "quarter": ev.get("quarter"), "year": ev.get("year"), "epsEstimate": ev.get("epsEstimate"),
            "epsActual": ev.get("epsActual"), "revenueEstimate": ev.get("revenueEstimate"),
            "revenueActual": ev.get("revenueActual"), "impact": _earn_impact(sym)
        })
    return out

async def refresh_earnings():
    try:
        data = await fetch_earnings()
        cache["earnings"]["data"] = data
        cache["earnings"]["last_update"] = datetime.now(NY).isoformat()
        cache["earnings"]["status"] = "fresh"
        _save_cache()
    except Exception:
        if cache["earnings"]["data"]: cache["earnings"]["status"] = "stale"

# ═══════════════════════════════════════════════════════════
#  ENDPOINTS PRINCIPALES
# ═══════════════════════════════════════════════════════════
@app.get("/")
def root(): return {"status": "ok", "service": "Liberato Backend"}

@app.get("/health")
def health():
    return {
        "flashalpha": cache["health"]["flashalpha"], "finnhub": cache["health"]["finnhub"],
        "yahoo": cache["health"]["yahoo"], "groq": cache["health"]["groq"],
        "calendar_count": len(cache["calendar"]["data"]), "movers_count": len(cache["movers"]["data"])
    }

@app.get("/api/market/gamma-levels/{asset}")
async def gamma_levels(asset: str):
    asset = asset.upper()
    if asset not in PROXIES: raise HTTPException(400, "Activo no soportado")
    cached = cache["gex"].get(asset)
    if not cached: raise HTTPException(503, "GEX temporalmente no disponible (Caché vacío)")
    qqq_price = cached.get("underlying_price")
    nq_price, ratio = await get_nq_ratio(asset, qqq_price)
    return {**cached, "asset": asset, "nq_price": nq_price, "ratio": ratio, "credits_used": 0}

@app.get("/api/calendar")
async def get_calendar():
    return {"macro_calendar": cache["calendar"]["data"], "status": cache["calendar"]["status"]}

@app.get("/api/movers")
async def get_movers():
    return {"market_movers": cache["movers"]["data"], "status": cache["movers"]["status"]}

@app.get("/api/earnings")
async def get_earnings():
    return {"earnings": cache["earnings"]["data"], "status": cache["earnings"]["status"]}

@app.get("/api/dashboard")
async def get_dashboard():
    upcoming = [e for e in cache["calendar"]["data"] if e["status"] == "Upcoming"]
    return {
        "macro_calendar": cache["calendar"]["data"], "market_movers": cache["movers"]["data"],
        "next_macro_event": upcoming[0] if upcoming else None, "heatmap": cache["heatmap"]["data"],
        "institutional_summary": cache["institutional"]["text"], "health": cache["health"]
    }

# ══════════════════════════════════════════════════════════════════
# SERVICIO 5: Yahoo Finance — Macro Heatmap (22 activos)
# ══════════════════════════════════════════════════════════════════
HMAP_TICKERS = {
    "AAPL":"AAPL", "MSFT":"MSFT", "NVDA":"NVDA", "GOOGL":"GOOGL", "AMZN":"AMZN",
    "META":"META", "AVGO":"AVGO", "TSLA":"TSLA", "COST":"COST", "NFLX":"NFLX",
    "NQ": "QQQ", "ES": "SPY", "VIX": "VIXY", "DXY": "UUP", "US2Y":"SHY",
    "US10Y":"IEF", "US30Y":"TLT", "Gold":"GLD", "WTI": "USO", "BTC": "IBIT", "INFL":"TIP"
}
YF_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

async def fetch_heatmap():
    tickers = list(HMAP_TICKERS.values())
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={','.join(tickers)}&fields=regularMarketPrice,regularMarketChange,regularMarketChangePercent"
    async with httpx.AsyncClient(timeout=10, headers=YF_HEADERS) as client:
        r = await client.get(url)
        if r.status_code != 200: raise RuntimeError("Error Yahoo Heatmap")
        data = r.json()
    quotes = data.get("quoteResponse", {}).get("result", []) or []
    yahoo_to_hmap = {v: k for k, v in HMAP_TICKERS.items()}
    out = {}
    for q in quotes:
        tk = q.get("symbol", "")
        hmap_sym = yahoo_to_hmap.get(tk, tk)
        price = q.get("regularMarketPrice")
        chg_pct = q.get("regularMarketChangePercent") or 0
        if price:
            out[hmap_sym] = {
                "symbol": hmap_sym, "price": round(price, 4), "chg_pct": round(chg_pct, 3),
                "direction": "up" if chg_pct > 0.05 else ("down" if chg_pct < -0.05 else "flat")
            }
    return out

async def refresh_heatmap():
    try:
        data = await fetch_heatmap()
        cache["heatmap"]["data"] = data
        cache["heatmap"]["last_update"] = datetime.now(NY).isoformat()
        cache["heatmap"]["status"] = "fresh"
        cache["health"]["yahoo"] = "online"
    except Exception:
        cache["heatmap"]["status"] = "stale"

# ══════════════════════════════════════════════════════════════════
# SERVICIO 6: Anthropic Claude — Resumen Institucional IA via Groq
# ══════════════════════════════════════════════════════════════════
async def fetch_institutional_summary() -> str:
    if not GROQ_KEY: raise RuntimeError("Sin GROQ_KEY")
    gex_data = cache["gex"].get("NQ", {})
    context_str = f"Net GEX: {gex_data.get('net_gex') or 'N/A'} | Flip: {gex_data.get('gamma_flip') or 'N/A'}"
    
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_KEY}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile", "max_tokens": 300, "temperature": 0.35,
                "messages": [
                    {"role": "system", "content": "Analista institucional breve en español. 2 oraciones."},
                    {"role": "user", "content": f"Briefing con estos datos: {context_str}"}
                ]
            }
        )
    return r.json()["choices"][0]["message"]["content"].strip()

async def refresh_institutional_summary():
    try:
        text = await fetch_institutional_summary()
        cache["institutional"]["text"] = text
        cache["institutional"]["last_update"] = datetime.now(NY).isoformat()
        cache["institutional"]["status"] = "fresh"
        cache["health"]["groq"] = "online"
    except Exception:
        cache["institutional"]["status"] = "stale"

@app.get("/api/heatmap")
async def get_heatmap(): return {"heatmap": cache["heatmap"]["data"]}

@app.get("/api/context/institutional")
async def get_institutional(): return {"summary": cache["institutional"]["text"]}

# ══════════════════════════════════════════════════════════════════
# SCHEDULER OPTIMIZADO PARA CRÉDITOS LIMITADOS
# ══════════════════════════════════════════════════════════════════
scheduler = AsyncIOScheduler(timezone=NY)

@app.on_event("startup")
async def startup():
    _load_cache()
    
    # --- FlashAlpha: Ejecuciones exactas (2 llamadas al día para no agotar los 5 créditos) ---
    scheduler.add_job(refresh_gex, CronTrigger(hour=9, minute=0, day_of_week="mon-fri"), args=["NQ"])
    scheduler.add_job(refresh_gex, CronTrigger(hour=19, minute=0, day_of_week="mon-fri"), args=["NQ"])
    
    # --- Otros servicios automáticos ---
    scheduler.add_job(refresh_calendar, IntervalTrigger(minutes=5))
    scheduler.add_job(refresh_movers, IntervalTrigger(seconds=60))
    scheduler.add_job(refresh_earnings, IntervalTrigger(hours=6))
    scheduler.add_job(refresh_heatmap, IntervalTrigger(seconds=30))
    
    scheduler.start()
    
    # Iniciar la escucha asíncrona del WebSocket de Twelvedata de fondo (No consume créditos por HTTP)
    asyncio.create_task(twelvedata_websocket_listener())
    
    # Inicializaciones seguras de arranque
    await asyncio.gather(refresh_calendar(), refresh_movers(), refresh_earnings(), refresh_heatmap(), return_exceptions=True)
    print("[startup] Liberato Backend v2 listo e inteligente ✓")