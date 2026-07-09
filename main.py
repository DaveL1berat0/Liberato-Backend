"""
Liberato Backend v3.0 — Production Ready
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ARQUITECTURA DE CRÉDITOS (nunca se agotan):
  FlashAlpha   → 2 llamadas/día: 9:00AM + 7:00PM ET (de 5 disponibles)
  TwelveData   → WebSocket: 8 símbolos real-time, sin créditos REST
                 REST batch: 13 símbolos cada 15min (≈350 créditos/día de 800)
  Finnhub      → Calendar 5min / Movers 60s / Earnings 6h (sin límite claro)
  Groq         → 2 llamadas/día: 9:05AM + 12:00PM ET (gratis generoso)
  Alpha Vantage→ Solo /api/company on-demand (25 créditos/día)
"""

import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os, time, asyncio, json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import websockets

# ══ CREDENCIALES (solo Railway Variables, nunca en código) ════════════════════
FLASHALPHA_KEY   = os.getenv("FLASHALPHA_KEY",   "").strip()
FINNHUB_KEY      = os.getenv("FINNHUB_KEY",      "")
RAPIDAPI_KEY  = os.getenv("RAPIDAPI_KEY", "")          # calendario tiempo real
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "economic-calendar-api-tradingeconomics.p.rapidapi.com")
# ── APIs nuevas (respaldo/complemento del calendario) ──
# RapidAPI: soportar ambos nombres de variable (RAPIDAPI_KEY o x-rapidapi-key)
def _clean_key(*names):
    """Devuelve la primera env var válida, ignorando placeholders comunes."""
    placeholders = {"aqui-tu-clave-rapidapi", "aqui-tu-secreto", "whsec_aqui-tu-secreto",
                    "tu-clave", "your-key", "your_key", "changeme", "xxx", ""}
    for n in names:
        v = os.getenv(n)
        if v and v.strip().lower() not in placeholders and "aqui-tu" not in v.lower():
            return v.strip()
    return ""

# RapidAPI: ignora el placeholder "aqui-tu-clave-rapidapi", usa x-rapidapi-key (la real)
RAPIDAPI_KEY = _clean_key("x-rapidapi-key", "X_RAPIDAPI_KEY", "RAPIDAPI_KEY")
# Si la variable de Railway tiene un host viejo/muerto, corregirlo al que funciona
# (TradingEconomics). Así no depende de que actualices la variable manualmente.
_DEAD_RAPIDAPI_HOSTS = (
    "economic-calendar.p.rapidapi.com",
    "ultimate-economic-calendar.p.rapidapi.com",  # cayó por 402 DEPLOYMENT_DISABLED
    "", None,
)
if RAPIDAPI_HOST in _DEAD_RAPIDAPI_HOSTS:
    RAPIDAPI_HOST = "economic-calendar-api-tradingeconomics.p.rapidapi.com"
# FMP (Financial Modeling Prep) — calendario económico, 250 req/día free
FMP_KEY = (os.getenv("FMP_KEY") or os.getenv("FMP_API_KEY") or os.getenv("FINANCIAL_MODELING_PREP_KEY") or os.getenv("mfp") or os.getenv("MFP") or os.getenv("FMP"))
if FMP_KEY:
    FMP_KEY = FMP_KEY.strip()
FMP_BASE = "https://financialmodelingprep.com/stable"
FMP_BASE_LEGACY = "https://financialmodelingprep.com/api/v3"
# Contacto / soporte (Gmail SMTP)
GMAIL_USER         = os.getenv("GMAIL_USER", "")           # correo emisor
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")   # App Password de Gmail
SUPPORT_EMAIL      = os.getenv("SUPPORT_EMAIL", "SupportLiberatoCommunity@gmail.com").strip()
GROQ_KEY         = os.getenv("GROQ_KEY",         "").strip()
TWELVEDATA_KEY   = os.getenv("TWELVEDATA_KEY",   "").strip()

# ══ GUARDIÁN UNIVERSAL DE PRESUPUESTO DE APIs ════════════════════════
# Un solo sistema protege TODOS los APIs. Cuenta llamadas y FRENA antes
# de pasar el límite. Soporta ventana diaria (UTC) o por minuto.
# Hace IMPOSIBLE agotar cualquier API.
#
# Config por API: (límite_seguro, tipo_ventana). El límite_seguro deja
# margen bajo el límite real del proveedor.
API_BUDGETS = {
    "twelvedata":  {"limit": int(os.getenv("TD_DAILY_LIMIT", "700")),  "window": "day"},
    "finnhub":     {"limit": int(os.getenv("FH_MINUTE_LIMIT", "55")),  "window": "minute"},
    "flashalpha":  {"limit": int(os.getenv("FA_DAILY_LIMIT", "95")),   "window": "day"},   # real 100
    "fmp":         {"limit": int(os.getenv("FMP_DAILY_LIMIT", "230")), "window": "day"},   # real 250
    "alphavantage":{"limit": int(os.getenv("AV_DAILY_LIMIT", "22")),   "window": "day"},   # real 25
    "groq":        {"limit": int(os.getenv("GROQ_DAILY_LIMIT", "950")),"window": "day"},   # llamadas/día
}
# Estado de uso por API: {"window_key": str, "used": int}
_api_usage = {name: {"window_key": None, "used": 0} for name in API_BUDGETS}

def _window_key(window):
    """Clave de la ventana actual: por día (UTC) o por minuto (UTC)."""
    try:
        now = datetime.now(timezone.utc)
    except Exception:
        now = datetime.utcnow()
    return now.strftime("%Y-%m-%d") if window == "day" else now.strftime("%Y-%m-%d %H:%M")

def budget_ok(api, cost=1):
    """True si caben 'cost' llamadas al 'api' SIN pasar su límite.
    Resetea el contador automáticamente al cambiar la ventana."""
    cfg = API_BUDGETS.get(api)
    if not cfg:
        return True   # API sin límite configurado → permitir
    st = _api_usage[api]
    wk = _window_key(cfg["window"])
    if st["window_key"] != wk:
        st["window_key"] = wk
        st["used"] = 0
    return (st["used"] + cost) <= cfg["limit"]

def budget_charge(api, cost=1):
    """Registra 'cost' llamadas usadas del 'api'."""
    cfg = API_BUDGETS.get(api)
    if not cfg:
        return
    st = _api_usage[api]
    wk = _window_key(cfg["window"])
    if st["window_key"] != wk:
        st["window_key"] = wk
        st["used"] = 0
    st["used"] += cost

# ── Compatibilidad: wrappers con los nombres antiguos (TD y Finnhub) ──
TD_DAILY_LIMIT  = API_BUDGETS["twelvedata"]["limit"]
FH_MINUTE_LIMIT = API_BUDGETS["finnhub"]["limit"]
def td_budget_ok(cost=1): return budget_ok("twelvedata", cost)
def td_charge(cost=1):    budget_charge("twelvedata", cost)
def fh_budget_ok(cost=1): return budget_ok("finnhub", cost)
def fh_charge(cost=1):    budget_charge("finnhub", cost)
# Compat con el monitor antiguo
_td_credits = _api_usage["twelvedata"]   # alias
_fh_calls   = _api_usage["finnhub"]

def _td_today():
    try:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return datetime.utcnow().strftime("%Y-%m-%d")

def td_budget_ok(cost=1):
    """Devuelve True si CABEN 'cost' créditos hoy SIN pasar el límite.
    Resetea el contador automáticamente cada día (UTC)."""
    today = _td_today()
    if _td_credits["day"] != today:
        _td_credits["day"] = today
        _td_credits["used"] = 0
    return (_td_credits["used"] + cost) <= TD_DAILY_LIMIT

def td_charge(cost=1):
    """Registra 'cost' créditos usados (llamar tras una llamada exitosa)."""
    today = _td_today()
    if _td_credits["day"] != today:
        _td_credits["day"] = today
        _td_credits["used"] = 0
    _td_credits["used"] += cost
ALPHA_VANTAGE_KEY= os.getenv("ALPHAVANTAGE_KEY", "").strip()
FINNHUB_WH_SECRET = os.getenv("FINNHUB_WEBHOOK_SECRET", "").strip()  # opcional: verifica autenticidad

FA_BASE = "https://lab.flashalpha.com"
# ── CONFIG FLASHALPHA ──────────────────────────────────────────────
# Plan actual: "free" usa QQQ summary + conversión a NQ (1 llamada).
# Plan "basic" usa NDX DIRECTO (sin conversión): niveles reales del
# Nasdaq-100 vía /v1/exposure/levels/NDX + /v1/exposure/gex/NDX.
# Para activar Basic: pon FLASHALPHA_PLAN=basic en Railway. Nada más.
FLASHALPHA_PLAN = os.getenv("FLASHALPHA_PLAN", "free").strip().lower()
# Símbolo índice del Nasdaq-100 para el plan Basic (índice directo).
FA_INDEX_SYMBOL = os.getenv("FA_INDEX_SYMBOL", "NQ=F")  # futuro CME directo (antes NDX).strip().upper()
FH_BASE = "https://finnhub.io/api/v1"
NY      = ZoneInfo("America/New_York")

# ══ APP ══════════════════════════════════════════════════════════════════════
app = FastAPI(title="Liberato Backend v3.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ══ CACHÉ UNIFICADA ══════════════════════════════════════════════════════════
cache = {
    "gex":           {},
    "heatmap":       {"data": {}, "last_update": None, "status": "offline"},
    "nq_ratio":      {"value": None, "nq_price": None, "qqq_price": None, "error_pts": None, "ts": None},
    "institutional": {"text": None, "last_update": None, "status": "offline"},
    "calendar":      {"data": [], "last_update": None, "status": "offline"},
    "movers":        {"data": [], "last_update": None, "status": "offline"},
    "earnings":      {"data": [], "last_update": None, "status": "offline"},
    "company":       {},
    "health": {
        "flashalpha":  "offline",
        "twelvedata":  "offline",
        "finnhub":     "offline",
        "groq":        "offline",
    },
}

# Persistencia a disco para sobrevivir reinicios de Railway
_PERSIST = "/tmp/lbc_v3.json"

def save_cache():
    try:
        snap = {
            "gex":      cache["gex"],
            "earnings": {"data": cache["earnings"]["data"]},
            "institutional": {"text": cache["institutional"]["text"],
                              "lu":   cache["institutional"]["last_update"]},
        }
        with open(_PERSIST, "w") as f:
            json.dump(snap, f)
    except Exception as e:
        print(f"[persist] error guardando: {e}")

def load_cache():
    try:
        with open(_PERSIST) as f:
            snap = json.load(f)
        if snap.get("gex"):
            cache["gex"] = snap["gex"]
        if snap.get("earnings", {}).get("data"):
            cache["earnings"]["data"]   = snap["earnings"]["data"]
            cache["earnings"]["status"] = "stale"
        if snap.get("institutional", {}).get("text"):
            cache["institutional"]["text"]        = snap["institutional"]["text"]
            cache["institutional"]["last_update"] = snap["institutional"].get("lu")
            cache["institutional"]["status"]      = "stale"
        print(f"[persist] cache restaurado: {len(cache['earnings']['data'])} earnings")
    except FileNotFoundError:
        print("[persist] primer arranque sin datos previos")
    except Exception as e:
        print(f"[persist] error cargando: {e}")

# ══ TWELVEDATA WEBSOCKET (una sola conexión, todos los símbolos) ═════════════
# 8 símbolos real-time vía WebSocket — sin créditos REST
# WebSocket SOLO para lo que necesita baja latencia: precio NQ en vivo.
# QQQ se mantiene para el cálculo del ratio NQ/QQQ de respaldo.
# Las acciones del heatmap pasaron a REST /quote (cambio diario real).
WS_SYMBOLS = ["QQQ"]
_ws_task   = None   # referencia única para evitar múltiples conexiones

async def twelvedata_ws():
    """WebSocket único y persistente. Se reconecta automáticamente."""
    if not TWELVEDATA_KEY:
        cache["health"]["twelvedata"] = "offline-no-key"
        return
    uri = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVEDATA_KEY}"
    backoff = 5
    while True:
        try:
            async with websockets.connect(uri, ping_interval=30, ping_timeout=15) as ws:
                await ws.send(json.dumps({
                    "action":  "subscribe",
                    "params":  {"symbols": ",".join(WS_SYMBOLS)}
                }))
                cache["health"]["twelvedata"] = "online"
                backoff = 5
                print(f"[ws] conectado — {len(WS_SYMBOLS)} símbolos")
                async for raw in ws:
                    msg = json.loads(raw)
                    evt = msg.get("event")
                    if evt != "price":
                        continue
                    sym     = msg.get("symbol", "")
                    price   = float(msg.get("price", 0) or 0)
                    chg_pct = float(msg.get("change_percent", 0) or 0)
                    if not price:
                        continue
                    if sym == "NQ1!":
                        cache["nq_ratio"]["nq_price"] = price
                        cache["heatmap"]["data"]["NQ"] = {
                            "symbol":"NQ","price":round(price,2),
                            "chg_pct":round(chg_pct,3),
                            "direction":"up" if chg_pct>0.05 else("down" if chg_pct<-0.05 else"flat"),
                            "source":"direct",
                        }
                        qqq_px = cache["nq_ratio"].get("qqq_price")
                        if qqq_px and qqq_px > 100:
                            nr = round(price/qqq_px,6)
                            cache["nq_ratio"].update({"value":nr,"error_pts":0,"ts":datetime.now(NY).isoformat()})
                    elif sym == "QQQ":
                        cache["nq_ratio"]["qqq_price"] = price
                        if cache["heatmap"]["data"].get("NQ",{}).get("source") != "direct":
                            dr = cache["nq_ratio"].get("value") or 41.51
                            cache["heatmap"]["data"]["NQ"] = {
                                "symbol":"NQ","price":round(price*dr,2),
                                "chg_pct":round(chg_pct,3),
                                "direction":"up" if chg_pct>0.05 else("down" if chg_pct<-0.05 else"flat"),
                                "source":"estimated","ratio_used":dr,
                            }
                        nq_px = cache["nq_ratio"].get("nq_price")
                        if nq_px:
                            nr = round(nq_px/price,6)
                            if abs(nq_px-(price*nr)) > 25:
                                print(f"[ratio] QQQ/NQ ratio drift detected")
                            cache["nq_ratio"].update({"value":nr,"ts":datetime.now(NY).isoformat()})
                    if sym != "NQ1!":
                        cache["heatmap"]["data"][sym] = {
                            "symbol":sym,"price":round(price,4),
                            "chg_pct":round(chg_pct,3),
                            "direction":"up" if chg_pct>0.05 else("down" if chg_pct<-0.05 else"flat"),
                        }
                    cache["heatmap"]["last_update"] = datetime.now(NY).isoformat()
                    cache["heatmap"]["status"]      = "live"
        except Exception as e:
            cache["health"]["twelvedata"] = f"error-reconectando"
            print(f"[ws] caída: {e} — reintentando en {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 120)

# ══ TWELVEDATA REST (batch para los 13 símbolos restantes) ═══════════════════
# No están en el WebSocket → se actualizan via REST cada 15 min
REST_SYMBOLS = {
    "SPY":"SPY","VIXY":"VIXY","UUP":"UUP","SHY":"SHY","IEF":"IEF",
    "TLT":"TLT","GLD":"GLD","USO":"USO","IBIT":"IBIT","TIP":"TIP",
    "COST":"COST","NFLX":"NFLX","AVGO":"AVGO",
    # Acciones grandes movidas desde el WebSocket: el WS daba change_percent
    # que no era el cambio DIARIO confiable (NVDA salía verde estando en rojo).
    # Por /quote obtienen el percent_change diario real, igual que el resto.
    "AAPL":"AAPL","MSFT":"MSFT","NVDA":"NVDA","META":"META",
    "AMZN":"AMZN","TSLA":"TSLA","GOOGL":"GOOGL",
}

async def refresh_heatmap_finnhub():
    """Heatmap vía Finnhub /quote — 60 llamadas/min permite refresco rápido.
    Campo 'dp' = percent change DIARIO real (vs cierre previo). 'c' = precio.
    Finnhub es 1 símbolo por llamada; 20 símbolos = 20 llamadas (<60/min OK).
    Fuente PRIMARIA del heatmap. Si Finnhub falla (429/error), cae a TwelveData."""
    if not FINNHUB_KEY:
        await refresh_heatmap_rest()   # sin key Finnhub → usar TwelveData
        return
    all_syms = list(REST_SYMBOLS.keys())   # símbolos del heatmap (20)
    # GUARDIÁN: ¿caben las 20 llamadas en este minuto? Si no, usar TwelveData.
    if not fh_budget_ok(len(all_syms)):
        print(f"[heatmap-fh] sin presupuesto Finnhub este minuto ({_fh_calls['count']}/{FH_MINUTE_LIMIT}) — fallback TwelveData")
        await refresh_heatmap_rest()
        return
    loaded = 0; rate_limited = False
    async with httpx.AsyncClient(timeout=10) as client:
        # Llamadas en paralelo controlado (no más de ~20, cabe en 60/min)
        async def _one(sym):
            nonlocal loaded, rate_limited
            try:
                fh_charge(1)  # registrar la llamada
                r = await client.get(f"{FH_BASE}/quote",
                                     params={"symbol": sym, "token": FINNHUB_KEY})
                if r.status_code == 429:
                    rate_limited = True; return
                if r.status_code != 200:
                    return
                q = r.json() or {}
                price = q.get("c"); dp = q.get("dp")  # c=current, dp=percent change diario
                if price is None or price == 0:
                    return
                chg = dp if isinstance(dp, (int, float)) else 0
                cache["heatmap"]["data"][sym] = {
                    "symbol": sym, "price": round(float(price), 4),
                    "chg_pct": round(float(chg), 3),
                    "direction": "up" if chg > 0.05 else ("down" if chg < -0.05 else "flat"),
                    "source": "finnhub",
                }
                loaded += 1
            except Exception as e:
                print(f"[heatmap-fh] {sym} falló: {e}")
        await asyncio.gather(*[_one(s) for s in all_syms])
    if rate_limited or loaded == 0:
        # Finnhub saturado o sin datos → respaldo TwelveData (cambio diario real)
        print(f"[heatmap-fh] {'429 rate-limit' if rate_limited else '0 cargados'} → fallback TwelveData")
        await refresh_heatmap_rest()
        return
    cache["heatmap"]["last_update"] = datetime.now(NY).isoformat()
    if cache["heatmap"]["status"] != "live":
        cache["heatmap"]["status"] = "fresh"
    print(f"[heatmap-fh] ok: {loaded}/{len(all_syms)} símbolos (Finnhub, cambio diario)")


async def refresh_heatmap_rest():
    """Batch REST para los 13 símbolos macro (no en WebSocket).
    Una sola llamada = 13 créditos. Cada 15 min = ~350 créditos/día."""
    if not TWELVEDATA_KEY:
        return
    symbols = ",".join(REST_SYMBOLS.values())
    n_sym = len(REST_SYMBOLS)
    # GUARDIÁN: el heatmap por TwelveData cuesta n_sym créditos. Si no caben,
    # NO llamar (mantiene el último dato de Finnhub/cache). Nunca se pasa.
    if not td_budget_ok(n_sym):
        print(f"[heatmap-rest] sin presupuesto TwelveData ({_td_credits['used']}/{TD_DAILY_LIMIT}) — se mantiene cache")
        return
    # /quote da el percent_change DIARIO real (vs cierre anterior), NO el
    # cambio desde la última llamada. Esto arregla el bug de mostrar verde
    # un símbolo que en el día está en rojo.
    url = f"https://api.twelvedata.com/quote?symbol={symbols}&apikey={TWELVEDATA_KEY}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url)
        if r.status_code != 200:
            print(f"[heatmap-rest] error {r.status_code} — trying Yahoo fallback")
            await _heatmap_yahoo_fallback()
            return
        td_charge(len(REST_SYMBOLS))  # registrar créditos del batch
        data = r.json()
        sym_to_hmap = {v:k for k,v in REST_SYMBOLS.items()}
        loaded = 0
        # /quote con múltiples símbolos devuelve {symbol: {quote}}; con uno solo
        # devuelve el quote directo. Normalizar a dict de quotes.
        if "symbol" in data and "percent_change" in data:
            data = {data.get("symbol"): data}
        for td_sym, q in data.items():
            if not isinstance(q, dict):
                continue
            # percent_change = cambio % diario real vs cierre previo
            pc = q.get("percent_change")
            close = q.get("close") or q.get("price")
            if pc is None or close is None:
                continue
            hmap_sym = sym_to_hmap.get(td_sym, td_sym)
            try:
                price = float(close); chg_pct = float(pc)
            except (TypeError, ValueError):
                continue
            cache["heatmap"]["data"][hmap_sym] = {
                "symbol":hmap_sym,"price":round(price,4),
                "chg_pct":round(chg_pct,3),
                "direction":"up" if chg_pct>0.05 else("down" if chg_pct<-0.05 else"flat"),
            }
            loaded += 1
        if loaded == 0:
            print("[heatmap-rest] TwelveData returned 0 prices — weekend/closed market. Trying Yahoo.")
            await _heatmap_yahoo_fallback()
            return
        cache["heatmap"]["last_update"] = datetime.now(NY).isoformat()
        if cache["heatmap"]["status"] != "live":
            cache["heatmap"]["status"] = "fresh"
        print(f"[heatmap-rest] ok: {loaded} símbolos")
    except Exception as e:
        print(f"[heatmap-rest] error: {e} — trying Yahoo fallback")
        await _heatmap_yahoo_fallback()

async def _heatmap_yahoo_fallback():
    """Fallback para fines de semana / mercado cerrado.
    Yahoo Finance devuelve el último precio conocido incluso cuando el mercado está cerrado."""
    all_syms = list(REST_SYMBOLS.values()) + WS_SYMBOLS
    symbols_str = ",".join(all_syms)
    url = (f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols_str}"
           "&fields=regularMarketPrice,regularMarketChange,regularMarketChangePercent")
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    }
    try:
        async with httpx.AsyncClient(timeout=12, headers=headers) as client:
            r = await client.get(url)
        if r.status_code != 200:
            print(f"[heatmap-yahoo] {r.status_code}")
            return
        quotes = r.json().get("quoteResponse",{}).get("result",[]) or []
        sym_to_hmap = {v:k for k,v in REST_SYMBOLS.items()}
        ws_to_hmap  = {s:s for s in WS_SYMBOLS}  # WS syms map to themselves
        sym_to_hmap.update(ws_to_hmap)
        loaded = 0
        for q in quotes:
            ysym    = q.get("symbol","")
            hmap_sym= sym_to_hmap.get(ysym, ysym)
            price   = q.get("regularMarketPrice")
            chg_pct = q.get("regularMarketChangePercent") or 0
            if not price: continue
            cache["heatmap"]["data"][hmap_sym] = {
                "symbol":hmap_sym,"price":round(price,4),
                "chg_pct":round(chg_pct,3),
                "direction":"up" if chg_pct>0.05 else("down" if chg_pct<-0.05 else"flat"),
            }
            if ysym == "QQQ":
                cache["heatmap"]["data"]["NQ"] = {
                    "symbol":"NQ","price":round(price*(cache["nq_ratio"].get("value") or 41.51),2),
                    "chg_pct":round(chg_pct,3),
                    "direction":"up" if chg_pct>0.05 else("down" if chg_pct<-0.05 else"flat"),
                }
            loaded += 1
        cache["heatmap"]["last_update"] = datetime.now(NY).isoformat()
        cache["heatmap"]["status"]      = "stale-yahoo"
        print(f"[heatmap-yahoo] fallback ok: {loaded} símbolos")
    except Exception as e:
        print(f"[heatmap-yahoo] error: {e}")

# ══ FLASHALPHA — GEX (2 llamadas/día, nunca en startup) ══════════════════════
_gex_blocked_until = 0
_gex_ondemand_ts = 0   # debounce del refresh on-demand cuando el cache está frío
_gex_working_exp = None       # expiración que SÍ da datos GEX (cache del día)
_gex_working_exp_day = None   # día en que se cacheó (para resetear)   # timestamp: si hay 429, esperar 24h
_gex_expdates_cache = []      # expiraciones del día (1 llamada /options por día)
_gex_expdates_day   = None
_gex_maxpain_failed_day = None
_event_reactions = {}  # {evento: {t0, p0, p5}} — reacción del NQ a noticias  # si /maxpain falló hoy, no reintentar (ahorra créditos)

async def refresh_gex(asset="NQ"):
    """GEX desde FlashAlpha. NUNCA se llama en startup.
    Scheduler: 9:00 AM + 7:00 PM ET (2 créditos de 5 disponibles/día)."""
    global _gex_blocked_until
    if not FLASHALPHA_KEY:
        cache["health"]["flashalpha"] = "offline-no-key"
        return
    if time.time() < _gex_blocked_until:
        remaining = int((_gex_blocked_until - time.time()) / 3600)
        print(f"[gex] bloqueado por 429 — {remaining}h restantes")
        return
    # GUARDIÁN: un refresh GEX completo usa ~4 llamadas (levels+options+gex+
    # maxpain+summary). Si no caben en el presupuesto FlashAlpha, NO llamar.
    if not budget_ok("flashalpha", 5):
        st = _api_usage["flashalpha"]
        print(f"[gex] presupuesto FlashAlpha agotado ({st['used']}/{API_BUDGETS['flashalpha']['limit']}) — se mantiene cache")
        return
    try:
        if FLASHALPHA_PLAN == "basic":
            # ══ PLAN BASIC: NDX DIRECTO (sin conversión QQQ→NQ) ══════════════
            # 2 llamadas: levels (call/put/flip) + gex (net_gex + per-strike).
            await _refresh_gex_ndx(asset)
        else:
            # ══ PLAN FREE: QQQ summary + conversión a NQ (1 llamada) ═════════
            await _refresh_gex_qqq(asset)
    except Exception as e:
        cache["health"]["flashalpha"] = "error"
        print(f"[gex] excepción: {e}")


async def _refresh_gex_qqq(asset="NQ"):
    """PLAN FREE: usa /v1/stock/QQQ/summary y guarda niveles en escala QQQ.
    El endpoint /api/market/gamma-levels/NQ los convierte a NQ con ratio."""
    global _gex_blocked_until
    ticker = "QQQ"
    async with httpx.AsyncClient(timeout=12,
                                  headers={"X-Api-Key": FLASHALPHA_KEY}) as client:
        r = await client.get(f"{FA_BASE}/v1/stock/{ticker}/summary")
    if r.status_code == 200:
        d = r.json()
        ex = d.get("exposure", {}) or {}
        px = d.get("price",    {}) or {}
        def _lvl(v):
            if isinstance(v, dict):
                return v.get("strike") or v.get("price") or v.get("level")
            return v
        cw, pw, gf = _lvl(ex.get("call_wall")), _lvl(ex.get("put_wall")), _lvl(ex.get("gamma_flip"))
        as_of = d.get("as_of"); market_open = d.get("market_open")
        cache["gex"][asset] = {
            "underlying_price": px.get("mid") or px.get("last"),
            "call_wall": cw, "put_wall": pw, "gamma_flip": gf,
            "net_gex": ex.get("net_gex"), "regime": ex.get("regime"),
            "ticker": ticker, "as_of": as_of, "market_open": market_open,
            "source": "qqq-summary", "_ts": time.time(),
        }
        if cw is None and pw is None and gf is None:
            cache["health"]["flashalpha"] = "online-no-levels"
            print(f"[gex] ⚠️ 200 sin niveles (free no cubre call/put wall de QQQ). Keys: {list(ex.keys())}")
        else:
            cache["health"]["flashalpha"] = "online"
            print(f"[gex] ok (QQQ): flip={gf} call={cw} put={pw} as_of={as_of}")
        save_cache()
    elif r.status_code == 429:
        _gex_blocked_until = time.time() + 86400
        cache["health"]["flashalpha"] = "rate-limited-24h"
        print("[gex] 429 — bloqueado 24h")
    else:
        cache["health"]["flashalpha"] = f"error-{r.status_code}"
        print(f"[gex] error {r.status_code}")


def _today_et_str():
    """Fecha de hoy en ET como 'YYYY-MM-DD'."""
    from datetime import datetime
    try:
        return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    except Exception:
        return datetime.utcnow().strftime("%Y-%m-%d")


def _nearest_index_expiration():
    """Expiración más cercana para opciones de índice (NDX/SPX expiran
    Lun/Mié/Vie). Devuelve 'YYYY-MM-DD' del próximo día de expiración
    (hoy si aplica). Formato que pide FlashAlpha para single-expiry."""
    from datetime import datetime, timedelta
    try:
        now_et = datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        now_et = datetime.utcnow()
    d = now_et.date()
    # weekday(): Mon=0 ... Sun=6. NDX/SPX expiran Lun(0), Mié(2), Vie(4).
    # IMPORTANTE: empezar desde MAÑANA (i=1), NO hoy. La expiración de hoy
    # es 0DTE (same-day) y eso requiere Growth. La próxima expiración futura
    # es single-expiry normal, cubierta por Basic.
    exp_days = {0, 2, 4}
    for i in range(1, 9):
        cand = d + timedelta(days=i)
        if cand.weekday() in exp_days:
            return cand.strftime("%Y-%m-%d")
    return (d + timedelta(days=2)).strftime("%Y-%m-%d")


async def _refresh_gex_ndx(asset="NQ"):
    """PLAN BASIC: usa NDX DIRECTO. Niveles reales del Nasdaq-100, sin conversión.
       /v1/exposure/levels/NDX → call_wall, put_wall, gamma_flip, max_pain
       /v1/exposure/gex/NDX    → net_gex + per-strike (para validar walls)."""
    global _gex_blocked_until, _gex_expdates_day, _gex_expdates_cache, _gex_maxpain_failed_day
    sym = FA_INDEX_SYMBOL  # "NQ=F" (futuro CME directo) o "NDX"
    from urllib.parse import quote
    sym_url = quote(sym, safe="")  # NQ=F → NQ%3DF (requerido por FlashAlpha)
    # Costo dinámico: 1ª llamada del día ≈5 (incluye /options); siguientes ≈3
    _today_now = _today_et_str()
    _first_of_day = (_gex_expdates_day != _today_now)
    budget_charge("flashalpha", 5 if _first_of_day else 3)
    async with httpx.AsyncClient(timeout=12,
                                  headers={"X-Api-Key": FLASHALPHA_KEY}) as client:
        r_lvl = await client.get(f"{FA_BASE}/v1/exposure/levels/{sym_url}")
        if r_lvl.status_code == 429:
            _gex_blocked_until = time.time() + 86400
            cache["health"]["flashalpha"] = "rate-limited-24h"
            print(f"[gex] 429 ({sym} levels) — bloqueado 24h")
            return
        if r_lvl.status_code == 200:
            lv = (r_lvl.json() or {}).get("levels", {}) or {}
        else:
            # RESILIENTE: /levels puede no cubrir futuros en Basic.
            # NO abortamos: el /gex con expiration SÍ funciona (probado en vivo)
            # y de ahí derivamos walls + flip. Regla #1: si un dato no llega, "—".
            lv = {}
            print(f"[gex] /levels/{sym} {r_lvl.status_code} — continúo con /gex (walls derivados)")
        # Segunda llamada: net_gex + per-strike.
        # En Basic, /gex de índices requiere UN solo expiry (no 0DTE, no full-chain).
        # Estrategia robusta: consultar las expiraciones REALES de NDX y usar la
        # primera futura (evita 404 por fecha inexistente y 403 por 0DTE).
        net_gex = None; per_strike = None; exp = None; exp_dates = []
        gex_flip = None; gex_label = None  # del response /gex (flip del futuro)
        try:
            r_exp = await client.get(f"{FA_BASE}/v1/options/{sym_url}")
            if r_exp.status_code == 200:
                ed = r_exp.json() or {}
                exps = ed.get("expirations") or []
                # La lista puede ser de strings o de objetos {expiration, strikes}
                exp_dates = []
                for e in exps:
                    if isinstance(e, str): exp_dates.append(e)
                    elif isinstance(e, dict) and e.get("expiration"): exp_dates.append(e["expiration"])
                today_str = _today_et_str()
                # primera expiración estrictamente futura (evita 0DTE)
                future = sorted([d for d in exp_dates if d > today_str])
                _gex_expdates_cache = list(exp_dates)   # cachear para el resto del día
                _gex_expdates_day = today_str
                if future:
                    exp = future[0]
                    print(f"[gex] {sym} expiración elegida: {exp} (de {len(exp_dates)} disponibles)")
                else:
                    print(f"[gex] NDX sin expiraciones futuras en la lista: {exp_dates[:5]}")
            else:
                print(f"[gex] /options/{sym} status {r_exp.status_code}")
        except Exception as e:
            print(f"[gex] /options/{sym} falló: {e}")
        # Probar VARIAS expiraciones futuras hasta que una dé net_gex.
        # OPTIMIZACIÓN: si ya sabemos qué expiración funciona hoy, usarla
        # primero (ahorra llamadas probando fechas que dan 404).
        global _gex_working_exp, _gex_working_exp_day
        today_k = _today_et_str()
        if _gex_working_exp_day != today_k:
            _gex_working_exp = None          # nuevo día → resetear
            _gex_working_exp_day = today_k
        future_list = sorted([d for d in exp_dates if d > today_k])
        if not future_list and exp:
            future_list = [exp]
        # Poner la expiración que funcionó antes al frente de la cola
        if _gex_working_exp and _gex_working_exp in future_list:
            future_list = [_gex_working_exp] + [d for d in future_list if d != _gex_working_exp]
        for cand_exp in future_list[:4]:   # máximo 4 intentos
            try:
                r_gex = await client.get(f"{FA_BASE}/v1/exposure/gex/{sym_url}",
                                         params={"expiration": cand_exp})
                if r_gex.status_code == 200:
                    gd = r_gex.json() or {}
                    net_gex = gd.get("net_gex")
                    per_strike = gd.get("strikes")
                    gex_flip = gd.get("gamma_flip")       # flip REAL del futuro
                    gex_label = gd.get("net_gex_label")   # "positive"/"negative" de FA
                    exp = cand_exp
                    _gex_working_exp = cand_exp   # cachear la que funciona
                    print(f"[gex] /gex/{sym}?expiration={cand_exp} OK net_gex={net_gex} label={gex_label}")
                    break
                else:
                    print(f"[gex] /gex/{sym}?expiration={cand_exp} {r_gex.status_code}: {r_gex.text[:90]}")
            except Exception as e:
                print(f"[gex] intento {cand_exp} falló: {e}")
        # Respaldo: net_gex desde la respuesta de levels si existe
        if net_gex is None:
            net_gex = (r_lvl.json() or {}).get("net_gex")
            if net_gex is not None:
                print(f"[gex] net_gex tomado de /levels: {net_gex}")

    def _num(v):
        if isinstance(v, dict):
            return v.get("strike") or v.get("price") or v.get("level")
        return v
    cw = _num(lv.get("call_wall")); pw = _num(lv.get("put_wall"))
    gf = _num(lv.get("gamma_flip")); mp = _num(lv.get("max_pain"))
    # ── FALLBACK NQ=F: si /levels no dio walls, derivarlos del per-strike REAL ──
    # call_wall = strike con mayor call_gex · put_wall = strike con put_gex más negativo
    if (cw is None or pw is None) and isinstance(per_strike, list) and per_strike:
        try:
            _calls = [s for s in per_strike if (s.get("call_gex") or 0) > 0]
            _puts  = [s for s in per_strike if (s.get("put_gex") or 0) < 0]
            if cw is None and _calls:
                cw = max(_calls, key=lambda s: s.get("call_gex") or 0).get("strike")
            if pw is None and _puts:
                pw = min(_puts, key=lambda s: s.get("put_gex") or 0).get("strike")
            print(f"[gex] walls derivados del per-strike: CW={cw} PW={pw}")
        except Exception as _e:
            print(f"[gex] derivación de walls falló: {_e}")
    # flip: preferir el de /levels; si no llegó, usar el del /gex (futuro directo)
    if gf is None and gex_flip is not None:
        gf = gex_flip
        print(f"[gex] gamma_flip tomado de /gex: {gf}")
    # Max Pain viene de endpoint separado /v1/maxpain (Basic+). /levels no lo trae.
    if mp is None and _gex_maxpain_failed_day != _today_now:
        try:
            async with httpx.AsyncClient(timeout=12,
                                          headers={"X-Api-Key": FLASHALPHA_KEY}) as mpc:
                r_mp = await mpc.get(f"{FA_BASE}/v1/maxpain/{sym_url}")
            if r_mp.status_code == 200:
                mpd = r_mp.json() or {}
                mp = _num(mpd.get("max_pain") or mpd.get("maxpain") or mpd.get("max_pain_strike"))
                print(f"[gex] max_pain ({sym}): {mp}")
            else:
                _gex_maxpain_failed_day = _today_now   # no reintentar hoy (ahorra créditos)
                print(f"[gex] /maxpain/{sym} status {r_mp.status_code} — skip resto del día")
        except Exception as e:
            print(f"[gex] maxpain falló (no crítico): {e}")
    # ── FALLBACK: MAX PAIN DERIVADO del per-strike REAL (call_oi/put_oi) ──
    # Max pain = strike donde el payoff total de las opciones es MÍNIMO al expirar.
    # Es un cálculo sobre data real de la cadena (Regla #1: no es invento).
    if mp is None and isinstance(per_strike, list) and len(per_strike) >= 3:
        try:
            _sts = [s for s in per_strike if s.get("strike") is not None]
            _K   = [float(s["strike"]) for s in _sts]
            _coi = [float(s.get("call_oi") or 0) for s in _sts]
            _poi = [float(s.get("put_oi") or 0) for s in _sts]
            best_k, best_pay = None, None
            for k_exp in _K:
                pay = 0.0
                for i in range(len(_K)):
                    pay += _coi[i] * max(0.0, k_exp - _K[i])   # calls ITM
                    pay += _poi[i] * max(0.0, _K[i] - k_exp)   # puts ITM
                if best_pay is None or pay < best_pay:
                    best_pay, best_k = pay, k_exp
            if best_k is not None:
                mp = best_k
                print(f"[gex] max_pain derivado del per-strike (OI real): {mp}")
        except Exception as _e:
            print(f"[gex] derivación max_pain falló: {_e}")
    # ATM IV real del summary de NDX (para Expected Move real, no mock).
    atm_iv = None; exp_move = None
    fear_score = None; fear_rating = None; vix_value = None
    try:
        async with httpx.AsyncClient(timeout=12,
                                      headers={"X-Api-Key": FLASHALPHA_KEY}) as ivc:
            r_sum = await ivc.get(f"{FA_BASE}/v1/stock/{sym_url}/summary")
            # FALLBACK: si el summary del futuro no responde en Basic, usar NDX.
            # El bloque macro (Fear&Greed, VIX) es de MERCADO (idéntico en ambos)
            # y el ATM IV del NDX ≈ NQ (mismo subyacente Nasdaq-100).
            if r_sum.status_code != 200 and sym != "NDX":
                print(f"[gex] summary {sym} {r_sum.status_code} → fallback NDX (macro/IV)")
                budget_charge("flashalpha", 1)
                r_sum = await ivc.get(f"{FA_BASE}/v1/stock/NDX/summary")
        if r_sum.status_code == 200:
            sd = r_sum.json() or {}
            # HUECO CERRADO: futuros pueden dar 200 con summary PARCIAL (sin
            # bloque macro ni IV). Si falta lo esencial, re-pedir el de NDX.
            _has_macro = bool((sd.get("macro") or {}).get("fear_and_greed"))
            _has_iv = bool(sd.get("atm_iv") or (sd.get("volatility") or {}).get("atm_iv"))
            if (not _has_macro and not _has_iv) and sym != "NDX":
                print(f"[gex] summary {sym} 200 pero SIN macro/IV → fallback NDX")
                budget_charge("flashalpha", 1)
                r2 = await ivc.get(f"{FA_BASE}/v1/stock/NDX/summary")
                if r2.status_code == 200:
                    sd = r2.json() or {}
            vol = sd.get("volatility", {}) or {}
            # ATM IV puede venir como campo directo o anidado en 'volatility'.
            # Probar varias ubicaciones para robustez (la doc varía por símbolo).
            atm_iv = (sd.get("atm_iv") or vol.get("atm_iv") or
                      sd.get("atm_implied_volatility") or vol.get("iv") or
                      sd.get("iv"))
            # Precio: directo, en price{}, o como underlying.
            pr = sd.get("price", {})
            if isinstance(pr, (int, float)):
                spot = pr
            else:
                spot = ((pr or {}).get("mid") or (pr or {}).get("last") or
                        sd.get("spot") or sd.get("underlying_price") or
                        sd.get("last"))
            # ── FEAR & GREED + VIX del bloque macro (mismo summary, 0 llamadas extra) ──
            # Se muestran EXACTAMENTE como los manda FlashAlpha (sin traducir).
            _macro = sd.get("macro", {}) or {}
            _fg = _macro.get("fear_and_greed", {}) or {}
            fear_score  = _fg.get("score")
            fear_rating = _fg.get("rating")   # ej: "fear", "greed", "extreme fear"
            _vix = _macro.get("vix", {}) or {}
            vix_value = _vix.get("value")
            # ATM IV puede venir en fracción (0.249) o en % (24.9). Normalizar a %.
            if isinstance(atm_iv, (int, float)) and atm_iv < 3:
                atm_iv = round(atm_iv * 100, 2)
            # Expected Move diario = spot * (atm_iv/100) * sqrt(1/252)
            if atm_iv and spot:
                import math
                exp_move = round(spot * (atm_iv/100.0) * math.sqrt(1/252.0), 1)
            print(f"[gex] ATM IV (NDX): {atm_iv}  Exp Move: {exp_move}")
    except Exception as e:
        print(f"[gex] summary/atm_iv falló (no crítico): {e}")
    # NDX ya está en escala Nasdaq-100 (~NQ). NO se convierte.
    cache["gex"][asset] = {
        "underlying_price": None,         # no aplica (NDX directo, sin ratio)
        "call_wall": cw, "put_wall": pw, "gamma_flip": gf, "max_pain": mp,
        "net_gex": net_gex,
        "atm_iv": atm_iv,
        "expected_move": exp_move,
        "fear_score": fear_score,     # tal cual FlashAlpha (0-100)
        "fear_rating": fear_rating,   # tal cual FlashAlpha ("fear","greed",...)
        "vix": vix_value,
        # Régimen: EXACTAMENTE la etiqueta de FlashAlpha (net_gex_label).
        # Fallback al signo del net_gex solo si la etiqueta no llegó.
        "regime": (("trending" if "neg" in str(gex_label).lower() else "pinning")
                   if gex_label
                   else ("pinning" if (isinstance(net_gex,(int,float)) and net_gex>=0)
                         else "trending" if isinstance(net_gex,(int,float)) else None)),
        "ticker": sym, "as_of": None,
        "per_strike_count": len(per_strike) if isinstance(per_strike, list) else 0,
        "source": ("futures-direct" if "=" in sym else "index-direct"), "_ts": time.time(),
    }
    if cw is None and pw is None and gf is None:
        cache["health"]["flashalpha"] = "online-no-levels"
        print(f"[gex] ⚠️ NDX 200 sin niveles. keys={list(lv.keys())}")
    else:
        cache["health"]["flashalpha"] = "online"
        print(f"[gex] ok (NDX directo): flip={gf} call={cw} put={pw} maxpain={mp} netgex={net_gex}")
    save_cache()

# ══ FINNHUB — Calendar, Movers, Earnings (completamente restaurado) ══════════
EVENT_BLOCKLIST = [
    "bill auction","bond auction","note auction","tips auction","frn auction",
    "3-month","6-month","4-week","8-week","6-week","52-week",
    "mba ","mortgage","baker hughes","rig count","wasde",
    "eia ","api crude","cushing","distillate","gasoline",
    "redbook","money supply","tic flows","capital flows",
]
HIGH_KW = [
    "cpi","core cpi","ppi","core ppi","pce","core pce","fomc","fed interest",
    "federal funds","fed minutes","powell","non farm","nonfarm","gdp",
    "retail sales","ism manufacturing","ism services","jolts","adp",
    "initial jobless","jobless claims","unemployment claims","unemployment rate",
    "average hourly","philly fed","philadelphia fed","empire state",
    "consumer confidence","consumer sentiment","michigan","durable goods",
    "interest rate decision","rate projection","fed speech","goolsbee",
    "waller","williams","bostic","kashkari","fed governor","fed president",
]
MED_KW = [
    "housing starts","building permits","new home sales","existing home sales",
    "trade balance","factory orders","industrial production","capacity utilization",
    "business inventories","wholesale inventories","cb leading","leading index",
    "personal income","personal spending","consumer credit","construction spending",
    "chicago pmi","dallas fed","richmond fed","kansas fed","productivity",
]
US_HOLIDAYS = [
    "independence day","juneteenth","memorial day","labor day","thanksgiving",
    "christmas","new year","martin luther king","presidents day","bank holiday",
    "markets closed","columbus day","veterans day",
]

def _holiday(name):
    return any(h in (name or "").lower() for h in US_HOLIDAYS)

def _allowed(name):
    if not name: return False
    if _holiday(name): return True
    n = name.lower()
    for bad in EVENT_BLOCKLIST:
        if bad in n: return False
    return any(k in n for k in HIGH_KW + MED_KW)

def _impact(name, ff_impact):
    if _holiday(name): return "holiday"
    n = (name or "").lower()
    if any(k in n for k in HIGH_KW): return "high"
    if any(k in n for k in MED_KW):
        return "high" if ff_impact == "high" else "medium"
    return ff_impact or "medium"

# ═══ CALENDARIO EN TIEMPO REAL — Capa RapidAPI (rellena el "actual") ═══
# Descarta BLS/BEA/Census por su retraso de 1 día; RapidAPI da el actual
# a los minutos del release. Se fusiona con ForexFactory.
_RT_RELEVANT = [
    "non-farm","nonfarm","non farm","nfp","payroll","employment",
    "cpi","core cpi","ppi","core ppi","pce","inflation","inflation rate",
    "fomc","federal funds","interest rate","rate decision","fed","powell",
    "gdp","retail sales","ism","services pmi","manufacturing pmi","pmi",
    "jolts","adp","jobless claims","initial claims","unemployment","michigan",
    "consumer confidence","consumer sentiment","durable goods","building permits",
    "housing starts","trade balance","factory orders","industrial production",
]

def _rt_relevant(name):
    n = (name or "").lower()
    return any(k in n for k in _RT_RELEVANT)

# ── Parser de números económicos con formato de texto ────────────────────────
# Extrae el número a ESCALA NATURAL (57K → 57, €19.1B → 19.1, 4.2% → 4.2,
# A$-3.018B → -3.018). Arregla el bug del parser viejo, que no quitaba € £ ¥ ni
# B y devolvía None para esos casos. NO multiplica por el sufijo a propósito:
#   • la clasificación usa solo el SIGNO de (actual - forecast)
#   • el % se calcula como ratio → el sufijo se cancela
#   • y así el "diff" que muestra la card queda a la misma escala que "57K/110K"
# Como actual y forecast de un mismo evento siempre traen la misma unidad, la
# sorpresa y su signo son correctos sin multiplicar.
def _parse_econ_num(v):
    if v is None: return None
    s = str(v).strip()
    if not s: return None
    cleaned = re.sub(r"[^0-9.\-]", "", s)     # quita $, €, £, ¥, %, K/M/B/T, letras
    if cleaned in ("", "-", ".", "-.", "--"): return None
    try:
        return float(cleaned)
    except ValueError:
        return None

def _rt_classify(name, actual, consensus):
    """Sorpresa + clasificación NQ desde el dato en tiempo real."""
    a, c = _parse_econ_num(actual), _parse_econ_num(consensus)
    if a is None or c is None:
        return None, None
    surprise = round(a - c, 2)
    nl = (name or "").lower()
    higher_bearish = any(k in nl for k in ["cpi","ppi","inflation","claims","unemployment","jobless","pce"])
    if abs(surprise) < 0.001: cls = "Neutral"
    elif higher_bearish: cls = "Bearish" if surprise > 0 else "Bullish"
    else: cls = "Bullish" if surprise > 0 else "Bearish"
    return surprise, cls

_rapidapi_last_call = 0   # timestamp de la última llamada real a RapidAPI
_rapidapi_day = ""        # día ET actual (YYYY-MM-DD) del contador diario
_rapidapi_day_count = 0   # llamadas hechas hoy (plan free TradingEconomics: 100/día)
# ── ForexFactory: límite 2 descargas/5min (todas las URLs juntas) ──
FF_URLS = [
    "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
    "https://nfs.faireconomy.media/ff_calendar_nextweek.json",
]
_ff_last_fetch = -9999  # permite la primera descarga de inmediato
_ff_cache = []
_fmp_last_fetch = 0   # timestamp última llamada a FMP
_fmp_cache = []       # último resultado bueno de FMP        # último resultado bueno de ForexFactory (límite 2/5min)

_RT_NON_US_COUNTRIES = {
    "australia","canada","euro area","euro zone","eurozone","european union",
    "germany","france","italy","spain","netherlands","united kingdom","britain",
    "japan","china","new zealand","switzerland","mexico","brazil","india",
    "south korea","norway","sweden","au","ca","eu","gb","uk","jp","cn","nz",
    "ch","mx","br","in","kr","no","se",
}
_US_NAMES = {  # nombres inequívocamente US aunque falte el campo country
    "nfp","nonfarm","non farm","non-farm","fomc","jolts","adp","ism",
    "michigan","initial claims","jobless claims",
}

async def _fetch_rapidapi_actuals(client):
    """Consulta la API de TradingEconomics (RapidAPI) para el 'actual' en tiempo real.
    Devuelve SOLO eventos US high-impact ya publicados (resolved=true) con su
    actual/forecast/previous. Se fusiona con ForexFactory rellenando lo que FF
    todavía no marca. Si no está configurada o falla, devuelve [] (FF + FMP cubren).

    Presupuesto: plan free = 100 llamadas/día. Se llama solo en las ventanas de
    releases macro US y con guard de 3 min → ~50-75 llamadas/día, margen sano."""
    if not RAPIDAPI_KEY:
        return []
    # Kill-switch: pon RAPIDAPI_ENABLED=false en Railway para apagarla. Default ON.
    if os.getenv("RAPIDAPI_ENABLED", "true").lower() == "false":
        return []

    now_et = datetime.now(NY)
    is_weekday = now_et.weekday() < 5           # lun=0 ... vie=4
    h, m = now_et.hour, now_et.minute
    # Ventana de releases macro US (ET): 8:00-11:00am (NFP/CPI/PPI/Jobless/GDP/PCE/ISM)
    # y 1:45-2:30pm (FOMC). Fuera de ahí no salen datos nuevos → no gastamos llamadas.
    in_release_window = ((8 <= h < 11) or (h == 13 and m >= 45) or (h == 14 and m <= 30))
    if not (is_weekday and in_release_window):
        return cache.get("_rapidapi_cache", [])  # sirve lo último bueno

    global _rapidapi_last_call, _rapidapi_day, _rapidapi_day_count
    nowts = time.time()
    # ── CONTADOR DIARIO: hard-stop a 90/día (deja 10 de margen del límite 100) ──
    cur_day = now_et.strftime("%Y-%m-%d")
    if _rapidapi_day != cur_day:
        _rapidapi_day = cur_day
        _rapidapi_day_count = 0                   # reset al cambiar de día
    if _rapidapi_day_count >= 90:
        return cache.get("_rapidapi_cache", [])   # presupuesto diario agotado
    # Guard de frecuencia: máx 1 llamada cada 3 min dentro de la ventana.
    if nowts - _rapidapi_last_call < 180:
        return cache.get("_rapidapi_cache", [])
    _rapidapi_last_call = nowts
    _rapidapi_day_count += 1

    headers = {"X-RapidAPI-Key": RAPIDAPI_KEY, "X-RapidAPI-Host": RAPIDAPI_HOST}
    # NOTA: el filtro country=United States devolvía count:0 en esta API, así que
    # NO lo enviamos y filtramos US en código (el campo country SÍ viene en la
    # respuesta cuando se pide en `fields`). daysBehind=7 cubre los resultados US
    # de toda la semana de trading (NFP, CPI, etc. ya publicados) para que el
    # dashboard no se vea vacío entre releases; el merge es fill-only, sin overwrite.
    params = {
        "daysBehind": "7", "daysAhead": "0",
        "impact": "High", "resolved": "true",
        "descriptions": "false",   # sin generación IA → respuesta más rápida
        "sort": "asc", "limit": "80", "tz": "America/New_York",
        "fields": "id,date,eventName,country,impactLabel,actual,forecast,previous",
    }
    try:
        url = f"https://{RAPIDAPI_HOST}/calendar"
        r = await client.get(url, headers=headers, params=params, timeout=10)
        if r.status_code != 200:
            print(f"[rt-calendar] TradingEconomics status {r.status_code}: {r.text[:120]}")
            return cache.get("_rapidapi_cache", [])
        data = r.json()
        raw = data.get("events") if isinstance(data, dict) else data
        if not isinstance(raw, list):
            raw = []
        out = []
        for ev in raw:
            name = ev.get("eventName") or ev.get("event") or ev.get("title", "")
            if not name:
                continue
            country = (ev.get("country") or "").strip().lower()
            actual = ev.get("actual")
            av = str(actual or "")
            nl = name.lower()
            # Filtro US multi-señal:
            #  - si hay país y es claramente NO-US → descartar
            #  - US si: país US, o actual con "$" (no A$/C$/S$), o nombre US-específico
            if country and country in _RT_NON_US_COUNTRIES:
                continue
            is_us = (
                country in ("united states", "us", "usa", "u.s.",
                            "united states of america")
                or (av.startswith("$"))
                or any(k in nl for k in _US_NAMES)
            )
            # Si el país viene vacío y no hay señal US clara, igual lo dejamos pasar:
            # el merge por (título + fecha) contra ForexFactory (que solo tiene US)
            # actúa como filtro final y descarta lo que no calce.
            if country and not is_us:
                continue
            if not _rt_relevant(name):
                continue
            consensus = ev.get("forecast") or ev.get("consensus") or ev.get("estimate")
            previous = ev.get("previous") or ev.get("prev")
            date = ev.get("date") or ev.get("dateUtc") or ev.get("time", "")
            surprise, cls = _rt_classify(name, actual, consensus)
            out.append({
                "title": name, "date": date,
                "actual": str(actual) if actual is not None else None,
                "consensus": str(consensus) if consensus is not None else None,
                "previous": str(previous) if previous is not None else None,
                "surprise": surprise, "classification": cls,
            })
        released = sum(1 for e in out if e["actual"])
        print(f"[rt-calendar] TradingEconomics: {len(out)} eventos US "
              f"({released} con actual) [llamada {_rapidapi_day_count}/90 hoy]")
        cache["_rapidapi_cache"] = out
        return out
    except Exception as e:
        print(f"[rt-calendar] TradingEconomics error: {e}")
        return cache.get("_rapidapi_cache", [])

def _merge_rapidapi(ff_events, rt_actuals):
    """Fusiona el 'actual' de RapidAPI en los eventos (rellena lo que FF no tiene)."""
    def norm(t):
        t = (t or "").lower().strip().replace(" m/m","").replace(" y/y","").replace(" q/q","")
        return re.sub(r"\s+", " ", t).strip()
    rt_index = {}
    for e in rt_actuals:
        d = (e.get("date","") or "")[:10]
        rt_index[(norm(e["title"]), d)] = e
    for ev in ff_events:
        d = (ev.get("time","") or ev.get("date","") or "")[:10]
        rt = rt_index.get((norm(ev.get("title","") or ev.get("name","")), d))
        if rt and rt.get("actual"):
            if not ev.get("actual"):
                ev["actual"] = rt["actual"]; ev["status"] = "Released"
            if not ev.get("forecast") and rt.get("consensus"):
                ev["forecast"] = rt["consensus"]
            if not ev.get("previous") and rt.get("previous"):
                ev["previous"] = rt["previous"]
            if rt.get("surprise") is not None:
                ev["surprise"] = rt["surprise"]; ev["classification"] = rt["classification"]
    return ff_events


async def refresh_calendar():
    """Calendar with parallel fetch, Finnhub fallback, stale cache preservation."""
    FF_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json,*/*",
        "Cache-Control": "no-cache",
    }

    def _parse_ff_event(ev):
        if str(ev.get("country","")).upper() not in ("USD","US"): return None
        name = ev.get("title") or ev.get("event","")
        if not _allowed(name): return None
        ff_imp = str(ev.get("impact","")).lower()
        impact = _impact(name, ff_imp)
        if impact == "low": return None
        actual = ev.get("actual","")
        released = bool(actual and str(actual).strip())
        return {
            "title": name, "time": ev.get("date",""), "impact": impact,
            "actual": actual or None, "forecast": ev.get("forecast") or None,
            "previous": ev.get("previous") or None,
            "status": "Released" if released else "Upcoming",
            "type": "holiday" if _holiday(name) else "macro",
        }

    async def _fetch_ff(client, url):
        try:
            # IMPORTANTE: ForexFactory limita a 2 descargas cada 5 min (todas las
            # URLs juntas). NO usamos cache-busting porque eso fuerza descargas
            # repetidas y nos bloquean con "Request Denied". Dejamos que el CDN
            # sirva su versión (se actualiza solo cada pocos minutos de todas formas).
            r = await client.get(url, timeout=8)
            if r.status_code != 200:
                print(f"[ff] status {r.status_code} en {url[:50]}")
                return []
            # Detectar página de bloqueo "Request Denied" (HTML en vez de JSON)
            ctype = r.headers.get("content-type", "")
            if "json" not in ctype.lower():
                print(f"[ff] BLOQUEADO por ForexFactory (Request Denied) — usando otras fuentes")
                return []
            return [_parse_ff_event(ev) for ev in r.json()]
        except Exception as e:
            print(f"[calendar] FF {url}: {e}"); return []

    async def _fetch_finnhub_fallback(client):
        """Finnhub economic calendar as fallback source."""
        if not FINNHUB_KEY: return []
        try:
            now_et = datetime.now(NY)
            from_dt = now_et.strftime("%Y-%m-%d")
            to_dt   = (now_et + __import__('datetime').timedelta(days=7)).strftime("%Y-%m-%d")
            r = await client.get(f"{FH_BASE}/calendar/economic",
                params={"from": from_dt, "to": to_dt, "token": FINNHUB_KEY}, timeout=8)
            if r.status_code != 200: return []
            events = []
            for ev in r.json().get("economicCalendar", []):
                if ev.get("country","").upper() != "US": continue
                name = ev.get("event","")
                if not name: continue
                imp_map = {"high":"high","medium":"med","low":"low"}
                impact = imp_map.get(ev.get("importance","").lower(), "low")
                if impact == "low": continue
                actual = ev.get("actual")
                events.append({
                    "title": name, "time": ev.get("time",""), "impact": impact,
                    "actual": str(actual) if actual is not None else None,
                    "forecast": str(ev.get("estimate","")) if ev.get("estimate") else None,
                    "previous": str(ev.get("prev","")) if ev.get("prev") else None,
                    "status": "Released" if actual is not None else "Upcoming",
                    "type": "macro",
                })
            print(f"[calendar] Finnhub fallback: {len(events)} events")
            return events
        except Exception as e:
            print(f"[calendar] Finnhub fallback error: {e}"); return []

    async def _fetch_fmp(client):
        """FMP economic calendar. ⚠️ El endpoint /economic-calendar es de PAGO
        (devuelve 402 en plan free — no es límite de cuota, es muro de suscripción).
        Apagado por defecto. Si algún día pagas FMP, pon FMP_ENABLED=true en Railway."""
        if not FMP_KEY: return []
        if os.getenv("FMP_ENABLED", "false").lower() != "true":
            return []  # endpoint premium; evita llamadas 402 inútiles cada ciclo
        if not budget_ok("fmp", 1):
            print("[calendar] presupuesto FMP agotado — se omite FMP")
            return []
        budget_charge("fmp", 1)
        try:
            now_et = datetime.now(NY)
            frm = now_et.strftime("%Y-%m-%d")
            to  = (now_et + __import__('datetime').timedelta(days=7)).strftime("%Y-%m-%d")
            # Endpoint /stable/ nuevo (el legacy /api/v3/economic_calendar da 403
            # para cuentas creadas después de agosto 2025).
            r = await client.get(f"{FMP_BASE}/economic-calendar",
                params={"from": frm, "to": to, "apikey": FMP_KEY}, timeout=8)
            if r.status_code != 200:
                print(f"[calendar] FMP status {r.status_code}: {r.text[:120]}")
                return []
            data = r.json()
            if not isinstance(data, list): return []
            events = []
            for ev in data:
                if (ev.get("country","") or "").upper() not in ("US","USA","UNITED STATES"): continue
                name = ev.get("event","")
                if not name: continue
                imp_raw = (ev.get("impact","") or "").lower()
                imp_map = {"high":"high","medium":"med","low":"low"}
                impact = imp_map.get(imp_raw, "med")
                if impact == "low": continue
                actual = ev.get("actual")
                events.append({
                    "title": name, "time": ev.get("date",""), "impact": impact,
                    "actual": str(actual) if actual is not None else None,
                    "forecast": str(ev.get("estimate","")) if ev.get("estimate") is not None else None,
                    "previous": str(ev.get("previous","")) if ev.get("previous") is not None else None,
                    "status": "Released" if actual is not None else "Upcoming",
                    "type": "macro", "source": "fmp",
                })
            with_a = sum(1 for e in events if e["actual"])
            print(f"[calendar] FMP: {len(events)} eventos US, {with_a} con actual")
            return events
        except Exception as e:
            print(f"[calendar] FMP error: {e}"); return []

    stale_backup = list(cache["calendar"]["data"])  # preserve last known good

    async with httpx.AsyncClient(headers=FF_HEADERS, follow_redirects=True) as client:
        # ── Fetch AMBAS fuentes en paralelo (ForexFactory + Finnhub) ──────────
        # Merge para capturar resultados "actual" de cualquier fuente que los tenga.
        # Esto resuelve el caso Building Permits: si FF no tiene el actual,
        # Finnhub lo provee, y viceversa.
        # ── GUARD ForexFactory: límite 2 descargas/5min → descargamos cada 3 min ──
        # Entre descargas usamos la última versión cacheada de FF. Finnhub y
        # RapidAPI sí corren cada ciclo (tienen límites más altos).
        nowts = time.time()
        nowet = datetime.now(NY)
        # ═══ ORQUESTACIÓN MULTI-FUENTE CON PRESUPUESTO ═══════════════════
        # Cada fuente tiene su propio límite. Gastamos créditos solo cuando aporta.
        # Jerarquía del "actual": FMP (rápido, 250/día) → ForexFactory (gratis,
        # base) → RapidAPI (reserva, 10/mes solo eventos enormes).
        global _fmp_last_fetch, _fmp_cache, _ff_last_fetch, _ff_cache, _rapidapi_last_call
        h, m = nowet.hour, nowet.minute
        # Ventana de releases macro US (ET): 8:00-10:30am y 1:45-2:30pm
        in_release_window = ((8 <= h < 11) or (h == 13 and m >= 45) or (h == 14 and m <= 30))

        # ── ForexFactory: cada 3 min (límite 2/5min) ──
        fetch_ff_now = (nowts - _ff_last_fetch >= 180)
        if fetch_ff_now:
            _ff_last_fetch = nowts
            ff_tasks = [_fetch_ff(client, url) for url in FF_URLS]
        else:
            ff_tasks = []

        # ── FMP: fuente PRINCIPAL del actual (RapidAPI cayó por 402).
        # Presupuesto: 250/día. Usamos ~190/día con margen:
        #   sesión (8am-4pm ET): cada 3 min → ~160 llamadas
        #   fuera de sesión: cada 10 min → ~30 llamadas
        h_now = nowet.hour
        fmp_in_session = (8 <= h_now < 16)
        fmp_interval = 180 if fmp_in_session else 600  # 3 min vs 10 min
        fetch_fmp_now = bool(FMP_KEY) and (nowts - _fmp_last_fetch >= fmp_interval)
        if fetch_fmp_now:
            _fmp_last_fetch = nowts
            fmp_task = [_fetch_fmp(client)]
        else:
            fmp_task = []

        # ── RapidAPI: reserva para eventos enormes (guard interno ya lo limita) ──
        rapid_task = [_fetch_rapidapi_actuals(client)]

        all_tasks = ff_tasks + fmp_task + rapid_task
        all_results = await asyncio.gather(*all_tasks, return_exceptions=True)

        # Separar resultados por posición conocida
        n_ff = len(ff_tasks)
        n_fmp = len(fmp_task)
        ff_fresh = []
        for i in range(n_ff):
            if isinstance(all_results[i], list):
                ff_fresh.extend([e for e in all_results[i] if e])
        fmp_events = []
        if n_fmp:
            fmp_raw = all_results[n_ff]
            if isinstance(fmp_raw, list):
                fmp_events = [e for e in fmp_raw if e]
                if fmp_events: _fmp_cache = fmp_events  # cachear último bueno
        # Si no llamamos FMP este ciclo, usar su cache
        if not fmp_events and _fmp_cache:
            fmp_events = list(_fmp_cache)
        rt_raw = all_results[n_ff + n_fmp] if len(all_results) > n_ff + n_fmp else []
        rt_actuals = [e for e in rt_raw if e] if isinstance(rt_raw, list) else []

        # Cachear FF
        if ff_fresh:
            _ff_cache = ff_fresh
        ff_events = list(_ff_cache)
        # fh_events ya no se usa para actual (Finnhub calendar es premium/403)
        fh_events = []

        # ── MERGE: indexar por (título normalizado, fecha) ────────────────────
        def norm_key(e):
            title = (e.get("title","") or "").lower().strip()
            # Normalizar título (quitar variaciones comunes)
            title = title.replace(" m/m","").replace(" y/y","").replace(" q/q","").strip()
            date = (e.get("time","") or "")[:10]
            return (title, date)

        merged = {}
        # Primero ForexFactory (base)
        for e in ff_events:
            merged[norm_key(e)] = e
        # Luego FMP: fuente del "actual" rápida y confiable (250/día).
        # Si FMP tiene resultado y FF no → usar FMP. También rellena forecast/previous.
        for e in fmp_events:
            k = norm_key(e)
            if k in merged:
                existing = merged[k]
                if e.get("actual") and not existing.get("actual"):
                    existing["actual"] = e["actual"]
                    existing["status"] = "Released"
                if e.get("forecast") and not existing.get("forecast"):
                    existing["forecast"] = e["forecast"]
                if e.get("previous") and not existing.get("previous"):
                    existing["previous"] = e["previous"]
            else:
                merged[k] = e

        out = list(merged.values())

        # ── CAPA TIEMPO REAL: RapidAPI (máxima prioridad para el "actual") ──
        # Reserva para eventos enormes (NFP/CPI/FOMC). Su "actual" gana sobre
        # FF y FMP porque es el más cercano al release oficial.
        if rt_actuals:
            out = _merge_rapidapi(out, rt_actuals)

        # Si AMBAS fuentes fallaron → servir caché stale
        if not out:
            if stale_backup:
                print(f"[calendar] ambas fuentes fallaron — sirviendo stale ({len(stale_backup)} eventos)")
                cache["calendar"]["status"] = "stale"
            else:
                cache["calendar"]["status"] = "unavailable"
                print("[calendar] sin datos de ninguna fuente")
            return

    # ── Calcular sorpresa/desviación + clasificación para cada evento ─────────
    # Usa el parser robusto (_parse_econ_num): maneja K/M/B/T, monedas y %.
    for e in out:
        actual = _parse_econ_num(e.get("actual"))
        forecast = _parse_econ_num(e.get("forecast"))
        if actual is not None and forecast is not None:
            surprise = actual - forecast
            e["surprise"] = round(surprise, 2)
            e["surprise_pct"] = round((surprise / abs(forecast) * 100), 1) if forecast != 0 else None
            # Clasificación: inflación/desempleo alto = bearish; crecimiento alto = bullish
            name = (e.get("title","") or "").lower()
            higher_bearish = any(k in name for k in ["cpi","ppi","inflation","claims","unemployment","jobless"])
            beat = surprise > 0
            if abs(surprise) < 0.001:
                e["classification"] = "Neutral"
            elif higher_bearish:
                e["classification"] = "Bearish" if beat else "Bullish"
            else:
                e["classification"] = "Bullish" if beat else "Bearish"
        else:
            e["surprise"] = None
            e["classification"] = None

    seen, deduped = set(), []
    for e in out:
        k = (e["title"].lower().strip(), (e["time"] or "")[:16])
        if k in seen: continue
        seen.add(k); deduped.append(e)
    deduped.sort(key=lambda e: e.get("time",""))

    # Diagnóstico: registrar eventos de hoy con su estado de datos (debug ADP, etc.)
    today_iso = datetime.now(NY).strftime("%Y-%m-%d")
    for e in deduped:
        if (e.get("time","") or "").startswith(today_iso):
            has_data = "✓" if (e.get("forecast") or e.get("previous") or e.get("actual")) else "✗ SIN DATOS"
            print(f"[calendar] HOY: {e.get('title','')[:30]:<30} fc={e.get('forecast')} prev={e.get('previous')} act={e.get('actual')} [{has_data}]")

    if deduped:
        cache["calendar"]["data"]        = deduped
        cache["calendar"]["last_update"] = datetime.now(NY).isoformat()
        cache["calendar"]["status"]      = "fresh"
        released = sum(1 for e in deduped if e.get("status")=="Released")
        print(f"[calendar] ok: {len(deduped)} eventos ({released} con resultado)")
    elif stale_backup:
        cache["calendar"]["status"] = "stale"
        print("[calendar] parsed empty — keeping stale")

# ── Ultra High Impact News classifier ────────────────────────────────────────
# Each entry: keyword → (impact_score, scope, category, sentiment_hint)
MARKET_IMPACT_KW = {
    # Central Banks (highest priority — always market-wide)
    "federal reserve": (10.0,"Entire Market","Central Bank","bearish"),
    "fomc":            (10.0,"Entire Market","Central Bank","bearish"),
    "powell":          (9.8,"Entire Market","Central Bank","bearish"),
    "rate hike":       (9.5,"Entire Market","Monetary Policy","bearish"),
    "rate cut":        (9.5,"Entire Market","Monetary Policy","bullish"),
    "emergency meeting":(9.8,"Entire Market","Central Bank","bearish"),
    "quantitative":    (9.0,"Entire Market","Monetary Policy","bearish"),
    "fed chair":       (9.5,"Entire Market","Central Bank","bearish"),
    # Geopolitical
    "war":             (9.2,"Entire Market","Geopolitical","bearish"),
    "ceasefire":       (9.0,"Entire Market","Geopolitical","bullish"),
    "nuclear":         (9.8,"Entire Market","Geopolitical","bearish"),
    "nato":            (9.0,"Entire Market","Geopolitical","bearish"),
    "invasion":        (9.5,"Entire Market","Geopolitical","bearish"),
    "sanctions":       (8.8,"Entire Market","Geopolitical","bearish"),
    "trade war":       (9.2,"Entire Market","Geopolitical","bearish"),
    "tariff":          (8.8,"Entire Market","Trade Policy","bearish"),
    # Political
    "trump":           (8.5,"Entire Market","Political","bearish"),
    "executive order": (8.0,"Entire Market","Political","bearish"),
    "default":         (9.5,"Entire Market","Fiscal","bearish"),
    "debt ceiling":    (9.2,"Entire Market","Fiscal","bearish"),
    "government shutdown":(8.8,"Entire Market","Political","bearish"),
    # Macro Data (unexpected only — filter for surpasses/misses)
    "cpi":             (9.0,"Entire Market","Macro Data","bearish"),
    "ppi":             (8.5,"Entire Market","Macro Data","bearish"),
    "jobs report":     (9.0,"Entire Market","Macro Data","bearish"),
    "unemployment":    (8.5,"Entire Market","Macro Data","bearish"),
    "gdp":             (8.8,"Entire Market","Macro Data","bearish"),
    "recession":       (9.2,"Entire Market","Macro Data","bearish"),
    # Tech/Market leaders — score BAJO: NO pasan solos (umbral 8.5).
    # Solo aparecen si la noticia ALSO contiene un keyword sistémico mayor.
    "nvidia":          (6.5,"Technology","Corporate","bullish"),
    "nvda":            (6.5,"Technology","Corporate","bullish"),
    "apple":           (6.5,"Technology","Corporate","bullish"),
    "openai":          (7.0,"AI Sector","Corporate","bullish"),
    "tesla":           (6.5,"Auto/Tech","Corporate","bullish"),
    "microsoft":       (6.5,"Technology","Corporate","bullish"),
    # Figuras con capacidad real de mover mercados — SÍ son sistémicas
    "elon musk":       (8.8,"Tech/Market","Influencer","bearish"),
    "musk":            (8.6,"Tech/Market","Influencer","bearish"),
    "larry fink":      (9.0,"Entire Market","Institutional","bearish"),
    "blackrock":       (8.7,"Entire Market","Institutional","bearish"),
    "jamie dimon":     (8.6,"Entire Market","Institutional","bearish"),
    "jerome powell":   (9.8,"Entire Market","Central Bank","bearish"),
    "yellen":          (8.8,"Entire Market","Fiscal","bearish"),
}

MACRO_BLOCKLIST = [
    "penny stock","memecoin","dogecoin","nft","shiba","sports","celebrity",
    "coupon","discount","giveaway","sponsored","lottery","casino","dating",
    "health tip","recipe","travel deal","horoscope",
]

# Keywords that boost impact score (unexpected = bigger market move)
SURPRISE_AMPLIFIERS = [
    "unexpected","surprise","emergency","shock","unprecedented",
    "surges","crashes","collapses","explodes","halted","circuit breaker",
    "far above","far below","significantly","dramatically","historic",
]

SENTIMENT_BULL = ["rate cut","ceasefire","deal","stimulus","beat","approved","recovery","surge positive"]
SENTIMENT_BEAR = ["rate hike","war","invasion","crash","miss","recession","ban","tariff","hike","collapse","default"]

SOURCE_TIER = {
    "reuters":1,"bloomberg":1,"wsj":1,"wall street journal":1,"ap":1,
    "financial times":1,"ft":1,"federal reserve":1,"sec":1,
    "cnbc":2,"marketwatch":2,"barrons":2,"yahoo finance":2,
    "seekingalpha":3,"benzinga":3,"thestreet":3,
}

def _classify_impact_news(title, source, ts, calendar_titles=None):
    """Classify a news headline as Ultra High Impact or filter it out."""
    if not title: return None
    t = " " + title.lower() + " "

    # Hard blocklist
    for bad in MACRO_BLOCKLIST:
        if bad in t: return None

    # Find best matching keyword
    best_score, best_scope, best_category, best_sentiment_hint = 0.0, None, None, "bearish"
    for kw, (score, scope, category, sentiment) in MARKET_IMPACT_KW.items():
        if kw in t and score > best_score:
            best_score, best_scope, best_category = score, scope, category
            best_sentiment_hint = sentiment

    # Boost for surprise/unexpected language
    surprise_boost = 0
    for amp in SURPRISE_AMPLIFIERS:
        if amp in t: surprise_boost = 0.3; break
    best_score = min(10.0, best_score + surprise_boost)

    # Umbral mínimo — SOLO eventos SISTÉMICOS de alto impacto.
    # 8.5 excluye noticias corporativas aisladas (NVIDIA 7.5, Apple 7.5, Tesla 7.0)
    # y mantiene: Fed, CPI, NFP, geopolítica, Trump, tariffs, default, recession.
    if best_score < 8.5: return None

    # Cross-dedup: skip if matches a scheduled calendar event
    if calendar_titles:
        for cal_title in calendar_titles:
            cal_words = set(cal_title.lower().split())
            head_words = set(t.split())
            if len(cal_words) > 0 and len(cal_words & head_words) / len(cal_words) > 0.5:
                return None  # same event already in calendar

    # Sentiment
    sentiment = "Neutral"
    for b in SENTIMENT_BULL:
        if b in t: sentiment = "Bullish"; break
    for b in SENTIMENT_BEAR:
        if b in t: sentiment = "Bearish"; break

    # Source confidence
    src_lower = (source or "").lower()
    tier = next((v for k,v in SOURCE_TIER.items() if k in src_lower), 3)
    confidence = "High" if tier == 1 else ("Medium" if tier == 2 else "Standard")

    alert_level = "CRITICAL" if best_score >= 9.0 else ("HIGH" if best_score >= 8.0 else "ELEVATED")

    return {
        "headline": title,
        "impact_score": round(best_score, 1),
        "scope": best_scope,
        "category": best_category,
        "sentiment": sentiment,
        "source": source or "",
        "source_confidence": confidence,
        "alert_level": alert_level,
        "ts": ts or 0,
        "type": "ultra_impact",
    }

async def refresh_movers():
    """Ultra High Impact News — market-moving events only. No stock gainers/losers."""
    if not FINNHUB_KEY:
        cache["movers"]["status"] = "offline-no-key"; return

    stale_backup = list(cache["movers"]["data"])
    calendar_titles = [e.get("title","") for e in cache["calendar"]["data"]]

    try:
        async with httpx.AsyncClient(timeout=8) as client:
            # Fetch from multiple Finnhub categories in parallel
            tasks = [
                client.get(f"{FH_BASE}/news", params={"category":"general","token":FINNHUB_KEY}),
                client.get(f"{FH_BASE}/news", params={"category":"forex","token":FINNHUB_KEY}),
            ]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

        seen_keys, classified = set(), []
        for resp in responses:
            if isinstance(resp, Exception): continue
            if resp.status_code != 200: continue
            for item in resp.json():
                headline = item.get("headline","")
                key = headline.lower().strip()[:80]
                if key in seen_keys: continue
                seen_keys.add(key)
                result = _classify_impact_news(
                    headline,
                    item.get("source",""),
                    item.get("datetime", 0),
                    calendar_titles,
                )
                if result:
                    result["url"] = item.get("url","")
                    classified.append(result)

        classified.sort(key=lambda x: (x["impact_score"], x["ts"]), reverse=True)
        out = classified[:6]  # top 6 ultra-high-impact events

        if out:
            cache["movers"]["data"]        = out
            cache["movers"]["last_update"] = datetime.now(NY).isoformat()
            cache["movers"]["status"]      = "fresh"
            cache["health"]["finnhub"]     = "online"
            print(f"[movers] ok: {len(out)} ultra-impact events")
        elif stale_backup:
            cache["movers"]["status"] = "stale"
            print("[movers] no new ultra-impact events — keeping stale")
        else:
            cache["movers"]["status"] = "empty"
            print("[movers] no ultra-impact events found")

    except Exception as e:
        cache["movers"]["status"] = "error"
        print(f"[movers] error: {e}")
        if stale_backup:
            cache["movers"]["status"] = "stale"

EARN_EXTREME = {"AAPL","MSFT","NVDA","AMZN","META","GOOGL","GOOG","TSLA","AVGO","NFLX"}
EARN_HIGH    = {
    "AMD","INTC","QCOM","MU","TSM","ORCL","CRM","ADBE","CSCO","TXN","AMAT",
    "LRCX","PANW","CRWD","SNOW","PLTR","SMCI","MRVL","ARM","DELL","NOW","INTU",
    "UBER","SHOP","COIN","PYPL","COST","TMUS","ADP","ADI","KLAC","MCHP",
    "WDAY","FTNT","DDOG","ZS","NXPI",
}

def _earn_impact(sym):
    s = (sym or "").upper()
    if s in EARN_EXTREME: return "extreme"
    if s in EARN_HIGH:    return "high"
    return "medium"

async def refresh_earnings(days=45):
    if not FINNHUB_KEY: return
    today = datetime.now(NY).date()
    frm   = today.isoformat()
    to    = (today + timedelta(days=days)).isoformat()
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{FH_BASE}/calendar/earnings",
                                  params={"from":frm,"to":to,"token":FINNHUB_KEY})
        if r.status_code != 200: return
        data = r.json()
        rows = data.get("earningsCalendar",[]) if isinstance(data,dict) else []
        out  = []
        for ev in rows:
            sym = (ev.get("symbol") or "").upper()
            if not sym or not sym.replace(".","").isalpha() or len(sym)>6: continue
            impact = _earn_impact(sym)
            if impact not in ("extreme","high","medium"): continue
            out.append({
                "symbol":          sym,
                "date":            ev.get("date"),
                "hour":            ev.get("hour",""),
                "epsEstimate":     ev.get("epsEstimate"),
                "epsActual":       ev.get("epsActual"),
                "revenueEstimate": ev.get("revenueEstimate"),
                "revenueActual":   ev.get("revenueActual"),
                "impact":          impact,
            })
        out.sort(key=lambda e:(e.get("date",""),
                               {"extreme":0,"high":1,"medium":2}.get(e["impact"],9),
                               e["symbol"]))
        cache["earnings"]["data"]        = out
        cache["earnings"]["last_update"] = datetime.now(NY).isoformat()
        cache["earnings"]["status"]      = "fresh"
        cache["health"]["finnhub"]       = "online"
        save_cache()
        print(f"[earnings] ok: {len(out)}")
    except Exception as e:
        print(f"[earnings] error: {e}")

# ══ GROQ — Resumen Institucional (2x/día, solo con GEX real) ═════════════════
async def _session_profile_ctx():
    """IB de HOY + VAH/VAL/POC de la sesión RTH previa, derivados de velas 5min
    de TwelveData (QQQ×ratio → escala NQ). 1 llamada TD por briefing (~4/día).
    Aproximación de volume profile por bins de precio — etiquetada 'aprox'.
    Regla #1: si algo falla, devuelve [] y el briefing no lo menciona."""
    out = []
    if not TWELVEDATA_KEY or not budget_ok("twelvedata", 1):
        return out
    try:
        budget_charge("twelvedata", 1)
        url = ("https://api.twelvedata.com/time_series?symbol=QQQ&interval=5min"
               f"&outputsize=400&timezone=America/New_York&apikey={TWELVEDATA_KEY}")
        async with httpx.AsyncClient(timeout=12) as c:
            r = await c.get(url)
        vals = (r.json() or {}).get("values") or []
        if not vals:
            return out
        # ratio QQQ→NQ (misma lógica que gamma-levels)
        ratio = cache["nq_ratio"].get("value")
        if not ratio:
            nq_p = (cache["heatmap"]["data"].get("NQ", {}) or {}).get("price")
            qq_p = (cache["heatmap"]["data"].get("QQQ", {}) or {}).get("price")
            if nq_p and qq_p:
                ratio = nq_p / qq_p
        ratio = ratio or 41.51
        now_et = datetime.now(NY)
        today = now_et.strftime("%Y-%m-%d")
        # días de sesión presentes (excluyendo hoy) → el último es "ayer hábil"
        days = sorted({v["datetime"][:10] for v in vals if v.get("datetime")})
        prev_days = [d for d in days if d < today]
        prev = prev_days[-1] if prev_days else None

        def _rth(v, day):
            dt = v.get("datetime", "")
            if not dt.startswith(day): return False
            hm = dt[11:16]
            return "09:30" <= hm < "16:00"

        # ── VAH/VAL/POC de AYER (perfil aprox por bins con volumen real) ──
        if prev:
            import math
            bins = {}
            for v in vals:
                if not _rth(v, prev): continue
                try:
                    h = float(v["high"]); l = float(v["low"]); vol = float(v.get("volume") or 0)
                except Exception:
                    continue
                if vol <= 0 or h <= 0: continue
                step = 0.25  # bin QQQ (~10pts NQ)
                lo_b = math.floor(l / step); hi_b = math.floor(h / step)
                n = max(1, hi_b - lo_b + 1)
                per = vol / n
                for b in range(lo_b, hi_b + 1):
                    bins[b] = bins.get(b, 0) + per
            if bins:
                total = sum(bins.values())
                poc_b = max(bins, key=bins.get)
                # value area 70%: expandir desde el POC
                inc = {poc_b}; acc = bins[poc_b]
                lo_e, hi_e = poc_b, poc_b
                while acc < total * 0.70:
                    up = bins.get(hi_e + 1, 0); dn = bins.get(lo_e - 1, 0)
                    if up <= 0 and dn <= 0: break
                    if up >= dn: hi_e += 1; acc += up; inc.add(hi_e)
                    else:        lo_e -= 1; acc += dn; inc.add(lo_e)
                step = 0.25
                poc = (poc_b * step + step / 2) * ratio
                vah = ((hi_e + 1) * step) * ratio
                val = (lo_e * step) * ratio
                out.append(f"- Perfil sesión previa (aprox, escala NQ): VAH {vah:.0f} | POC {poc:.0f} | VAL {val:.0f}")

        # ── IB de HOY (primera hora RTH 9:30-10:30), si ya ocurrió ──
        if now_et.hour > 10 or (now_et.hour == 10 and now_et.minute >= 30):
            ib_h = None; ib_l = None
            for v in vals:
                dt = v.get("datetime", "")
                if not dt.startswith(today): continue
                hm = dt[11:16]
                if "09:30" <= hm < "10:30":
                    try:
                        h = float(v["high"]); l = float(v["low"])
                    except Exception:
                        continue
                    ib_h = h if ib_h is None else max(ib_h, h)
                    ib_l = l if ib_l is None else min(ib_l, l)
            if ib_h and ib_l:
                out.append(f"- Initial Balance hoy (aprox, escala NQ): IBH {ib_h*ratio:.0f} | IBL {ib_l*ratio:.0f}")
    except Exception as e:
        print(f"[institutional] perfil de sesión falló (no crítico): {e}")
    return out


async def refresh_institutional():
    """Motor de IA institucional — genera análisis desde CUALQUIER dato disponible.
    Funciona 24/7: con o sin GEX, mercado abierto o cerrado, fin de semana.
    Construye contexto rico desde gamma, precio, correlaciones, calendario y earnings."""
    if not GROQ_KEY:
        cache["health"]["groq"] = "offline-no-key"; return
    if not budget_ok("groq", 1):
        print("[institutional] presupuesto Groq agotado — se mantiene último resumen")
        return
    budget_charge("groq", 1)

    gex = cache["gex"].get("NQ", {}) or {}
    hm  = cache["heatmap"]["data"]
    cal = cache["calendar"]["data"]
    ern = cache["earnings"]["data"]

    # ── Construir contexto desde TODO lo disponible (no requiere GEX) ──────────
    ctx = []
    now_et = datetime.now(NY)
    hour = now_et.hour
    # Sesión actual
    if now_et.weekday() >= 5:
        session = "fin de semana (mercado cerrado)"
    elif hour < 9 or (hour == 9 and now_et.minute < 30):
        session = "pre-market"
    elif hour >= 16:
        session = "after-hours"
    else:
        session = "sesión regular"
    ctx.append(f"- Sesión: {session} ({now_et.strftime('%H:%M')} ET)")

    # Precio NQ (siempre disponible vía heatmap)
    nq_data = hm.get("NQ", {})
    nq_price = nq_data.get("price")
    qqq = gex.get("underlying_price") or (hm.get("QQQ", {}) or {}).get("price")
    if nq_price:
        ctx.append(f"- NQ Futures: {nq_price:.0f}")

    # Gamma (si está disponible)
    cw = gex.get("call_wall"); pw = gex.get("put_wall")
    gf = gex.get("gamma_flip"); ng = gex.get("net_gex")
    rg = gex.get("regime", "")
    has_gamma = bool(cw and pw and gf)
    if has_gamma:
        pdir = "sobre" if (nq_price and nq_price > gf) else "bajo"
        ctx.append(f"- Gamma: Call Wall {cw:.0f} | Put Wall {pw:.0f} | Flip {gf:.0f} | NQ {pdir} del flip")
        if ng: ctx.append(f"- Régimen dealer: {rg} | Net GEX: {ng:,.0f}")
        em = gex.get("expected_move"); iv = gex.get("atm_iv")
        if em: ctx.append(f"- Movimiento esperado: ±{em:.0f}pts | IV: {iv:.1f}%" if iv else f"- Movimiento esperado: ±{em:.0f}pts")
    else:
        ctx.append("- Gamma (GEX): pendiente de actualización (FlashAlpha 5x/día)")

    # ── INVENTARIO OVERNIGHT (derivado del cambio vs cierre previo) ──
    # chg_pct del NQ = posición del precio vs settlement anterior. En pre-market
    # esto ES el inventario nocturno: positivo = inventario largo, negativo = corto.
    nq_chg = nq_data.get("chg_pct")
    if nq_chg is not None:
        inv = ("largo" if nq_chg > 0.15 else "corto" if nq_chg < -0.15 else "balanceado")
        ctx.append(f"- Inventario overnight: {inv} ({nq_chg:+.2f}% vs cierre previo)")

    # ── SENTIMIENTO (Fear & Greed de FlashAlpha, tal cual) + VIX ──
    _fs = gex.get("fear_score"); _fr = gex.get("fear_rating")
    if _fs is not None or _fr:
        ctx.append(f"- Fear & Greed: {_fs if _fs is not None else '?'}/100 ({_fr or '?'})")
    _vx = gex.get("vix")
    if _vx is not None:
        ctx.append(f"- VIX: {_vx}")

    # ── DISTANCIAS del precio a la estructura dealer (contexto operativo) ──
    if has_gamma and nq_price:
        try:
            ctx.append(f"- Distancias: al Call Wall {cw - nq_price:+.0f}pts | al Put Wall {pw - nq_price:+.0f}pts | al Flip {gf - nq_price:+.0f}pts")
        except Exception:
            pass

    # Correlaciones macro (del heatmap, siempre disponible)
    macro_signals = []
    for k, lbl in [("VIXY","VIX"),("UUP","DXY"),("IEF","US10Y"),("GLD","Oro")]:
        d = hm.get(k, {})
        if d.get("chg_pct") is not None:
            macro_signals.append(f"{lbl} {d['chg_pct']:+.1f}%")
    if macro_signals:
        ctx.append(f"- Macro: {' | '.join(macro_signals)}")

    # Mega-caps (líderes del NQ)
    leaders = []
    for sym in ["NVDA","AAPL","MSFT","META","AMZN"]:
        d = hm.get(sym, {})
        if d.get("chg_pct") is not None:
            leaders.append(f"{sym} {d['chg_pct']:+.1f}%")
    if leaders:
        ctx.append(f"- Líderes: {' | '.join(leaders[:4])}")

    # Próximo evento macro
    upcoming = [e for e in cal if e.get("status") == "Upcoming"]
    if upcoming:
        ctx.append(f"- Próximo catalizador: {upcoming[0].get('title','')}")
    # Eventos ya publicados hoy con resultado
    today_str = now_et.strftime("%Y-%m-%d")
    released_today = [e for e in cal if e.get("status") == "Released" and (e.get("time","") or "").startswith(today_str)]
    if released_today:
        last = released_today[-1]
        ctx.append(f"- Último dato publicado: {last.get('title','')} (actual: {last.get('actual','—')}, esperado: {last.get('forecast','—')})")

    # Earnings de hoy
    earn_today = [e["symbol"] for e in ern if e.get("date") == today_str and e.get("impact") in ("extreme","high")]
    if earn_today:
        ctx.append(f"- Earnings hoy: {', '.join(earn_today[:5])}")

    # Perfil de sesión: VAH/VAL/POC de ayer + IB de hoy (data interna, sin APIs nuevas)
    try:
        ctx.extend(await _session_profile_ctx())
    except Exception:
        pass
    ctx_str = "\n".join(ctx)

    # ── Prompt adaptado a si hay gamma o no ───────────────────────────────────
    sys_msg = ("Eres el analista jefe de mesa de Liberato Community: un trader institucional de NQ Futures "
               "que razona con teoría de subasta y posicionamiento dealer. Tu lector opera setups según el "
               "régimen del mercado: analiza inventario nocturno, estructura y niveles antes de decidir. "
               "Respondes SOLO en español, 3-4 oraciones en prosa (nunca listas). "
               "ESTRUCTURA OBLIGATORIA de tu razonamiento: "
               "(1) INVENTARIO: qué hizo la noche y qué implica para la apertura/sesión (corrección de "
               "inventario, continuación, gap para llenar). "
               "(2) ESTRUCTURA DEALER: régimen gamma y posición del precio vs flip/walls — en gamma negativo "
               "usa lenguaje de momentum/expansión (los dealers persiguen el precio); en gamma positivo usa "
               "lenguaje de reversión/compresión (los dealers absorben). "
               "(3) ESCENARIO OPERATIVO: el escenario más probable HOY combinando inventario+régimen+sentimiento, "
               "con el nivel exacto que lo invalidaría. "
               "El sentimiento (Fear&Greed/VIX) modula tu tono: con miedo alto advierte de movimientos bruscos "
               "y rangos amplios; con codicia señala complacencia y grinds. "
               "Adapta el análisis a la sesión (pre-market → plan de apertura; regular → lectura intradía; "
               "after-hours/cerrado → balance del día y contexto para mañana). "
               "PROHIBIDO: frases plantilla como 'el mercado se encuentra en', 'la presencia de'. Varía tu "
               "vocabulario entre briefings. Eres honesto: si un dato falta, no lo inventas ni lo mencionas. "
               "Si el contexto incluye VAH/VAL/POC o Initial Balance, intégralos al análisis "
               "(aceptación dentro/fuera del área de valor, ruptura o falla del IB). "
               "Si NO vienen en los datos, no los menciones ni los inventes.")

    if has_gamma:
        usr_msg = (f"Datos de mesa ahora mismo:\n\n{ctx_str}\n\n"
                   "Escribe el briefing institucional siguiendo tu estructura (inventario → estructura dealer → "
                   "escenario con nivel de invalidación). Usa los números exactos de los niveles.")
    else:
        usr_msg = (f"Datos de mesa ahora mismo (sin GEX disponible aún):\n\n{ctx_str}\n\n"
                   "Escribe el briefing con lo disponible: inventario overnight, sentimiento, macro y líderes. "
                   "Da el escenario de apertura más probable y qué confirmarlo/invalidarlo. No inventes niveles de gamma.")

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization":f"Bearer {GROQ_KEY}","Content-Type":"application/json"},
                json={"model":"llama-3.3-70b-versatile","max_tokens":420,"temperature":0.65,
                      "messages":[{"role":"system","content":sys_msg},
                                  {"role":"user","content":usr_msg}]}
            )
        if r.status_code == 200:
            text = r.json()["choices"][0]["message"]["content"].strip()
            cache["institutional"]["text"]        = text
            cache["institutional"]["last_update"] = datetime.now(NY).isoformat()
            cache["institutional"]["status"]      = "fresh"
            cache["institutional"]["has_gamma"]   = has_gamma
            cache["health"]["groq"]               = "online"
            save_cache()
            print(f"[institutional] ok ({'con gamma' if has_gamma else 'sin gamma — contexto macro'})")
        else:
            cache["health"]["groq"] = f"error-{r.status_code}"
            print(f"[institutional] groq {r.status_code}")
    except Exception as e:
        cache["health"]["groq"] = "error"
        cache["institutional"]["status"] = "error"
        print(f"[institutional] error: {e}")

# ══ ALPHA VANTAGE — Company details (on-demand, max 3x/día) ══════════════════
async def get_company_av(sym):
    if not ALPHA_VANTAGE_KEY: return {}
    url = (f"https://www.alphavantage.co/query?function=OVERVIEW"
           f"&symbol={sym}&apikey={ALPHA_VANTAGE_KEY}")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url)
        if r.status_code != 200: return {}
        d = r.json()
        return {"name": d.get("Name"), "sector": d.get("Sector"),
                "marketCap": d.get("MarketCapitalization"),
                "eps": d.get("EPS"), "peRatio": d.get("PERatio"),
                "52wHigh": d.get("52WeekHigh"), "52wLow": d.get("52WeekLow")}
    except Exception:
        return {}

# ══ ENDPOINTS ════════════════════════════════════════════════════════════════
@app.get("/")
def root():
    return {"status":"ok","version":"3.0-FIX40","engine":"TwelveData Realtime + Finnhub + FlashAlpha"}

@app.get("/health")
def health():
    """Health check rico — estado real de cada servicio con razones y contexto."""
    import time as _t
    now = datetime.now(NY)
    is_weekend   = now.weekday() >= 5                 # Sábado=5, Domingo=6
    is_rth       = 9 <= now.hour < 16 and not is_weekend
    gex_data     = cache["gex"].get("NQ", {})
    gex_age_h    = round((time.time() - gex_data.get("_ts",0)) / 3600, 1) if gex_data.get("_ts") else None

    def svc(status, ok_msg, off_msg, extra=None):
        online = status not in ("offline","offline-no-key","error","error-503",
                                "rate-limited-24h","offline-503","stale","waiting-for-gex")
        icon = "✓" if online else "✗"
        return {"icon": icon, "status": status,
                "message": ok_msg if online else off_msg, **(extra or {})}

    return {
        # ── Flash ──────────────────────────────────────────────────────────────
        "flashalpha": svc(
            cache["health"]["flashalpha"],
            ok_msg  = "GEX datos disponibles — niveles reales de gamma activos",
            off_msg = ("Esperando horario de mercado — cron: 9:00 AM + 7:00 PM ET lun-vie"
                       if is_weekend else
                       "Sin llamadas aún hoy — scheduler a las 9:00 AM o 7:00 PM ET"),
            extra   = {
                "schedule":         "Lun-Vie 9:00 AM + 7:00 PM ET (2 de 5 créditos/día)",
                "credits_per_day":  "5 disponibles · 2 usados máximo",
                "weekend_behavior": "Sin llamadas en fin de semana — datos persisten en disco si hubo sesión previa",
                "gex_on_disk":      bool(gex_data),
                "gex_age_hours":    gex_age_h,
                "data": {k: gex_data.get(k) for k in ("call_wall","put_wall","gamma_flip","net_gex","regime")} if gex_data else None,
            }
        ),
        # ── TwelveData WebSocket ────────────────────────────────────────────────
        "twelvedata": svc(
            cache["health"]["twelvedata"],
            ok_msg  = "WebSocket activo — precios en tiempo real",
            off_msg = "WebSocket desconectado — reconectando automáticamente",
            extra   = {
                "type":             "WebSocket persistente (única conexión)",
                "realtime_symbols": ["QQQ","AAPL","MSFT","NVDA","META","AMZN","TSLA","GOOGL"],
                "rest_symbols":     "13 ETF macro cada 15 min (batch = 13 créditos/llamada)",
                "credits_rest":     "~350/800 créditos día en horario de mercado",
                "weekend_behavior": "WebSocket conectado pero sin precios (mercado cerrado)",
                "heatmap_count":    len(cache["heatmap"]["data"]),
                "heatmap_status":   cache["heatmap"]["status"],
                "note":             "Precios vía WS llegan desde 9:30 AM ET lun-vie" if (is_weekend or not is_rth) else "Recibiendo precios en tiempo real",
            }
        ),
        # ── Finnhub ─────────────────────────────────────────────────────────────
        "finnhub": svc(
            cache["health"]["finnhub"],
            ok_msg  = "Operativo — calendar, movers y earnings respondiendo",
            off_msg = "Finnhub sin respuesta — reintentará en próximo ciclo",
            extra   = {
                "services":      ["Economic Calendar (5min)", "Market Movers (60s)", "Earnings Calendar (6h)", "Company Details (on-demand)"],
                "calendar":      {"count": len(cache["calendar"]["data"]), "status": cache["calendar"]["status"], "last": cache["calendar"]["last_update"]},
                "movers":        {"count": len(cache["movers"]["data"]),   "status": cache["movers"]["status"],   "last": cache["movers"]["last_update"]},
                "earnings":      {"count": len(cache["earnings"]["data"]), "status": cache["earnings"]["status"], "last": cache["earnings"]["last_update"]},
                "weekend_behavior": "Calendar, movers y earnings funcionan 24/7 — no dependen del mercado",
            }
        ),
        # ── Groq ─────────────────────────────────────────────────────────────────
        "groq": svc(
            cache["health"]["groq"],
            ok_msg  = "Resumen institucional generado — Llama 3.3 activo",
            off_msg = ("Esperando datos GEX de FlashAlpha para generar resumen con contexto real"
                       if not gex_data else
                       "Resumen pendiente — próxima generación: 9:05 AM o 12:00 PM ET"),
            extra   = {
                "model":            "llama-3.3-70b-versatile (Groq)",
                "schedule":         "9:05 AM + 12:00 PM ET lun-vie",
                "requires":         "Datos reales de GEX (FlashAlpha) para contexto institucional",
                "credits":          "Gratis — sin límite relevante para 2 llamadas/día",
                "weekend_behavior": "Sin generación en fin de semana — resumen del viernes persiste en disco",
                "last_text":        (cache["institutional"]["text"][:80]+"…") if cache["institutional"]["text"] else None,
                "last_update":      cache["institutional"]["last_update"],
            }
        ),
        # ── Resumen ejecutivo ───────────────────────────────────────────────────
        "system": {
            "timestamp":       now.isoformat(),
            "is_weekend":      is_weekend,
            "is_rth":          is_rth,
            "market_session":  "CERRADO — fin de semana" if is_weekend else ("RTH ACTIVO" if is_rth else "Pre/Post Market"),
            "all_online":      all(v == "online" for v in cache["health"].values()),
            "ready_for_rth":   bool(gex_data) and cache["health"]["finnhub"] == "online",
        },
        # ── Servicios verificados ──────────────────────────────────────────────
        "verified_today": {
            "finnhub_calendar":  cache["calendar"]["status"] == "fresh",
            "finnhub_movers":    cache["movers"]["status"]   == "fresh",
            "finnhub_earnings":  cache["earnings"]["status"] == "fresh",
            "twelvedata_ws":     cache["health"]["twelvedata"] == "online",
            "disk_persistence":  bool(cache["gex"].get("NQ") or cache["institutional"]["text"] or cache["earnings"]["data"]),
        },
    }

# Calcula la próxima ventana programada de FlashAlpha (19:00, 9:00, 9:15, 9:45 ET)
def _next_gex_window():
    """Devuelve la próxima hora ET en que se actualizará el GEX."""
    windows = [(9,0),(9,15),(9,45),(19,0)]  # 4 ventanas estratégicas
    now = datetime.now(NY)
    now_min = now.hour*60 + now.minute
    today_windows = sorted([h*60+m for h,m in windows])
    # Buscar la próxima ventana hoy
    for wm in today_windows:
        if wm > now_min:
            wh, wmin = wm//60, wm%60
            return {"time": f"{wh:02d}:{wmin:02d} ET", "is_today": True}
    # No quedan hoy → primera de mañana (9:00 si es día hábil)
    nxt = now + timedelta(days=1)
    # Saltar fin de semana
    while nxt.weekday() >= 5:
        nxt = nxt + timedelta(days=1)
    return {"time": "09:00 ET", "is_today": False, "date": nxt.strftime("%d-%b")}

_candles_cache = {}   # {tf: {"ts": epoch, "data": {...}}}

@app.get("/api/admin/budget")
async def budget_status(key: str = ""):
    """Monitor de presupuesto de APIs en tiempo real.
    Uso: /api/admin/budget?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    real_limits = {"twelvedata":800,"finnhub":60,"flashalpha":100,
                   "fmp":250,"alphavantage":25,"groq":1000}
    out = {}
    for api, cfg in API_BUDGETS.items():
        st = _api_usage[api]
        out[api] = {
            "usados": st["used"],
            "limite_seguro": cfg["limit"],
            "limite_real_proveedor": real_limits.get(api, "?"),
            "restantes": cfg["limit"] - st["used"],
            "ventana": cfg["window"],
            "ventana_actual": st["window_key"],
            "pct_usado": round(st["used"]/cfg["limit"]*100, 1) if cfg["limit"] else 0,
        }
    return out


@app.get("/api/admin/diag-candles-iv")
async def diag_candles_iv(key: str = ""):
    """Diagnóstico: muestra qué responde TwelveData (velas) y FlashAlpha
    (summary/atm_iv) en CRUDO, para ver por qué fallan.
    Uso: /api/admin/diag-candles-iv?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    out = {}
    # ── 1. Probar time_series para cada símbolo candidato ──
    out["velas"] = {}
    for sym in ("NQ", "NDX", "QQQ"):
        try:
            url = (f"https://api.twelvedata.com/time_series?symbol={sym}"
                   f"&interval=5min&outputsize=3&apikey={TWELVEDATA_KEY}")
            async with httpx.AsyncClient(timeout=12) as client:
                r = await client.get(url)
            body = r.json() if r.status_code == 200 else r.text[:200]
            # Resumir: status TD, si trae values, y mensaje de error si hay
            info = {"http": r.status_code}
            if isinstance(body, dict):
                info["td_status"] = body.get("status")
                info["has_values"] = bool(body.get("values"))
                info["n_values"] = len(body.get("values", []))
                if body.get("message"):
                    info["message"] = body.get("message")[:160]
                if body.get("code"):
                    info["code"] = body.get("code")
            else:
                info["raw"] = body
            out["velas"][sym] = info
        except Exception as e:
            out["velas"][sym] = {"error": str(e)}
    # ── 2. Probar el summary de NDX (atm_iv) ──
    try:
        async with httpx.AsyncClient(timeout=12,
                                      headers={"X-Api-Key": FLASHALPHA_KEY}) as c:
            r = await c.get(f"{FA_BASE}/v1/stock/{FA_INDEX_SYMBOL}/summary")
        out["summary_http"] = r.status_code
        if r.status_code == 200:
            sd = r.json() or {}
            # Mostrar las CLAVES de nivel superior y de volatility para ubicar atm_iv
            out["summary_keys"] = list(sd.keys())
            out["volatility_keys"] = list((sd.get("volatility", {}) or {}).keys()) if isinstance(sd.get("volatility"), dict) else "no-vol-dict"
            # Buscar cualquier campo que contenga 'iv'
            iv_fields = {}
            def _scan(d, prefix=""):
                if isinstance(d, dict):
                    for k, v in d.items():
                        if "iv" in k.lower() or "volat" in k.lower():
                            iv_fields[prefix+k] = v if not isinstance(v,(dict,list)) else "..."
                        if isinstance(v, dict):
                            _scan(v, prefix+k+".")
            _scan(sd)
            out["campos_iv_encontrados"] = iv_fields
        else:
            out["summary_body"] = r.text[:200]
    except Exception as e:
        out["summary_error"] = str(e)
    return out


@app.get("/api/market/candles/NQ")
async def market_candles(tf: str = "5"):
    """Velas REALES de NQ vía TwelveData time_series (sin CORS, server-side).
    tf: '5','15','30' minutos. Devuelve OHLC en escala NQ real.
    Cacheado 90s para no agotar créditos de TwelveData (múltiples
    clientes / auto-refresh comparten la misma llamada)."""
    if not TWELVEDATA_KEY:
        return {"status":"no-key","candles":[]}
    # Cache de 90s por timeframe
    cached = _candles_cache.get(tf)
    if cached and (time.time() - cached["ts"]) < 90:
        return cached["data"]
    interval = {"5":"5min","15":"15min","30":"30min"}.get(tf, "5min")
    outputsize = {"5":"78","15":"78","30":"120"}.get(tf, "78")
    # TwelveData free probablemente NO tiene el futuro NQ. Estrategia:
    # 1) intentar NQ/NDX directos (si el plan los tuviera, ideal, escala NQ)
    # 2) si no, usar QQQ (sí disponible en free) y convertir con el ratio.
    #    Para VELAS de gráfico/momentum la conversión es aceptable (no se
    #    operan entradas con el precio exacto de la vela, a diferencia del GEX).
    ratio = cache["nq_ratio"].get("value") or 41.51
    candidates = [("NQ", 1.0), ("NDX", 1.0), ("QQQ", ratio)]
    for sym, mult in candidates:
        # GUARDIÁN: no llamar si no hay presupuesto (evita 429 garantizado)
        if not td_budget_ok(1):
            print(f"[candles] presupuesto TwelveData agotado ({_td_credits['used']}/{TD_DAILY_LIMIT}) — sirviendo cache")
            cached = _candles_cache.get(tf)
            if cached:
                return {**cached["data"], "budget_note": "limite-diario-alcanzado-cache"}
            return {"status":"budget-reached","candles":[],
                    "used":_td_credits["used"],"limit":TD_DAILY_LIMIT}
        try:
            url = (f"https://api.twelvedata.com/time_series?symbol={sym}"
                   f"&interval={interval}&outputsize={outputsize}&apikey={TWELVEDATA_KEY}")
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.get(url)
            td_charge(1)  # registrar el crédito usado
            if r.status_code != 200:
                continue
            d = r.json()
            vals = d.get("values")
            if not vals:
                continue
            out = []
            for v in reversed(vals):
                try:
                    import datetime as _dt
                    t = _dt.datetime.strptime(v["datetime"], "%Y-%m-%d %H:%M:%S")
                    ts = int(t.replace(tzinfo=NY).timestamp())
                    out.append({
                        "time": ts,
                        "open": round(float(v["open"])*mult,2),
                        "high": round(float(v["high"])*mult,2),
                        "low":  round(float(v["low"])*mult,2),
                        "close":round(float(v["close"])*mult,2),
                    })
                except (KeyError, ValueError):
                    continue
            if out:
                # source indica claramente la procedencia y si hubo conversión
                src = "twelvedata-"+sym + ("" if mult==1.0 else f"-x{ratio}")
                result = {"status":"ok","symbol":sym,"interval":interval,
                          "candles":out,"source":src,
                          "converted": mult != 1.0}
                _candles_cache[tf] = {"ts": time.time(), "data": result}
                print(f"[candles] {sym} ok ({len(out)} velas){' convertido' if mult!=1.0 else ''}")
                return result
        except Exception as e:
            print(f"[candles] {sym} {interval} falló: {e}")
            continue
    return {"status":"no-data","candles":[]}


@app.get("/api/market/gamma-levels/NQ")
async def gamma_levels():
    """GEX desde cache. FlashAlpha se llama en 4 ventanas: 19:00, 9:00, 9:15, 9:45 ET.
    Expone timestamp exacto + próxima actualización programada para que el usuario
    valide si los niveles son de hoy y a qué hora se obtuvieron."""
    gex = cache["gex"].get("NQ")
    if not gex:
        # Cache frío — típico tras un redeploy de Railway (borra el cache en memoria).
        # Disparar UN refresh en background para repoblar fear/vix/expected_move/GEX.
        # Self-guarded: refresh_gex ya verifica presupuesto FlashAlpha y bloqueo 429.
        # Debounce de 5 min para no spamear ni quemar créditos si el frontend poletea.
        global _gex_ondemand_ts
        _now = time.time()
        if _now - _gex_ondemand_ts > 300:
            _gex_ondemand_ts = _now
            asyncio.create_task(refresh_gex())
            print("[gex] cache frío → refresh on-demand disparado (repuebla fear/vix/em)")
        return {"status": "loading", "message": "GEX cargando — refresca en unos segundos",
                "last_call_ts": None, "next_update": _next_gex_window()}
    qqq = gex.get("underlying_price")
    # Ratio: 1) dinámico del WebSocket (más preciso), 2) calculado del heatmap, 3) default
    ratio = cache["nq_ratio"].get("value")
    if not ratio:
        # Respaldo 1: precios reales del heatmap (NQ / QQQ)
        try:
            hm = cache["heatmap"]["data"]
            nq_p  = hm.get("NQ", {}).get("price")
            qqq_p = hm.get("QQQ", {}).get("price")
            if nq_p and qqq_p and qqq_p > 100:
                ratio = round(nq_p / qqq_p, 6)
                print(f"[ratio] del heatmap: {ratio}")
        except Exception:
            pass
    if not ratio and qqq and qqq > 100:
        # Respaldo 2: underlying_price de QQQ (FlashAlpha) + precio NQ live
        try:
            nq_live = (cache.get("nq_price", {}) or {}).get("value")
            if nq_live and nq_live > 10000:
                ratio = round(nq_live / qqq, 6)
                print(f"[ratio] de NQ_live/QQQ_flashalpha: {ratio}")
        except Exception:
            pass
    ratio = ratio or 41.51
    nq  = round(qqq*ratio,2) if qqq else None
    # NQ=F (futuro) y NDX (índice) YA están en escala NQ: NO convertir NUNCA.
    # Solo QQQ (plan free, escala ETF ~725) se convierte con ratio.
    is_direct = str(gex.get("source") or "").endswith("-direct")
    def _to_nq(v):
        if is_direct:
            return v  # ya en escala NQ (futures-direct o ndx-direct), sin conversión
        return round(v*ratio, 2) if isinstance(v, (int, float)) else v
    gex_nq = dict(gex)
    gex_nq["call_wall"]  = _to_nq(gex.get("call_wall"))
    gex_nq["put_wall"]   = _to_nq(gex.get("put_wall"))
    gex_nq["gamma_flip"] = _to_nq(gex.get("gamma_flip"))
    if gex.get("max_pain") is not None:
        gex_nq["max_pain"] = _to_nq(gex.get("max_pain"))
    gex_nq["conversion"] = "none-direct" if is_direct else f"qqq-ratio-{ratio}"
    # Timestamp: preferir as_of de FlashAlpha (cuándo se CALCULÓ el dato).
    # FlashAlpha da as_of en ISO UTC; si no, usar _ts (cuándo llamamos).
    as_of = gex.get("as_of")
    ts = None
    if as_of:
        try:
            # as_of viene como "2026-06-26T14:30:00Z" → convertir a timestamp
            ts = datetime.fromisoformat(as_of.replace("Z","+00:00")).timestamp()
        except Exception:
            ts = None
    if not ts:
        ts = gex.get("_ts")
    if not ts:
        try:
            if os.path.exists(CACHE_FILE):
                ts = os.path.getmtime(CACHE_FILE)
        except Exception:
            ts = None
    last_call_iso = None
    last_call_is_today = False
    age_seconds = None
    if ts:
        dt_et = datetime.fromtimestamp(ts, NY)
        last_call_iso = dt_et.isoformat()
        last_call_is_today = (dt_et.date() == datetime.now(NY).date())
        age_seconds = int((datetime.now(NY) - dt_et).total_seconds())
    return {**gex_nq, "asset":"NQ", "nq_price":nq,
            "ratio":cache["nq_ratio"].get("value") or 41.51, "credits_used":0,
            "last_call_ts": last_call_iso,
            "last_call_is_today": last_call_is_today,
            "age_seconds": age_seconds,
            "next_update": _next_gex_window()}

@app.get("/api/heatmap")
async def get_heatmap():
    """22 activos: 8 vía WebSocket real-time + 14 vía REST batch cada 15min."""
    data = cache["heatmap"]["data"]
    if not data:
        # Dispara carga inicial si está vacío
        await refresh_heatmap_rest()
    return {
        "heatmap":      cache["heatmap"]["data"],
        "last_update":  cache["heatmap"]["last_update"],
        "status":       cache["heatmap"]["status"],
        "count":        len(cache["heatmap"]["data"]),
        "realtime":     WS_SYMBOLS,
        "nq_ratio":     cache["nq_ratio"],
    }

@app.get("/api/version")
async def get_version():
    """Confirma qué versión del backend está desplegada + diagnóstico de estado."""
    try:
        fa_usage = _api_usage.get("flashalpha") if "_api_usage" in globals() else None
    except Exception:
        fa_usage = None
    return {
        "version": "v2026.07.09-session",
        "ws_symbols": WS_SYMBOLS,
        "has_nq1": "NQ1!" in WS_SYMBOLS,
        "has_dynamic_ratio": True,
        "nq_ratio_current": cache["nq_ratio"].get("value"),
        "flashalpha_usage": fa_usage,
        "gex_cache_warm": bool(cache["gex"].get("NQ")),
        "calendar_status": cache["calendar"].get("status"),
        "movers_status": cache["movers"].get("status"),
        "heatmap_status": cache["heatmap"].get("status"),
        "build": "session-2026-07-09",
    }

@app.get("/api/calendar")
async def get_calendar():
    """Devuelve caché INMEDIATAMENTE. Refresco en segundo plano (no bloquea).
    Incluye el precio NQ actual para que el frontend calcule el impacto inmediato."""
    last = cache["calendar"]["last_update"]
    is_stale = not last or (datetime.now(NY) - datetime.fromisoformat(last)).total_seconds() > 120
    if is_stale:
        asyncio.create_task(refresh_calendar())
    # FIX: "próximo" = Upcoming Y con fecha/hora futura (descarta eventos viejos
    # que quedaron como Upcoming porque nunca recibieron su 'actual').
    _now_et = datetime.now(NY)
    def _ev_is_future(e):
        try:
            _d = e.get("time") or e.get("date") or e.get("datetime") or ""
            if not _d:
                return True  # sin fecha, no lo descartamos
            _dt = datetime.fromisoformat(_d.replace("Z", "+00:00"))
            if _dt.tzinfo is None:
                _dt = _dt.replace(tzinfo=NY)
            return _dt >= _now_et - timedelta(hours=2)  # margen de 2h post-release
        except Exception:
            return True
    upcoming = [e for e in cache["calendar"]["data"]
                if e.get("status")=="Upcoming" and _ev_is_future(e)]
    # Precio NQ actual — para cálculo de reacción del mercado post-publicación
    nq_now = (cache["heatmap"]["data"].get("NQ", {}) or {}).get("price")
    # ── MOTOR DE REACCIÓN NQ (1 vela de 5 min tras la noticia) ──
    # Al detectar un evento recién Released: registra el precio NQ (p0).
    # Pasados ≥5 min: registra p5 y calcula la digestión = p5 - p0 en puntos.
    # Regla #1: solo con precios reales del heatmap; si faltan, no se inventa.
    global _event_reactions
    try:
        _now_ts = datetime.now(NY).timestamp()
        for e in cache["calendar"]["data"]:
            if e.get("status") != "Released":
                continue
            _k = f"{e.get('title','')}|{e.get('time','') or e.get('date','')}"
            _r = _event_reactions.get(_k)
            if _r is None and nq_now:
                _event_reactions[_k] = {"t0": _now_ts, "p0": nq_now, "p5": None}
            elif _r and _r.get("p5") is None and nq_now and (_now_ts - _r["t0"]) >= 300:
                _r["p5"] = nq_now
            _r = _event_reactions.get(_k)
            if _r and _r.get("p5") is not None:
                e["nq_reaction_pts"] = round(_r["p5"] - _r["p0"], 2)
                e["nq_reaction_window"] = "5min"
            elif _r:
                e["nq_reaction_pts"] = None  # aún midiendo (ventana de 5 min)
    except Exception as _e:
        print(f"[calendar] motor de reacción falló (no crítico): {_e}")
    return {
        "macro_calendar":   cache["calendar"]["data"],
        "next_macro_event": upcoming[0] if upcoming else None,
        "last_update":      cache["calendar"]["last_update"],
        "status":           cache["calendar"]["status"],
        "count":            len(cache["calendar"]["data"]),
        "nq_price_now":     nq_now,
    }

@app.get("/api/movers")
async def get_movers():
    """Devuelve caché INMEDIATAMENTE. Refresco en segundo plano — sin 'Failed to fetch'."""
    last = cache["movers"]["last_update"]
    is_stale = not last or (datetime.now(NY) - datetime.fromisoformat(last)).total_seconds() > 45
    if is_stale:
        asyncio.create_task(refresh_movers())
    return {
        "market_movers": cache["movers"]["data"],
        "last_update":   cache["movers"]["last_update"],
        "status":        cache["movers"]["status"],
        "count":         len(cache["movers"]["data"]),
    }

@app.get("/api/earnings")
async def get_earnings():
    last = cache["earnings"]["last_update"]
    if not last or (datetime.now(NY) - datetime.fromisoformat(last)).total_seconds() > 21600:
        await refresh_earnings()
    return {
        "earnings":    cache["earnings"]["data"],
        "last_update": cache["earnings"]["last_update"],
        "status":      cache["earnings"]["status"],
        "count":       len(cache["earnings"]["data"]),
    }

def _fmt_rev(v):
    """Format revenue estimate to readable string."""
    if v is None: return None
    try:
        v = float(v)
        if v >= 1e9:  return f"${v/1e9:.1f}B"
        if v >= 1e6:  return f"${v/1e6:.0f}M"
        return f"${v:,.0f}"
    except: return str(v)

@app.get("/api/company/{ticker}")
async def get_company(ticker: str):
    sym = ticker.upper().strip()
    cached = cache["company"].get(sym)
    if cached and time.time() - cached.get("ts",0) < 86400:
        return cached["data"]
    data = await get_company_av(sym)
    if FINNHUB_KEY:
        # Llamadas paralelas a Finnhub — ~300ms en vez de ~900ms secuencial
        async with httpx.AsyncClient(timeout=8) as client:
            urls = {
                "profile": f"{FH_BASE}/stock/profile2",
                "earnings": f"{FH_BASE}/stock/earnings",
                "metric":   f"{FH_BASE}/stock/metric",
            }
            params = {
                "profile":  {"symbol": sym, "token": FINNHUB_KEY},
                "earnings": {"symbol": sym, "limit": 8, "token": FINNHUB_KEY},
                "metric":   {"symbol": sym, "metric": "all", "token": FINNHUB_KEY},
            }
            responses = await asyncio.gather(
                client.get(urls["profile"],  params=params["profile"]),
                client.get(urls["earnings"], params=params["earnings"]),
                client.get(urls["metric"],   params=params["metric"]),
                return_exceptions=True
            )
            rp, rh, rm = responses

            # ── Perfil: nombre, sector, market cap ──────────────────────────
            if not isinstance(rp, Exception) and rp.status_code == 200:
                p = rp.json() or {}
                mc_raw = p.get("marketCapitalization")
                mc_fmt = (f"${mc_raw/1e6:.2f}T" if mc_raw and mc_raw>=1e6
                          else f"${mc_raw/1e3:.1f}B" if mc_raw and mc_raw>=1e3
                          else f"${mc_raw:.0f}M" if mc_raw else None)
                data.update({
                    "name":      data.get("name") or p.get("name"),
                    "sector":    data.get("sector") or p.get("finnhubIndustry"),
                    "country":   p.get("country"),
                    "logo":      p.get("logo"),
                    "marketCap": data.get("marketCap") or mc_fmt,
                })

            # ── Historial: últimos 4 quarters ────────────────────────────────
            if not isinstance(rh, Exception) and rh.status_code == 200:
                rows = rh.json() or []
                rows = sorted(rows, key=lambda r: r.get("period",""), reverse=True)
                hist = []
                for row in rows[:4]:
                    est    = row.get("estimate")
                    act    = row.get("actual")
                    q      = row.get("quarter"); y = row.get("year")
                    period = row.get("period","")
                    label  = f"Q{q} {y}" if q and y else period
                    sp     = row.get("surprisePercent")
                    beat   = None
                    if est is not None and act is not None:
                        beat = "beat" if float(act) >= float(est) else "miss"
                    hist.append({
                        "period":          label,
                        "date":            period,
                        "epsEstimate":     round(float(est),2) if est is not None else None,
                        "epsActual":       round(float(act),2) if act is not None else None,
                        "surprise":        row.get("surprise"),
                        "surprisePercent": round(float(sp),2) if sp is not None else None,
                        "result":          beat,
                    })
                if hist:
                    data["history"] = hist
                    if len(hist) >= 2:
                        try:
                            a0 = hist[0].get("epsActual"); a1 = hist[-1].get("epsActual")
                            if a0 and a1 and a1 != 0:
                                g = (a0 - a1) / abs(a1) * 100
                                data["epsGrowthYoY"] = f"{'+' if g>=0 else ''}{g:.1f}%"
                        except: pass

            # ── Métricas: EPS growth YoY (si no calculado del historial) ────
            if not data.get("epsGrowthYoY"):
                if not isinstance(rm, Exception) and rm.status_code == 200:
                    m = (rm.json() or {}).get("metric",{}) or {}
                    epsg = m.get("epsGrowthTTMYoy") or m.get("epsGrowthQuarterlyYoy")
                    if epsg is not None:
                        data["epsGrowthYoY"] = f"{'+' if epsg>=0 else ''}{epsg:.1f}%"
    # ── EPS ESTIMADO: 3 fuentes en cascada ────────────────────────────────────
    # Fuente 1: cache de earnings (45 días ya cargados desde Finnhub calendar)
    all_upcoming = [e for e in cache["earnings"]["data"]
                    if e.get("symbol","").upper() == sym and not e.get("epsActual")]
    all_upcoming.sort(key=lambda e: e.get("date",""))
    next_earn = all_upcoming[0] if all_upcoming else None

    if next_earn:
        eps_est = next_earn.get("epsEstimate")
        data["nextEpsEstimate"] = round(float(eps_est), 2) if eps_est is not None else None
        data["nextRevEstimate"] = _fmt_rev(next_earn.get("revenueEstimate"))
        data["nextDate"]        = next_earn.get("date")
        data["nextHour"]        = next_earn.get("hour", "")

    # Fuente 2: si no está en los 45 días, buscar directamente en Finnhub calendar
    # con ventana de 120 días (cubre empresas que reportan en 46-120 días)
    if FINNHUB_KEY and not data.get("nextEpsEstimate"):
        try:
            _from = datetime.now(NY).date().isoformat()
            _to   = (datetime.now(NY).date() + timedelta(days=120)).isoformat()
            async with httpx.AsyncClient(timeout=6) as _cc:
                _rc = await _cc.get(f"{FH_BASE}/calendar/earnings",
                                    params={"from": _from, "to": _to,
                                            "symbol": sym, "token": FINNHUB_KEY})
            if _rc.status_code == 200:
                _rows = (_rc.json() or {}).get("earningsCalendar", []) or []
                # Filter future (no actual yet) and sort by date
                _future = sorted(
                    [r for r in _rows if not r.get("epsActual")],
                    key=lambda r: r.get("date","")
                )
                if _future:
                    _nxt = _future[0]
                    _eps = _nxt.get("epsEstimate")
                    if _eps is not None:
                        data["nextEpsEstimate"] = round(float(_eps), 2)
                    data["nextDate"] = _nxt.get("date")
                    data["nextHour"] = _nxt.get("hour","")
        except Exception:
            pass

    # Fuente 3: /stock/eps-estimate — consenso de analistas (respaldo final)
    if FINNHUB_KEY and not data.get("nextEpsEstimate"):
        try:
            async with httpx.AsyncClient(timeout=5) as _ec:
                _re = await _ec.get(f"{FH_BASE}/stock/eps-estimate",
                                    params={"symbol": sym, "freq": "quarterly",
                                            "token": FINNHUB_KEY})
            if _re.status_code == 200:
                _ests = (_re.json() or {}).get("data") or []
                _today = datetime.now(NY).date().isoformat()
                _ests_sorted = sorted(_ests, key=lambda e: e.get("period",""))
                _future_ests = [e for e in _ests_sorted
                                if str(e.get("period",""))[:7] >= _today[:7]]
                if _future_ests:
                    _ne = _future_ests[0]
                    _ev = (_ne.get("epsAvg") or _ne.get("epsMean")
                           or _ne.get("epsEstimate") or _ne.get("estimate"))
                    if _ev is not None:
                        data["nextEpsEstimate"] = round(float(_ev), 2)
                        if not data.get("nextDate"):
                            data["nextDate"] = _ne.get("period")
        except Exception:
            pass

    result = {"symbol": sym, **data}
    cache["company"][sym] = {"data": result, "ts": time.time()}
    return result

@app.get("/api/context/institutional")
async def get_institutional():
    """Resumen IA de Groq. Refresco en segundo plano (no bloquea).
    Genera análisis 24/7 desde cualquier dato disponible — con o sin GEX."""
    last = cache["institutional"]["last_update"]
    # Refrescar cada 10min — en segundo plano para no bloquear la respuesta
    if not last or (datetime.now(NY) - datetime.fromisoformat(last)).total_seconds() > 600:
        asyncio.create_task(refresh_institutional())
    text = cache["institutional"]["text"]
    if not text:
        # Aún generándose — el frontend muestra su resumen local mientras tanto
        return {"summary": None, "status": "generating",
                "note": "IA generando análisis — frontend usa resumen local"}
    return {"summary":text, "last_update":cache["institutional"]["last_update"],
            "status":cache["institutional"]["status"],
            "has_gamma":cache["institutional"].get("has_gamma", False)}


# ══ WEBHOOK: Finnhub → actualización instantánea cuando una empresa reporta ═══
# Registro: finnhub.io/dashboard → Webhooks → URL: {RAILWAY_URL}/api/webhooks/finnhub
from fastapi import Request

@app.get("/api/webhooks/finnhub")
def finnhub_webhook_status():
    """GET — confirma que el webhook está activo. Finnhub usará POST."""
    return {
        "status":   "active",
        "endpoint": "/api/webhooks/finnhub",
        "method":   "POST",
        "events":   ["earnings"],
        "message":  "Webhook operativo. Registra esta URL en finnhub.io/dashboard → Webhooks.",
        "protected": bool(FINNHUB_WH_SECRET),
    }

@app.post("/api/webhooks/finnhub")
async def finnhub_webhook(request: Request):
    """Recibe eventos de Finnhub en tiempo real.
    Cuando una empresa reporta earnings, actualiza el cache inmediatamente.
    Latencia real: <60 segundos desde el reporte hasta el dashboard."""
    try:
        # Verificar secreto si está configurado
        if FINNHUB_WH_SECRET:
            token = request.headers.get("X-Finnhub-Secret", "")
            if token != FINNHUB_WH_SECRET:
                return {"status": "unauthorized"}

        payload = await request.json()
        event_type = payload.get("type","")

        # ── Earnings event ────────────────────────────────────────────────────
        if event_type in ("earnings", "earningsRelease", "earningsCalendar"):
            data = payload.get("data") or payload
            sym  = (data.get("symbol") or payload.get("symbol","")).upper()
            if not sym:
                return {"status": "ignored", "reason": "no symbol"}

            eps_actual = data.get("epsActual") or data.get("actual")
            rev_actual = data.get("revenueActual") or data.get("revenue")
            eps_est    = data.get("epsEstimate") or data.get("estimate")
            period     = data.get("period") or data.get("date","")

            print(f"[webhook] EARNINGS: {sym} | EPS actual={eps_actual} est={eps_est}")

            # 1. Update our earnings cache
            updated = False
            for earn in cache["earnings"]["data"]:
                if earn.get("symbol","").upper() == sym and earn.get("date","")[:7] == period[:7]:
                    if eps_actual is not None:
                        earn["epsActual"]     = round(float(eps_actual), 2)
                    if rev_actual is not None:
                        earn["revenueActual"] = rev_actual
                    if eps_est is not None:
                        earn["epsEstimate"]   = round(float(eps_est), 2)
                    earn["_webhook_ts"] = time.time()
                    updated = True
                    break

            # 2. If not found in upcoming, add to cache as reported
            if not updated and eps_actual is not None:
                beat = None
                if eps_est is not None:
                    beat = "beat" if float(eps_actual) >= float(eps_est) else "miss"
                cache["earnings"]["data"].insert(0, {
                    "symbol":        sym,
                    "date":          period,
                    "epsActual":     round(float(eps_actual), 2) if eps_actual else None,
                    "epsEstimate":   round(float(eps_est), 2)    if eps_est    else None,
                    "revenueActual": rev_actual,
                    "impact":        _earn_impact(sym),
                    "_from_webhook": True,
                })

            # 3. Invalidate company cache so next open fetches fresh
            if sym in cache["company"]:
                del cache["company"][sym]
                print(f"[webhook] company cache invalidado: {sym}")

            # 4. Persist updated earnings to disk
            save_cache()

            # 5. Si es empresa de alto impacto (NQ), regenerar resumen IA
            if _earn_impact(sym) in ("extreme","high") and cache["gex"].get("NQ"):
                asyncio.create_task(refresh_institutional())
                print(f"[webhook] regenerando resumen IA por earnings de {sym}")

            return {
                "status":  "processed",
                "symbol":  sym,
                "updated": updated,
                "impact":  _earn_impact(sym),
            }

        # ── Otros eventos (ignorados por ahora) ──────────────────────────────
        return {"status": "ignored", "type": event_type}

    except Exception as e:
        print(f"[webhook] error: {e}")
        return {"status": "error", "detail": str(e)}

@app.get("/api/dashboard")
async def get_dashboard():
    """Endpoint agregado — todo en una sola llamada."""
    # FIX: "próximo" = Upcoming Y con fecha/hora futura (descarta eventos viejos
    # que quedaron como Upcoming porque nunca recibieron su 'actual').
    _now_et = datetime.now(NY)
    def _ev_is_future(e):
        try:
            _d = e.get("time") or e.get("date") or e.get("datetime") or ""
            if not _d:
                return True  # sin fecha, no lo descartamos
            _dt = datetime.fromisoformat(_d.replace("Z", "+00:00"))
            if _dt.tzinfo is None:
                _dt = _dt.replace(tzinfo=NY)
            return _dt >= _now_et - timedelta(hours=2)  # margen de 2h post-release
        except Exception:
            return True
    upcoming = [e for e in cache["calendar"]["data"]
                if e.get("status")=="Upcoming" and _ev_is_future(e)]
    movers   = cache["movers"]["data"]
    breaking = next((m for m in movers if m.get("score",0)>=95), None)
    gex = cache["gex"].get("NQ",{})
    qqq = gex.get("underlying_price")
    return {
        "gamma_levels":        {**gex,"nq_price":round(qqq*(cache["nq_ratio"].get("value") or 41.51),2) if qqq else None} if gex else None,
        "heatmap":             cache["heatmap"]["data"],
        "macro_calendar":      cache["calendar"]["data"],
        "market_movers":       movers,
        "breaking_popup":      breaking,
        "next_macro_event":    upcoming[0] if upcoming else None,
        "earnings":            cache["earnings"]["data"][:20],
        "institutional_summary": cache["institutional"]["text"],
        "health":              cache["health"],
        "last_update": {
            "heatmap":      cache["heatmap"]["last_update"],
            "calendar":     cache["calendar"]["last_update"],
            "movers":       cache["movers"]["last_update"],
            "earnings":     cache["earnings"]["last_update"],
            "institutional":cache["institutional"]["last_update"],
        }
    }

# ══ SCHEDULER ════════════════════════════════════════════════════════════════
scheduler = AsyncIOScheduler(timezone=NY)

@app.on_event("startup")
async def startup():
    load_cache()
    cache["company"] = {}   # clear company cache on startup — ensures new endpoint logic runs

    # ── TwelveData WebSocket: una sola tarea persistente ──────────────────
    asyncio.create_task(twelvedata_ws())

    # ── TwelveData REST: batch 13 símbolos macro cada 15min en RTH ────────
    scheduler.add_job(refresh_heatmap_finnhub,
                      CronTrigger(day_of_week="mon-fri", hour="7-16", minute="*"))  # cada 1 min vía Finnhub  # batch 13 símbolos c/10min, 8-17 ET (702 créd/día=88%)

    # ── FlashAlpha GEX: SOLO 9am + 7pm ET (2 créditos de 5/día) ──────────
    # FlashAlpha GEX: 5 horarios exactos — máx 5 créditos/día    # ── FlashAlpha GEX: 4 ventanas (límite 5/día, deja 1 para pruebas) ──
    # Estrategia para day trading: el estudiante analiza el gráfico ANTES de
    # operar, así que necesita niveles frescos en premarket, no tras la apertura.
    # ── FlashAlpha GEX (plan Basic, 100 llamadas/día) ────────────────
    # Con NDX directo cada refresh usa ~3 llamadas (levels+gex+maxpain).
    # Horario ampliado: premarket + apertura + media mañana + sesión.
    # ~14 ventanas × 3 = ~42 llamadas/día (bajo el límite de 100).
    # GEX cada 20 min · 7:00-13:00 ET (ventana operativa de los estudiantes).
    # ~19 refreshes/día × ~3 créditos c/u (options 1x/día, maxpain skip si falló)
    # = ~60 créditos, bajo el guard de 90. Después de la 1PM no hay llamadas.
    scheduler.add_job(refresh_gex,
                      CronTrigger(hour="7-12", minute="0,12,24,36,48", day_of_week="mon-fri"))
    scheduler.add_job(refresh_gex,
                      CronTrigger(hour=13, minute=0, day_of_week="mon-fri"))

    # ── Finnhub Calendar: cada 5 minutos ──────────────────────────────────
    scheduler.add_job(refresh_calendar, IntervalTrigger(seconds=30))  # latencia máx ~45s

    # ── Finnhub Movers: cada 60 segundos ──────────────────────────────────
    scheduler.add_job(refresh_movers, IntervalTrigger(seconds=45))

    # ── Finnhub Earnings: cada 6 horas ────────────────────────────────────
    scheduler.add_job(refresh_earnings, IntervalTrigger(hours=6))

    # ── Groq Institutional: 9:05 AM + 12:00 PM ET lun-vie ─────────────────
    # Resumen IA: cada 30min durante horario extendido (premarket→afterhours)
        # Groq — 4 eventos clave del mercado (4 llamadas/día en horario hábil):
    scheduler.add_job(refresh_institutional, CronTrigger(hour=9,  minute=0,  day_of_week="mon-fri"))  # contexto premarket
    scheduler.add_job(refresh_institutional, CronTrigger(hour=9,  minute=30, day_of_week="mon-fri"))  # apertura del mercado
    scheduler.add_job(refresh_institutional, CronTrigger(hour=9,  minute=45, day_of_week="mon-fri"))  # confirmación de flujo
    scheduler.add_job(refresh_institutional, CronTrigger(hour=16, minute=0,  day_of_week="mon-fri"))

    scheduler.start()

    # ── GEX al arrancar: NO se llama a FlashAlpha en cada redeploy ──────────────
    # ANTES: cada redeploy disparaba un refresh_gex() → con muchos redeploys en un
    # día, FlashAlpha respondía 429 y bloqueaba el GEX 24h. Ahora el GEX se carga
    # (a) on-demand cuando alguien abre el dashboard (/api/gamma-levels con cache
    # frío dispara un refresh, self-guarded por presupuesto + 429), y (b) en las
    # ventanas programadas (9:00/7:00 PM ET). Así los redeploys ya no queman crédito.
    async def _gex_boot():
        try:
            g = cache["gex"].get("NQ", {}) or {}
            if g:
                print(f"[startup] cache GEX presente — sin llamada")
            else:
                print(f"[startup] sin GEX en cache — cargará on-demand (primer visitante) o en ventana 9:00/19:00 ET")
        except Exception as e:
            print(f"[startup] gex boot falló: {e}")
    asyncio.create_task(_gex_boot())

    # ── Carga inicial: todo excepto FlashAlpha (ahorra créditos) ──────────
    print("="*60)
    print("🟢 LIBERATO BACKEND v2026.06.25-FIX11 — BUILD CORRECTO")
    print("="*60)
    print("[startup] cargando datos iniciales...")
    await asyncio.gather(
        refresh_calendar(),
        refresh_movers(),
        refresh_earnings(),
        refresh_heatmap_rest(),   # primera carga del batch REST
        return_exceptions=True
    )

    # ── GEX: desde disco si existe, sino espera al scheduler de las 9am ───
    if cache["gex"].get("NQ"):
        print("[startup] GEX cargado desde disco ✓ (sin llamada a FlashAlpha)")
    else:
        print("[startup] Sin GEX en disco — cargará a las 9:00 AM ET (ahorra créditos)")
    # Generar resumen IA inmediatamente con los datos disponibles (con o sin GEX)
    asyncio.create_task(refresh_institutional())

    print("[startup] Liberato Backend v3.0 listo ✓")


# ═══════════════════════════════════════════════════════════════════════════
#  CONTACTO — recibe el formulario del home y envía correo a soporte
# ═══════════════════════════════════════════════════════════════════════════
_contact_rate = {}  # rate limiting simple anti-spam por IP

@app.post("/api/contact")
async def contact_form(request: Request):
    """
    Recibe {name, subject, description} del formulario de contacto y envía
    un correo a SUPPORT_EMAIL vía Gmail SMTP.
    Protección anti-spam: rate limiting + validación + honeypot (en frontend).
    """
    # Rate limiting: máximo 3 mensajes por IP cada 10 minutos
    ip = request.client.host if request.client else "unknown"
    now = time.time()
    bucket = [t for t in _contact_rate.get(ip, []) if now - t < 600]
    if len(bucket) >= 3:
        raise HTTPException(429, "Demasiados mensajes. Espera unos minutos.")
    bucket.append(now)
    _contact_rate[ip] = bucket

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "Datos inválidos")

    name = (body.get("name") or "").strip()
    subject = (body.get("subject") or "").strip()
    description = (body.get("description") or "").strip()

    # Validación de campos obligatorios
    if not name or not subject or not description:
        raise HTTPException(400, "Todos los campos son obligatorios")
    if len(name) > 120 or len(subject) > 200 or len(description) > 5000:
        raise HTTPException(400, "Contenido demasiado largo")
    if len(description) < 10:
        raise HTTPException(400, "La descripción es demasiado corta")

    # Construir y enviar el correo
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        print(f"[contact] ⚠️ Gmail no configurado — mensaje de {name} no enviado")
        print(f"[contact] Asunto: {subject} | {description[:80]}")
        # Devolver éxito igual para no romper la UX (el mensaje queda en logs)
        return {"success": True, "note": "logged"}

    html_body = f"""
    <div style="font-family:sans-serif;max-width:600px;margin:0 auto;">
      <h2 style="color:#C9A84C;">Nuevo mensaje de contacto · Liberato Community</h2>
      <p><strong>Nombre:</strong> {name}</p>
      <p><strong>Asunto:</strong> {subject}</p>
      <p><strong>Descripción:</strong></p>
      <p style="background:#f5f5f5;padding:14px;border-radius:8px;white-space:pre-wrap;">{description}</p>
      <hr>
      <p style="color:#888;font-size:12px;">Enviado desde el formulario de contacto de Liberato Community · IP: {ip}</p>
    </div>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"[Contacto Liberato] {subject}"
    msg["From"] = f"Liberato Community <{GMAIL_USER}>"
    msg["To"] = SUPPORT_EMAIL
    msg["Reply-To"] = GMAIL_USER  # respuestas van al emisor; el nombre va en el cuerpo
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_USER, SUPPORT_EMAIL, msg.as_string())
        print(f"[contact] ✓ Mensaje de {name} enviado a {SUPPORT_EMAIL}")
        return {"success": True}
    except Exception as e:
        print(f"[contact] ✗ Error enviando: {e}")
        raise HTTPException(500, "No se pudo enviar el mensaje. Intenta más tarde.")


# ═══════════════════════════════════════════════════════════════════════════
#  ENDPOINT DE PRUEBA MANUAL — dispara llamadas a FlashAlpha/Groq bajo demanda
#  Útil para probar sin esperar a las ventanas programadas.
#  Protegido con clave: agrega ?key=TU_CLAVE en la URL.
# ═══════════════════════════════════════════════════════════════════════════
ADMIN_KEY = os.getenv("ADMIN_KEY", "liberato2026")  # cámbiala en Railway si quieres

@app.get("/api/admin/refresh-gex")
async def manual_refresh_gex(key: str = ""):
    """Dispara una llamada manual a FlashAlpha (GEX). Uso: /api/admin/refresh-gex?key=TU_CLAVE"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    try:
        await refresh_gex()
        gex = cache["gex"].get("NQ")
        if gex:
            is_ndx = str(gex.get("source") or "").endswith("-direct")
            if is_ndx:
                # NQ=F/NDX directo: los niveles YA están en escala NQ. NO convertir.
                return {
                    "success": True,
                    "message": f"FlashAlpha {gex.get('ticker','?')} directo ✓ (sin conversión)",
                    "source": gex.get("source"),
                    "call_wall": gex.get("call_wall"),
                    "put_wall": gex.get("put_wall"),
                    "gamma_flip": gex.get("gamma_flip"),
                    "max_pain": gex.get("max_pain"),
                    "net_gex": gex.get("net_gex"),
                    "timestamp": gex.get("_ts"),
                }
            # Modo free (QQQ): convertir a NQ con ratio
            ratio = cache["nq_ratio"].get("value") or 41.51
            def _nq(v): return round(v*ratio,2) if isinstance(v,(int,float)) else v
            return {
                "success": True,
                "message": "FlashAlpha llamado manualmente ✓ (QQQ→NQ)",
                "source": "qqq-converted",
                "gamma_flip_QQQ": gex.get("gamma_flip"),
                "gamma_flip_NQ": _nq(gex.get("gamma_flip")),
                "call_wall_NQ": _nq(gex.get("call_wall")),
                "put_wall_NQ": _nq(gex.get("put_wall")),
                "ratio": ratio,
                "timestamp": gex.get("_ts"),
            }
        return {"success": False, "message": "FlashAlpha respondió pero sin datos GEX. Revisa la clave FLASHALPHA_KEY."}
    except Exception as e:
        return {"success": False, "error": str(e), "hint": "Revisa que FLASHALPHA_KEY esté configurada en Railway"}

@app.get("/api/admin/refresh-institutional")
async def manual_refresh_institutional(key: str = ""):
    """Dispara una llamada manual a Groq (resumen institucional). Uso: ?key=TU_CLAVE"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    try:
        await refresh_institutional()
        return {"success": True, "message": "Groq llamado manualmente ✓",
                "summary": cache.get("institutional", {}).get("text", "sin datos"),
                "status": cache.get("institutional", {}).get("status", "?")}
    except Exception as e:
        return {"success": False, "error": str(e), "hint": "Revisa que GROQ_KEY esté bien en Railway"}


# ═══════════════════════════════════════════════════════════════════════════
#  DIAGNÓSTICO FLASHALPHA — verifica plan, acceso a QQQ, y respuesta cruda
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/diag-ndx")
async def diag_ndx(key: str = ""):
    """Prueba NDX DIRECTO (plan Basic). Úsalo tras activar Basic para confirmar
    que los niveles reales del Nasdaq-100 llegan sin conversión.
    Uso: /api/admin/diag-ndx?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    sym = FA_INDEX_SYMBOL
    out = {"symbol": sym, "plan_configurado": FLASHALPHA_PLAN,
           "key_present": bool(FLASHALPHA_KEY)}
    if not FLASHALPHA_KEY:
        return {**out, "error": "no hay FLASHALPHA_KEY"}
    try:
        async with httpx.AsyncClient(timeout=12,
                                      headers={"X-Api-Key": FLASHALPHA_KEY}) as client:
            r_lvl = await client.get(f"{FA_BASE}/v1/exposure/levels/{sym}")
            out["levels_status"] = r_lvl.status_code
            if r_lvl.status_code == 200:
                lv = (r_lvl.json() or {}).get("levels", {}) or {}
                out["levels"] = {
                    "call_wall": lv.get("call_wall"), "put_wall": lv.get("put_wall"),
                    "gamma_flip": lv.get("gamma_flip"), "max_pain": lv.get("max_pain"),
                }
                out["interpretacion"] = "✅ NDX directo FUNCIONA — Basic activo"
            elif r_lvl.status_code == 403:
                out["levels_body"] = r_lvl.text[:200]
                out["interpretacion"] = "❌ 403: el plan NO cubre índices. ¿Ya activaste Basic en FlashAlpha?"
            elif r_lvl.status_code == 429:
                out["interpretacion"] = "⚠️ 429: quota agotada. Espera al reset (00:00 UTC)."
            else:
                out["levels_body"] = r_lvl.text[:200]
            # Paso 1: obtener expiraciones REALES de NDX
            r_exp = await client.get(f"{FA_BASE}/v1/options/{sym_url}")
            out["options_status"] = r_exp.status_code
            exp = None
            if r_exp.status_code == 200:
                ed = r_exp.json() or {}
                exps = ed.get("expirations") or []
                exp_dates = []
                for e in exps:
                    if isinstance(e, str): exp_dates.append(e)
                    elif isinstance(e, dict) and e.get("expiration"): exp_dates.append(e["expiration"])
                out["expiraciones_disponibles"] = exp_dates[:6]
                today_str = _today_et_str()
                future = sorted([d for d in exp_dates if d > today_str])
                exp = future[0] if future else None
                out["expiracion_elegida"] = exp
            else:
                out["options_body"] = r_exp.text[:160]
            # Paso 2: probar VARIAS expiraciones hasta que una dé GEX
            future = sorted([d for d in exp_dates if d > _today_et_str()]) if exp_dates else ([exp] if exp else [])
            out["gex_intentos"] = []
            gex_ok = False
            for cand in future[:4]:
                r_gex = await client.get(f"{FA_BASE}/v1/exposure/gex/{sym}",
                                         params={"expiration": cand})
                out["gex_intentos"].append({"exp": cand, "status": r_gex.status_code})
                if r_gex.status_code == 200:
                    gd = r_gex.json() or {}
                    strikes = gd.get("strikes")
                    out["gex_status"] = 200
                    out["gex_expiracion_ok"] = cand
                    out["net_gex"] = gd.get("net_gex")
                    out["net_gex_label"] = gd.get("net_gex_label")
                    out["per_strike_count"] = len(strikes) if isinstance(strikes, list) else 0
                    out["gex_interpretacion"] = f"✅ /gex FUNCIONA con expiración {cand}"
                    gex_ok = True
                    break
            if not gex_ok:
                out["gex_status"] = "todos fallaron"
                out["gex_body_ultimo"] = r_gex.text[:200] if future else "sin expiraciones"
                out["gex_interpretacion"] = "⚠️ Ninguna expiración dio GEX. Los 3 niveles (levels) SÍ funcionan; net_gex es secundario."
    except Exception as e:
        out["error"] = str(e)
    return out


@app.get("/api/admin/diag-flashalpha")
async def diag_flashalpha(key: str = ""):
    """Diagnóstico completo de FlashAlpha: plan, quota, y qué devuelve para QQQ.
    Uso: /api/admin/diag-flashalpha?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    out = {"flashalpha_key_present": bool(FLASHALPHA_KEY),
           "plan_configurado": FLASHALPHA_PLAN,
           "simbolo_indice": FA_INDEX_SYMBOL,
           "nota": "Si plan=basic usa NDX directo; si plan=free usa QQQ+conversión"}
    if not FLASHALPHA_KEY:
        return {**out, "error": "No hay FLASHALPHA_KEY configurada en Railway"}
    try:
        async with httpx.AsyncClient(timeout=15,
                                      headers={"X-Api-Key": FLASHALPHA_KEY}) as client:
            # 1. ¿Qué plan tengo?
            try:
                acc = await client.get(f"{FA_BASE}/v1/account")
                if acc.status_code == 200:
                    a = acc.json()
                    out["plan"] = a.get("plan")
                    out["daily_limit"] = a.get("daily_limit")
                    out["usage_today"] = a.get("usage_today")
                    out["remaining"] = a.get("remaining")
                else:
                    out["account_status"] = acc.status_code
            except Exception as e:
                out["account_error"] = str(e)
            # 2. ¿Qué devuelve QQQ summary? (lo que usa el dashboard)
            try:
                r = await client.get(f"{FA_BASE}/v1/stock/QQQ/summary")
                out["qqq_summary_status"] = r.status_code
                if r.status_code == 200:
                    d = r.json()
                    ex = d.get("exposure", {}) or {}
                    out["qqq_as_of"] = d.get("as_of")
                    out["qqq_market_open"] = d.get("market_open")
                    out["qqq_call_wall"] = ex.get("call_wall")
                    out["qqq_put_wall"] = ex.get("put_wall")
                    out["qqq_gamma_flip"] = ex.get("gamma_flip")
                    out["qqq_exposure_keys"] = list(ex.keys())
                else:
                    out["qqq_error_body"] = r.text[:300]
            except Exception as e:
                out["qqq_error"] = str(e)
            # 3. Probar el endpoint /v1/exposure/levels/QQQ (alternativa, Basic+)
            try:
                lv = await client.get(f"{FA_BASE}/v1/exposure/levels/QQQ")
                out["levels_qqq_status"] = lv.status_code
                if lv.status_code == 200:
                    out["levels_qqq"] = lv.json().get("levels")
                elif lv.status_code in (403, 429):
                    out["levels_qqq_error"] = lv.json()
            except Exception as e:
                out["levels_error"] = str(e)
    except Exception as e:
        out["fatal_error"] = str(e)
    return out


# ═══════════════════════════════════════════════════════════════════════════
#  DIAGNÓSTICO CALENDARIO — qué devuelve cada fuente para el "actual"
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/diag-calendar")
async def diag_calendar(key: str = ""):
    """Diagnóstico: muestra qué trae cada fuente del calendario (FF, Finnhub, RapidAPI).
    Uso: /api/admin/diag-calendar?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    out = {"sources": {}}
    async with httpx.AsyncClient(timeout=15) as client:
        # 1. ForexFactory
        try:
            r = await client.get(FF_URLS[0])
            ct = r.headers.get("content-type","")
            if "json" in ct.lower():
                data = r.json()
                # Buscar eventos US de hoy con actual
                today = datetime.now(NY).strftime("%Y-%m-%d")
                us_today = [e for e in data if e.get("country")=="USD" and today in str(e.get("date",""))]
                with_actual = [e for e in us_today if e.get("actual")]
                out["sources"]["forexfactory"] = {
                    "status": r.status_code, "blocked": False,
                    "total_events": len(data),
                    "us_today": len(us_today),
                    "us_today_with_actual": len(with_actual),
                    "sample": [{"title": e.get("title"), "actual": e.get("actual"),
                                "forecast": e.get("forecast"), "prev": e.get("previous")}
                               for e in us_today[:5]],
                }
            else:
                out["sources"]["forexfactory"] = {"status": r.status_code, "blocked": True,
                                                   "note": "Request Denied (rate limit)"}
        except Exception as e:
            out["sources"]["forexfactory"] = {"error": str(e)}
        # 2. Finnhub
        try:
            now_et = datetime.now(NY)
            r = await client.get(f"{FH_BASE}/calendar/economic",
                params={"from": now_et.strftime("%Y-%m-%d"),
                        "to": now_et.strftime("%Y-%m-%d"), "token": FINNHUB_KEY})
            if r.status_code == 200:
                body = r.json()
                cal = body.get("economicCalendar", []) if isinstance(body, dict) else []
                us = [e for e in cal if e.get("country","").upper()=="US"]
                with_actual = [e for e in us if e.get("actual") is not None]
                out["sources"]["finnhub"] = {
                    "status": 200, "total": len(cal), "us": len(us),
                    "us_with_actual": len(with_actual),
                    "raw_keys": list(body.keys()) if isinstance(body, dict) else "not-dict",
                    "sample": [{"event": e.get("event"), "actual": e.get("actual"),
                                "estimate": e.get("estimate"), "prev": e.get("prev")}
                               for e in us[:5]],
                }
            else:
                out["sources"]["finnhub"] = {"status": r.status_code, "body": r.text[:200]}
        except Exception as e:
            out["sources"]["finnhub"] = {"error": str(e)}
        # 3. RapidAPI — TradingEconomics /calendar (fuente del "actual" en vivo)
        try:
            if RAPIDAPI_KEY:
                headers = {"X-RapidAPI-Key": RAPIDAPI_KEY, "X-RapidAPI-Host": RAPIDAPI_HOST}
                # Diag: ventana ancha (5 días) para ver eventos US recientes reales
                # y poder validar el filtro. La PRODUCCIÓN usa daysBehind=2 (instant).
                r = await client.get(f"https://{RAPIDAPI_HOST}/calendar",
                    headers=headers, params={
                        "daysBehind": "5", "daysAhead": "0", "impact": "High",
                        "resolved": "true", "descriptions": "false", "limit": "80",
                        "tz": "America/New_York",
                        "fields": "id,date,eventName,country,impactLabel,actual,forecast,previous"})
                out["sources"]["rapidapi"] = {"status": r.status_code, "host": RAPIDAPI_HOST}
                if r.status_code == 200:
                    d = r.json()
                    raw = d.get("events") if isinstance(d, dict) else d
                    raw = raw if isinstance(raw, list) else []
                    us = [e for e in raw if (e.get("country","") or "").strip().lower()
                          not in _RT_NON_US_COUNTRIES and _rt_relevant(e.get("eventName",""))]
                    with_actual = [e for e in us if e.get("actual")]
                    out["sources"]["rapidapi"]["total"] = len(raw)
                    out["sources"]["rapidapi"]["us_relevant"] = len(us)
                    out["sources"]["rapidapi"]["us_with_actual"] = len(with_actual)
                    out["sources"]["rapidapi"]["daily_calls"] = _rapidapi_day_count
                    # RAW: TODOS los eventos tal cual (para validar país/nombre/actual)
                    out["sources"]["rapidapi"]["raw_events"] = [
                        {"country": e.get("country"), "eventName": e.get("eventName"),
                         "actual": e.get("actual"), "forecast": e.get("forecast")}
                        for e in raw[:20]]
                    out["sources"]["rapidapi"]["us_sample"] = [
                        {"eventName": e.get("eventName"), "country": e.get("country"),
                         "actual": e.get("actual"), "forecast": e.get("forecast"),
                         "previous": e.get("previous")}
                        for e in us[:6]]
                else:
                    out["sources"]["rapidapi"]["body"] = r.text[:200]
            else:
                out["sources"]["rapidapi"] = {"note": "No RAPIDAPI_KEY configurada"}
        except Exception as e:
            out["sources"]["rapidapi"] = {"error": str(e)}
        # 4. FMP (Financial Modeling Prep)
        try:
            if FMP_KEY:
                now_et = datetime.now(NY)
                frm = now_et.strftime("%Y-%m-%d")
                # Consultar desde AYER (los eventos pasados ya deben tener actual)
                frm_past = (now_et - timedelta(days=2)).strftime("%Y-%m-%d")
                to_diag = (now_et + timedelta(days=1)).strftime("%Y-%m-%d")
                r = await client.get(f"{FMP_BASE}/economic-calendar",
                    params={"from": frm_past, "to": to_diag, "apikey": FMP_KEY})
                out["sources"]["fmp"] = {"status": r.status_code}
                if r.status_code == 200:
                    d = r.json()
                    if isinstance(d, list):
                        us = [e for e in d if (e.get("country","") or "").upper() in ("US","USA","UNITED STATES")]
                        with_a = [e for e in us if e.get("actual") is not None]
                        high = [e for e in us if (e.get("impact","") or "").lower()=="high"]
                        out["sources"]["fmp"]["total"] = len(d)
                        out["sources"]["fmp"]["us"] = len(us)
                        out["sources"]["fmp"]["us_high_impact"] = len(high)
                        out["sources"]["fmp"]["us_with_actual"] = len(with_a)
                        # Mostrar eventos de ALTO impacto (los que importan)
                        out["sources"]["fmp"]["sample_high_impact"] = [
                            {"event": e.get("event"), "date": e.get("date"),
                             "actual": e.get("actual"), "estimate": e.get("estimate"),
                             "previous": e.get("previous")} for e in high[:8]]
                        # Mostrar los que SÍ tienen actual (si hay)
                        out["sources"]["fmp"]["sample_with_actual"] = [
                            {"event": e.get("event"), "actual": e.get("actual")}
                            for e in with_a[:5]]
                    else:
                        out["sources"]["fmp"]["note"] = "respuesta no es lista"
                        out["sources"]["fmp"]["body"] = str(d)[:200]
                else:
                    out["sources"]["fmp"]["body"] = r.text[:200]
            else:
                out["sources"]["fmp"] = {"note": "No FMP_KEY configurada"}
        except Exception as e:
            out["sources"]["fmp"] = {"error": str(e)}
    return out


# ═══════════════════════════════════════════════════════════════════════════
#  DIAGNÓSTICO DE VARIABLES — verifica qué keys están configuradas en Railway
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/diag-env")
async def diag_env(key: str = ""):
    """Muestra qué variables de entorno detecta el sistema (sin exponer las keys
    completas, solo si están presentes y sus primeros caracteres).
    Uso: /api/admin/diag-env?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    def mask(v):
        if not v: return None
        s = str(v)
        return f"{s[:4]}...{s[-3:]} ({len(s)} chars)" if len(s) > 8 else "***corta***"
    # Revisar TODOS los nombres posibles de cada variable
    return {
        "FMP": {
            "FMP_KEY": mask(os.getenv("FMP_KEY")),
            "FMP_API_KEY": mask(os.getenv("FMP_API_KEY")),
            "FINANCIAL_MODELING_PREP_KEY": mask(os.getenv("FINANCIAL_MODELING_PREP_KEY")),
            "mfp": mask(os.getenv("mfp")),
            "MFP": mask(os.getenv("MFP")),
            "FMP": mask(os.getenv("FMP")),
            "_detectada_por_codigo": mask(FMP_KEY),
        },
        "RAPIDAPI": {
            "RAPIDAPI_KEY": mask(os.getenv("RAPIDAPI_KEY")),
            "x-rapidapi-key": mask(os.getenv("x-rapidapi-key")),
            "X_RAPIDAPI_KEY": mask(os.getenv("X_RAPIDAPI_KEY")),
            "_detectada_por_codigo": mask(RAPIDAPI_KEY),
            "_host": RAPIDAPI_HOST,
        },
        "FLASHALPHA": {
            "_detectada": mask(FLASHALPHA_KEY),
        },
        "otras": {
            "FINNHUB": mask(FINNHUB_KEY),
            "TWELVEDATA": mask(TWELVEDATA_KEY),
            "GROQ": mask(GROQ_KEY),
        },
        "ayuda": "Si '_detectada_por_codigo' es null, el código NO está leyendo esa key. Revisa el nombre de la variable en Railway.",
        "ADVERTENCIAS": {
            "fmp": ("✅ FMP key detectada" if FMP_KEY else "❌ No hay FMP key (revisa la variable MFP en Railway)"),
            "rapidapi": ("✅ RapidAPI usando x-rapidapi-key (real)"
                         if (RAPIDAPI_KEY and "aqui-tu" not in RAPIDAPI_KEY and len(RAPIDAPI_KEY) > 20)
                         else "⚠️ RapidAPI key inválida o placeholder"),
            "rapidapi_host": f"Host activo: {RAPIDAPI_HOST}",
        }
    }


# ═══════════════════════════════════════════════════════════════════════════
#  DIAGNÓSTICO RAPIDAPI — prueba varios endpoints y muestra cuál funciona
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/diag-rapidapi")
async def diag_rapidapi(key: str = ""):
    """Prueba los endpoints de la Ultimate Economic Calendar para ver cuál responde.
    Uso: /api/admin/diag-rapidapi?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    if not RAPIDAPI_KEY:
        return {"error": "No hay RAPIDAPI_KEY"}
    headers = {"X-RapidAPI-Key": RAPIDAPI_KEY, "X-RapidAPI-Host": RAPIDAPI_HOST}
    today = datetime.now(NY)
    params = {"from": today.strftime("%Y-%m-%d"),
              "to": (today + timedelta(days=1)).strftime("%Y-%m-%d"), "countries": "US"}
    paths = ["/economic-events/tradingview", "/economic-events", "/economic-events/investing"]
    out = {"host": RAPIDAPI_HOST, "resultados": {}}
    async with httpx.AsyncClient(timeout=12) as client:
        for p in paths:
            try:
                r = await client.get(f"https://{RAPIDAPI_HOST}{p}",
                                     headers=headers, params=params)
                entry = {"status": r.status_code}
                if r.status_code == 200:
                    try:
                        d = r.json()
                        if isinstance(d, list):
                            entry["tipo"] = "lista"; entry["total"] = len(d)
                            entry["sample_keys"] = list(d[0].keys())[:10] if d else []
                        elif isinstance(d, dict):
                            entry["tipo"] = "dict"; entry["keys"] = list(d.keys())[:10]
                        entry["FUNCIONA"] = "✅ USAR ESTE"
                    except Exception:
                        entry["body"] = r.text[:150]
                else:
                    entry["body"] = r.text[:120]
                out["resultados"][p] = entry
            except Exception as e:
                out["resultados"][p] = {"error": str(e)[:100]}
    return out
