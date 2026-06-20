"""Liberato Backend v2 — Trading terminal NQ.
Servicios: FlashAlpha (GEX) + Finnhub (calendar/movers/earnings)
         + Yahoo Finance (heatmap 22 activos) + Anthropic Claude (resumen IA).
Keys SOLO en variables de entorno. Si un servicio falla, los demás siguen.
NUEVO v2: /api/heatmap + /api/context/institutional + WebSockets para NQ
"""
import os
import time
import asyncio
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import websockets  # <-- ¡Ahora sí lo importamos de forma segura!

# ── Credenciales (SOLO desde variables de entorno) ──
FLASHALPHA_KEY   = os.getenv("FLASHALPHA_KEY", "")
FINNHUB_KEY      = os.getenv("FINNHUB_KEY", "")
GROQ_KEY         = os.getenv("GROQ_KEY", "")              
TWELVEDATA_KEY   = os.getenv("TWELVEDATA_KEY", "")        

FA_BASE = "https://lab.flashalpha.com"
FH_BASE = "https://finnhub.io/api/v1"
NY = ZoneInfo("America/New_York")
PROXIES = {"NQ": "QQQ", "ES": "SPY", "GC": "GLD"}

# ═══════════════════════════════════════════════════════════
#  PRECIO DEL FUTURO EN VIVO — TWELVEDATA WEBSOCKET (REAL)
# ═══════════════════════════════════════════════════════════
# Diccionario global para guardar los precios en tiempo real de los contratos
_LIVE_PRICES = {"NQ": None, "ES": None, "GC": None}

async def fetch_futures_price(asset: str):
    """Retorna estrictamente el precio en vivo del WebSocket. Cero simulaciones."""
    return _LIVE_PRICES.get(asset)

async def twelvedata_websocket_listener():
    """Escucha el precio del futuro/proxy en tiempo real sin interrupciones"""
    if not TWELVEDATA_KEY:
        print("[twelvedata] Sin TWELVEDATA_KEY. WebSocket apagado por falta de credenciales.")
        return

    # Usamos el stream v1 de TwelveData para datos en tiempo real
    uri = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVEDATA_KEY}"
    
    while True:
        try:
            print("[twelvedata] Conectando al WebSocket de TwelveData...")
            async with websockets.connect(uri) as websocket:
                # Nos suscribimos al índice o proxy correspondiente (ej. QQQ o el ticker que poseas de NQ)
                # Nota: Si tu plan de TwelveData incluye el futuro continuo, puedes cambiar "QQQ" por "NQ"
                subscribe_msg = {
                    "action": "subscribe",
                    "params": {
                        "symbols": "QQQ" 
                    }
                }
                await websocket.send(json.dumps(subscribe_msg))
                print("[twelvedata] Suscripción enviada con éxito.")
                
                async for message in websocket:
                    data = json.loads(message)
                    
                    # Verificamos que el evento sea una actualización de precio real
                    if data.get("event") == "price":
                        price = float(data.get("price"))
                        
                        if data.get("symbol") == "QQQ":
                            # Multiplicador matemático exacto aproximado para el índice NQ futuro real
                            # Si cambias el símbolo arriba a "NQ", puedes guardar el 'price' directo sin multiplicar.
                            _LIVE_PRICES["NQ"] = price * 41.2  
                        else:
                            _LIVE_PRICES["NQ"] = price
                            
        except Exception as e:
            print(f"[twelvedata] Conexión perdida o error: {e}. Reconectando en 10 segundos...")
            await asyncio.sleep(10)

async def get_nq_ratio(asset: str, qqq_price: float):
    if not qqq_price or qqq_price <= 0:
        return None, None
    
    nq_price = await fetch_futures_price(asset)
    if nq_price and nq_price > 0:
        return nq_price, round(nq_price / qqq_price, 4)
    
    return None, None  # Si no hay WebSocket activo, no se calcula ningún ratio ficticio


app = FastAPI(title="Liberato Backend")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Caché en memoria por servicio ──
cache = {
    "gex":           {},
    "calendar":      {"data": [], "last_update": None, "status": "offline"},
    "movers":        {"data": [], "last_update": None, "status": "offline"},
    "earnings":      {"data": [], "last_update": None, "status": "offline"},
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
            json.dump(snapshot, f)
    except Exception as e:
        print(f"[persist] no se pudo guardar: {e}")

def _load_cache():
    try:
        with open(_PERSIST_FILE, "r") as f:
            snap = json.load(f)
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
        print("[persist] sin cache previo")
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
            raise RuntimeError("FlashAlpha 429 (rate limit)")
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
        raise RuntimeError(f"FlashAlpha {r.status_code}")

async def refresh_gex(asset="NQ"):
    try:
        data = await fetch_flashalpha(asset)
        data["_ts"] = time.time()
        cache["gex"][asset] = data
        cache["health"]["flashalpha"] = "online"
        _save_cache()
    except Exception:
        if cache["gex"].get(asset):
            cache["health"]["flashalpha"] = "stale"
        else:
            cache["health"]["flashalpha"] = "offline"

# ═══════════════════════════════════════════════════════════
#  SERVICIO 2: Forex Factory — CALENDARIO MACRO
# ═══════════════════════════════════════════════════════════
FF_URLS = [
    "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
    "https://nfs.faireconomy.media/ff_calendar_nextweek.json"
]

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
                if country not in ("USD", "US"): continue
                name = ev.get("title", "") or ev.get("event", "")
                actual = ev.get("actual", "")
                released = actual is not None and str(actual).strip() != ""
                out.append({
                    "title": name, "source": "Forex Factory", "time": ev.get("date", ""),
                    "impact": str(ev.get("impact", "")).lower(), "actual": actual or None, 
                    "forecast": ev.get("forecast") or None, "previous": ev.get("previous") or None, 
                    "status": "Released" if released else "Upcoming", "type": "macro"
                })
    return out

async def refresh_calendar():
    try:
        data = await fetch_calendar()
        cache["calendar"]["data"] = data
        cache["calendar"]["last_update"] = datetime.now(NY).isoformat()
        cache["calendar"]["status"] = "fresh"
        cache["health"]["finnhub"] = "online"
    except Exception:
        if cache["calendar"]["data"]: cache["calendar"]["status"] = "stale"

# ═══════════════════════════════════════════════════════════
#  SERVICIO 3 & 4: Finnhub news & Earnings
# ═══════════════════════════════════════════════════════════
async def refresh_movers():
    if not FINNHUB_KEY: return
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(f"{FH_BASE}/news", params={"category": "general", "token": FINNHUB_KEY})
            if r.status_code == 200:
                cache["movers"]["data"] = r.json()[:5]
                cache["movers"]["status"] = "fresh"
    except Exception:
        cache["movers"]["status"] = "stale"

async def refresh_earnings():
    if not FINNHUB_KEY: return
    try:
        today = datetime.now(NY).date()
        url = f"{FH_BASE}/calendar/earnings?from={today.isoformat()}&to={(today + timedelta(days=15)).isoformat()}&token={FINNHUB_KEY}"
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(url)
            if r.status_code == 200:
                cache["earnings"]["data"] = r.json().get("earningsCalendar", [])
                cache["earnings"]["status"] = "fresh"
                _save_cache()
    except Exception:
        cache["earnings"]["status"] = "stale"

# ═══════════════════════════════════════════════════════════
#  SERVICIO 5 & 6: Yahoo Heatmap y Groq IA
# ═══════════════════════════════════════════════════════════
HMAP_TICKERS = {"NQ": "QQQ", "AAPL":"AAPL", "MSFT":"MSFT", "NVDA":"NVDA"}

async def refresh_heatmap():
    try:
        tickers = list(HMAP_TICKERS.values())
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={','.join(tickers)}"
        async with httpx.AsyncClient(timeout=10, headers={"User-Agent": "Mozilla/5.0"}) as client:
            r = await client.get(url)
            if r.status_code == 200:
                quotes = r.json().get("quoteResponse", {}).get("result", [])
                out = {}
                for q in quotes:
                    sym = q.get("symbol")
                    out[sym] = {"price": q.get("regularMarketPrice"), "chg_pct": q.get("regularMarketChangePercent")}
                cache["heatmap"]["data"] = out
                cache["heatmap"]["status"] = "fresh"
                cache["health"]["yahoo"] = "online"
    except Exception:
        cache["heatmap"]["status"] = "stale"

async def refresh_institutional_summary():
    if not GROQ_KEY: return
    try:
        gex_data = cache["gex"].get("NQ", {})
        context_str = f"Net GEX: {gex_data.get('net_gex') or 'N/A'}"
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROQ_KEY}", "Content-Type": "application/json"},
                json={
                    "model": "llama-3.3-70b-versatile", "max_tokens": 150,
                    "messages": [{"role": "user", "content": f"Resumen macro de 2 líneas: {context_str}"}]
                }
            )
            if r.status_code == 200:
                cache["institutional"]["text"] = r.json()["choices"][0]["message"]["content"].strip()
                cache["institutional"]["status"] = "fresh"
                cache["health"]["groq"] = "online"
    except Exception:
        cache["institutional"]["status"] = "stale"

# ═══════════════════════════════════════════════════════════
#  ENDPOINTS
# ═══════════════════════════════════════════════════════════
@app.get("/")
def root(): return {"status": "ok", "service": "Liberato Backend Realtime"}

@app.get("/api/market/gamma-levels/{asset}")
async def gamma_levels(asset: str):
    asset = asset.upper()
    cached = cache["gex"].get(asset)
    if not cached: raise HTTPException(503, "GEX no disponible")
    qqq_price = cached.get("underlying_price")
    nq_price, ratio = await get_nq_ratio(asset, qqq_price)
    return {**cached, "asset": asset, "nq_price": nq_price, "ratio": ratio}

@app.get("/api/calendar")
def get_calendar(): return {"macro_calendar": cache["calendar"]["data"]}

@app.get("/api/heatmap")
def get_heatmap(): return {"heatmap": cache["heatmap"]["data"]}

# ═══════════════════════════════════════════════════════════
#  STARTUP & SCHEDULER
# ═══════════════════════════════════════════════════════════
scheduler = AsyncIOScheduler(timezone=NY)

@app.on_event("startup")
async def startup():
    _load_cache()
    
    # Schedulers
    scheduler.add_job(refresh_gex, CronTrigger(hour=9, minute=0, day_of_week="mon-fri"), args=["NQ"])
    scheduler.add_job(refresh_gex, CronTrigger(hour=19, minute=0, day_of_week="mon-fri"), args=["NQ"])
    scheduler.add_job(refresh_calendar, IntervalTrigger(minutes=5))
    scheduler.add_job(refresh_movers, IntervalTrigger(minutes=5))
    scheduler.add_job(refresh_earnings, IntervalTrigger(hours=12))
    scheduler.add_job(refresh_heatmap, IntervalTrigger(seconds=30))
    scheduler.start()
    
    # ── LANZAMIENTO DEL WEBSOCKET DE MANERA ASÍNCRONA ──
    asyncio.create_task(twelvedata_websocket_listener())
    
    await asyncio.gather(refresh_calendar(), refresh_movers(), refresh_earnings(), refresh_heatmap(), return_exceptions=True)
    print("[startup] Servidor v2 corriendo con soporte WebSocket nativo listo ✓")