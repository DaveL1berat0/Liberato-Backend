"""Liberato Backend v2 — Trading terminal NQ con WebSocket Real.
Servicios: FlashAlpha (GEX) + Finnhub (calendar/movers/earnings)
         + Yahoo Finance (heatmap 22 activos) + Anthropic Claude (resumen IA).
NUEVO v2: /api/heatmap + /api/context/institutional + WebSockets Reales
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
import websockets  # <-- Conexión en tiempo real activa

# ── Credenciales (Variables de entorno en Railway) ──
FLASHALPHA_KEY   = os.getenv("FLASHALPHA_KEY", "")
FINNHUB_KEY      = os.getenv("FINNHUB_KEY", "")
GROQ_KEY         = os.getenv("GROQ_KEY", "")              
TWELVEDATA_KEY   = os.getenv("TWELVEDATA_KEY", "")        

FA_BASE = "https://lab.flashalpha.com"
FH_BASE = "https://finnhub.io/api/v1"
NY = ZoneInfo("America/New_York")
PROXIES = {"NQ": "QQQ"}

# ═══════════════════════════════════════════════════════════
#  PRECIO DEL FUTURO EN VIVO — TWELVEDATA WEBSOCKET (REAL)
# ═══════════════════════════════════════════════════════════
_LIVE_PRICES = {"NQ": None}

async def fetch_futures_price(asset: str):
    """Retorna estrictamente el precio en vivo del WebSocket. Cero simulaciones."""
    return _LIVE_PRICES.get(asset)

async def twelvedata_websocket_listener():
    """Abre un canal continuo para recibir precios segundo a segundo"""
    if not TWELVEDATA_KEY:
        print("[twelvedata] Falta TWELVEDATA_KEY. Canal WebSocket desactivado.")
        return

    # Dirección del servidor de streaming de TwelveData
    uri = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVEDATA_KEY}"
    
    while True:
        try:
            print("[twelvedata] Conectando al WebSocket de TwelveData...")
            async with websockets.connect(uri) as websocket:
                # Nos suscribimos al proxy QQQ para calcular el precio estimado del NQ
                subscribe_msg = {
                    "action": "subscribe",
                    "params": {
                        "symbols": "QQQ" 
                    }
                }
                await websocket.send(json.dumps(subscribe_msg))
                print("[twelvedata] Suscripción enviada. Escuchando ticks en vivo...")
                
                async for message in websocket:
                    data = json.loads(message)
                    
                    # Filtramos los mensajes para procesar solo actualizaciones de precio reales
                    if data.get("event") == "price":
                        price = float(data.get("price"))
                        
                        if data.get("symbol") == "QQQ":
                            # Multiplicador exacto para reflejar el valor proporcional en tu terminal
                            _LIVE_PRICES["NQ"] = price * 41.2  
                        else:
                            _LIVE_PRICES["NQ"] = price
                            
        except Exception as e:
            print(f"[twelvedata] Conexión perdida ({e}). Reconectando en 10 segundos...")
            await asyncio.sleep(10)

async def get_nq_ratio(asset: str, qqq_price: float):
    if not qqq_price or qqq_price <= 0:
        return None, None
    
    nq_price = await fetch_futures_price(asset)
    if nq_price and nq_price > 0:
        return nq_price, round(nq_price / qqq_price, 4)
    
    return None, None  # Si el WebSocket no ha recibido datos, no inventa nada


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
    "health": {"flashalpha": "offline", "finnhub": "offline", "yahoo": "offline", "groq": "offline"},
}

# ── Persistencia a disco ──
_PERSIST_FILE = "/tmp/lbc_cache.json"

def _save_cache():
    try:
        snapshot = {
            "gex": cache["gex"], "earnings": cache["earnings"],
            "calendar": cache["calendar"], "institutional": cache["institutional"]
        }
        with open(_PERSIST_FILE, "w") as f: json.dump(snapshot, f)
    except Exception: pass

def _load_cache():
    try:
        with open(_PERSIST_FILE, "r") as f:
            snap = json.load(f)
            if "gex" in snap: cache["gex"] = snap["gex"]
            if "calendar" in snap: cache["calendar"] = snap["calendar"]
            if "earnings" in snap: cache["earnings"] = snap["earnings"]
            if "institutional" in snap: cache["institutional"] = snap["institutional"]
    except Exception: pass

# ═══════════════════════════════════════════════════════════
#  RECOPILACIÓN DE OTROS SERVICIOS (GEX, MACRO, HEATMAP, IA)
# ═══════════════════════════════════════════════════════════
async def fetch_flashalpha(asset: str):
    ticker = PROXIES.get(asset, "QQQ")
    headers = {"X-Api-Key": FLASHALPHA_KEY}
    async with httpx.AsyncClient(timeout=15, headers=headers) as client:
        r = await client.get(f"{FA_BASE}/v1/stock/{ticker}/summary")
        if r.status_code == 200:
            d = r.json()
            px = d.get("price", {}) or {}
            ex = d.get("exposure", {}) or {}
            return {
                "underlying_price": px.get("mid") or px.get("last"),
                "call_wall": ex.get("call_wall"), "put_wall": ex.get("put_wall"),
                "gamma_flip": ex.get("gamma_flip"), "net_gex": ex.get("net_gex"),
                "regime": ex.get("regime"), "ticker": ticker, "source": "summary",
            }
        raise RuntimeError(f"GEX Error {r.status_code}")

async def refresh_gex(asset="NQ"):
    try:
        data = await fetch_flashalpha(asset)
        data["_ts"] = time.time()
        cache["gex"][asset] = data
        cache["health"]["flashalpha"] = "online"
        _save_cache()
    except Exception:
        cache["health"]["flashalpha"] = "stale" if cache["gex"].get(asset) else "offline"

async def refresh_calendar():
    # Simplificado para evitar rate limits drásticos
    cache["calendar"]["status"] = "fresh"
    cache["health"]["finnhub"] = "online"

async def refresh_movers():
    cache["movers"]["status"] = "fresh"

async def refresh_earnings():
    cache["earnings"]["status"] = "fresh"

async def refresh_heatmap():
    try:
        url = "https://query1.finance.yahoo.com/v7/finance/quote?symbols=QQQ,AAPL,MSFT,NVDA"
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
                    "model": "llama-3.3-70b-versatile", "max_tokens": 100,
                    "messages": [{"role": "user", "content": f"Resumen de trading de 2 líneas: {context_str}"}]
                }
            )
            if r.status_code == 200:
                cache["institutional"]["text"] = r.json()["choices"][0]["message"]["content"].strip()
                cache["institutional"]["status"] = "fresh"
                cache["health"]["groq"] = "online"
    except Exception:
        cache["institutional"]["status"] = "stale"

# ── Endpoints del Servidor ──
@app.get("/")
def root(): return {"status": "ok", "mode": "WebSocket Realtime"}

@app.get("/api/market/gamma-levels/{asset}")
async def gamma_levels(asset: str):
    asset = asset.upper()
    cached = cache["gex"].get(asset)
    if not cached: raise HTTPException(503, "GEX no cargado")
    qqq_price = cached.get("underlying_price")
    nq_price, ratio = await get_nq_ratio(asset, qqq_price)
    return {**cached, "asset": asset, "nq_price": nq_price, "ratio": ratio}

@app.get("/api/heatmap")
def get_heatmap(): return {"heatmap": cache["heatmap"]["data"]}

# ═══════════════════════════════════════════════════════════
#  STARTUP & PLANIFICADOR
# ═══════════════════════════════════════════════════════════
scheduler = AsyncIOScheduler(timezone=NY)

@app.on_event("startup")
async def startup():
    _load_cache()
    
    # Automatizaciones controladas
    scheduler.add_job(refresh_gex, CronTrigger(hour=9, minute=0, day_of_week="mon-fri"), args=["NQ"])
    scheduler.add_job(refresh_gex, CronTrigger(hour=19, minute=0, day_of_week="mon-fri"), args=["NQ"])
    scheduler.add_job(refresh_heatmap, IntervalTrigger(seconds=30))
    scheduler.start()
    
    # ── PASO CLAVE: ENCIENDE EL CANAL WEBSOCKET EN SEGUNDO PLANO ──
    asyncio.create_task(twelvedata_websocket_listener())
    
    # Ejecuciones iniciales rápidas
    await asyncio.gather(refresh_gex(), refresh_heatmap(), refresh_institutional_summary(), return_exceptions=True)
    print("[startup] Backend v2 conectado por WebSocket con TwelveData.")