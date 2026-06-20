"""Liberato Backend v2.1 — Parche Clínico de Conectividad.
Solución a errores de autenticación, timeouts de librería y limpieza de headers.
"""
import os
import time
import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import websockets

# ── Credenciales (Variables de entorno en Railway) ──
FLASHALPHA_KEY   = os.getenv("FLASHALPHA_KEY", "").strip()
FINNHUB_KEY      = os.getenv("FINNHUB_KEY", "").strip()
GROQ_KEY         = os.getenv("GROQ_KEY", "").strip()              
TWELVEDATA_KEY   = os.getenv("TWELVEDATA_KEY", "").strip()        

FA_BASE = "https://lab.flashalpha.com"
FH_BASE = "https://finnhub.io/api/v1"
NY = ZoneInfo("America/New_York")
PROXIES = {"NQ": "QQQ"}

_LIVE_PRICES = {"NQ": None}

async def fetch_futures_price(asset: str):
    return _LIVE_PRICES.get(asset)

async def twelvedata_websocket_listener():
    if not TWELVEDATA_KEY:
        cache["health"]["twelvedata"] = {"status": "offline", "reason": "Falta TWELVEDATA_KEY"}
        return

    uri = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVEDATA_KEY}"
    
    while True:
        try:
            # CORRECCIÓN: Se remueve el argumento inválido 'timeout'
            async with websockets.connect(uri) as websocket:
                subscribe_msg = {"action": "subscribe", "params": {"symbols": "QQQ"}}
                await websocket.send(json.dumps(subscribe_msg))
                cache["health"]["twelvedata"] = {"status": "online", "reason": "Conectado y escuchando ticks"}
                
                async for message in websocket:
                    data = json.loads(message)
                    if data.get("event") == "price":
                        price_val = data.get("price")
                        symbol_val = data.get("symbol")
                        if price_val is not None:
                            price = float(price_val)
                            if symbol_val == "QQQ":
                                _LIVE_PRICES["NQ"] = price * 41.2  
                            else:
                                _LIVE_PRICES["NQ"] = price
                            cache["health"]["twelvedata"] = {"status": "online", "reason": f"Último tick: {datetime.now().strftime('%H:%M:%S')}"}
                            
        except Exception as e:
            cache["health"]["twelvedata"] = {"status": "error", "reason": f"WebSocket: {type(e).__name__} - {str(e)}"}
            await asyncio.sleep(15)

app = FastAPI(title="Liberato Backend")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

cache = {
    "gex":           {},
    "calendar":      {"data": [], "last_update": None},
    "movers":        {"data": [], "last_update": None},
    "earnings":      {"data": [], "last_update": None},
    "heatmap":       {"data": {}, "last_update": None},
    "institutional": {"text": None, "last_update": None},
    "health": {
        "flashalpha":  {"status": "waiting", "reason": "Esperando ejecución"},
        "finnhub":     {"status": "waiting", "reason": "Esperando ejecución"},
        "yahoo":       {"status": "waiting", "reason": "Esperando ejecución"},
        "groq":        {"status": "waiting", "reason": "Esperando ejecución"},
        "twelvedata":  {"status": "waiting", "reason": "Esperando ejecución"}
    },
}

async def refresh_gex(asset="NQ"):
    if not FLASHALPHA_KEY:
        cache["health"]["flashalpha"] = {"status": "offline", "reason": "Falta FLASHALPHA_KEY"}
        return
    ticker = PROXIES.get(asset, "QQQ")
    headers = {"X-Api-Key": FLASHALPHA_KEY}
    
    try:
        async with httpx.AsyncClient(timeout=10, headers=headers) as client:
            r = await client.get(f"{FA_BASE}/v1/stock/{ticker}/summary")
            if r.status_code == 200:
                d = r.json()
                px = d.get("price", {}) or {}
                ex = d.get("exposure", {}) or {}
                cache["gex"][asset] = {
                    "underlying_price": px.get("mid") or px.get("last"),
                    "call_wall": ex.get("call_wall"), "put_wall": ex.get("put_wall"),
                    "gamma_flip": ex.get("gamma_flip"), "net_gex": ex.get("net_gex"),
                    "regime": ex.get("regime"), "ticker": ticker, "source": "summary", "_ts": time.time()
                }
                cache["health"]["flashalpha"] = {"status": "online", "reason": "Datos GEX descargados"}
            elif r.status_code == 429:
                cache["health"]["flashalpha"] = {"status": "error", "reason": "HTTP 429: Rate limit excedido temporalmente por FlashAlpha."}
            else:
                cache["health"]["flashalpha"] = {"status": "error", "reason": f"FlashAlpha HTTP {r.status_code}. Verifique validez de la Key."}
    except Exception as e:
        cache["health"]["flashalpha"] = {"status": "error", "reason": f"Fallo GEX: {str(e)}"}

async def refresh_calendar():
    if not FINNHUB_KEY:
        cache["health"]["finnhub"] = {"status": "offline", "reason": "Falta FINNHUB_KEY"}
        return
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{FH_BASE}/calendar/economic?token={FINNHUB_KEY}")
            if r.status_code == 200:
                cache["calendar"]["data"] = r.json().get("economicCalendar", [])
                cache["health"]["finnhub"] = {"status": "online", "reason": "Calendario sincronizado correctamente."}
            else:
                cache["health"]["finnhub"] = {"status": "error", "reason": f"Finnhub HTTP {r.status_code}. Key inválida o vencida."}
    except Exception as e:
        cache["health"]["finnhub"] = {"status": "error", "reason": f"Fallo Finnhub: {str(e)}"}

async def refresh_heatmap():
    try:
        url = "https://query1.finance.yahoo.com/v7/finance/quote?symbols=QQQ,AAPL,MSFT,NVDA"
        # CORRECCIÓN: Petición limpia sin arrastrar headers de otras APIs
        async with httpx.AsyncClient(timeout=10, headers={"User-Agent": "Mozilla/5.0"}) as client:
            r = await client.get(url)
            if r.status_code == 200:
                quotes = r.json().get("quoteResponse", {}).get("result", [])
                out = {}
                for q in quotes:
                    sym = q.get("symbol")
                    out[sym] = {"price": q.get("regularMarketPrice"), "chg_pct": q.get("regularMarketChangePercent")}
                cache["heatmap"]["data"] = out
                cache["health"]["yahoo"] = {"status": "online", "reason": "Heatmap sincronizado con Yahoo Finance."}
            else:
                cache["health"]["yahoo"] = {"status": "error", "reason": f"Yahoo Finance HTTP {r.status_code}"}
    except Exception as e:
        cache["health"]["yahoo"] = {"status": "error", "reason": f"Fallo Yahoo: {str(e)}"}

async def refresh_institutional_summary():
    if not GROQ_KEY:
        cache["health"]["groq"] = {"status": "offline", "reason": "Falta GROQ_KEY"}
        return
    gex_data = cache["gex"].get("NQ", {})
    if not gex_data:
        cache["health"]["groq"] = {"status": "error", "reason": "Esperando datos válidos de GEX para procesar el resumen."}
        return
    try:
        context_str = f"Net GEX: {gex_data.get('net_gex')}"
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROQ_KEY}", "Content-Type": "application/json"},
                json={
                    "model": "llama-3.3-70b-versatile", "max_tokens": 100,
                    "messages": [{"role": "user", "content": f"Resumen institucional: {context_str}"}]
                }
            )
            if r.status_code == 200:
                cache["institutional"]["text"] = r.json()["choices"][0]["message"]["content"].strip()
                cache["health"]["groq"] = {"status": "online", "reason": "Reporte de IA generado exitosamente."}
            else:
                cache["health"]["groq"] = {"status": "error", "reason": f"Groq HTTP {r.status_code}."}
    except Exception as e:
        cache["health"]["groq"] = {"status": "error", "reason": f"Fallo Groq: {str(e)}"}

@app.get("/")
def root(): return {"status": "ok"}

@app.get("/api/health")
def get_health_status():
    return {"timestamp": datetime.now(NY).isoformat(), "providers": cache["health"]}

@app.get("/api/dashboard")
async def get_dashboard():
    return {
        "macro_calendar": cache["calendar"]["data"],
        "heatmap": cache["heatmap"]["data"],
        "institutional_summary": cache["institutional"]["text"],
        "health_audit": cache["health"]
    }

scheduler = AsyncIOScheduler(timezone=NY)

@app.on_event("startup")
async def startup():
    scheduler.add_job(refresh_heatmap, IntervalTrigger(seconds=45))
    scheduler.start()
    
    asyncio.create_task(twelvedata_websocket_listener())
    
    await refresh_gex()
    await refresh_calendar()
    await refresh_heatmap()
    await refresh_institutional_summary()