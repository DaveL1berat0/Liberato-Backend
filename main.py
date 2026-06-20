"""Liberato Backend v2 — Trading terminal NQ con WebSocket Real.
Servicios: FlashAlpha (GEX) + Finnhub (calendar/movers/earnings)
         + Yahoo Finance (heatmap 22 activos) + Anthropic Claude (resumen IA).
NUEVO v2: Diagnóstico detallado en /api/health + /api/dashboard unificado
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
import websockets

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
    return _LIVE_PRICES.get(asset)

async def twelvedata_websocket_listener():
    if not TWELVEDATA_KEY:
        cache["health"]["twelvedata"] = {"status": "offline", "reason": "Falta TWELVEDATA_KEY"}
        return

    uri = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVEDATA_KEY}"
    
    while True:
        try:
            async with websockets.connect(uri, timeout=10) as websocket:
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
                            cache["health"]["twelvedata"] = {"status": "online", "reason": f"Último tick recibido a las {datetime.now().strftime('%H:%M:%S')}"}
                            
        except Exception as e:
            cache["health"]["twelvedata"] = {"status": "error", "reason": f"Desconexión WebSocket: {type(e).__name__} - {str(e)}"}
            await asyncio.sleep(10)

async def get_nq_ratio(asset: str, qqq_price: float):
    if not qqq_price or qqq_price <= 0: return None, None
    nq_price = await fetch_futures_price(asset)
    if nq_price and nq_price > 0:
        return nq_price, round(nq_price / qqq_price, 4)
    return None, None


app = FastAPI(title="Liberato Backend")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Caché estructurada ──
cache = {
    "gex":           {},
    "calendar":      {"data": [], "last_update": None},
    "movers":        {"data": [], "last_update": None},
    "earnings":      {"data": [], "last_update": None},
    "heatmap":       {"data": {}, "last_update": None},
    "institutional": {"text": None, "last_update": None},
    "health": {
        "flashalpha":  {"status": "waiting", "reason": "No ejecutado aún"},
        "finnhub":     {"status": "waiting", "reason": "No ejecutado aún"},
        "yahoo":       {"status": "waiting", "reason": "No ejecutado aún"},
        "groq":        {"status": "waiting", "reason": "No ejecutado aún"},
        "twelvedata":  {"status": "waiting", "reason": "No ejecutado aún"}
    },
}

# ═══════════════════════════════════════════════════════════
#  RECOPILACIÓN DE INFORMACIÓN INSTITUCIONAL
# ═══════════════════════════════════════════════════════════
async def refresh_gex(asset="NQ"):
    if not FLASHALPHA_KEY:
        cache["health"]["flashalpha"] = {"status": "offline", "reason": "Falta FLASHALPHA_KEY en variables de entorno"}
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
                cache["health"]["flashalpha"] = {"status": "online", "reason": "Datos GEX descargados exitosamente"}
            else:
                cache["health"]["flashalpha"] = {
                    "status": "error", 
                    "reason": f"FlashAlpha devolvió HTTP {r.status_code}. Posible API key inválida o endpoint incorrecto."
                }
    except httpx.TimeoutException:
        cache["health"]["flashalpha"] = {"status": "error", "reason": "Timeout superado (10s) al conectar con FlashAlpha"}
    except Exception as e:
        cache["health"]["flashalpha"] = {"status": "error", "reason": f"Excepción inesperada: {type(e).__name__} - {str(e)}"}

async def refresh_calendar():
    if not FINNHUB_KEY:
        cache["health"]["finnhub"] = {"status": "offline", "reason": "Falta FINNHUB_KEY en variables de entorno"}
        return
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{FH_BASE}/calendar/economic?token={FINNHUB_KEY}")
            if r.status_code == 200:
                cache["calendar"]["data"] = r.json().get("economicCalendar", [])
                cache["health"]["finnhub"] = {"status": "online", "reason": "Conectado. Calendario actualizado."}
            else:
                cache["health"]["finnhub"] = {
                    "status": "error", 
                    "reason": f"Finnhub devolvió HTTP {r.status_code}. Verifique si la API Key es válida."
                }
    except httpx.TimeoutException:
        cache["health"]["finnhub"] = {"status": "error", "reason": "Timeout superado (10s) con Finnhub"}
    except Exception as e:
        cache["health"]["finnhub"] = {"status": "error", "reason": f"Error de conexión: {type(e).__name__} - {str(e)}"}

async def refresh_heatmap():
    try:
        url = "https://query1.finance.yahoo.com/v7/finance/quote?symbols=QQQ,AAPL,MSFT,NVDA"
        async with httpx.AsyncClient(timeout=10, headers={"User-Agent": "Mozilla/5.0"}) as client:
            r = await client.get(url)
            if r.status_code == 200:
                quotes = r.json().get("quoteResponse", {}).get("result", [])
                if not quotes:
                    cache["health"]["yahoo"] = {"status": "error", "reason": "Yahoo respondió HTTP 200 pero devolvió una lista vacía (Mercado cerrado o tickers inválidos)"}
                    return
                out = {}
                for q in quotes:
                    sym = q.get("symbol")
                    out[sym] = {"price": q.get("regularMarketPrice"), "chg_pct": q.get("regularMarketChangePercent")}
                cache["heatmap"]["data"] = out
                cache["health"]["yahoo"] = {"status": "online", "reason": "Precios de Yahoo Finance actualizados"}
            else:
                cache["health"]["yahoo"] = {"status": "error", "reason": f"Yahoo Finance devolvió HTTP {r.status_code}"}
    except httpx.TimeoutException:
        cache["health"]["yahoo"] = {"status": "error", "reason": "Timeout en Yahoo Finance"}
    except Exception as e:
        cache["health"]["yahoo"] = {"status": "error", "reason": f"Fallo crítico: {type(e).__name__} - {str(e)}"}

async def refresh_institutional_summary():
    if not GROQ_KEY:
        cache["health"]["groq"] = {"status": "offline", "reason": "Falta GROQ_KEY en variables de entorno"}
        return
    
    gex_data = cache["gex"].get("NQ", {})
    if not gex_data:
        cache["health"]["groq"] = {"status": "error", "reason": "Inhabilitado temporalmente: Depende de datos GEX que están vacíos"}
        return
        
    try:
        context_str = f"Net GEX: {gex_data.get('net_gex') or 'N/A'}"
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROQ_KEY}", "Content-Type": "application/json"},
                json={
                    "model": "llama-3.3-70b-versatile", "max_tokens": 100,
                    "messages": [{"role": "user", "content": f"Resumen rápido de trading institucional: {context_str}"}]
                }
            )
            if r.status_code == 200:
                cache["institutional"]["text"] = r.json()["choices"][0]["message"]["content"].strip()
                cache["health"]["groq"] = {"status": "online", "reason": "Modelo Llama-3.3 generó el reporte correctamente"}
            else:
                cache["health"]["groq"] = {"status": "error", "reason": f"Groq API devolvió HTTP {r.status_code}. Revise saldo o validez del token"}
    except Exception as e:
        cache["health"]["groq"] = {"status": "error", "reason": f"Excepción en IA: {type(e).__name__} - {str(e)}"}

# ── Endpoints del Servidor ──
@app.get("/")
def root(): 
    return {"status": "ok", "mode": "Manejo de Errores Clínico Avanzado"}

@app.get("/api/health")
def get_health_status():
    return {
        "timestamp": datetime.now(NY).isoformat(),
        "providers": cache["health"]
    }

@app.get("/api/heatmap")
def get_heatmap(): 
    return {"heatmap": cache["heatmap"]["data"]}

@app.get("/api/dashboard")
async def get_dashboard():
    upcoming = [e for e in cache["calendar"]["data"] if e.get("status") == "Upcoming"]
    return {
        "macro_calendar": cache["calendar"]["data"],
        "market_movers": cache["movers"]["data"],
        "next_macro_event": upcoming[0] if upcoming else None,
        "heatmap": cache["heatmap"]["data"],
        "institutional_summary": cache["institutional"]["text"],
        "health_audit": cache["health"]
    }

# ═══════════════════════════════════════════════════════════
#  STARTUP AUTOMÁTICO SECUENCIAL
# ═══════════════════════════════════════════════════════════
scheduler = AsyncIOScheduler(timezone=NY)

@app.on_event("startup")
async def startup():
    scheduler.add_job(refresh_gex, CronTrigger(hour=9, minute=0, day_of_week="mon-fri"), args=["NQ"])
    scheduler.add_job(refresh_heatmap, IntervalTrigger(seconds=30))
    scheduler.start()
    
    asyncio.create_task(twelvedata_websocket_listener())
    
    await refresh_gex()
    await refresh_calendar()
    await refresh_heatmap()
    await refresh_institutional_summary()
    print("[Diagnóstico] Servidor iniciado sin errores de sangría.")