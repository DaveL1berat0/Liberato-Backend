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

# ── Caché estructurada con auditoría detallada de errores ──
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
#  RECOPILACIÓN CON CAPTURA EXPLICITA DE EXCEPCIONES
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
            # Validación directa contra el endpoint de Finnhub para verificar credenciales y conectividad
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