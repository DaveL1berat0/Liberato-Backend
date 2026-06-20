"""Liberato Backend v2.3 — Consolidación Total en TwelveData Realtime.
Elimina dependencias conflictivas de APIs externas y unifica Heatmap + NQ.
"""
import os
import time
import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import websockets

# ── Credenciales ──
FLASHALPHA_KEY   = os.getenv("FLASHALPHA_KEY", "").strip()
GROQ_KEY         = os.getenv("GROQ_KEY", "").strip()              
TWELVEDATA_KEY   = os.getenv("TWELVEDATA_KEY", "").strip()        

FA_BASE = "https://lab.flashalpha.com"
NY = ZoneInfo("America/New_York")
PROXIES = {"NQ": "QQQ"}

_LIVE_PRICES = {"NQ": None}

async def fetch_futures_price(asset: str):
    return _LIVE_PRICES.get(asset)

# INSTANCIA FASTAPI (Faltaba en la versión anterior)
app = FastAPI(title="Liberato Backend")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Caché Estructurada Unificada ──
cache = {
    "gex":           {},
    "heatmap":       {"data": {}, "last_update": None},
    "institutional": {"text": None, "last_update": None},
    "health": {
        "flashalpha":  {"status": "waiting", "reason": "Esperando ejecución"},
        "yahoo":       {"status": "online", "reason": "Sustituido por TwelveData Realtime"},
        "groq":        {"status": "waiting", "reason": "Esperando datos de GEX"},
        "twelvedata":  {"status": "waiting", "reason": "Iniciando WebSocket"}
    },
}

# ═══════════════════════════════════════════════════════════
#  TWELVEDATA WEBSOCKET MÁSTER (NQ + HEATMAP EN VIVO)
# ═══════════════════════════════════════════════════════════
async def twelvedata_websocket_listener():
    if not TWELVEDATA_KEY:
        cache["health"]["twelvedata"] = {"status": "offline", "reason": "Falta TWELVEDATA_KEY"}
        return

    uri = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVEDATA_KEY}"
    symbols_to_subscribe = "QQQ,AAPL,MSFT,NVDA"
    
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                subscribe_msg = {
                    "action": "subscribe", 
                    "params": {"symbols": symbols_to_subscribe}
                }
                await websocket.send(json.dumps(subscribe_msg))
                cache["health"]["twelvedata"] = {"status": "online", "reason": "Conectado y escuchando NQ, AAPL, MSFT, NVDA"}
                
                async for message in websocket:
                    data = json.loads(message)
                    if data.get("event") == "price":
                        sym = data.get("symbol")
                        price_val = float(data.get("price"))
                        
                        # Guardar precio para NQ Ratio
                        if sym == "QQQ":
                            _LIVE_PRICES["NQ"] = price_val * 41.2
                            
                        # Mapeo directo al Heatmap en tiempo real
                        cache["heatmap"]["data"][sym] = {
                            "price": price_val,
                            "chg_pct": data.get("change_percent", 0.0)
                        }
                        cache["heatmap"]["last_update"] = time.time()
                        
        except Exception as e:
            cache["health"]["twelvedata"] = {"status": "error", "reason": f"WebSocket crash: {str(e)}"}
            await asyncio.sleep(15)

# ═══════════════════════════════════════════════════════════
#  PROVEEDORES ADICIONALES
# ═══════════════════════════════════════════════════════════
async def refresh_gex(asset="NQ"):
    if not FLASHALPHA_KEY:
        cache["health"]["flashalpha"] = {"status": "offline", "reason": "Falta FLASHALPHA_KEY"}
        return
    ticker = PROXIES.get(asset, "QQQ")
    try:
        async with httpx.AsyncClient(timeout=10, headers={"X-Api-Key": FLASHALPHA_KEY}) as client:
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
                cache["health"]["flashalpha"] = {"status": "error", "reason": "HTTP 429: Rate limit excedido por FlashAlpha."}
            else:
                cache["health"]["flashalpha"] = {"status": "error", "reason": f"FlashAlpha HTTP {r.status_code}"}
    except Exception as e:
        cache["health"]["flashalpha"] = {"status": "error", "reason": f"Fallo GEX: {str(e)}"}

async def refresh_institutional_summary():
    if not GROQ_KEY:
        cache["health"]["groq"] = {"status": "offline", "reason": "Falta GROQ_KEY"}
        return
    gex_data = cache["gex"].get("NQ", {})
    if not gex_data:
        cache["health"]["groq"] = {"status": "error", "reason": "Esperando datos válidos de GEX."}
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
                cache["health"]["groq"] = {"status": "online", "reason": "Reporte de IA generado."}
            else:
                cache["health"]["groq"] = {"status": "error", "reason": f"Groq HTTP {r.status_code}."}
    except Exception as e:
        cache["health"]["groq"] = {"status": "error", "reason": f"Fallo Groq: {str(e)}"}

# ── Endpoints ──
@app.get("/")
def root(): return {"status": "ok", "engine": "TwelveData Unified Realtime"}

@app.get("/api/health")
def get_health_status():
    return {"timestamp": datetime.now(NY).isoformat(), "providers": cache["health"]}

@app.get("/api/dashboard")
async def get_dashboard():
    return {
        "heatmap": cache["heatmap"]["data"],
        "institutional_summary": cache["institutional"]["text"],
        "health_audit": cache["health"]
    }

# ── Startup ──
scheduler = AsyncIOScheduler(timezone=NY)

@app.on_event("startup")
async def startup():
    scheduler.start()
    asyncio.create_task(twelvedata_websocket_listener())
    await refresh_gex()
    await refresh_institutional_summary()