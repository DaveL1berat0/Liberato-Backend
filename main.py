"""Liberato Backend — proxy FlashAlpha GEX con caché. Deploy en Railway/Render."""
import os, time
from datetime import datetime
from zoneinfo import ZoneInfo
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

FLASHALPHA_KEY = os.getenv("FLASHALPHA_KEY", "HBBqRgMkQWk0rSZeIy9sHAfCNsfcTaFabOVUpqQ0")
BASE = "https://lab.flashalpha.com"
NY = ZoneInfo("America/New_York")
PROXIES = {"NQ": "QQQ", "ES": "SPY", "GC": "GLD"}

app = FastAPI(title="Liberato Backend")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

cache: dict = {}  # { "NQ": {data..., "_ts": epoch} }

async def fetch_flashalpha(asset: str):
    """Llama FlashAlpha server-side (sin CORS) y normaliza."""
    ticker = PROXIES.get(asset, "QQQ")
    headers = {"X-Api-Key": FLASHALPHA_KEY}
    async with httpx.AsyncClient(timeout=15, headers=headers) as client:
        # 1° summary (dual-mode, trae todo)
        r = await client.get(f"{BASE}/v1/stock/{ticker}/summary")
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
        # 2° gex con expiración (plan Free)
        today = datetime.now(NY).strftime("%Y-%m-%d")
        r = await client.get(f"{BASE}/v1/exposure/gex/{ticker}", params={"expiration": today})
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
        raise HTTPException(status_code=r.status_code, detail=f"FlashAlpha {r.status_code}")

async def refresh(asset="NQ"):
    try:
        data = await fetch_flashalpha(asset)
        data["_ts"] = time.time()
        cache[asset] = data
        print(f"[cron] {asset} actualizado: {data.get('source')}")
    except Exception as e:
        print(f"[cron] error {asset}: {e}")

@app.get("/")
def root():
    return {"status": "ok", "service": "Liberato Backend"}

@app.get("/api/market/gamma-levels/{asset}")
async def gamma_levels(asset: str):
    asset = asset.upper()
    if asset not in PROXIES:
        raise HTTPException(400, "Activo no soportado")
    # Si no hay caché o tiene +6h, refrescar al vuelo
    if asset not in cache or time.time() - cache[asset].get("_ts", 0) > 6 * 3600:
        await refresh(asset)
    if asset not in cache:
        raise HTTPException(502, "Sin datos de FlashAlpha")
    return {**cache[asset], "asset": asset, "credits_used": 0}

scheduler = AsyncIOScheduler(timezone=NY)

@app.on_event("startup")
async def startup():
    for h, m in [(9, 0), (9, 30), (9, 45)]:
        scheduler.add_job(refresh, CronTrigger(hour=h, minute=m, day_of_week="mon-fri"), args=["NQ"])
    scheduler.start()
    await refresh("NQ")  # primera carga al arrancar
