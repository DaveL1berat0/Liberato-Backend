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
        raise HTTPException(status_code=r.status_code, detail=f"FlashAlpha {r.status_code}")

async def refresh_gex(asset="NQ"):
    try:
        data = await fetch_flashalpha(asset)
        data["_ts"] = time.time()
        cache["gex"][asset] = data
        cache["health"]["flashalpha"] = "online"
        print(f"[gex] {asset} ok: {data.get('source')}")
    except Exception as e:
        cache["health"]["flashalpha"] = "stale" if cache["gex"].get(asset) else "offline"
        print(f"[gex] error {asset}: {e}")

# ═══════════════════════════════════════════════════════════
#  SERVICIO 2: Finnhub — CALENDARIO MACRO (Fuente 1)
# ═══════════════════════════════════════════════════════════
# Eventos macro permitidos (filtro por keywords en el nombre del evento)
ALLOWED_EVENTS = [
    "CPI", "Core CPI", "PPI", "Core PPI", "PCE", "Core PCE",
    "FOMC", "Fed Interest Rate", "Interest Rate Decision", "Federal Funds",
    "Fed Minutes", "Powell", "Fed Chair",
    "Non Farm", "Nonfarm", "Unemployment Rate", "Average Hourly Earnings",
    "GDP", "Retail Sales", "ISM Manufacturing", "ISM Services",
    "ISM Non-Manufacturing", "JOLTS", "ADP",
    "Michigan", "Consumer Sentiment", "Consumer Expectations",
    "Current Conditions", "Inflation Expectations",
    "Initial Jobless", "Continuing Claims", "Jobless Claims",
    "Wholesale Inventories", "Durable Goods", "Housing Starts", "Building Permits",
]

def _event_allowed(name: str) -> bool:
    if not name:
        return False
    n = name.lower()
    return any(kw.lower() in n for kw in ALLOWED_EVENTS)

def _impact_label(impact) -> str:
    # Finnhub usa 1/2/3 o low/medium/high
    s = str(impact).lower()
    if s in ("3", "high"):   return "high"
    if s in ("2", "medium"): return "medium"
    if s in ("1", "low"):    return "low"
    return "medium"

async def fetch_calendar():
    """Calendario económico US, filtrado High+Medium + eventos permitidos."""
    if not FINNHUB_KEY:
        raise RuntimeError("FINNHUB_KEY no configurada")
    today = datetime.now(NY)
    frm = today.strftime("%Y-%m-%d")
    to  = (today + timedelta(days=14)).strftime("%Y-%m-%d")
    async with httpx.AsyncClient(timeout=3) as client:  # timeout 3s (regla)
        r = await client.get(f"{FH_BASE}/calendar/economic",
                             params={"from": frm, "to": to, "token": FINNHUB_KEY})
        if r.status_code != 200:
            raise RuntimeError(f"Finnhub calendar {r.status_code}")
        payload = r.json()
    rows = payload.get("economicCalendar", []) or payload.get("data", []) or []
    out = []
    for ev in rows:
        country = ev.get("country", "")
        if country not in ("US", "United States"):
            continue
        name = ev.get("event", "") or ev.get("name", "")
        if not _event_allowed(name):
            continue
        impact = _impact_label(ev.get("impact"))
        if impact == "low":
            continue
        # Estado: si tiene 'actual' ya salió
        actual = ev.get("actual")
        released = actual is not None and actual != ""
        out.append({
            "title": name,
            "source": "Finnhub",
            "time": ev.get("time", ""),
            "impact": impact,
            "actual": actual,
            "forecast": ev.get("estimate"),
            "previous": ev.get("prev"),
            "status": "Released" if released else "Upcoming",
            "type": "macro",
        })
    # Ordenar por hora ascendente (calendario sí es cronológico)
    out.sort(key=lambda e: e.get("time") or "")
    return out

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
        "calendar_last_update": cache["calendar"]["last_update"],
        "calendar_count": len(cache["calendar"]["data"]),
    }

@app.get("/api/market/gamma-levels/{asset}")
async def gamma_levels(asset: str):
    asset = asset.upper()
    if asset not in PROXIES:
        raise HTTPException(400, "Activo no soportado")
    if asset not in cache["gex"] or time.time() - cache["gex"][asset].get("_ts", 0) > 6 * 3600:
        await refresh_gex(asset)
    if asset not in cache["gex"]:
        raise HTTPException(502, "Sin datos de FlashAlpha")
    return {**cache["gex"][asset], "asset": asset, "credits_used": 0}

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

# ═══════════════════════════════════════════════════════════
#  SCHEDULER
# ═══════════════════════════════════════════════════════════
scheduler = AsyncIOScheduler(timezone=NY)

@app.on_event("startup")
async def startup():
    # GEX: 9:00, 9:30, 9:45 ET lun-vie
    for h, m in [(9, 0), (9, 30), (9, 45)]:
        scheduler.add_job(refresh_gex, CronTrigger(hour=h, minute=m, day_of_week="mon-fri"), args=["NQ"])
    # Calendario: cada 5 min (regla)
    scheduler.add_job(refresh_calendar, IntervalTrigger(minutes=5))
    scheduler.start()
    # Primera carga al arrancar (no bloquea si una falla)
    await asyncio.gather(refresh_gex("NQ"), refresh_calendar(), return_exceptions=True)
