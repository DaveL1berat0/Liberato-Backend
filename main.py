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

import os, time, asyncio, json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import websockets

# ══ CREDENCIALES (solo Railway Variables, nunca en código) ════════════════════
FLASHALPHA_KEY   = os.getenv("FLASHALPHA_KEY",   "").strip()
FINNHUB_KEY      = os.getenv("FINNHUB_KEY",      "").strip()
GROQ_KEY         = os.getenv("GROQ_KEY",         "").strip()
TWELVEDATA_KEY   = os.getenv("TWELVEDATA_KEY",   "").strip()
ALPHA_VANTAGE_KEY= os.getenv("ALPHAVANTAGE_KEY", "").strip()
FINNHUB_WH_SECRET = os.getenv("FINNHUB_WEBHOOK_SECRET", "").strip()  # opcional: verifica autenticidad

FA_BASE = "https://lab.flashalpha.com"
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
WS_SYMBOLS = ["QQQ","AAPL","MSFT","NVDA","META","AMZN","TSLA","GOOGL"]
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
}

async def refresh_heatmap_rest():
    """Batch REST para los 13 símbolos macro (no en WebSocket).
    Una sola llamada = 13 créditos. Cada 15 min = ~350 créditos/día."""
    if not TWELVEDATA_KEY:
        return
    symbols = ",".join(REST_SYMBOLS.values())
    url = f"https://api.twelvedata.com/price?symbol={symbols}&apikey={TWELVEDATA_KEY}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url)
        if r.status_code != 200:
            print(f"[heatmap-rest] error {r.status_code} — trying Yahoo fallback")
            await _heatmap_yahoo_fallback()
            return
        data = r.json()
        sym_to_hmap = {v:k for k,v in REST_SYMBOLS.items()}
        loaded = 0
        for td_sym, result in data.items():
            if not isinstance(result, dict) or not result.get("price"):
                continue
            hmap_sym = sym_to_hmap.get(td_sym, td_sym)
            price    = float(result["price"])
            prev     = cache["heatmap"]["data"].get(hmap_sym, {}).get("price")
            chg_pct  = ((price-prev)/prev*100) if prev else 0
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
_gex_blocked_until = 0   # timestamp: si hay 429, esperar 24h

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
    ticker = "QQQ"
    try:
        async with httpx.AsyncClient(timeout=12,
                                      headers={"X-Api-Key": FLASHALPHA_KEY}) as client:
            r = await client.get(f"{FA_BASE}/v1/stock/{ticker}/summary")
        if r.status_code == 200:
            d = r.json()
            ex = d.get("exposure", {}) or {}
            px = d.get("price",    {}) or {}
            cache["gex"][asset] = {
                "underlying_price": px.get("mid") or px.get("last"),
                "call_wall":  ex.get("call_wall"),
                "put_wall":   ex.get("put_wall"),
                "gamma_flip": ex.get("gamma_flip"),
                "net_gex":    ex.get("net_gex"),
                "regime":     ex.get("regime"),
                "ticker":     ticker,
                "_ts":        time.time(),
            }
            cache["health"]["flashalpha"] = "online"
            save_cache()
            print(f"[gex] ok: flip={cache['gex'][asset].get('gamma_flip')}")
        elif r.status_code == 429:
            _gex_blocked_until = time.time() + 86400   # esperar 24h
            cache["health"]["flashalpha"] = "rate-limited-24h"
            print("[gex] 429 — bloqueado 24h para conservar créditos")
        else:
            cache["health"]["flashalpha"] = f"error-{r.status_code}"
            print(f"[gex] error {r.status_code}")
    except Exception as e:
        cache["health"]["flashalpha"] = "error"
        print(f"[gex] excepción: {e}")

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

async def refresh_calendar():
    """Calendar with parallel fetch, Finnhub fallback, stale cache preservation."""
    FF_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json,*/*",
        "Cache-Control": "no-cache",
    }
    FF_URLS = [
        "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
        "https://nfs.faireconomy.media/ff_calendar_nextweek.json",
        "https://cdn-nfs.faireconomy.media/ff_calendar_thisweek.json",
        "https://cdn-nfs.faireconomy.media/ff_calendar_nextweek.json",
    ]

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
            r = await client.get(url, timeout=8)
            if r.status_code != 200: return []
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

    stale_backup = list(cache["calendar"]["data"])  # preserve last known good

    async with httpx.AsyncClient(headers=FF_HEADERS, follow_redirects=True) as client:
        # ── Fetch AMBAS fuentes en paralelo (ForexFactory + Finnhub) ──────────
        # Merge para capturar resultados "actual" de cualquier fuente que los tenga.
        # Esto resuelve el caso Building Permits: si FF no tiene el actual,
        # Finnhub lo provee, y viceversa.
        ff_tasks = [_fetch_ff(client, url) for url in FF_URLS]
        all_tasks = ff_tasks + [_fetch_finnhub_fallback(client)]
        results = await asyncio.gather(*all_tasks, return_exceptions=True)

        ff_events, fh_events = [], []
        for i, res in enumerate(results):
            if isinstance(res, list):
                if i < len(ff_tasks): ff_events.extend([e for e in res if e])
                else: fh_events.extend([e for e in res if e])

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
        # Luego Finnhub: si tiene "actual" y el de FF no, usar el de Finnhub
        for e in fh_events:
            k = norm_key(e)
            if k in merged:
                existing = merged[k]
                # Si Finnhub tiene resultado y el existente no → actualizar resultado
                if e.get("actual") and not existing.get("actual"):
                    existing["actual"] = e["actual"]
                    existing["status"] = "Released"
                # Completar forecast/previous si faltan
                if e.get("forecast") and not existing.get("forecast"):
                    existing["forecast"] = e["forecast"]
                if e.get("previous") and not existing.get("previous"):
                    existing["previous"] = e["previous"]
            else:
                merged[k] = e

        out = list(merged.values())

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
    def parse_num(v):
        if v is None: return None
        try:
            return float(str(v).replace("%","").replace("K","").replace("M","").replace(",","").replace("$","").strip())
        except (ValueError, AttributeError):
            return None

    for e in out:
        actual = parse_num(e.get("actual"))
        forecast = parse_num(e.get("forecast"))
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
    # Tech/Market leaders (sector impact only unless systemic)
    "nvidia":          (7.5,"Technology","Corporate","bullish"),
    "nvda":            (7.5,"Technology","Corporate","bullish"),
    "apple":           (7.5,"Technology","Corporate","bullish"),
    "openai":          (7.8,"AI Sector","Corporate","bullish"),
    "elon musk":       (7.5,"Tech/Market","Corporate","bearish"),
    "tesla":           (7.0,"Auto/Tech","Corporate","bullish"),
    "microsoft":       (7.2,"Technology","Corporate","bullish"),
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

    # Minimum threshold — only market-moving events
    if best_score < 7.5: return None

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
async def refresh_institutional():
    """Motor de IA institucional — genera análisis desde CUALQUIER dato disponible.
    Funciona 24/7: con o sin GEX, mercado abierto o cerrado, fin de semana.
    Construye contexto rico desde gamma, precio, correlaciones, calendario y earnings."""
    if not GROQ_KEY:
        cache["health"]["groq"] = "offline-no-key"; return

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

    ctx_str = "\n".join(ctx)

    # ── Prompt adaptado a si hay gamma o no ───────────────────────────────────
    sys_msg = ("Eres el analista institucional jefe de Liberato Community, especializado en NQ Futures "
               "y order flow. Respondes SOLO en español, con tono profesional e institucional. "
               "SIEMPRE exactamente 2-3 oraciones. Nunca listas ni bullets. "
               "Explicas QUÉ está pasando, POR QUÉ, y la IMPLICACIÓN para el trader de NQ. "
               "Eres preciso y honesto: si un dato no está disponible, no lo inventas.")

    if has_gamma:
        usr_msg = (f"Analiza el contexto institucional actual y genera un briefing (2-3 oraciones):\n\n{ctx_str}\n\n"
                   "Incluye los niveles exactos de gamma y explica el sesgo. Menciona catalizadores si son relevantes.")
    else:
        usr_msg = (f"Genera un briefing institucional de contexto de mercado (2-3 oraciones):\n\n{ctx_str}\n\n"
                   "Como aún no hay datos de gamma exposure, enfoca el análisis en el precio del NQ, las "
                   "correlaciones macro, el comportamiento de los líderes tecnológicos y los próximos catalizadores. "
                   "Explica el contexto y qué vigilar. No inventes niveles de gamma que no tienes.")

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization":f"Bearer {GROQ_KEY}","Content-Type":"application/json"},
                json={"model":"llama-3.3-70b-versatile","max_tokens":300,"temperature":0.4,
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
    return {"status":"ok","version":"3.0","engine":"TwelveData Realtime + Finnhub + FlashAlpha"}

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

@app.get("/api/market/gamma-levels/NQ")
async def gamma_levels():
    """GEX desde cache. FlashAlpha SOLO se llama en horarios programados.
    Expone el timestamp de la última llamada exitosa para que el frontend
    muestre 'Actualizado HH:MM AM · DD mes' y el usuario verifique frescura."""
    gex = cache["gex"].get("NQ")
    if not gex:
        return {"status": "no-data", "message": "GEX no disponible aún — próxima carga programada",
                "last_call_ts": None}
    qqq = gex.get("underlying_price")
    nq  = round(qqq*(cache["nq_ratio"].get("value") or 41.51),2) if qqq else None
    # Timestamp de la última llamada exitosa a FlashAlpha (unix → ISO en ET)
    ts = gex.get("_ts")
    last_call_iso = None
    last_call_is_today = False
    if ts:
        dt_et = datetime.fromtimestamp(ts, NY)
        last_call_iso = dt_et.isoformat()
        last_call_is_today = (dt_et.date() == datetime.now(NY).date())
    return {**gex, "asset":"NQ", "nq_price":nq,
            "ratio":cache["nq_ratio"].get("value") or 41.51, "credits_used":0,
            "last_call_ts": last_call_iso,
            "last_call_is_today": last_call_is_today}

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
    """Confirma qué versión del backend está desplegada."""
    return {
        "version": "v2026.06.24-FIX7",
        "ws_symbols": WS_SYMBOLS,
        "has_nq1": "NQ1!" in WS_SYMBOLS,
        "has_dynamic_ratio": True,
        "nq_ratio_current": cache["nq_ratio"].get("value"),
        "gex_schedule": GEX_SCHEDULE,
        "gex_calls_today": _gex_daily_count,
        "calendar_status": cache["calendar"].get("status"),
        "movers_status": cache["movers"].get("status"),
        "build": "complete-audit-fix",
    }

@app.get("/api/calendar")
async def get_calendar():
    """Devuelve caché INMEDIATAMENTE. Refresco en segundo plano (no bloquea).
    Incluye el precio NQ actual para que el frontend calcule el impacto inmediato."""
    last = cache["calendar"]["last_update"]
    is_stale = not last or (datetime.now(NY) - datetime.fromisoformat(last)).total_seconds() > 120
    if is_stale:
        asyncio.create_task(refresh_calendar())
    upcoming = [e for e in cache["calendar"]["data"] if e.get("status")=="Upcoming"]
    # Precio NQ actual — para cálculo de reacción del mercado post-publicación
    nq_now = (cache["heatmap"]["data"].get("NQ", {}) or {}).get("price")
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
    upcoming = [e for e in cache["calendar"]["data"] if e.get("status")=="Upcoming"]
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
    scheduler.add_job(refresh_heatmap_rest,
                      CronTrigger(day_of_week="mon-fri", hour="4-20", minute="*/15"))

    # ── FlashAlpha GEX: SOLO 9am + 7pm ET (2 créditos de 5/día) ──────────
    # FlashAlpha GEX: 5 horarios exactos — máx 5 créditos/día
    scheduler.add_job(refresh_gex, CronTrigger(hour=19, minute=0,  day_of_week="mon-fri"))
    scheduler.add_job(refresh_gex, CronTrigger(hour=9,  minute=0,  day_of_week="mon-fri"))
    scheduler.add_job(refresh_gex, CronTrigger(hour=9,  minute=15, day_of_week="mon-fri"))
    scheduler.add_job(refresh_gex, CronTrigger(hour=9,  minute=30, day_of_week="mon-fri"))
    scheduler.add_job(refresh_gex, CronTrigger(hour=9,  minute=45, day_of_week="mon-fri"))

    # ── Finnhub Calendar: cada 5 minutos ──────────────────────────────────
    scheduler.add_job(refresh_calendar, IntervalTrigger(minutes=2))

    # ── Finnhub Movers: cada 60 segundos ──────────────────────────────────
    scheduler.add_job(refresh_movers, IntervalTrigger(seconds=45))

    # ── Finnhub Earnings: cada 6 horas ────────────────────────────────────
    scheduler.add_job(refresh_earnings, IntervalTrigger(hours=6))

    # ── Groq Institutional: 9:05 AM + 12:00 PM ET lun-vie ─────────────────
    # Resumen IA: cada 30min durante horario extendido (premarket→afterhours)
    scheduler.add_job(refresh_institutional, IntervalTrigger(minutes=30))

    scheduler.start()

    # ── Carga inicial: todo excepto FlashAlpha (ahorra créditos) ──────────
    print("="*60)
    print("🟢 LIBERATO BACKEND v2026.06.24-FIX7 — BUILD CORRECTO")
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
