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
                    # Precio NQ en vivo (QQQ × ratio)
                    if sym == "QQQ":
                        cache["heatmap"]["data"]["NQ"] = {
                            "symbol":"NQ","price":round(price*41.2,2),
                            "chg_pct":round(chg_pct,3),
                            "direction":"up" if chg_pct>0.05 else("down" if chg_pct<-0.05 else"flat"),
                        }
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
                    "symbol":"NQ","price":round(price*41.2,2),
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
    out = []
    urls = [
        "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
        "https://nfs.faireconomy.media/ff_calendar_nextweek.json",
        "https://cdn-nfs.faireconomy.media/ff_calendar_thisweek.json",
        "https://cdn-nfs.faireconomy.media/ff_calendar_nextweek.json",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
    }
    async with httpx.AsyncClient(timeout=12, headers=headers, follow_redirects=True) as client:
        for url in urls:
            try:
                r = await client.get(url)
                if r.status_code != 200: continue
                for ev in r.json():
                    if str(ev.get("country","")).upper() not in ("USD","US"): continue
                    name = ev.get("title") or ev.get("event","")
                    if not _allowed(name): continue
                    ff_imp = str(ev.get("impact","")).lower()
                    impact = _impact(name, ff_imp)
                    if impact == "low": continue
                    actual   = ev.get("actual","")
                    released = bool(actual and str(actual).strip())
                    out.append({
                        "title":    name,
                        "time":     ev.get("date",""),
                        "impact":   impact,
                        "actual":   actual or None,
                        "forecast": ev.get("forecast") or None,
                        "previous": ev.get("previous") or None,
                        "status":   "Released" if released else "Upcoming",
                        "type":     "holiday" if _holiday(name) else "macro",
                    })
            except Exception as e:
                print(f"[calendar] {url}: {e}")
    seen, deduped = set(), []
    for e in out:
        k = (e["title"].lower().strip(), e["time"][:16])
        if k in seen: continue
        seen.add(k); deduped.append(e)
    deduped.sort(key=lambda e: e.get("time",""))
    cache["calendar"]["data"]        = deduped
    cache["calendar"]["last_update"] = datetime.now(NY).isoformat()
    cache["calendar"]["status"]      = "fresh" if deduped else "empty"
    print(f"[calendar] ok: {len(deduped)} eventos")

MOVER_KW = {
    "nvidia":95,"nvda":95,"apple":94,"aapl":94,"tesla":92,"tsla":92,
    "microsoft":90,"msft":90,"amazon":88,"amzn":88,"meta":85,
    "broadcom":80,"avgo":80,"amd":75,"alphabet":88,"googl":88,"google":88,
    "intel":60,"qualcomm":72,"qcom":72,"openai":90,
    "federal reserve":98,"fed ":98,"fomc":100,"powell":98,
    "trump":97,"tariff":97,"tariffs":97,"china":85,"trade war":90,
    "inflation":88,"cpi":100,"ppi":100,"interest rate":90,
    "rate cut":92,"rate hike":92,"recession":88,"nasdaq":85,"s&p 500":85,
    "semiconductor":80,"artificial intelligence":78," ai ":75,
}
MOVER_BLOCK = [
    "penny stock","otc","small cap","memecoin","dogecoin","nft","shiba",
    "sports","entertainment","celebrity","gossip","lottery","casino",
    "coupon","discount","giveaway","sponsored",
]

def _score(title):
    if not title: return 0, None
    t = " " + title.lower() + " "
    for bad in MOVER_BLOCK:
        if bad in t: return 0, None
    best_sc, best_sym = 0, None
    for kw, sc in MOVER_KW.items():
        if kw in t and sc > best_sc:
            best_sc = sc; best_sym = kw.strip().upper()[:8]
    return best_sc, best_sym

async def refresh_movers():
    if not FINNHUB_KEY: return
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(f"{FH_BASE}/news",
                                  params={"category":"general","token":FINNHUB_KEY})
        if r.status_code != 200: return
        scored, seen = [], set()
        for n in r.json():
            title = n.get("headline","")
            key   = title.lower().strip()[:60]
            if key in seen: continue
            sc, sym = _score(title)
            if sc < 60: continue
            seen.add(key)
            scored.append({
                "title":  title,
                "source": n.get("source",""),
                "ts":     n.get("datetime",0),
                "url":    n.get("url",""),
                "impact": "ultra" if sc>=95 else ("high" if sc>=85 else "medium"),
                "score":  sc,
                "symbol": sym,
                "type":   "mover",
            })
        scored.sort(key=lambda x: x["score"], reverse=True)
        out, used = [], set()
        for m in scored:
            s = m["symbol"]
            if s in used: continue
            used.add(s); out.append(m)
            if len(out) >= 5: break
        cache["movers"]["data"]        = out
        cache["movers"]["last_update"] = datetime.now(NY).isoformat()
        cache["movers"]["status"]      = "fresh"
        cache["health"]["finnhub"]     = "online"
        print(f"[movers] ok: {len(out)}")
    except Exception as e:
        print(f"[movers] error: {e}")

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
    if not GROQ_KEY:
        cache["health"]["groq"] = "offline-no-key"; return
    gex = cache["gex"].get("NQ",{})
    if not gex or not gex.get("gamma_flip"):
        cache["health"]["groq"] = "waiting-for-gex"
        print("[institutional] esperando datos GEX reales"); return
    hm  = cache["heatmap"]["data"]
    cal = cache["calendar"]["data"]
    ern = cache["earnings"]["data"]
    cw  = gex.get("call_wall"); pw = gex.get("put_wall")
    gf  = gex.get("gamma_flip"); ng = gex.get("net_gex")
    rg  = gex.get("regime","desconocido")
    qqq = gex.get("underlying_price")
    nq  = round(qqq*41.2,0) if qqq else None
    ctx = []
    if cw and pw and gf:
        pdir = "sobre" if (nq and nq>gf) else "bajo"
        ctx.append(f"- Gamma: Call Wall {cw:.0f} | Put Wall {pw:.0f} | Flip {gf:.1f} | NQ ~{nq:.0f} ({pdir} del flip)")
        if ng: ctx.append(f"- Régimen: {rg} | Net GEX: {ng:,.0f}")
    for k,lbl in [("VIXY","VIX"),("UUP","DXY"),("IEF","US10Y"),("NVDA","NVDA")]:
        d = hm.get(k,{})
        if d.get("chg_pct"):
            ctx.append(f"- {lbl}: {d['chg_pct']:+.1f}%")
    upcoming = [e for e in cal if e.get("status")=="Upcoming"]
    if upcoming: ctx.append(f"- Próximo macro: {upcoming[0].get('title','')}")
    today_str = datetime.now(NY).strftime("%Y-%m-%d")
    earn_today= [e["symbol"] for e in ern if e.get("date")==today_str
                 and e.get("impact") in ("extreme","high")]
    if earn_today: ctx.append(f"- Earnings hoy: {', '.join(earn_today)}")
    ctx_str = "\n".join(ctx) if ctx else "Datos de mercado en espera."
    sys_msg = ("Eres el analista institucional de Liberato Community para NQ Futures. "
               "Respondes SOLO en español. SIEMPRE exactamente 2-3 oraciones concisas. "
               "Nunca listas, nunca bullets, nunca más de 3 oraciones.")
    usr_msg = (f"Genera un briefing institucional profesional en español (2-3 oraciones):\n\n{ctx_str}\n\n"
               "Incluye los niveles exactos de gamma. Explica el sesgo para el trader de NQ. "
               "Si hay catalizadores importantes, menciónalos al final.")
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization":f"Bearer {GROQ_KEY}","Content-Type":"application/json"},
                json={"model":"llama-3.3-70b-versatile","max_tokens":300,"temperature":0.35,
                      "messages":[{"role":"system","content":sys_msg},
                                  {"role":"user","content":usr_msg}]}
            )
        if r.status_code == 200:
            text = r.json()["choices"][0]["message"]["content"].strip()
            cache["institutional"]["text"]        = text
            cache["institutional"]["last_update"] = datetime.now(NY).isoformat()
            cache["institutional"]["status"]      = "fresh"
            cache["health"]["groq"]               = "online"
            save_cache()
            print("[institutional] ok")
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
    """GEX desde cache. FlashAlpha SOLO se llama 9am + 7pm ET (2 créditos/día)."""
    gex = cache["gex"].get("NQ")
    if not gex:
        raise HTTPException(503, "GEX no disponible aún — próxima carga: 9:00 AM ET")
    qqq = gex.get("underlying_price")
    nq  = round(qqq*41.2,2) if qqq else None
    return {**gex, "asset":"NQ", "nq_price":nq, "ratio":41.2, "credits_used":0}

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
    }

@app.get("/api/calendar")
async def get_calendar():
    last = cache["calendar"]["last_update"]
    if not last or (datetime.now(NY) - datetime.fromisoformat(last)).total_seconds() > 300:
        await refresh_calendar()
    upcoming = [e for e in cache["calendar"]["data"] if e.get("status")=="Upcoming"]
    return {
        "macro_calendar":   cache["calendar"]["data"],
        "next_macro_event": upcoming[0] if upcoming else None,
        "last_update":      cache["calendar"]["last_update"],
        "status":           cache["calendar"]["status"],
        "count":            len(cache["calendar"]["data"]),
    }

@app.get("/api/movers")
async def get_movers():
    last = cache["movers"]["last_update"]
    if not last or (datetime.now(NY) - datetime.fromisoformat(last)).total_seconds() > 120:
        await refresh_movers()
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
    # Enrich with next earnings data from our cache (already loaded from Finnhub calendar)
    next_earn = next(
        (e for e in cache["earnings"]["data"]
         if e.get("symbol","").upper() == sym and not e.get("epsActual")),
        None
    )
    if next_earn:
        data["nextEpsEstimate"]  = next_earn.get("epsEstimate")
        data["nextRevEstimate"]  = _fmt_rev(next_earn.get("revenueEstimate"))
        data["nextDate"]         = next_earn.get("date")
        data["nextHour"]         = next_earn.get("hour","")

    result = {"symbol": sym, **data}
    cache["company"][sym] = {"data": result, "ts": time.time()}
    return result

@app.get("/api/context/institutional")
async def get_institutional():
    """Resumen IA. Generado 2x/día. Requiere GEX real de FlashAlpha."""
    last = cache["institutional"]["last_update"]
    if not last or (datetime.now(NY) - datetime.fromisoformat(last)).total_seconds() > 900:
        await refresh_institutional()
    text = cache["institutional"]["text"]
    if not text:
        raise HTTPException(503, "Resumen no disponible aún — esperando datos GEX")
    return {"summary":text, "last_update":cache["institutional"]["last_update"],
            "status":cache["institutional"]["status"]}

@app.get("/api/dashboard")
async def get_dashboard():
    """Endpoint agregado — todo en una sola llamada."""
    upcoming = [e for e in cache["calendar"]["data"] if e.get("status")=="Upcoming"]
    movers   = cache["movers"]["data"]
    breaking = next((m for m in movers if m.get("score",0)>=95), None)
    gex = cache["gex"].get("NQ",{})
    qqq = gex.get("underlying_price")
    return {
        "gamma_levels":        {**gex,"nq_price":round(qqq*41.2,2) if qqq else None} if gex else None,
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

    # ── TwelveData WebSocket: una sola tarea persistente ──────────────────
    asyncio.create_task(twelvedata_ws())

    # ── TwelveData REST: batch 13 símbolos macro cada 15min en RTH ────────
    scheduler.add_job(refresh_heatmap_rest,
                      CronTrigger(day_of_week="mon-fri", hour="4-20", minute="*/15"))

    # ── FlashAlpha GEX: SOLO 9am + 7pm ET (2 créditos de 5/día) ──────────
    scheduler.add_job(refresh_gex, CronTrigger(hour=9,  minute=0,  day_of_week="mon-fri"))
    scheduler.add_job(refresh_gex, CronTrigger(hour=19, minute=0,  day_of_week="mon-fri"))

    # ── Finnhub Calendar: cada 5 minutos ──────────────────────────────────
    scheduler.add_job(refresh_calendar, IntervalTrigger(minutes=5))

    # ── Finnhub Movers: cada 60 segundos ──────────────────────────────────
    scheduler.add_job(refresh_movers, IntervalTrigger(seconds=60))

    # ── Finnhub Earnings: cada 6 horas ────────────────────────────────────
    scheduler.add_job(refresh_earnings, IntervalTrigger(hours=6))

    # ── Groq Institutional: 9:05 AM + 12:00 PM ET lun-vie ─────────────────
    scheduler.add_job(refresh_institutional, CronTrigger(hour=9,  minute=5, day_of_week="mon-fri"))
    scheduler.add_job(refresh_institutional, CronTrigger(hour=12, minute=0, day_of_week="mon-fri"))

    scheduler.start()

    # ── Carga inicial: todo excepto FlashAlpha (ahorra créditos) ──────────
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
        # Si hay GEX, intentar resumen IA inmediatamente
        asyncio.create_task(refresh_institutional())
    else:
        print("[startup] Sin GEX en disco — cargará a las 9:00 AM ET (ahorra créditos)")

    print("[startup] Liberato Backend v3.0 listo ✓")
