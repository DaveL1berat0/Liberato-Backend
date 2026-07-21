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

import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os, time, asyncio, json
from urllib.parse import quote   # usado a nivel módulo (config del instrumento) y en varias funciones
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import websockets

# ══ CREDENCIALES (solo Railway Variables, nunca en código) ════════════════════
FLASHALPHA_KEY   = os.getenv("FLASHALPHA_KEY",   "").strip()
FINNHUB_KEY      = os.getenv("FINNHUB_KEY",      "")
RAPIDAPI_KEY  = os.getenv("RAPIDAPI_KEY", "")          # calendario tiempo real
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "economic-calendar-api-tradingeconomics.p.rapidapi.com")
# ── APIs nuevas (respaldo/complemento del calendario) ──
# RapidAPI: soportar ambos nombres de variable (RAPIDAPI_KEY o x-rapidapi-key)
def _clean_key(*names):
    """Devuelve la primera env var válida, ignorando placeholders comunes."""
    placeholders = {"aqui-tu-clave-rapidapi", "aqui-tu-secreto", "whsec_aqui-tu-secreto",
                    "tu-clave", "your-key", "your_key", "changeme", "xxx", ""}
    for n in names:
        v = os.getenv(n)
        if v and v.strip().lower() not in placeholders and "aqui-tu" not in v.lower():
            return v.strip()
    return ""

# RapidAPI: ignora el placeholder "aqui-tu-clave-rapidapi", usa x-rapidapi-key (la real)
RAPIDAPI_KEY = _clean_key("x-rapidapi-key", "X_RAPIDAPI_KEY", "RAPIDAPI_KEY")
# Si la variable de Railway tiene un host viejo/muerto, corregirlo al que funciona
# (TradingEconomics). Así no depende de que actualices la variable manualmente.
_DEAD_RAPIDAPI_HOSTS = (
    "economic-calendar.p.rapidapi.com",
    "ultimate-economic-calendar.p.rapidapi.com",  # cayó por 402 DEPLOYMENT_DISABLED
    "", None,
)
if RAPIDAPI_HOST in _DEAD_RAPIDAPI_HOSTS:
    RAPIDAPI_HOST = "economic-calendar-api-tradingeconomics.p.rapidapi.com"
# FMP (Financial Modeling Prep) — calendario económico, 250 req/día free
FMP_KEY = (os.getenv("FMP_KEY") or os.getenv("FMP_API_KEY") or os.getenv("FINANCIAL_MODELING_PREP_KEY") or os.getenv("mfp") or os.getenv("MFP") or os.getenv("FMP"))
if FMP_KEY:
    FMP_KEY = FMP_KEY.strip()
FMP_BASE = "https://financialmodelingprep.com/stable"
FMP_BASE_LEGACY = "https://financialmodelingprep.com/api/v3"
# Contacto / soporte (Gmail SMTP)
GMAIL_USER         = os.getenv("GMAIL_USER", "")           # correo emisor
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")   # App Password de Gmail
SUPPORT_EMAIL      = os.getenv("SUPPORT_EMAIL", "SupportLiberatoCommunity@gmail.com").strip()
GROQ_KEY         = os.getenv("GROQ_KEY",         "").strip()
TWELVEDATA_KEY   = os.getenv("TWELVEDATA_KEY",   "").strip()

# ══ GUARDIÁN UNIVERSAL DE PRESUPUESTO DE APIs ════════════════════════
# Un solo sistema protege TODOS los APIs. Cuenta llamadas y FRENA antes
# de pasar el límite. Soporta ventana diaria (UTC) o por minuto.
# Hace IMPOSIBLE agotar cualquier API.
#
# Config por API: (límite_seguro, tipo_ventana). El límite_seguro deja
# margen bajo el límite real del proveedor.
API_BUDGETS = {
    "twelvedata":  {"limit": int(os.getenv("TD_DAILY_LIMIT", "700")),  "window": "day"},
    "finnhub":     {"limit": int(os.getenv("FH_MINUTE_LIMIT", "55")),  "window": "minute"},
    "flashalpha":  {"limit": int(os.getenv("FA_DAILY_LIMIT", "95")),   "window": "day"},   # real 100
    "fmp":         {"limit": int(os.getenv("FMP_DAILY_LIMIT", "230")), "window": "day"},   # real 250
    "alphavantage":{"limit": int(os.getenv("AV_DAILY_LIMIT", "22")),   "window": "day"},   # real 25
    "groq":        {"limit": int(os.getenv("GROQ_DAILY_LIMIT", "950")),"window": "day"},   # llamadas/día
}
# Estado de uso por API: {"window_key": str, "used": int}
_api_usage = {name: {"window_key": None, "used": 0} for name in API_BUDGETS}

def _window_key(window):
    """Clave de la ventana actual: por día (UTC) o por minuto (UTC)."""
    try:
        now = datetime.now(timezone.utc)
    except Exception:
        now = datetime.utcnow()
    return now.strftime("%Y-%m-%d") if window == "day" else now.strftime("%Y-%m-%d %H:%M")

def budget_ok(api, cost=1):
    """True si caben 'cost' llamadas al 'api' SIN pasar su límite.
    Resetea el contador automáticamente al cambiar la ventana."""
    cfg = API_BUDGETS.get(api)
    if not cfg:
        return True   # API sin límite configurado → permitir
    st = _api_usage[api]
    wk = _window_key(cfg["window"])
    if st["window_key"] != wk:
        st["window_key"] = wk
        st["used"] = 0
    return (st["used"] + cost) <= cfg["limit"]

def budget_charge(api, cost=1):
    """Registra 'cost' llamadas usadas del 'api'."""
    cfg = API_BUDGETS.get(api)
    if not cfg:
        return
    st = _api_usage[api]
    wk = _window_key(cfg["window"])
    if st["window_key"] != wk:
        st["window_key"] = wk
        st["used"] = 0
    st["used"] += cost

# ── Compatibilidad: wrappers con los nombres antiguos (TD y Finnhub) ──
TD_DAILY_LIMIT  = API_BUDGETS["twelvedata"]["limit"]
FH_MINUTE_LIMIT = API_BUDGETS["finnhub"]["limit"]
def td_budget_ok(cost=1): return budget_ok("twelvedata", cost)
def td_charge(cost=1):    budget_charge("twelvedata", cost)
def fh_budget_ok(cost=1): return budget_ok("finnhub", cost)
def fh_charge(cost=1):    budget_charge("finnhub", cost)
# Compat con el monitor antiguo
_td_credits = _api_usage["twelvedata"]   # alias
# Asegurar las claves que usan td_budget_ok/td_charge (evita KeyError: 'day').
# El sistema nuevo inicializa con window_key/used; el contador diario necesita 'day'.
_td_credits.setdefault("day", None)
_td_credits.setdefault("used", 0)
_fh_calls   = _api_usage["finnhub"]
_fh_calls.setdefault("day", None)
_fh_calls.setdefault("used", 0)

# ⚠️ Aquí vivía una SEGUNDA definición de td_budget_ok/td_charge (contador por
# clave "day") que sobrescribía a los wrappers de arriba. Como _td_credits es un
# ALIAS de _api_usage["twelvedata"], los dos contadores compartían "used" pero
# detectaban la ventana con claves distintas ("day" vs "window_key"): la primera
# llamada del sistema nuevo de cada día veía su window_key viejo y ponía used=0,
# borrando lo que el sistema viejo ya había cobrado → subconteo → gasto de más.
# Eliminadas: ahora TODO TwelveData pasa por budget_ok/budget_charge. Mismo
# límite (TD_DAILY_LIMIT sale de API_BUDGETS) y misma ventana diaria UTC.
ALPHA_VANTAGE_KEY= os.getenv("ALPHAVANTAGE_KEY", "").strip()
FINNHUB_WH_SECRET = os.getenv("FINNHUB_WEBHOOK_SECRET", "").strip()  # opcional: verifica autenticidad

# ═══════════════════════════════════════════════════════════════════════════
#  RATIO ES/SPY — derivado de dato REAL, nunca hardcodeado
# ═══════════════════════════════════════════════════════════════════════════
#  Historia: el ratio NQ/QQQ vivía como constante 41.51. Se suponía dinámico,
#  pero el único sitio que lo derivaba estaba dentro del WebSocket de TwelveData,
#  que se apagó (TD_WEBSOCKET=off) por quemar créditos. Resultado: nq_ratio_current
#  = null en producción → el precio mostrado era QQQ × 41.51, una constante
#  congelada desde hacía meses. Peor: la "verificación" dividía entre QQQ un
#  número que se había calculado multiplicando por QQQ → circular, siempre 41.51.
#
#  Aquí NO se hardcodea ningún ratio. Dos fuentes, ambas dato real:
#    1) spot del futuro que manda FlashAlpha (exacto) ÷ SPY del heatmap
#    2) SPX real de Finnhub (^GSPC) ÷ SPY real  → ES ≈ SPX + basis (~0.1%)
#  Si ninguna hay → None → la UI muestra "—". Regla #1: nunca un número inventado.
def _set_px_ratio_from_spot(spot):
    """Deriva el ratio instrumento/ETF con el spot REAL del futuro."""
    try:
        etf = (cache["heatmap"]["data"].get(FA_PROXY_ETF, {}) or {}).get("price")
        if spot and etf and etf > 10:
            r = round(float(spot) / float(etf), 6)
            cache["px_ratio"].update({"value": r, "spot": float(spot),
                                      "etf_price": float(etf), "source": "flashalpha-spot",
                                      "ts": datetime.now(NY).isoformat()})
            return r
    except Exception as e:
        print(f"[ratio] no se pudo derivar del spot: {e}")
    return None

def get_px_ratio():
    """Ratio actual. Deriva de SPX/SPY real si no hay spot de FlashAlpha.
    Devuelve None si no hay dato real — el llamador debe mostrar '—'."""
    v = cache["px_ratio"].get("value")
    if v:
        return v
    try:
        hm  = cache["heatmap"]["data"]
        etf = (hm.get(FA_PROXY_ETF, {}) or {}).get("price")
        idx = (hm.get(FA_CASH_INDEX, {}) or {}).get("price")   # índice cash (NDX/SPX)
        if idx and etf and etf > 10:
            r = round(float(idx) / float(etf), 6)
            cache["px_ratio"].update({"value": r, "spot": None, "etf_price": float(etf),
                                      "source": f"{FA_CASH_INDEX.lower()}/{FA_PROXY_ETF.lower()}",
                                      "ts": datetime.now(NY).isoformat()})
            return r
    except Exception:
        pass
    return None   # sin dato real → "—", nunca una constante

FA_BASE = "https://lab.flashalpha.com"
# ── CONFIG FLASHALPHA ──────────────────────────────────────────────
# Plan actual: "free" usa QQQ summary + conversión a NQ (1 llamada).
# Plan "basic" usa NDX DIRECTO (sin conversión): niveles reales del
# Nasdaq-100 vía /v1/exposure/levels/NDX + /v1/exposure/gex/NDX.
# Para activar Basic: pon FLASHALPHA_PLAN=basic en Railway. Nada más.
FLASHALPHA_PLAN = os.getenv("FLASHALPHA_PLAN", "free").strip().lower()
# ════════════════════════════════════════════════════════════════════════════
#  INSTRUMENTO — punto ÚNICO de cambio del backend
# ════════════════════════════════════════════════════════════════════════════
#  Dave opera el NQ (Nasdaq-100). Su estrategia es del Nasdaq, no del S&P.
#  (Hubo un rodeo por el ES en jul-2026; se revirtió a NQ el 16-jul.)
#  El plan Basic sirve los futuros CME directos: los niveles llegan YA en puntos
#  del índice (conversion="none-direct"), sin ratio. Verificado con
#  /api/admin/diag-symbol?sym=NQ%3DF → 200 + call_wall/put_wall/gamma_flip reales.
#  Para volver al ES: NQ=F→ES=F, QQQ→SPY, NQ→ES, NDX→SPX, NQ1!→ES1!, ^NDX→^GSPC.
FA_INDEX_SYMBOL = os.getenv("FA_INDEX_SYMBOL", "NQ=F")  # futuro CME directo
# Proxy para precio/velas: TwelveData free no da futuros, solo el ETF.
# NQ→QQQ. El ratio NO se hardcodea (antes vivía como 41.51): ver get_px_ratio.
FA_PROXY_ETF    = os.getenv("FA_PROXY_ETF", "QQQ").strip().upper()
FA_ASSET        = os.getenv("FA_ASSET", "NQ").strip().upper()   # clave de cache y etiqueta
# Índice CASH del mismo mercado (Nasdaq-100 = NDX). Se usa para:
#  · el fallback de macro (Fear&Greed/VIX) cuando el summary del futuro falla,
#  · derivar el ratio índice/ETF de respaldo (NDX/QQQ) sin depender de FlashAlpha.
FA_MACRO_FALLBACK = os.getenv("FA_MACRO_FALLBACK", "NDX").strip().upper()
FA_CASH_INDEX     = os.getenv("FA_CASH_INDEX", FA_MACRO_FALLBACK).strip().upper()  # clave heatmap
FA_YAHOO_INDEX    = os.getenv("FA_YAHOO_INDEX", "%5ENDX").strip()  # ^NDX url-encoded
# Símbolo del futuro en el WebSocket de TwelveData (hoy APAGADO por defecto:
# TD_WEBSOCKET=off — cobraba por tick y quemó 10.000+ créditos/día).
FA_WS_FUTURE    = os.getenv("FA_WS_FUTURE", "NQ1!").strip().upper()
# Refreshes de GEX/día segun el cron (ver setup del scheduler). Solo para textos:
# el numero real lo manda el CronTrigger.
GEX_REFRESHES_PER_DAY = 28
FH_BASE = "https://finnhub.io/api/v1"
NY      = ZoneInfo("America/New_York")

# ══ APP ══════════════════════════════════════════════════════════════════════
app = FastAPI(title="Liberato Backend v3.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ══ CACHÉ UNIFICADA ══════════════════════════════════════════════════════════
cache = {
    "gex":           {},
    "heatmap":       {"data": {}, "last_update": None, "status": "offline"},
    # px_ratio: ratio instrumento/ETF (ES/SPY) SIEMPRE derivado de dato real.
    # Sustituye a nq_ratio (NQ/QQQ), que en la práctica quedaba en None y caía a
    # la constante 41.51. 'source' dice de dónde salió: flashalpha-spot | spx/spy.
    "px_ratio":      {"value": None, "spot": None, "etf_price": None, "source": None, "ts": None},
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
_PERSIST = os.getenv("PERSIST_PATH", "/tmp/lbc_v3.json")  # con Railway Volume: /data/lbc_v3.json

def save_cache():
    try:
        snap = {
            # El contador de APIs DEBE persistir: vive en memoria y se reseteaba a
            # 0 en CADA redeploy, así que el guardián de presupuesto creía tener
            # cuota y seguía llamando contra una ya agotada. El proveedor sí lleva
            # la cuenta real (FlashAlpha: 100/día, reset 00:00 UTC).
            "api_usage": _api_usage,
            "gex":      cache["gex"],
            "earnings": {"data": cache["earnings"]["data"]},
            "institutional": {"text": cache["institutional"]["text"],
                              "lu":   cache["institutional"]["last_update"]},
            # Persistidos para sobrevivir redeploys (el Volume /data los retiene):
            # sin esto, un redeploy al mediodía borraba los 'actual' del calendario
            # y las noticias high-impact acumuladas del día.
            "calendar": {"data": cache["calendar"]["data"],
                         "lu":   cache["calendar"]["last_update"]},
            "rapidapi_actuals": cache.get("_rapidapi_cache", []),
            "movers_seen": cache.get("_movers_seen", {}),
            "movers": {"data": cache["movers"]["data"],
                       "lu":   cache["movers"]["last_update"]},
        }
        with open(_PERSIST, "w") as f:
            json.dump(snap, f)
    except Exception as e:
        print(f"[persist] error guardando: {e}")

def load_cache():
    try:
        with open(_PERSIST) as f:
            snap = json.load(f)
        if snap.get("api_usage"):
            # Restaurar contadores SOLO si siguen en la misma ventana (día/minuto);
            # si la ventana cambió, budget_ok() los resetea solo.
            for _api, _st in snap["api_usage"].items():
                if _api in _api_usage and isinstance(_st, dict):
                    _api_usage[_api].update({"window_key": _st.get("window_key"),
                                             "used": _st.get("used", 0)})
            print(f"[persist] contadores restaurados: "
                  f"flashalpha={_api_usage['flashalpha']['used']}")
        if snap.get("gex"):
            # Solo restaurar el GEX del instrumento que operamos AHORA.
            # El Volume de Railway retiene datos entre redeploys, así que tras la
            # migración NQ→ES el snapshot trae niveles del Nasdaq. Cargarlos sería
            # mostrar walls de otro instrumento (Regla #1). Se descartan.
            _keep = {k: v for k, v in snap["gex"].items() if k == FA_ASSET}
            _drop = [k for k in snap["gex"] if k != FA_ASSET]
            if _drop:
                print(f"[persist] GEX descartado de otro instrumento: {_drop} "
                      f"(operamos {FA_ASSET})")
            if _keep:
                cache["gex"] = _keep
        if snap.get("earnings", {}).get("data"):
            cache["earnings"]["data"]   = snap["earnings"]["data"]
            cache["earnings"]["status"] = "stale"
        if snap.get("institutional", {}).get("text"):
            cache["institutional"]["text"]        = snap["institutional"]["text"]
            cache["institutional"]["last_update"] = snap["institutional"].get("lu")
            cache["institutional"]["status"]      = "stale"
        if snap.get("calendar", {}).get("data"):
            cache["calendar"]["data"]        = snap["calendar"]["data"]
            cache["calendar"]["last_update"] = snap["calendar"].get("lu")
            cache["calendar"]["status"]      = "stale"
        if snap.get("rapidapi_actuals"):
            cache["_rapidapi_cache"] = snap["rapidapi_actuals"]
        if snap.get("movers_seen"):
            cache["_movers_seen"] = snap["movers_seen"]
        if snap.get("movers", {}).get("data"):
            cache["movers"]["data"]        = snap["movers"]["data"]
            cache["movers"]["last_update"] = snap["movers"].get("lu")
            cache["movers"]["status"]      = "stale"
        print(f"[persist] cache restaurado: {len(cache['earnings']['data'])} earnings, "
              f"{len(cache['calendar']['data'])} eventos calendario, "
              f"{len(cache.get('_rapidapi_cache', []))} actuals TE, "
              f"{len(cache['movers']['data'])} movers")
    except FileNotFoundError:
        print("[persist] primer arranque sin datos previos")
    except Exception as e:
        print(f"[persist] error cargando: {e}")

# ══ TWELVEDATA WEBSOCKET (una sola conexión, todos los símbolos) ═════════════
# 8 símbolos real-time vía WebSocket — sin créditos REST
# WebSocket SOLO para lo que necesita baja latencia: precio NQ en vivo.
# El ETF proxy se mantiene para el cálculo del ratio de respaldo (ES/SPY).
# Las acciones del heatmap pasaron a REST /quote (cambio diario real).
# (El WS está apagado por defecto: TD_WEBSOCKET=off — quemaba créditos por tick.)
WS_SYMBOLS = [FA_PROXY_ETF]
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
                    if sym == FA_WS_FUTURE:
                        cache["px_ratio"]["spot"] = price
                        cache["heatmap"]["data"][FA_ASSET] = {
                            "symbol":FA_ASSET,"price":round(price,2),
                            "chg_pct":round(chg_pct,3),
                            "direction":"up" if chg_pct>0.05 else("down" if chg_pct<-0.05 else"flat"),
                            "source":"direct",
                        }
                        qqq_px = cache["px_ratio"].get("etf_price")
                        if qqq_px and qqq_px > 0:
                            nr = round(price/qqq_px,6)
                            cache["px_ratio"].update({"value":nr,"error_pts":0,"ts":datetime.now(NY).isoformat()})
                    elif sym == FA_PROXY_ETF:
                        cache["px_ratio"]["etf_price"] = price
                        if cache["heatmap"]["data"].get(FA_ASSET,{}).get("source") != "direct":
                            dr = get_px_ratio()
                            # Sin ratio real no se publica el tile (Regla #1):
                            # antes hacía price*dr con dr=None → TypeError.
                            if dr:
                                cache["heatmap"]["data"][FA_ASSET] = {
                                    "symbol":FA_ASSET,"price":round(price*dr,2),
                                    "chg_pct":round(chg_pct,3),
                                    "direction":"up" if chg_pct>0.05 else("down" if chg_pct<-0.05 else"flat"),
                                    "source":"estimated","ratio_used":dr,
                                }
                        nq_px = cache["px_ratio"].get("spot")
                        if nq_px:
                            nr = round(nq_px/price,6)
                            if abs(nq_px-(price*nr)) > 25:
                                print(f"[ratio] drift {FA_PROXY_ETF}/{FA_ASSET} detectado")
                            cache["px_ratio"].update({"value":nr,"ts":datetime.now(NY).isoformat()})
                    if sym != FA_WS_FUTURE:
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
    # QQQ es el ETF proxy del NQ (FA_PROXY_ETF): get_px_ratio() lo lee de aquí
    # para derivar el ratio NDX/QQQ de respaldo. SPY se mantiene como correlación
    # (un NQ-trader vigila la divergencia S&P vs Nasdaq).
    "QQQ":"QQQ",
    "SPY":"SPY","VIXY":"VIXY","UUP":"UUP","SHY":"SHY","IEF":"IEF",
    "TLT":"TLT","GLD":"GLD","USO":"USO","IBIT":"IBIT","TIP":"TIP",
    "COST":"COST","NFLX":"NFLX","AVGO":"AVGO",
    # Acciones grandes movidas desde el WebSocket: el WS daba change_percent
    # que no era el cambio DIARIO confiable (NVDA salía verde estando en rojo).
    # Por /quote obtienen el percent_change diario real, igual que el resto.
    "AAPL":"AAPL","MSFT":"MSFT","NVDA":"NVDA","META":"META",
    "AMZN":"AMZN","TSLA":"TSLA","GOOGL":"GOOGL",
}

async def refresh_heatmap_finnhub():
    """Heatmap vía Finnhub /quote — 60 llamadas/min permite refresco rápido.
    Campo 'dp' = percent change DIARIO real (vs cierre previo). 'c' = precio.
    Finnhub es 1 símbolo por llamada; 20 símbolos = 20 llamadas (<60/min OK).
    Fuente PRIMARIA del heatmap. Si Finnhub falla (429/error), cae a TwelveData."""
    # Índices reales (VIX/DXY/yields/Gold/WTI/BTC) vía Yahoo — Finnhub NO los tiene.
    # Se dispara aquí porque este ciclo SÍ corre cada minuto en RTH (throttle 4min).
    asyncio.create_task(refresh_real_indices())
    if not FINNHUB_KEY:
        await refresh_heatmap_rest()   # sin key Finnhub → usar TwelveData
        return
    all_syms = list(REST_SYMBOLS.keys())   # símbolos del heatmap (20)
    # GUARDIÁN: ¿caben las 20 llamadas en este minuto? Si no, usar TwelveData.
    if not fh_budget_ok(len(all_syms)):
        print(f"[heatmap-fh] sin presupuesto Finnhub este minuto ({_fh_calls['count']}/{FH_MINUTE_LIMIT}) — fallback TwelveData")
        await refresh_heatmap_rest()
        return
    loaded = 0; rate_limited = False
    async with httpx.AsyncClient(timeout=10) as client:
        # Llamadas en paralelo controlado (no más de ~20, cabe en 60/min)
        async def _one(sym):
            nonlocal loaded, rate_limited
            try:
                fh_charge(1)  # registrar la llamada
                r = await client.get(f"{FH_BASE}/quote",
                                     params={"symbol": sym, "token": FINNHUB_KEY})
                if r.status_code == 429:
                    rate_limited = True; return
                if r.status_code != 200:
                    return
                q = r.json() or {}
                price = q.get("c"); dp = q.get("dp")  # c=current, dp=percent change diario
                if price is None or price == 0:
                    return
                chg = dp if isinstance(dp, (int, float)) else 0
                cache["heatmap"]["data"][sym] = {
                    "symbol": sym, "price": round(float(price), 4),
                    "chg_pct": round(float(chg), 3),
                    "direction": "up" if chg > 0.05 else ("down" if chg < -0.05 else "flat"),
                    "source": "finnhub",
                }
                loaded += 1
            except Exception as e:
                print(f"[heatmap-fh] {sym} falló: {e}")
        await asyncio.gather(*[_one(s) for s in all_syms])
    if rate_limited or loaded == 0:
        # Finnhub saturado o sin datos → respaldo TwelveData (cambio diario real)
        print(f"[heatmap-fh] {'429 rate-limit' if rate_limited else '0 cargados'} → fallback TwelveData")
        await refresh_heatmap_rest()
        return
    cache["heatmap"]["last_update"] = datetime.now(NY).isoformat()
    if cache["heatmap"]["status"] != "live":
        cache["heatmap"]["status"] = "fresh"
    print(f"[heatmap-fh] ok: {loaded}/{len(all_syms)} símbolos (Finnhub, cambio diario)")


async def refresh_heatmap_rest():
    """Batch REST para los 13 símbolos macro (no en WebSocket).
    Una sola llamada = 13 créditos. Cada 15 min = ~350 créditos/día."""
    # Índices reales (Yahoo) en paralelo — throttle interno de 4 min, cero créditos
    asyncio.create_task(refresh_real_indices())
    if not TWELVEDATA_KEY:
        return
    symbols = ",".join(REST_SYMBOLS.values())
    n_sym = len(REST_SYMBOLS)
    # GUARDIÁN: el heatmap por TwelveData cuesta n_sym créditos. Si no caben,
    # NO llamar (mantiene el último dato de Finnhub/cache). Nunca se pasa.
    if not td_budget_ok(n_sym):
        print(f"[heatmap-rest] sin presupuesto TwelveData ({_td_credits['used']}/{TD_DAILY_LIMIT}) — usando Yahoo")
        await _heatmap_yahoo_fallback()   # Yahoo cubre los macro sin gastar créditos
        return
    # /quote da el percent_change DIARIO real (vs cierre anterior), NO el
    # cambio desde la última llamada. Esto arregla el bug de mostrar verde
    # un símbolo que en el día está en rojo.
    url = f"https://api.twelvedata.com/quote?symbol={symbols}&apikey={TWELVEDATA_KEY}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url)
        if r.status_code != 200:
            print(f"[heatmap-rest] error {r.status_code} — trying Yahoo fallback")
            await _heatmap_yahoo_fallback()
            return
        td_charge(len(REST_SYMBOLS))  # registrar créditos del batch
        data = r.json()
        sym_to_hmap = {v:k for k,v in REST_SYMBOLS.items()}
        loaded = 0
        # /quote con múltiples símbolos devuelve {symbol: {quote}}; con uno solo
        # devuelve el quote directo. Normalizar a dict de quotes.
        if "symbol" in data and "percent_change" in data:
            data = {data.get("symbol"): data}
        for td_sym, q in data.items():
            if not isinstance(q, dict):
                continue
            # percent_change = cambio % diario real vs cierre previo
            pc = q.get("percent_change")
            close = q.get("close") or q.get("price")
            if pc is None or close is None:
                continue
            hmap_sym = sym_to_hmap.get(td_sym, td_sym)
            try:
                price = float(close); chg_pct = float(pc)
            except (TypeError, ValueError):
                continue
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
            if ysym == FA_PROXY_ETF:
                # Tile sintético del futuro derivado del ETF. Antes hacía
                # price*(get_px_ratio()) sin comprobar None → TypeError. Ahora,
                # sin ratio real no se publica el tile (Regla #1: mejor "—").
                _r = get_px_ratio()
                if _r:
                    cache["heatmap"]["data"][FA_ASSET] = {
                        "symbol":FA_ASSET,"price":round(price*_r,2),
                        "chg_pct":round(chg_pct,3),
                        "direction":"up" if chg_pct>0.05 else("down" if chg_pct<-0.05 else"flat"),
                    }
            loaded += 1
        cache["heatmap"]["last_update"] = datetime.now(NY).isoformat()
        cache["heatmap"]["status"]      = "stale-yahoo"
        print(f"[heatmap-yahoo] fallback ok: {loaded} símbolos")
    except Exception as e:
        print(f"[heatmap-yahoo] error: {e}")

# ══ ÍNDICES REALES (Yahoo, gratis, sin key) ══════════════════════════════════
# Elimina los "—" del panel de correlaciones y da a los tiles macro su nivel y
# dirección VERDADEROS. Los ETF proxy (VIXY/UUP/IEF...) quedan solo de fallback:
#   · VIX/VXN reales (índices CBOE, no el ETF)  · DXY real (ICE)
#   · Yields reales vía futuros de yield del CME (2YY=F / 10Y=F / 30Y=F cotizan
#     el yield DIRECTO → dirección correcta, sin la inversión del precio de los
#     ETF de bonos)  · Gold GC=F · WTI CL=F · ES=F real · SPX ^GSPC · BTC spot
_REAL_INDICES = {
    "VIX": "^VIX", "VXN": "^VXN", "DXY": "DX-Y.NYB",
    "US10Y": "10Y=F", "US2Y": "2YY=F", "US30Y": "30Y=F",
    "Gold": "GC=F", "WTI": "CL=F", "NQ": "NQ=F", "NDX": "^NDX", "BTC": "BTC-USD",
}
_indices_last_ts = 0
# ⚠️ Aquí había una firma huérfana de refresh_real_indices() con solo docstring y
# sin cuerpo (no-op) que quedó de una edición: la versión real está más abajo y la
# sobrescribía. Eliminada — no cambia el comportamiento, solo quita el señuelo.
# ══ ÍNDICES REALES (Finnhub, gratis, verificable) ════════════════════════════
# Elimina los "—" del panel de correlaciones. Vía Finnhub /quote (misma key que
# ya funciona para el heatmap). Símbolos que Finnhub free SÍ soporta:
#   · Índices: usamos ETF proxy líquidos que Finnhub cotiza bien y cuyo % diario
#     ES el del subyacente (el nivel mostrado es el del ETF, marcado como tal).
#   · VIX real vía ^VIX (Finnhub lo soporta como índice).
# Nota: Finnhub free no da futuros; para DXY/yields/oro usamos los ETF proxy
# (UUP/IEF/GLD/USO) cuyo movimiento % refleja el subyacente. IEF se invierte
# (bono → yield). Es data REAL y verificable, sin el bloqueo cookie de Yahoo.
# VERIFICADO 16-jul-2026 en producción: Finnhub free NO sirve símbolos de índice
# (^VIX y ^GSPC nunca llegaron al heatmap; los proxies ETF y BTC sí). Se quitan de
# aquí para no gastar 2 llamadas/ciclo en respuestas vacías:
#   · SPX → refresh_spx_yahoo() (Yahoo /v8/chart, gratis)
#   · VIX → llega del summary de FlashAlpha (bloque macro), que ya se pide para el
#           GEX; y VIXY (ETF de volatilidad) sigue en el heatmap con precio real.
_FH_INDICES = {
    "DXY": ("UUP", 1, True),     # proxy dólar (ETF)
    "US10Y": ("IEF", -1, True),  # proxy bonos 10Y → yield inverso
    "US2Y": ("SHY", -1, True),
    "US30Y": ("TLT", -1, True),
    "Gold": ("GLD", 1, True),
    "WTI": ("USO", 1, True),
    "BTC": ("BINANCE:BTCUSDT", 1, False),
}
# ── SPX vía Yahoo: Finnhub free NO sirve símbolos de índice ────────────────
# Verificado 16-jul-2026 en producción: de los 9 de _FH_INDICES, los proxies ETF
# (UUP/IEF/SHY/TLT/GLD/USO) y BTC llegan, pero los índices cash (^VIX, ^NDX/^GSPC)
# NO — Finnhub free no los cubre. Sin el índice cash, el ratio índice/ETF se queda
# sin su único respaldo, así que si FlashAlpha no tiene cuota: ratio=None → sin
# velas → sin precio → dashboard vacío. Un solo punto de fallo para todo el chart.
# Yahoo /v8/finance/chart sí da ^NDX/^GSPC sin cookie ni crumb y sin coste.
# ⚠️ OJO: Yahoo BLOQUEA IPs de datacenter (429 desde Railway — verificado con
# diag-yahoo). Este job solo puebla el índice cuando la IP no está bloqueada; el
# camino fiable en producción es el spot que da FlashAlpha en el path directo.
_spx_last_ts = 0
_YAHOO_UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                           "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"}
async def refresh_cash_index_yahoo():
    """Publica el índice cash (FA_CASH_INDEX, ej. NDX) en el heatmap vía Yahoo.
    Gratis: no consume créditos de ninguna API nuestra."""
    global _spx_last_ts
    now = time.time()
    if now - _spx_last_ts < 120:   # Yahoo rate-limita: 1 llamada / 2 min basta
        return
    _spx_last_ts = now
    try:
        url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{FA_YAHOO_INDEX}"
               "?range=1d&interval=5m")
        async with httpx.AsyncClient(timeout=10, headers=_YAHOO_UA) as c:
            r = await c.get(url)
        if r.status_code != 200:
            print(f"[cash-idx] {FA_CASH_INDEX} {r.status_code} (rate-limit?) — mantiene el último real")
            return
        res = ((r.json() or {}).get("chart", {}).get("result") or [None])[0]
        if not res:
            return
        m = res.get("meta", {}) or {}
        px = m.get("regularMarketPrice")
        prev = m.get("chartPreviousClose") or m.get("previousClose")
        if not px:
            return
        chg = round(((px - prev) / prev) * 100, 3) if prev else None
        cache["heatmap"]["data"][FA_CASH_INDEX] = {
            "symbol": FA_CASH_INDEX, "price": round(float(px), 2),
            "chg_pct": chg,
            "direction": ("up" if (chg or 0) > 0.03 else
                          ("down" if (chg or 0) < -0.03 else "flat")),
            "source": "yahoo-index",
        }
        # Con el índice cash real + ETF real, el ratio ya es derivable sin FlashAlpha.
        r2 = get_px_ratio()
        print(f"[cash-idx] {FA_CASH_INDEX}={px} chg={chg}% | ratio derivado={r2}")
    except Exception as e:
        print(f"[cash-idx] error (no crítico): {e}")

_indices_last_ts = 0
async def refresh_real_indices():
    """Niveles reales vía Finnhub (throttle 4 min). Verificable, sin cookie/crumb."""
    global _indices_last_ts
    now = time.time()
    if now - _indices_last_ts < 240:
        return
    if not FINNHUB_KEY:
        return
    _indices_last_ts = now
    loaded = 0
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            async def _one(disp, ysym, sign, is_proxy):
                nonlocal loaded
                try:
                    if not fh_budget_ok(1):
                        return
                    fh_charge(1)
                    r = await client.get(f"{FH_BASE}/quote",
                                         params={"symbol": ysym, "token": FINNHUB_KEY})
                    if r.status_code != 200:
                        return
                    q = r.json() or {}
                    price, dp = q.get("c"), q.get("dp")
                    if price in (None, 0) or dp is None:
                        return
                    chg = float(dp) * sign
                    cache["heatmap"]["data"][disp] = {
                        "symbol": disp,
                        "price": (None if is_proxy else round(float(price), 4)),
                        "chg_pct": round(chg, 3),
                        "direction": "up" if chg > 0.03 else ("down" if chg < -0.03 else "flat"),
                        "source": "finnhub-index" + ("-proxy" if is_proxy else ""),
                    }
                    loaded += 1
                except Exception:
                    return
            await asyncio.gather(*[_one(d, s, sg, p) for d, (s, sg, p) in _FH_INDICES.items()])
        if loaded:
            print(f"[indices] {loaded} índices reales (Finnhub)")
    except Exception as e:
        print(f"[indices] error: {e}")
    return
async def _refresh_real_indices_OLD_yahoo():
    global _indices_last_ts
    now = time.time()
    if now - _indices_last_ts < 240:
        return
    _indices_last_ts = now
    ua = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"}
    y2d = {v: k for k, v in _REAL_INDICES.items()}
    loaded = 0
    try:
        url = ("https://query1.finance.yahoo.com/v7/finance/quote?symbols="
               + ",".join(_REAL_INDICES.values())
               + "&fields=regularMarketPrice,regularMarketChangePercent")
        async with httpx.AsyncClient(timeout=12, headers=ua) as client:
            r = await client.get(url)
            quotes = []
            if r.status_code == 200:
                quotes = r.json().get("quoteResponse", {}).get("result", []) or []
            if not quotes:
                # Fallback v8 por símbolo (v7 a veces exige cookie/crumb)
                for ysym in _REAL_INDICES.values():
                    try:
                        r2 = await client.get(
                            f"https://query1.finance.yahoo.com/v8/finance/chart/{ysym}"
                            "?range=1d&interval=1d")
                        if r2.status_code != 200:
                            continue
                        meta = (r2.json().get("chart", {}).get("result") or [{}])[0].get("meta", {})
                        px, pc = meta.get("regularMarketPrice"), meta.get("chartPreviousClose")
                        if px is None:
                            continue
                        chg = ((px - pc) / pc * 100) if pc else 0
                        quotes.append({"symbol": ysym, "regularMarketPrice": px,
                                       "regularMarketChangePercent": chg})
                    except Exception:
                        continue
        for q in quotes:
            key = y2d.get(q.get("symbol", ""))
            px = q.get("regularMarketPrice")
            if not key or px is None:
                continue
            chg = q.get("regularMarketChangePercent") or 0
            cache["heatmap"]["data"][key] = {
                "symbol": key, "price": round(float(px), 4),
                "chg_pct": round(float(chg), 3),
                "direction": "up" if chg > 0.03 else ("down" if chg < -0.03 else "flat"),
                "source": "yahoo-index",
            }
            loaded += 1
        if loaded:
            print(f"[indices] {loaded} índices reales (Yahoo)")
    except Exception as e:
        print(f"[indices] error: {e}")  # se conserva lo último real

# ══ FLASHALPHA — GEX (2 llamadas/día, nunca en startup) ══════════════════════
_gex_blocked_until = 0
_gex_ondemand_ts = 0   # debounce del refresh on-demand cuando el cache está frío
_gex_working_exp = None       # expiración que SÍ da datos GEX (cache del día)
_gex_working_exp_day = None   # día en que se cacheó (para resetear)   # timestamp: si hay 429, esperar 24h
_gex_expdates_cache = []      # expiraciones del día (1 llamada /options por día)
_gex_maxpain_val = None       # max pain del día (sale del OI → cambia ~1 vez/día)
_gex_maxpain_day = None

class _SkipOptions(Exception):
    """Señal interna: las expiraciones salen del cache del día, no de la API.
    No es un error — se captura explícitamente para no reportarlo como fallo."""

def _fa_charge(n=1):
    """Cobra n requests REALES a FlashAlpha.

    Se llama en CADA request, no en bloque. El proveedor cuenta requests HTTP y el
    contador debe contar lo mismo o miente. Y mentía: cobraba 3 fijos por refresh
    mientras hacía 5-9 requests (levels + options + gex[hasta 4 intentos] +
    maxpain + summary). Resultado: el panel decía 86/95 y FlashAlpha respondía
    "Quota exceeded: 100/100". La cuota estaba condenada por diseño."""
    budget_charge("flashalpha", n)
_gex_expdates_day = None      # día del cache de expiraciones
_gex_maxpain_val = None       # max pain del día (sale del OI: cambia ~1 vez/día)
_gex_maxpain_day = None
_gex_summary_cache = None     # bloque macro (Fear&Greed/VIX/IV): cambia lento
_gex_summary_ts = 0
GEX_SUMMARY_TTL = int(os.getenv("GEX_SUMMARY_TTL", "3600"))   # 1h → ~4 llamadas/día

def _fa_charge(n=1):
    """Cobra n llamadas REALES a FlashAlpha. Se llama en CADA request, no en
    bloque: el proveedor cuenta requests HTTP, y el contador debe contar lo mismo
    o miente (era el caso: cobraba 3 por refresh y hacía 5-9)."""
    budget_charge("flashalpha", n)
_gex_expdates_day   = None
_gex_maxpain_failed_day = None
_event_reactions = {}  # {evento: {t0, p0, p5}} — reacción del NQ a noticias  # si /maxpain falló hoy, no reintentar (ahorra créditos)

async def refresh_gex(asset=FA_ASSET):
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
    # GUARDIÁN: un refresh GEX completo usa ~4 llamadas (levels+options+gex+
    # maxpain+summary). Si no caben en el presupuesto FlashAlpha, NO llamar.
    if not budget_ok("flashalpha", 5):
        st = _api_usage["flashalpha"]
        print(f"[gex] presupuesto FlashAlpha agotado ({st['used']}/{API_BUDGETS['flashalpha']['limit']}) — se mantiene cache")
        return
    try:
        if FLASHALPHA_PLAN == "basic":
            # ══ PLAN BASIC: NDX DIRECTO (sin conversión QQQ→NQ) ══════════════
            # 2 llamadas: levels (call/put/flip) + gex (net_gex + per-strike).
            await _refresh_gex_ndx(asset)
        else:
            # ══ PLAN FREE: QQQ summary + conversión a NQ (1 llamada) ═════════
            await _refresh_gex_qqq(asset)
    except Exception as e:
        cache["health"]["flashalpha"] = "error"
        print(f"[gex] excepción: {e}")


async def _refresh_gex_qqq(asset=FA_ASSET):
    """PLAN FREE: usa /v1/stock/<ETF>/summary y guarda niveles en escala del ETF.
    El endpoint /api/market/gamma-levels/ES los convierte con el ratio real."""
    global _gex_blocked_until
    ticker = FA_PROXY_ETF
    async with httpx.AsyncClient(timeout=12,
                                  headers={"X-Api-Key": FLASHALPHA_KEY}) as client:
        r = await client.get(f"{FA_BASE}/v1/stock/{ticker}/summary")
    if r.status_code == 200:
        d = r.json()
        ex = d.get("exposure", {}) or {}
        px = d.get("price",    {}) or {}
        def _lvl(v):
            if isinstance(v, dict):
                return v.get("strike") or v.get("price") or v.get("level")
            return v
        cw, pw, gf = _lvl(ex.get("call_wall")), _lvl(ex.get("put_wall")), _lvl(ex.get("gamma_flip"))
        as_of = d.get("as_of"); market_open = d.get("market_open")
        cache["gex"][asset] = {
            "underlying_price": px.get("mid") or px.get("last"),
            "call_wall": cw, "put_wall": pw, "gamma_flip": gf,
            "net_gex": ex.get("net_gex"), "regime": ex.get("regime"),
            "ticker": ticker, "as_of": as_of, "market_open": market_open,
            "source": "etf-summary", "_ts": time.time(),
        }
        if cw is None and pw is None and gf is None:
            cache["health"]["flashalpha"] = "online-no-levels"
            print(f"[gex] ⚠️ 200 sin niveles (free no cubre call/put wall de {ticker}). Keys: {list(ex.keys())}")
        else:
            cache["health"]["flashalpha"] = "online"
            print(f"[gex] ok ({ticker}): flip={gf} call={cw} put={pw} as_of={as_of}")
        # Archivar el snapshot: cada refresh cuesta creditos y es irrepetible.
        append_gex_history(asset, cache["gex"][asset])
        save_cache()
    elif r.status_code == 429:
        _gex_blocked_until = time.time() + 86400
        cache["health"]["flashalpha"] = "rate-limited-24h"
        print("[gex] 429 — bloqueado 24h")
    else:
        cache["health"]["flashalpha"] = f"error-{r.status_code}"
        print(f"[gex] error {r.status_code}")


def _today_et_str():
    """Fecha de hoy en ET como 'YYYY-MM-DD'."""
    from datetime import datetime
    try:
        return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    except Exception:
        return datetime.utcnow().strftime("%Y-%m-%d")


def _nearest_index_expiration():
    """Expiración más cercana para opciones de índice (NDX/SPX expiran
    Lun/Mié/Vie). Devuelve 'YYYY-MM-DD' del próximo día de expiración
    (hoy si aplica). Formato que pide FlashAlpha para single-expiry."""
    from datetime import datetime, timedelta
    try:
        now_et = datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        now_et = datetime.utcnow()
    d = now_et.date()
    # weekday(): Mon=0 ... Sun=6. NDX/SPX expiran Lun(0), Mié(2), Vie(4).
    # IMPORTANTE: empezar desde MAÑANA (i=1), NO hoy. La expiración de hoy
    # es 0DTE (same-day) y eso requiere Growth. La próxima expiración futura
    # es single-expiry normal, cubierta por Basic.
    exp_days = {0, 2, 4}
    for i in range(1, 9):
        cand = d + timedelta(days=i)
        if cand.weekday() in exp_days:
            return cand.strftime("%Y-%m-%d")
    return (d + timedelta(days=2)).strftime("%Y-%m-%d")


async def _refresh_gex_ndx(asset=FA_ASSET):
    """PLAN BASIC: usa NDX DIRECTO. Niveles reales del Nasdaq-100, sin conversión.
       /v1/exposure/levels/NDX → call_wall, put_wall, gamma_flip, max_pain
       /v1/exposure/gex/NDX    → net_gex + per-strike (para validar walls)."""
    global _gex_blocked_until, _gex_expdates_day, _gex_expdates_cache, _gex_maxpain_failed_day
    sym = FA_INDEX_SYMBOL  # "NQ=F" (futuro CME directo)
    from urllib.parse import quote
    sym_url = quote(sym, safe="")  # NQ=F → NQ%3DF (requerido por FlashAlpha)
    # El coste NO se cobra en bloque. Antes: budget_charge(3) fijo, mientras el
    # refresh hacía 5-9 requests reales (levels + options + gex[hasta 4 intentos]
    # + maxpain + summary). Por eso el contador decía 86/95 mientras el proveedor
    # cortaba con "Quota exceeded 100/100": contaba ~140. Ahora cada request se
    # cobra donde se hace, con _fa_charge(), y el contador dice la verdad.
    _today_now = _today_et_str()
    _first_of_day = (_gex_expdates_day != _today_now)
    async with httpx.AsyncClient(timeout=12,
                                  headers={"X-Api-Key": FLASHALPHA_KEY}) as client:
        _fa_charge()
        r_lvl = await client.get(f"{FA_BASE}/v1/exposure/levels/{sym_url}")
        if r_lvl.status_code == 429:
            _gex_blocked_until = time.time() + 86400
            cache["health"]["flashalpha"] = "rate-limited-24h"
            print(f"[gex] 429 ({sym} levels) — bloqueado 24h")
            return
        if r_lvl.status_code == 200:
            lv = (r_lvl.json() or {}).get("levels", {}) or {}
        else:
            # RESILIENTE: /levels puede no cubrir futuros en Basic.
            # NO abortamos: el /gex con expiration SÍ funciona (probado en vivo)
            # y de ahí derivamos walls + flip. Regla #1: si un dato no llega, "—".
            lv = {}
            print(f"[gex] /levels/{sym} {r_lvl.status_code} — continúo con /gex (walls derivados)")
        # Segunda llamada: net_gex + per-strike.
        # En Basic, /gex de índices requiere UN solo expiry (no 0DTE, no full-chain).
        # Estrategia robusta: consultar las expiraciones REALES de NDX y usar la
        # primera futura (evita 404 por fecha inexistente y 403 por 0DTE).
        net_gex = None; per_strike = None; exp = None; exp_dates = []
        gex_flip = None; gex_label = None  # del response /gex (flip del futuro)
        # Las expiraciones cambian UNA vez al día. El cache ya existía
        # (_gex_expdates_cache, comentado como "1 llamada /options por día") pero
        # NUNCA se leía para saltarse la llamada: se pedía en los 28 refreshes
        # → 27 requests tirados cada día. Ahora solo se pide el primero del día.
        _usar_cache_exp = (not _first_of_day) and bool(_gex_expdates_cache)
        if _usar_cache_exp:
            exp_dates = list(_gex_expdates_cache)
            _fut = sorted([d for d in exp_dates if d > _today_now])
            if _fut:
                exp = _fut[0]
            print(f"[gex] expiraciones del cache del día ({len(exp_dates)}) — 0 créditos")
        try:
            if _usar_cache_exp:
                raise _SkipOptions("cache")
            _fa_charge()
            r_exp = await client.get(f"{FA_BASE}/v1/options/{sym_url}")
            if r_exp.status_code == 200:
                ed = r_exp.json() or {}
                exps = ed.get("expirations") or []
                # La lista puede ser de strings o de objetos {expiration, strikes}
                exp_dates = []
                for e in exps:
                    if isinstance(e, str): exp_dates.append(e)
                    elif isinstance(e, dict) and e.get("expiration"): exp_dates.append(e["expiration"])
                today_str = _today_et_str()
                # primera expiración estrictamente futura (evita 0DTE)
                future = sorted([d for d in exp_dates if d > today_str])
                _gex_expdates_cache = list(exp_dates)   # cachear para el resto del día
                _gex_expdates_day = today_str
                if future:
                    exp = future[0]
                    print(f"[gex] {sym} expiración elegida: {exp} (de {len(exp_dates)} disponibles)")
                else:
                    print(f"[gex] NDX sin expiraciones futuras en la lista: {exp_dates[:5]}")
            else:
                print(f"[gex] /options/{sym} status {r_exp.status_code}")
        except _SkipOptions:
            pass   # no es un error: se usó el cache del día (0 créditos)
        except Exception as e:
            print(f"[gex] /options/{sym} falló: {e}")
        # Probar VARIAS expiraciones futuras hasta que una dé net_gex.
        # OPTIMIZACIÓN: si ya sabemos qué expiración funciona hoy, usarla
        # primero (ahorra llamadas probando fechas que dan 404).
        global _gex_working_exp, _gex_working_exp_day
        today_k = _today_et_str()
        if _gex_working_exp_day != today_k:
            _gex_working_exp = None          # nuevo día → resetear
            _gex_working_exp_day = today_k
        future_list = sorted([d for d in exp_dates if d > today_k])
        if not future_list and exp:
            future_list = [exp]
        # Poner la expiración que funcionó antes al frente de la cola
        if _gex_working_exp and _gex_working_exp in future_list:
            future_list = [_gex_working_exp] + [d for d in future_list if d != _gex_working_exp]
        for cand_exp in future_list[:4]:   # máximo 4 intentos
            try:
                _fa_charge()   # cada intento de expiración es 1 request real
                r_gex = await client.get(f"{FA_BASE}/v1/exposure/gex/{sym_url}",
                                         params={"expiration": cand_exp})
                if r_gex.status_code == 200:
                    gd = r_gex.json() or {}
                    net_gex = gd.get("net_gex")
                    per_strike = gd.get("strikes")
                    gex_flip = gd.get("gamma_flip")       # flip REAL del futuro
                    gex_label = gd.get("net_gex_label")   # "positive"/"negative" de FA
                    exp = cand_exp
                    _gex_working_exp = cand_exp   # cachear la que funciona
                    print(f"[gex] /gex/{sym}?expiration={cand_exp} OK net_gex={net_gex} label={gex_label}")
                    break
                else:
                    print(f"[gex] /gex/{sym}?expiration={cand_exp} {r_gex.status_code}: {r_gex.text[:90]}")
            except Exception as e:
                print(f"[gex] intento {cand_exp} falló: {e}")
        # Respaldo: net_gex desde la respuesta de levels si existe
        if net_gex is None:
            net_gex = (r_lvl.json() or {}).get("net_gex")
            if net_gex is not None:
                print(f"[gex] net_gex tomado de /levels: {net_gex}")

    def _num(v):
        if isinstance(v, dict):
            return v.get("strike") or v.get("price") or v.get("level")
        return v
    cw = _num(lv.get("call_wall")); pw = _num(lv.get("put_wall"))
    gf = _num(lv.get("gamma_flip")); mp = _num(lv.get("max_pain"))
    # ── FALLBACK ES=F: si /levels no dio walls, derivarlos del per-strike REAL ──
    # call_wall = strike con mayor call_gex · put_wall = strike con put_gex más negativo
    if (cw is None or pw is None) and isinstance(per_strike, list) and per_strike:
        try:
            _calls = [s for s in per_strike if (s.get("call_gex") or 0) > 0]
            _puts  = [s for s in per_strike if (s.get("put_gex") or 0) < 0]
            if cw is None and _calls:
                cw = max(_calls, key=lambda s: s.get("call_gex") or 0).get("strike")
            if pw is None and _puts:
                pw = min(_puts, key=lambda s: s.get("put_gex") or 0).get("strike")
            print(f"[gex] walls derivados del per-strike: CW={cw} PW={pw}")
        except Exception as _e:
            print(f"[gex] derivación de walls falló: {_e}")
    # flip: preferir el de /levels; si no llegó, usar el del /gex (futuro directo)
    if gf is None and gex_flip is not None:
        gf = gex_flip
        print(f"[gex] gamma_flip tomado de /gex: {gf}")
    # Max Pain viene de endpoint separado /v1/maxpain (Basic+). /levels no lo trae.
    # Max pain sale del OPEN INTEREST → cambia ~1 vez al día, pero se pedía en los
    # 28 refreshes. Cacheado por día: 27 requests menos.
    global _gex_maxpain_val, _gex_maxpain_day
    if mp is None and _gex_maxpain_day == _today_now and _gex_maxpain_val is not None:
        mp = _gex_maxpain_val
        print(f"[gex] max_pain del cache del día: {mp} — 0 créditos")
    elif mp is None and _gex_maxpain_failed_day != _today_now:
        try:
            async with httpx.AsyncClient(timeout=12,
                                          headers={"X-Api-Key": FLASHALPHA_KEY}) as mpc:
                _fa_charge()
                r_mp = await mpc.get(f"{FA_BASE}/v1/maxpain/{sym_url}")
            if r_mp.status_code == 200:
                mpd = r_mp.json() or {}
                mp = _num(mpd.get("max_pain") or mpd.get("maxpain") or mpd.get("max_pain_strike"))
                if mp is not None:
                    _gex_maxpain_val = mp      # cache del día → no repetir 27 veces
                    _gex_maxpain_day = _today_now
                print(f"[gex] max_pain ({sym}): {mp}")
            else:
                _gex_maxpain_failed_day = _today_now   # no reintentar hoy (ahorra créditos)
                print(f"[gex] /maxpain/{sym} status {r_mp.status_code} — skip resto del día")
        except Exception as e:
            print(f"[gex] maxpain falló (no crítico): {e}")
    # ── FALLBACK: MAX PAIN DERIVADO del per-strike REAL (call_oi/put_oi) ──
    # Max pain = strike donde el payoff total de las opciones es MÍNIMO al expirar.
    # Es un cálculo sobre data real de la cadena (Regla #1: no es invento).
    if mp is None and isinstance(per_strike, list) and len(per_strike) >= 3:
        try:
            _sts = [s for s in per_strike if s.get("strike") is not None]
            _K   = [float(s["strike"]) for s in _sts]
            _coi = [float(s.get("call_oi") or 0) for s in _sts]
            _poi = [float(s.get("put_oi") or 0) for s in _sts]
            best_k, best_pay = None, None
            for k_exp in _K:
                pay = 0.0
                for i in range(len(_K)):
                    pay += _coi[i] * max(0.0, k_exp - _K[i])   # calls ITM
                    pay += _poi[i] * max(0.0, _K[i] - k_exp)   # puts ITM
                if best_pay is None or pay < best_pay:
                    best_pay, best_k = pay, k_exp
            if best_k is not None:
                mp = best_k
                print(f"[gex] max_pain derivado del per-strike (OI real): {mp}")
        except Exception as _e:
            print(f"[gex] derivación max_pain falló: {_e}")
    # ATM IV real del summary de NDX (para Expected Move real, no mock).
    # spot se inicializa aquí a propósito: se asigna dentro del try de abajo y se
    # lee DESPUÉS del except. Sin esto, si el summary falla → NameError.
    atm_iv = None; exp_move = None; spot = None
    fear_score = None; fear_rating = None; vix_value = None
    # NOTA sobre cachear el /summary: se evaluó y se DESCARTÓ. Da Fear&Greed y VIX
    # (macro, lentos) pero TAMBIÉN el `spot` del futuro, y del spot sale el ratio
    # ES/SPY que usan el precio y las velas. Cachearlo 1h ahorraría ~21 créditos
    # pero dejaría el ratio con hasta 1h de antigüedad. Con 86/100 ya cabemos, así
    # que no se cambia frescura del dato por créditos que no hacen falta.
    try:
        async with httpx.AsyncClient(timeout=12,
                                      headers={"X-Api-Key": FLASHALPHA_KEY}) as ivc:
            _fa_charge()
            r_sum = await ivc.get(f"{FA_BASE}/v1/stock/{sym_url}/summary")
            # FALLBACK de MACRO. Antes caía a NDX con el argumento de que "el ATM
            # IV del NDX ≈ NQ (mismo subyacente Nasdaq-100)" — cierto operando el
            # NQ, FALSO operando el ES: la IV del Nasdaq no es la del S&P. Se pasa
            # al índice del instrumento (FA_MACRO_FALLBACK=SPX) y, sobre todo, del
            # fallback SOLO se acepta el bloque MACRO (Fear&Greed/VIX), que es de
            # mercado e idéntico para todos. La IV y el expected move del fallback
            # se DESCARTAN: sin IV del propio instrumento, "—" (Regla #1).
            _used_fallback = False
            if r_sum.status_code != 200 and sym != FA_MACRO_FALLBACK:
                if budget_ok("flashalpha", 1):
                    print(f"[gex] summary {sym} {r_sum.status_code} → fallback "
                          f"{FA_MACRO_FALLBACK} (SOLO macro)")
                    budget_charge("flashalpha", 1)
                    r_sum = await ivc.get(f"{FA_BASE}/v1/stock/{FA_MACRO_FALLBACK}/summary")
                    _used_fallback = True
                else:
                    print("[gex] sin presupuesto para el fallback de macro — se omite")
        if r_sum.status_code == 200:
            sd = r_sum.json() or {}
            # HUECO CERRADO: futuros pueden dar 200 con summary PARCIAL (sin
            # bloque macro ni IV). Si falta lo esencial, re-pedir el de NDX.
            _has_macro = bool((sd.get("macro") or {}).get("fear_and_greed"))
            _has_iv = bool(sd.get("atm_iv") or (sd.get("volatility") or {}).get("atm_iv"))
            if (not _has_macro and not _has_iv) and sym != FA_MACRO_FALLBACK \
               and budget_ok("flashalpha", 1):
                print(f"[gex] summary {sym} 200 pero SIN macro/IV → fallback "
                      f"{FA_MACRO_FALLBACK} (SOLO macro)")
                budget_charge("flashalpha", 1)
                r2 = await ivc.get(f"{FA_BASE}/v1/stock/{FA_MACRO_FALLBACK}/summary")
                if r2.status_code == 200:
                    sd = r2.json() or {}
                    _used_fallback = True
            vol = sd.get("volatility", {}) or {}
            # ATM IV puede venir como campo directo o anidado en 'volatility'.
            # Probar varias ubicaciones para robustez (la doc varía por símbolo).
            atm_iv = (sd.get("atm_iv") or vol.get("atm_iv") or
                      sd.get("atm_implied_volatility") or vol.get("iv") or
                      sd.get("iv"))
            # Regla #1: si 'sd' viene del fallback, su IV es la de OTRO índice.
            # El bloque macro (Fear&Greed/VIX) sí es de mercado y se conserva;
            # la IV y el expected move se descartan → la UI muestra "—".
            if _used_fallback:
                if atm_iv is not None:
                    print(f"[gex] IV del fallback {FA_MACRO_FALLBACK} DESCARTADA "
                          f"(no es la de {sym})")
                atm_iv = None
            # Precio: directo, en price{}, o como underlying.
            pr = sd.get("price", {})
            if isinstance(pr, (int, float)):
                spot = pr
            else:
                spot = ((pr or {}).get("mid") or (pr or {}).get("last") or
                        sd.get("spot") or sd.get("underlying_price") or
                        sd.get("last"))
            # ── FEAR & GREED + VIX del bloque macro (mismo summary, 0 llamadas extra) ──
            # Se muestran EXACTAMENTE como los manda FlashAlpha (sin traducir).
            _macro = sd.get("macro", {}) or {}
            _fg = _macro.get("fear_and_greed", {}) or {}
            fear_score  = _fg.get("score")
            fear_rating = _fg.get("rating")   # ej: "fear", "greed", "extreme fear"
            _vix = _macro.get("vix", {}) or {}
            vix_value = _vix.get("value")
            # ATM IV puede venir en fracción (0.249) o en % (24.9). Normalizar a %.
            if isinstance(atm_iv, (int, float)) and atm_iv < 3:
                atm_iv = round(atm_iv * 100, 2)
            # Expected Move diario = spot * (atm_iv/100) * sqrt(1/252)
            if atm_iv and spot:
                import math
                exp_move = round(spot * (atm_iv/100.0) * math.sqrt(1/252.0), 1)
            print(f"[gex] ATM IV (NDX): {atm_iv}  Exp Move: {exp_move}")
    except Exception as e:
        print(f"[gex] summary/atm_iv falló (no crítico): {e}")
    # El futuro directo ya está en escala del índice (~ES). Los niveles NO se convierten.
    # OJO: 'spot' SÍ se guarda (antes se tiraba con underlying_price=None). Es el
    # precio REAL del subyacente que manda FlashAlpha, y es la fuente honesta del
    # ratio ES/SPY que usan el precio del heatmap y las velas del chart. Sin él, el
    # ratio quedaba congelado en una constante (bug histórico: 41.51 para NQ/QQQ).
    if spot:
        _set_px_ratio_from_spot(spot)
    cache["gex"][asset] = {
        "underlying_price": spot,         # precio real del subyacente (para el ratio)
        "call_wall": cw, "put_wall": pw, "gamma_flip": gf, "max_pain": mp,
        "net_gex": net_gex,
        "atm_iv": atm_iv,
        "expected_move": exp_move,
        "fear_score": fear_score,     # tal cual FlashAlpha (0-100)
        "fear_rating": fear_rating,   # tal cual FlashAlpha ("fear","greed",...)
        "vix": vix_value,
        # Régimen: EXACTAMENTE la etiqueta de FlashAlpha (net_gex_label).
        # Fallback al signo del net_gex solo si la etiqueta no llegó.
        "regime": (("trending" if "neg" in str(gex_label).lower() else "pinning")
                   if gex_label
                   else ("pinning" if (isinstance(net_gex,(int,float)) and net_gex>=0)
                         else "trending" if isinstance(net_gex,(int,float)) else None)),
        "ticker": sym, "as_of": None,
        "per_strike_count": len(per_strike) if isinstance(per_strike, list) else 0,
        "source": ("futures-direct" if "=" in sym else "index-direct"), "_ts": time.time(),
    }
    if cw is None and pw is None and gf is None:
        cache["health"]["flashalpha"] = "online-no-levels"
        print(f"[gex] ⚠️ {sym} 200 sin niveles. keys={list(lv.keys())}")
    else:
        cache["health"]["flashalpha"] = "online"
        print(f"[gex] ok ({sym} directo): flip={gf} call={cw} put={pw} maxpain={mp} netgex={net_gex}")
    # Publicar el SPOT del futuro en el heatmap como tile directo. Sin esto, el
    # frontend no tiene el precio del NQ en el heatmap (Finnhub no da futuros) y
    # cae a QQQ×ratio; si el ratio se calculó mal (1.0), muestra el precio del ETF
    # (~708) con etiqueta del NQ → viola la Regla #1. Con el tile directo, el
    # frontend usa el spot real tal cual, sin ratio.
    if isinstance(spot, (int, float)) and spot > 0:
        _prev = (cache["heatmap"]["data"].get(FA_ASSET, {}) or {}).get("price")
        _chg = None
        try:
            _etf_hm = (cache["heatmap"]["data"].get(FA_PROXY_ETF, {}) or {})
            _chg = _etf_hm.get("chg_pct")   # el % del futuro ≈ el del ETF proxy
        except Exception:
            pass
        cache["heatmap"]["data"][FA_ASSET] = {
            "symbol": FA_ASSET, "price": round(float(spot), 2),
            "chg_pct": _chg,
            "direction": ("up" if (_chg or 0) > 0.03 else
                          ("down" if (_chg or 0) < -0.03 else "flat")),
            "source": "direct",
        }
    # Archivar el snapshot: cada refresh cuesta creditos y es irrepetible.
    append_gex_history(asset, cache["gex"][asset])
    save_cache()

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

# ═══ CALENDARIO EN TIEMPO REAL — Capa RapidAPI (rellena el "actual") ═══
# Descarta BLS/BEA/Census por su retraso de 1 día; RapidAPI da el actual
# a los minutos del release. Se fusiona con ForexFactory.
_RT_RELEVANT = [
    "non-farm","nonfarm","non farm","nfp","payroll","employment",
    "cpi","core cpi","ppi","core ppi","pce","inflation","inflation rate",
    "fomc","federal funds","interest rate","rate decision","fed","powell",
    "gdp","retail sales","ism","services pmi","manufacturing pmi","pmi",
    "jolts","adp","jobless claims","initial claims","unemployment","michigan",
    "consumer confidence","consumer sentiment","durable goods","building permits",
    "housing starts","trade balance","factory orders","industrial production",
]

def _rt_relevant(name):
    n = (name or "").lower()
    return any(k in n for k in _RT_RELEVANT)

# ── Parser de números económicos con formato de texto ────────────────────────
# Extrae el número a ESCALA NATURAL (57K → 57, €19.1B → 19.1, 4.2% → 4.2,
# A$-3.018B → -3.018). Arregla el bug del parser viejo, que no quitaba € £ ¥ ni
# B y devolvía None para esos casos. NO multiplica por el sufijo a propósito:
#   • la clasificación usa solo el SIGNO de (actual - forecast)
#   • el % se calcula como ratio → el sufijo se cancela
#   • y así el "diff" que muestra la card queda a la misma escala que "57K/110K"
# Como actual y forecast de un mismo evento siempre traen la misma unidad, la
# sorpresa y su signo son correctos sin multiplicar.
def _parse_econ_num(v):
    if v is None: return None
    s = str(v).strip()
    if not s: return None
    cleaned = re.sub(r"[^0-9.\-]", "", s)     # quita $, €, £, ¥, %, K/M/B/T, letras
    if cleaned in ("", "-", ".", "-.", "--"): return None
    try:
        return float(cleaned)
    except ValueError:
        return None

def _rt_classify(name, actual, consensus):
    """Sorpresa + clasificación del instrumento desde el dato en tiempo real."""
    a, c = _parse_econ_num(actual), _parse_econ_num(consensus)
    if a is None or c is None:
        return None, None
    surprise = round(a - c, 2)
    nl = (name or "").lower()
    higher_bearish = any(k in nl for k in ["cpi","ppi","inflation","claims","unemployment","jobless","pce"])
    if abs(surprise) < 0.001: cls = "Neutral"
    elif higher_bearish: cls = "Bearish" if surprise > 0 else "Bullish"
    else: cls = "Bullish" if surprise > 0 else "Bearish"
    return surprise, cls

_rapidapi_last_call = 0   # timestamp de la última llamada real a RapidAPI
_rapidapi_day = ""        # día ET actual (YYYY-MM-DD) del contador diario
_rapidapi_day_count = 0   # llamadas hechas hoy (plan free TradingEconomics: 100/día)
# ── ForexFactory: límite 2 descargas/5min (todas las URLs juntas) ──
FF_URLS = [
    "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
    "https://nfs.faireconomy.media/ff_calendar_nextweek.json",
]
_ff_last_fetch = -9999  # permite la primera descarga de inmediato
_ff_cache = []
_fmp_last_fetch = 0   # timestamp última llamada a FMP
_fmp_cache = []       # último resultado bueno de FMP        # último resultado bueno de ForexFactory (límite 2/5min)

_RT_NON_US_COUNTRIES = {
    "australia","canada","euro area","euro zone","eurozone","european union",
    "germany","france","italy","spain","netherlands","united kingdom","britain",
    "japan","china","new zealand","switzerland","mexico","brazil","india",
    "south korea","norway","sweden","au","ca","eu","gb","uk","jp","cn","nz",
    "ch","mx","br","in","kr","no","se",
}
_US_NAMES = {  # nombres inequívocamente US aunque falte el campo country
    "nfp","nonfarm","non farm","non-farm","fomc","jolts","adp","ism",
    "michigan","initial claims","jobless claims",
}

async def _fetch_rapidapi_actuals(client):
    """Consulta la API de TradingEconomics (RapidAPI) para el 'actual' en tiempo real.
    Devuelve SOLO eventos US high-impact ya publicados (resolved=true) con su
    actual/forecast/previous. Se fusiona con ForexFactory rellenando lo que FF
    todavía no marca. Si no está configurada o falla, devuelve [] (FF + FMP cubren).

    Presupuesto: plan free = 100 llamadas/día. Se llama solo en las ventanas de
    releases macro US y con guard de 3 min → ~50-75 llamadas/día, margen sano."""
    if not RAPIDAPI_KEY:
        return []
    # Kill-switch: pon RAPIDAPI_ENABLED=false en Railway para apagarla. Default ON.
    if os.getenv("RAPIDAPI_ENABLED", "true").lower() == "false":
        return []

    now_et = datetime.now(NY)
    is_weekday = now_et.weekday() < 5           # lun=0 ... vie=4
    h, m = now_et.hour, now_et.minute
    # ── SOLUCIÓN PERMANENTE: dos capas de polling ───────────────────────────
    # · RÁFAGA en ventanas de release (8-11am, 1:45-2:30pm ET): 1 llamada / 4 min
    #   → captura el 'actual' minutos después de publicarse.
    # · BACKFILL horario el resto del día hábil (6am-6pm ET): 1 llamada / 55 min
    #   → garantiza que los 'actual' del día SIEMPRE llegan aunque un redeploy
    #     borre el cache, la fuente publique tarde o el evento caiga fuera de
    #     ventana. Nunca más un calendario con 'Esperando dato…' eterno.
    # Presupuesto: ~56 (ráfaga) + ~9 (backfill) ≈ 65 de 100/día. Margen sano.
    in_release_window = ((8 <= h < 11) or (h == 13 and m >= 45) or (h == 14 and m <= 30))
    in_backfill_hours = (6 <= h < 18)
    if not is_weekday:
        return cache.get("_rapidapi_cache", [])   # fin de semana: no hay releases US
    if in_release_window:
        min_gap = 360        # ráfaga: cada 6 min (2 llamadas/ciclo = resueltos+próximos)
    elif in_backfill_hours:
        min_gap = 3300       # backfill: cada 55 min
    else:
        return cache.get("_rapidapi_cache", [])   # madrugada: sin llamadas

    global _rapidapi_last_call, _rapidapi_day, _rapidapi_day_count
    nowts = time.time()
    # ── CONTADOR DIARIO: hard-stop a 85/día (margen del límite 100) ──
    cur_day = now_et.strftime("%Y-%m-%d")
    if _rapidapi_day != cur_day:
        _rapidapi_day = cur_day
        _rapidapi_day_count = 0                   # reset al cambiar de día
    if _rapidapi_day_count >= 85:
        return cache.get("_rapidapi_cache", [])   # presupuesto diario agotado
    if nowts - _rapidapi_last_call < min_gap:
        return cache.get("_rapidapi_cache", [])
    _rapidapi_last_call = nowts
    _rapidapi_day_count += 2  # 2 llamadas por ciclo (resueltos + próximos)

    headers = {"X-RapidAPI-Key": RAPIDAPI_KEY, "X-RapidAPI-Host": RAPIDAPI_HOST}
    # NOTA: el filtro country=United States devolvía count:0 en esta API, así que
    # NO lo enviamos y filtramos US en código (el campo country SÍ viene en la
    # respuesta cuando se pide en `fields`). daysBehind=7 cubre los resultados US
    # de toda la semana de trading (NFP, CPI, etc. ya publicados) para que el
    # dashboard no se vea vacío entre releases; el merge es fill-only, sin overwrite.
    def _parse_te_events(raw):
        parsed = []
        for ev in raw if isinstance(raw, list) else []:
            name = ev.get("eventName") or ev.get("event") or ev.get("title", "")
            if not name:
                continue
            country = (ev.get("country") or "").strip().lower()
            actual = ev.get("actual")
            av = str(actual or "")
            nl = name.lower()
            if country and country in _RT_NON_US_COUNTRIES:
                continue
            is_us = (country in ("united states", "us", "usa", "u.s.",
                                 "united states of america")
                     or (av.startswith("$")) or any(k in nl for k in _US_NAMES))
            if country and not is_us:
                continue
            if not _rt_relevant(name):
                continue
            consensus = ev.get("forecast") or ev.get("consensus") or ev.get("estimate")
            previous = ev.get("previous") or ev.get("prev")
            date = ev.get("date") or ev.get("dateUtc") or ev.get("time", "")
            surprise, cls = _rt_classify(name, actual, consensus)
            parsed.append({
                "title": name, "date": date,
                "actual": str(actual) if actual is not None else None,
                "consensus": str(consensus) if consensus is not None else None,
                "previous": str(previous) if previous is not None else None,
                "surprise": surprise, "classification": cls,
            })
        return parsed

    base_params = {
        "impact": "High", "descriptions": "false",
        "sort": "asc", "limit": "80", "tz": "America/New_York",
        "fields": "id,date,eventName,country,impactLabel,actual,forecast,previous",
    }
    try:
        url = f"https://{RAPIDAPI_HOST}/calendar"
        # (A) RESUELTOS últimos 7 días → traen el 'actual' (NFP ya publicado, etc.)
        rp = await client.get(url, headers=headers, timeout=10, params={
            **base_params, "daysBehind": "7", "daysAhead": "0", "resolved": "true"})
        # (B) PRÓXIMOS 3 días → traen forecast/previous ANTES del release, así el
        #     NFP de hoy muestra previo/forecast desde temprano (no "Esperando").
        ru = await client.get(url, headers=headers, timeout=10, params={
            **base_params, "daysBehind": "0", "daysAhead": "3", "resolved": "false"})
        out = []
        for r in (rp, ru):
            if r.status_code != 200:
                print(f"[rt-calendar] TradingEconomics status {r.status_code}: {r.text[:100]}")
                continue
            data = r.json()
            raw = data.get("events") if isinstance(data, dict) else data
            out.extend(_parse_te_events(raw))
        # Dedup por (título canónico + día): si un evento vino en ambas, prefiere
        # el que tenga 'actual' (el resuelto gana sobre el próximo).
        merged = {}
        for e in out:
            k = (_canon_event(e["title"]), (e.get("date","") or "")[:10])
            if k not in merged or (e.get("actual") and not merged[k].get("actual")):
                merged[k] = e
        out = list(merged.values())
        if not out and not (rp.status_code == 200 or ru.status_code == 200):
            return cache.get("_rapidapi_cache", [])
        released = sum(1 for e in out if e["actual"])
        upcoming = sum(1 for e in out if not e["actual"] and e["consensus"])
        print(f"[rt-calendar] TradingEconomics: {len(out)} eventos US "
              f"({released} con actual, {upcoming} próximos con forecast) "
              f"[llamadas {_rapidapi_day_count}/85 hoy]")
        cache["_rapidapi_cache"] = out
        return out
    except Exception as e:
        print(f"[rt-calendar] TradingEconomics error: {e}")
        return cache.get("_rapidapi_cache", [])

# ── Canonicalización de nombres de eventos ───────────────────────────────────
# ForexFactory y TradingEconomics usan nombres DISTINTOS para el mismo evento
# (ej. "Unemployment Claims" vs "Initial Jobless Claims"). Sin esto, el merge no
# rellena el 'actual' aunque una fuente lo tenga. Mapea variantes a una clave común.
_EVENT_ALIASES = [
    # ── Claims (continuing ANTES que jobless: "continuing jobless claims" no debe caer en jobless) ──
    ("continuing_claims",  ["continued jobless claims", "continuing jobless claims", "continued claims", "continuing claims"]),
    ("jobless_claims",     ["unemployment claims", "initial jobless claims", "jobless claims", "initial claims"]),
    # ── Empleo (adp ANTES que nfp: "adp non-farm..." no debe caer en nfp) ──
    ("adp",                ["adp non-farm employment change", "adp employment change", "adp nonfarm", "adp employment", "adp"]),
    ("nfp",                ["non-farm employment change", "nonfarm payrolls", "non farm payrolls", "non-farm payrolls", "nonfarm payroll", "nfp"]),
    ("avg_earnings_mom",   ["average hourly earnings mom"]),
    ("avg_earnings_yoy",   ["average hourly earnings yoy"]),
    ("unemployment_rate",  ["unemployment rate"]),
    ("participation_rate", ["participation rate"]),
    # ── Inflación: CORE antes que base; mom/yoy separados ──
    ("core_cpi_mom",       ["core cpi mom", "core inflation rate mom", "core consumer price index mom"]),
    ("core_cpi_yoy",       ["core cpi yoy", "core inflation rate yoy", "core consumer price index yoy"]),
    ("cpi_mom",            ["cpi mom", "inflation rate mom", "consumer price index mom"]),
    ("cpi_yoy",            ["cpi yoy", "inflation rate yoy", "consumer price index yoy"]),
    ("core_ppi",           ["core ppi", "core producer prices", "core producer price index"]),
    ("ppi",                ["ppi mom", "ppi yoy", "producer price index", "producer prices", "ppi"]),
    ("core_pce",           ["core pce"]),
    ("pce",                ["pce price index", "pce"]),
    # ── Consumo / retail: CORE / ex-autos antes que base ──
    ("core_retail_sales",  ["core retail sales", "retail sales ex autos", "retail sales ex auto", "retail sales control group"]),
    ("retail_sales",       ["retail sales"]),
    # ── Actividad / sentimiento ──
    ("ism_services",       ["ism services pmi", "ism non-manufacturing pmi", "ism services", "services pmi"]),
    ("ism_manufacturing",  ["ism manufacturing pmi", "ism manufacturing", "manufacturing pmi"]),
    ("gdp",                ["gdp growth rate", "gross domestic product", "advance gdp", "gdp"]),
    ("michigan",           ["michigan consumer sentiment", "uom consumer sentiment", "consumer sentiment", "michigan"]),
    ("consumer_confidence",["consumer confidence", "cb consumer confidence"]),
    ("jolts",              ["jolts job openings", "jolts", "job openings"]),
    # ── Tasas ──
    ("fomc",               ["fomc", "federal funds rate", "interest rate decision", "fed interest rate", "fed funds rate"]),
    # ── Bienes / vivienda: CORE antes que base ──
    ("core_durable_goods", ["core durable goods"]),
    ("durable_goods",      ["durable goods orders", "durable goods"]),
    ("building_permits",   ["building permits"]),
    ("housing_starts",     ["housing starts"]),
    ("existing_home_sales",["existing home sales"]),
    ("new_home_sales",     ["new home sales"]),
]

def _canon_event(name):
    """Reduce un nombre de evento a una clave canónica común entre fuentes.
    Unifica la notación de periodo (m/m == mom, y/y == yoy, q/q == qoq) SIN
    colapsar mom con yoy, para que 'CPI m/m' y 'CPI y/y' sigan siendo distintos."""
    n = re.sub(r"\s+", " ", (name or "").lower().strip())
    # m/m ↔ mom, y/y ↔ yoy, q/q ↔ qoq (misma medida, distinta escritura entre fuentes)
    n = n.replace("m/m", "mom").replace("y/y", "yoy").replace("q/q", "qoq")
    n = re.sub(r"\s+", " ", n).strip()
    for canon, aliases in _EVENT_ALIASES:
        for a in aliases:
            if a in n:
                return canon
    # Sin alias conocido → devolver el nombre normalizado (mom/yoy se conservan
    # como tokens distintos, así dos variantes del mismo evento NO colisionan).
    return n

def _merge_rapidapi(ff_events, rt_actuals):
    """Fusiona TradingEconomics con ForexFactory:
    1) Rellena el 'actual'/forecast/previous que a FF le falta (match por nombre
       canónico + fecha → 'Unemployment Claims' ≡ 'Initial Jobless Claims').
    2) AÑADE eventos US que FF ya no lista (ej. NFP de la semana pasada, que sale
       del feed 'thisweek') con su previo/forecast/resultado completos — así un
       evento reciente y relevante nunca aparece vacío ni desaparece del panel."""
    def norm(t):
        return _canon_event(t)
    rt_index = {}
    for e in rt_actuals:
        d = (e.get("date","") or "")[:10]
        rt_index[(norm(e["title"]), d)] = e
    ff_keys = set()
    for ev in ff_events:
        d = (ev.get("time","") or ev.get("date","") or "")[:10]
        key = (norm(ev.get("title","") or ev.get("name","")), d)
        ff_keys.add(key)
        rt = rt_index.get(key)
        if rt and rt.get("actual"):
            if not ev.get("actual"):
                ev["actual"] = rt["actual"]; ev["status"] = "Released"
            if not ev.get("forecast") and rt.get("consensus"):
                ev["forecast"] = rt["consensus"]
            if not ev.get("previous") and rt.get("previous"):
                ev["previous"] = rt["previous"]
            if rt.get("surprise") is not None:
                ev["surprise"] = rt["surprise"]; ev["classification"] = rt["classification"]
    # (2) Inyectar eventos de TE que FF no tiene. Incluye PRÓXIMOS (con forecast/
    # previous aunque falte actual) para que el NFP de hoy muestre datos desde
    # temprano, no solo tras publicarse.
    for key, rt in rt_index.items():
        if key in ff_keys:
            continue
        if not (rt.get("actual") or rt.get("consensus") or rt.get("previous")):
            continue
        ff_events.append({
            "title": rt["title"], "time": rt.get("date",""), "impact": "high",
            "actual": rt.get("actual"),
            "forecast": rt.get("consensus"), "previous": rt.get("previous"),
            "status": "Released" if rt.get("actual") else "Upcoming",
            "type": "macro",
            "surprise": rt.get("surprise"), "classification": rt.get("classification"),
            "_from": "tradingeconomics",
        })
    return ff_events


async def refresh_calendar():
    """Calendar with parallel fetch, Finnhub fallback, stale cache preservation."""
    FF_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json,*/*",
        "Cache-Control": "no-cache",
    }

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
            # IMPORTANTE: ForexFactory limita a 2 descargas cada 5 min (todas las
            # URLs juntas). NO usamos cache-busting porque eso fuerza descargas
            # repetidas y nos bloquean con "Request Denied". Dejamos que el CDN
            # sirva su versión (se actualiza solo cada pocos minutos de todas formas).
            r = await client.get(url, timeout=8)
            if r.status_code != 200:
                print(f"[ff] status {r.status_code} en {url[:50]}")
                return []
            # Detectar página de bloqueo "Request Denied" (HTML en vez de JSON)
            ctype = r.headers.get("content-type", "")
            if "json" not in ctype.lower():
                print(f"[ff] BLOQUEADO por ForexFactory (Request Denied) — usando otras fuentes")
                return []
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

    async def _fetch_fmp(client):
        """FMP economic calendar. ⚠️ El endpoint /economic-calendar es de PAGO
        (devuelve 402 en plan free — no es límite de cuota, es muro de suscripción).
        Apagado por defecto. Si algún día pagas FMP, pon FMP_ENABLED=true en Railway."""
        if not FMP_KEY: return []
        if os.getenv("FMP_ENABLED", "false").lower() != "true":
            return []  # endpoint premium; evita llamadas 402 inútiles cada ciclo
        if not budget_ok("fmp", 1):
            print("[calendar] presupuesto FMP agotado — se omite FMP")
            return []
        budget_charge("fmp", 1)
        try:
            now_et = datetime.now(NY)
            frm = now_et.strftime("%Y-%m-%d")
            to  = (now_et + __import__('datetime').timedelta(days=7)).strftime("%Y-%m-%d")
            # Endpoint /stable/ nuevo (el legacy /api/v3/economic_calendar da 403
            # para cuentas creadas después de agosto 2025).
            r = await client.get(f"{FMP_BASE}/economic-calendar",
                params={"from": frm, "to": to, "apikey": FMP_KEY}, timeout=8)
            if r.status_code != 200:
                print(f"[calendar] FMP status {r.status_code}: {r.text[:120]}")
                return []
            data = r.json()
            if not isinstance(data, list): return []
            events = []
            for ev in data:
                if (ev.get("country","") or "").upper() not in ("US","USA","UNITED STATES"): continue
                name = ev.get("event","")
                if not name: continue
                imp_raw = (ev.get("impact","") or "").lower()
                imp_map = {"high":"high","medium":"med","low":"low"}
                impact = imp_map.get(imp_raw, "med")
                if impact == "low": continue
                actual = ev.get("actual")
                events.append({
                    "title": name, "time": ev.get("date",""), "impact": impact,
                    "actual": str(actual) if actual is not None else None,
                    "forecast": str(ev.get("estimate","")) if ev.get("estimate") is not None else None,
                    "previous": str(ev.get("previous","")) if ev.get("previous") is not None else None,
                    "status": "Released" if actual is not None else "Upcoming",
                    "type": "macro", "source": "fmp",
                })
            with_a = sum(1 for e in events if e["actual"])
            print(f"[calendar] FMP: {len(events)} eventos US, {with_a} con actual")
            return events
        except Exception as e:
            print(f"[calendar] FMP error: {e}"); return []

    stale_backup = list(cache["calendar"]["data"])  # preserve last known good

    async with httpx.AsyncClient(headers=FF_HEADERS, follow_redirects=True) as client:
        # ── Fetch AMBAS fuentes en paralelo (ForexFactory + Finnhub) ──────────
        # Merge para capturar resultados "actual" de cualquier fuente que los tenga.
        # Esto resuelve el caso Building Permits: si FF no tiene el actual,
        # Finnhub lo provee, y viceversa.
        # ── GUARD ForexFactory: límite 2 descargas/5min → descargamos cada 3 min ──
        # Entre descargas usamos la última versión cacheada de FF. Finnhub y
        # RapidAPI sí corren cada ciclo (tienen límites más altos).
        nowts = time.time()
        nowet = datetime.now(NY)
        # ═══ ORQUESTACIÓN MULTI-FUENTE CON PRESUPUESTO ═══════════════════
        # Cada fuente tiene su propio límite. Gastamos créditos solo cuando aporta.
        # Jerarquía del "actual": FMP (rápido, 250/día) → ForexFactory (gratis,
        # base) → RapidAPI (reserva, 10/mes solo eventos enormes).
        global _fmp_last_fetch, _fmp_cache, _ff_last_fetch, _ff_cache, _rapidapi_last_call
        h, m = nowet.hour, nowet.minute
        # Ventana de releases macro US (ET): 8:00-10:30am y 1:45-2:30pm
        in_release_window = ((8 <= h < 11) or (h == 13 and m >= 45) or (h == 14 and m <= 30))

        # ── ForexFactory: cada 3 min (límite 2/5min) ──
        fetch_ff_now = (nowts - _ff_last_fetch >= 180)
        if fetch_ff_now:
            _ff_last_fetch = nowts
            ff_tasks = [_fetch_ff(client, url) for url in FF_URLS]
        else:
            ff_tasks = []

        # ── FMP: fuente PRINCIPAL del actual (RapidAPI cayó por 402).
        # Presupuesto: 250/día. Usamos ~190/día con margen:
        #   sesión (8am-4pm ET): cada 3 min → ~160 llamadas
        #   fuera de sesión: cada 10 min → ~30 llamadas
        h_now = nowet.hour
        fmp_in_session = (8 <= h_now < 16)
        fmp_interval = 180 if fmp_in_session else 600  # 3 min vs 10 min
        fetch_fmp_now = bool(FMP_KEY) and (nowts - _fmp_last_fetch >= fmp_interval)
        if fetch_fmp_now:
            _fmp_last_fetch = nowts
            fmp_task = [_fetch_fmp(client)]
        else:
            fmp_task = []

        # ── RapidAPI: reserva para eventos enormes (guard interno ya lo limita) ──
        rapid_task = [_fetch_rapidapi_actuals(client)]

        all_tasks = ff_tasks + fmp_task + rapid_task
        all_results = await asyncio.gather(*all_tasks, return_exceptions=True)

        # Separar resultados por posición conocida
        n_ff = len(ff_tasks)
        n_fmp = len(fmp_task)
        ff_fresh = []
        for i in range(n_ff):
            if isinstance(all_results[i], list):
                ff_fresh.extend([e for e in all_results[i] if e])
        fmp_events = []
        if n_fmp:
            fmp_raw = all_results[n_ff]
            if isinstance(fmp_raw, list):
                fmp_events = [e for e in fmp_raw if e]
                if fmp_events: _fmp_cache = fmp_events  # cachear último bueno
        # Si no llamamos FMP este ciclo, usar su cache
        if not fmp_events and _fmp_cache:
            fmp_events = list(_fmp_cache)
        rt_raw = all_results[n_ff + n_fmp] if len(all_results) > n_ff + n_fmp else []
        rt_actuals = [e for e in rt_raw if e] if isinstance(rt_raw, list) else []

        # Cachear FF
        if ff_fresh:
            _ff_cache = ff_fresh
        ff_events = list(_ff_cache)
        # fh_events ya no se usa para actual (Finnhub calendar es premium/403)
        fh_events = []

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
        # Luego FMP: fuente del "actual" rápida y confiable (250/día).
        # Si FMP tiene resultado y FF no → usar FMP. También rellena forecast/previous.
        for e in fmp_events:
            k = norm_key(e)
            if k in merged:
                existing = merged[k]
                if e.get("actual") and not existing.get("actual"):
                    existing["actual"] = e["actual"]
                    existing["status"] = "Released"
                if e.get("forecast") and not existing.get("forecast"):
                    existing["forecast"] = e["forecast"]
                if e.get("previous") and not existing.get("previous"):
                    existing["previous"] = e["previous"]
            else:
                merged[k] = e

        out = list(merged.values())

        # ── CAPA TIEMPO REAL: RapidAPI (máxima prioridad para el "actual") ──
        # Reserva para eventos enormes (NFP/CPI/FOMC). Su "actual" gana sobre
        # FF y FMP porque es el más cercano al release oficial.
        if rt_actuals:
            out = _merge_rapidapi(out, rt_actuals)

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
    # Usa el parser robusto (_parse_econ_num): maneja K/M/B/T, monedas y %.
    for e in out:
        actual = _parse_econ_num(e.get("actual"))
        forecast = _parse_econ_num(e.get("forecast"))
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

    # Diagnóstico: registrar eventos de hoy con su estado de datos (debug ADP, etc.)
    today_iso = datetime.now(NY).strftime("%Y-%m-%d")
    for e in deduped:
        if (e.get("time","") or "").startswith(today_iso):
            has_data = "✓" if (e.get("forecast") or e.get("previous") or e.get("actual")) else "✗ SIN DATOS"
            print(f"[calendar] HOY: {e.get('title','')[:30]:<30} fc={e.get('forecast')} prev={e.get('previous')} act={e.get('actual')} [{has_data}]")

    if deduped:
        cache["calendar"]["data"]        = deduped
        cache["calendar"]["last_update"] = datetime.now(NY).isoformat()
        cache["calendar"]["status"]      = "fresh"
        save_cache()  # persistir en Volume: los 'actual' del día sobreviven redeploys
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
    # Tech/Market leaders — score BAJO: NO pasan solos (umbral 8.5).
    # Solo aparecen si la noticia ALSO contiene un keyword sistémico mayor.
    "nvidia":          (6.5,"Technology","Corporate","bullish"),
    "nvda":            (6.5,"Technology","Corporate","bullish"),
    "apple":           (6.5,"Technology","Corporate","bullish"),
    "openai":          (7.0,"AI Sector","Corporate","bullish"),
    "tesla":           (6.5,"Auto/Tech","Corporate","bullish"),
    "microsoft":       (6.5,"Technology","Corporate","bullish"),
    # Figuras con capacidad real de mover mercados — SÍ son sistémicas
    "elon musk":       (8.8,"Tech/Market","Influencer","bearish"),
    "musk":            (8.6,"Tech/Market","Influencer","bearish"),
    "larry fink":      (9.0,"Entire Market","Institutional","bearish"),
    "blackrock":       (8.7,"Entire Market","Institutional","bearish"),
    "jamie dimon":     (8.6,"Entire Market","Institutional","bearish"),
    "jerome powell":   (9.8,"Entire Market","Central Bank","bearish"),
    "yellen":          (8.8,"Entire Market","Fiscal","bearish"),
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

    # Umbral mínimo — SOLO eventos SISTÉMICOS de alto impacto.
    # 8.5 excluye noticias corporativas aisladas (NVIDIA 7.5, Apple 7.5, Tesla 7.0)
    # y mantiene: Fed, CPI, NFP, geopolítica, Trump, tariffs, default, recession.
    if best_score < 8.5: return None

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
            fh_charge(2)  # 2 llamadas /news — registrar para contabilidad exacta
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

        # ── MEMORIA ACUMULADA (solución permanente) ─────────────────────────
        # El feed de Finnhub ROTA: una noticia crítica puede aparecer en un fetch
        # y no venir en el siguiente. Antes eso la borraba del panel y "volvían
        # las viejas". Ahora cada noticia clasificada se acumula en un store con
        # TTL de 12h; el top-6 se rankea sobre TODO lo visto, no solo el último
        # fetch. Una noticia solo sale del panel por antigüedad o por ser
        # superada en score — nunca porque el feed dejó de incluirla.
        store = cache.setdefault("_movers_seen", {})
        now_ts = time.time()
        for it in classified:
            key = (it.get("title") or "")[:80].lower().strip()
            if not key:
                continue
            prev = store.get(key)
            if prev:
                first = prev.get("_first_seen", now_ts)
                prev.update(it)
                prev["_first_seen"] = first
            else:
                it["_first_seen"] = now_ts
                store[key] = it
        # Poda por TTL: fuera noticias con timestamp (o primera vista) > 12h
        cutoff = now_ts - 12 * 3600
        for k in list(store):
            v = store[k]
            news_ts = v.get("ts") or 0
            if (news_ts and news_ts < cutoff) or (not news_ts and v.get("_first_seen", now_ts) < cutoff):
                del store[k]
        ranked = sorted(store.values(),
                        key=lambda x: (x.get("impact_score", 0), x.get("ts", 0)),
                        reverse=True)
        out = [{k: v for k, v in it.items() if k != "_first_seen"} for it in ranked[:6]]

        if out:
            cache["movers"]["data"]        = out
            cache["movers"]["last_update"] = datetime.now(NY).isoformat()
            cache["movers"]["status"]      = "fresh"
            cache["health"]["finnhub"]     = "online"
            save_cache()  # persistir en Volume: sobrevive redeploys
            print(f"[movers] ok: {len(out)} ultra-impact (store: {len(store)} en 12h)")
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
async def _session_profile_ctx():
    """IB de HOY + VAH/VAL/POC de la sesión RTH previa, derivados de velas 5min
    de TwelveData (QQQ×ratio → escala NQ). 1 llamada TD por briefing (~4/día).
    Aproximación de volume profile por bins de precio — etiquetada 'aprox'.
    Regla #1: si algo falla, devuelve [] y el briefing no lo menciona."""
    out = []
    if not TWELVEDATA_KEY or not budget_ok("twelvedata", 1):
        return out
    try:
        budget_charge("twelvedata", 1)
        url = ("https://api.twelvedata.com/time_series?symbol=QQQ&interval=5min"
               f"&outputsize=400&timezone=America/New_York&apikey={TWELVEDATA_KEY}")
        async with httpx.AsyncClient(timeout=12) as c:
            r = await c.get(url)
        vals = (r.json() or {}).get("values") or []
        if not vals:
            return out
        # ratio QQQ→NQ (misma lógica que gamma-levels)
        # get_px_ratio() ya deriva de spot real o de SPX/SPY real. Aquí NO se
        # reintenta la "verificación" que había antes (dividir el precio del
        # heatmap entre el del ETF): ese precio se calculaba multiplicando el ETF
        # por el ratio, así que dividirlo devolvía el mismo ratio → circular,
        # siempre confirmaba la constante. Y NO hay fallback hardcodeado: si no
        # hay dato real, ratio queda None y el llamador muestra "—" (Regla #1).
        ratio = get_px_ratio()
        if not ratio:
            return out   # sin ratio real no se estima perfil: mejor nada que mal
        now_et = datetime.now(NY)
        today = now_et.strftime("%Y-%m-%d")
        # días de sesión presentes (excluyendo hoy) → el último es "ayer hábil"
        days = sorted({v["datetime"][:10] for v in vals if v.get("datetime")})
        prev_days = [d for d in days if d < today]
        prev = prev_days[-1] if prev_days else None

        def _rth(v, day):
            dt = v.get("datetime", "")
            if not dt.startswith(day): return False
            hm = dt[11:16]
            return "09:30" <= hm < "16:00"

        # ── VAH/VAL/POC de AYER (perfil aprox por bins con volumen real) ──
        if prev:
            import math
            bins = {}
            for v in vals:
                if not _rth(v, prev): continue
                try:
                    h = float(v["high"]); l = float(v["low"]); vol = float(v.get("volume") or 0)
                except Exception:
                    continue
                if vol <= 0 or h <= 0: continue
                step = 0.25  # bin QQQ (~10pts NQ)
                lo_b = math.floor(l / step); hi_b = math.floor(h / step)
                n = max(1, hi_b - lo_b + 1)
                per = vol / n
                for b in range(lo_b, hi_b + 1):
                    bins[b] = bins.get(b, 0) + per
            if bins:
                total = sum(bins.values())
                poc_b = max(bins, key=bins.get)
                # value area 70%: expandir desde el POC
                inc = {poc_b}; acc = bins[poc_b]
                lo_e, hi_e = poc_b, poc_b
                while acc < total * 0.70:
                    up = bins.get(hi_e + 1, 0); dn = bins.get(lo_e - 1, 0)
                    if up <= 0 and dn <= 0: break
                    if up >= dn: hi_e += 1; acc += up; inc.add(hi_e)
                    else:        lo_e -= 1; acc += dn; inc.add(lo_e)
                step = 0.25
                poc = (poc_b * step + step / 2) * ratio
                vah = ((hi_e + 1) * step) * ratio
                val = (lo_e * step) * ratio
                out.append(f"- Perfil sesión previa (aprox, escala {FA_ASSET}): VAH {vah:.0f} | POC {poc:.0f} | VAL {val:.0f}")

        # ── IB de HOY (primera hora RTH 9:30-10:30), si ya ocurrió ──
        if now_et.hour > 10 or (now_et.hour == 10 and now_et.minute >= 30):
            ib_h = None; ib_l = None
            for v in vals:
                dt = v.get("datetime", "")
                if not dt.startswith(today): continue
                hm = dt[11:16]
                if "09:30" <= hm < "10:30":
                    try:
                        h = float(v["high"]); l = float(v["low"])
                    except Exception:
                        continue
                    ib_h = h if ib_h is None else max(ib_h, h)
                    ib_l = l if ib_l is None else min(ib_l, l)
            if ib_h and ib_l:
                out.append(f"- Initial Balance hoy (aprox, escala {FA_ASSET}): IBH {ib_h*ratio:.0f} | IBL {ib_l*ratio:.0f}")
    except Exception as e:
        print(f"[institutional] perfil de sesión falló (no crítico): {e}")
    return out


async def refresh_institutional():
    """Motor de IA institucional — genera análisis desde CUALQUIER dato disponible.
    Funciona 24/7: con o sin GEX, mercado abierto o cerrado, fin de semana.
    Construye contexto rico desde gamma, precio, correlaciones, calendario y earnings."""
    if not GROQ_KEY:
        cache["health"]["groq"] = "offline-no-key"; return
    if not budget_ok("groq", 1):
        print("[institutional] presupuesto Groq agotado — se mantiene último resumen")
        return
    budget_charge("groq", 1)

    gex = cache["gex"].get(FA_ASSET, {}) or {}
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

    # Precio del instrumento operado (vía heatmap)
    nq_data = hm.get(FA_ASSET, {})
    nq_price = nq_data.get("price")
    qqq = gex.get("underlying_price") or (hm.get(FA_PROXY_ETF, {}) or {}).get("price")
    if nq_price:
        ctx.append(f"- {FA_ASSET} Futures: {nq_price:.0f}")

    # Gamma (si está disponible)
    cw = gex.get("call_wall"); pw = gex.get("put_wall")
    gf = gex.get("gamma_flip"); ng = gex.get("net_gex")
    rg = gex.get("regime", "")
    has_gamma = bool(cw and pw and gf)
    if has_gamma:
        pdir = "sobre" if (nq_price and nq_price > gf) else "bajo"
        ctx.append(f"- Gamma: Call Wall {cw:.0f} | Put Wall {pw:.0f} | Flip {gf:.0f} | {FA_ASSET} {pdir} del flip")
        if ng: ctx.append(f"- Régimen dealer: {rg} | Net GEX: {ng:,.0f}")
        em = gex.get("expected_move"); iv = gex.get("atm_iv")
        if em: ctx.append(f"- Movimiento esperado: ±{em:.0f}pts | IV: {iv:.1f}%" if iv else f"- Movimiento esperado: ±{em:.0f}pts")
    else:
        ctx.append(f"- Gamma (GEX): pendiente de actualización (FlashAlpha {GEX_REFRESHES_PER_DAY}x/día)")

    # ── INVENTARIO OVERNIGHT (derivado del cambio vs cierre previo) ──
    # chg_pct del NQ = posición del precio vs settlement anterior. En pre-market
    # esto ES el inventario nocturno: positivo = inventario largo, negativo = corto.
    nq_chg = nq_data.get("chg_pct")
    if nq_chg is not None:
        inv = ("largo" if nq_chg > 0.15 else "corto" if nq_chg < -0.15 else "balanceado")
        ctx.append(f"- Inventario overnight: {inv} ({nq_chg:+.2f}% vs cierre previo)")

    # ── SENTIMIENTO (Fear & Greed de FlashAlpha, tal cual) + VIX ──
    _fs = gex.get("fear_score"); _fr = gex.get("fear_rating")
    if _fs is not None or _fr:
        ctx.append(f"- Fear & Greed: {_fs if _fs is not None else '?'}/100 ({_fr or '?'})")
    _vx = gex.get("vix")
    if _vx is not None:
        ctx.append(f"- VIX: {_vx}")

    # ── DISTANCIAS del precio a la estructura dealer (contexto operativo) ──
    if has_gamma and nq_price:
        try:
            ctx.append(f"- Distancias: al Call Wall {cw - nq_price:+.0f}pts | al Put Wall {pw - nq_price:+.0f}pts | al Flip {gf - nq_price:+.0f}pts")
        except Exception:
            pass

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

    # Perfil de sesión: VAH/VAL/POC de ayer + IB de hoy (data interna, sin APIs nuevas)
    try:
        ctx.extend(await _session_profile_ctx())
    except Exception:
        pass
    ctx_str = "\n".join(ctx)

    # ── Prompt adaptado a si hay gamma o no ───────────────────────────────────
    sys_msg = (f"Eres el analista jefe de mesa de Liberato Community: un trader institucional de {FA_ASSET} Futures "
               "que razona con teoría de subasta y posicionamiento dealer. Tu lector opera setups según el "
               "régimen del mercado: analiza inventario nocturno, estructura y niveles antes de decidir. "
               "Respondes SOLO en español, 3-4 oraciones en prosa (nunca listas). "
               "ESTRUCTURA OBLIGATORIA de tu razonamiento: "
               "(1) INVENTARIO: qué hizo la noche y qué implica para la apertura/sesión (corrección de "
               "inventario, continuación, gap para llenar). "
               "(2) ESTRUCTURA DEALER: régimen gamma y posición del precio vs flip/walls — en gamma negativo "
               "usa lenguaje de momentum/expansión (los dealers persiguen el precio); en gamma positivo usa "
               "lenguaje de reversión/compresión (los dealers absorben). "
               "(3) ESCENARIO OPERATIVO: el escenario más probable HOY combinando inventario+régimen+sentimiento, "
               "con el nivel exacto que lo invalidaría. "
               "El sentimiento (Fear&Greed/VIX) modula tu tono: con miedo alto advierte de movimientos bruscos "
               "y rangos amplios; con codicia señala complacencia y grinds. "
               "Adapta el análisis a la sesión (pre-market → plan de apertura; regular → lectura intradía; "
               "after-hours/cerrado → balance del día y contexto para mañana). "
               "PROHIBIDO: frases plantilla como 'el mercado se encuentra en', 'la presencia de'. Varía tu "
               "vocabulario entre briefings. Eres honesto: si un dato falta, no lo inventas ni lo mencionas. "
               "Si el contexto incluye VAH/VAL/POC o Initial Balance, intégralos al análisis "
               "(aceptación dentro/fuera del área de valor, ruptura o falla del IB). "
               "Si NO vienen en los datos, no los menciones ni los inventes.")

    if has_gamma:
        usr_msg = (f"Datos de mesa ahora mismo:\n\n{ctx_str}\n\n"
                   "Escribe el briefing institucional siguiendo tu estructura (inventario → estructura dealer → "
                   "escenario con nivel de invalidación). Usa los números exactos de los niveles.")
    else:
        usr_msg = (f"Datos de mesa ahora mismo (sin GEX disponible aún):\n\n{ctx_str}\n\n"
                   "Escribe el briefing con lo disponible: inventario overnight, sentimiento, macro y líderes. "
                   "Da el escenario de apertura más probable y qué confirmarlo/invalidarlo. No inventes niveles de gamma.")

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization":f"Bearer {GROQ_KEY}","Content-Type":"application/json"},
                json={"model":"llama-3.3-70b-versatile","max_tokens":420,"temperature":0.65,
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
    return {"status":"ok","version":"3.0-FIX40","engine":"TwelveData Realtime + Finnhub + FlashAlpha"}

@app.get("/health")
def health():
    """Health check rico — estado real de cada servicio con razones y contexto."""
    import time as _t
    now = datetime.now(NY)
    is_weekend   = now.weekday() >= 5                 # Sábado=5, Domingo=6
    is_rth       = 9 <= now.hour < 16 and not is_weekend
    gex_data     = cache["gex"].get(FA_ASSET, {})
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
                "realtime_symbols": WS_SYMBOLS + ["AAPL","MSFT","NVDA","META","AMZN","TSLA","GOOGL"],
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
            "disk_persistence":  bool(cache["gex"].get(FA_ASSET) or cache["institutional"]["text"] or cache["earnings"]["data"]),
        },
    }

# Calcula la próxima ventana programada de FlashAlpha (19:00, 9:00, 9:15, 9:45 ET)
def _next_gex_window():
    """Devuelve la próxima hora ET en que se actualizará el GEX."""
    windows = [(9,0),(9,15),(9,45),(19,0)]  # 4 ventanas estratégicas
    now = datetime.now(NY)
    now_min = now.hour*60 + now.minute
    today_windows = sorted([h*60+m for h,m in windows])
    # Buscar la próxima ventana hoy
    for wm in today_windows:
        if wm > now_min:
            wh, wmin = wm//60, wm%60
            return {"time": f"{wh:02d}:{wmin:02d} ET", "is_today": True}
    # No quedan hoy → primera de mañana (9:00 si es día hábil)
    nxt = now + timedelta(days=1)
    # Saltar fin de semana
    while nxt.weekday() >= 5:
        nxt = nxt + timedelta(days=1)
    return {"time": "09:00 ET", "is_today": False, "date": nxt.strftime("%d-%b")}

_candles_cache = {}   # {tf: {"ts": epoch, "data": {...}}}

@app.get("/api/admin/budget")
async def budget_status(key: str = ""):
    """Monitor de presupuesto de APIs en tiempo real.
    Uso: /api/admin/budget?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    real_limits = {"twelvedata":800,"finnhub":60,"flashalpha":100,
                   "fmp":250,"alphavantage":25,"groq":1000}
    out = {}
    for api, cfg in API_BUDGETS.items():
        st = _api_usage[api]
        out[api] = {
            "usados": st["used"],
            "limite_seguro": cfg["limit"],
            "limite_real_proveedor": real_limits.get(api, "?"),
            "restantes": cfg["limit"] - st["used"],
            "ventana": cfg["window"],
            "ventana_actual": st["window_key"],
            "pct_usado": round(st["used"]/cfg["limit"]*100, 1) if cfg["limit"] else 0,
        }
    return out


@app.get("/api/admin/diag-candles-iv")
async def diag_candles_iv(key: str = ""):
    """Diagnóstico: muestra qué responde TwelveData (velas) y FlashAlpha
    (summary/atm_iv) en CRUDO, para ver por qué fallan.
    Uso: /api/admin/diag-candles-iv?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    out = {}
    # ── 1. Probar time_series para cada símbolo candidato ──
    out["velas"] = {}
    for sym in (FA_ASSET, FA_INDEX_SYMBOL, FA_PROXY_ETF):
        try:
            url = (f"https://api.twelvedata.com/time_series?symbol={sym}"
                   f"&interval=5min&outputsize=3&apikey={TWELVEDATA_KEY}")
            async with httpx.AsyncClient(timeout=12) as client:
                r = await client.get(url)
            body = r.json() if r.status_code == 200 else r.text[:200]
            # Resumir: status TD, si trae values, y mensaje de error si hay
            info = {"http": r.status_code}
            if isinstance(body, dict):
                info["td_status"] = body.get("status")
                info["has_values"] = bool(body.get("values"))
                info["n_values"] = len(body.get("values", []))
                if body.get("message"):
                    info["message"] = body.get("message")[:160]
                if body.get("code"):
                    info["code"] = body.get("code")
            else:
                info["raw"] = body
            out["velas"][sym] = info
        except Exception as e:
            out["velas"][sym] = {"error": str(e)}
    # ── 2. Probar el summary de NDX (atm_iv) ──
    try:
        async with httpx.AsyncClient(timeout=12,
                                      headers={"X-Api-Key": FLASHALPHA_KEY}) as c:
            r = await c.get(f"{FA_BASE}/v1/stock/{FA_INDEX_SYMBOL}/summary")
        out["summary_http"] = r.status_code
        if r.status_code == 200:
            sd = r.json() or {}
            # Mostrar las CLAVES de nivel superior y de volatility para ubicar atm_iv
            out["summary_keys"] = list(sd.keys())
            out["volatility_keys"] = list((sd.get("volatility", {}) or {}).keys()) if isinstance(sd.get("volatility"), dict) else "no-vol-dict"
            # Buscar cualquier campo que contenga 'iv'
            iv_fields = {}
            def _scan(d, prefix=""):
                if isinstance(d, dict):
                    for k, v in d.items():
                        if "iv" in k.lower() or "volat" in k.lower():
                            iv_fields[prefix+k] = v if not isinstance(v,(dict,list)) else "..."
                        if isinstance(v, dict):
                            _scan(v, prefix+k+".")
            _scan(sd)
            out["campos_iv_encontrados"] = iv_fields
        else:
            out["summary_body"] = r.text[:200]
    except Exception as e:
        out["summary_error"] = str(e)
    return out


# Instrumento operado: NQ. Ambas rutas (/NQ y /ES) apuntan a la misma función y
# devuelven el instrumento configurado en FA_ASSET — el path del símbolo es solo
# etiqueta de URL. Se mantienen las dos durante la transición del deploy para no
# romper ningún frontend a medio publicar.
@app.get("/api/market/candles/NQ")
@app.get("/api/market/candles/ES")
async def market_candles(tf: str = "5"):
    """Velas REALES del instrumento (FA_ASSET) via TwelveData (sin CORS, server-side).
    tf: '5','15','30' minutos. Devuelve OHLC en escala ES real.
    Cacheado 90s para no agotar créditos de TwelveData (múltiples
    clientes / auto-refresh comparten la misma llamada).
    Blindado: cualquier error interno devuelve JSON limpio, nunca 500
    (un 500 rompe el header CORS y llena la consola de errores)."""
    try:
        return await _market_candles_impl(tf)
    except Exception as e:
        print(f"[candles] error no manejado: {e}")
        cached = _candles_cache.get(tf)
        if cached:
            return {**cached["data"], "note": "error-sirviendo-cache"}
        return {"status": "error", "candles": [], "detail": str(e)[:120]}

def _resample_candles(base, tf_min):
    """Agrega velas 5m REALES en buckets de tf_min (15/30) con OHLC estándar:
    open=primera, high=máx, low=mín, close=última. Es la misma data del mercado
    re-agrupada — jamás inventada (Regla #1)."""
    if tf_min <= 5:
        return base
    span = tf_min * 60
    buckets, order = {}, []
    for c in base:
        b = c["time"] - (c["time"] % span)
        if b not in buckets:
            buckets[b] = {"time": b, "open": c["open"], "high": c["high"],
                          "low": c["low"], "close": c["close"]}
            order.append(b)
        else:
            k = buckets[b]
            if c["high"] > k["high"]: k["high"] = c["high"]
            if c["low"]  < k["low"]:  k["low"]  = c["low"]
            k["close"] = c["close"]
    return [buckets[b] for b in order]

async def _fetch_yahoo_5m_base():
    """Fallback de velas 5m vía Yahoo (gratis, sin límite de créditos). Usa ES=F
    (futuro real del ES) → sin conversión, escala exacta. Si ES=F fallara, cae
    a QQQ×ratio. Garantiza que el chart SIEMPRE tenga velas reales aunque
    TwelveData se agote."""
    ua = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"}
    ratio = get_px_ratio()
    # El futuro directo (ES=F) NO necesita ratio: mult=1.0, ya viene en puntos del
    # índice. Solo la conversión del ETF lo necesita, así que el candidato del ETF
    # se añade únicamente si hay ratio REAL (sin él sería la escala equivocada;
    # antes aquí se caía a la constante 41.51).
    # OJO: esta función tenía un `if not ratio: return None` al principio que
    # abortaba ANTES de intentar ES=F — bloqueaba el único camino gratis y sin
    # créditos que funciona cuando FlashAlpha está sin cuota.
    candidates = [(FA_INDEX_SYMBOL, 1.0)]
    if ratio:
        candidates.append((FA_PROXY_ETF, ratio))
    else:
        print(f"[candles] sin ratio real → solo se intenta {FA_INDEX_SYMBOL} directo")
    for ysym, mult in candidates:
        try:
            url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ysym}"
                   "?range=5d&interval=5m&includePrePost=true")
            async with httpx.AsyncClient(timeout=15, headers=ua) as client:
                r = await client.get(url)
            if r.status_code != 200:
                continue
            res = (r.json().get("chart", {}).get("result") or [None])[0]
            if not res:
                continue
            ts = res.get("timestamp") or []
            q = (res.get("indicators", {}).get("quote") or [{}])[0]
            o, h, l, c = q.get("open", []), q.get("high", []), q.get("low", []), q.get("close", [])
            out = []
            for i in range(len(ts)):
                try:
                    if o[i] is None or h[i] is None or l[i] is None or c[i] is None:
                        continue
                    out.append({"time": int(ts[i]),
                                "open": round(float(o[i])*mult, 2),
                                "high": round(float(h[i])*mult, 2),
                                "low":  round(float(l[i])*mult, 2),
                                "close":round(float(c[i])*mult, 2)})
                except (IndexError, TypeError, ValueError):
                    continue
            if len(out) >= 5:
                src = "yahoo-" + ysym + ("" if mult == 1.0 else f"-x{ratio}")
                print(f"[candles] base 5m {ysym} ok vía Yahoo ({len(out)} velas)")
                return {"status": "ok", "symbol": ysym, "interval": "5min",
                        "candles": out, "source": src, "converted": mult != 1.0}
        except Exception as e:
            print(f"[candles] Yahoo {ysym} falló: {e}")
            continue
    return None

async def _fetch_alphavantage_5m_base():
    """Respaldo de velas 5m vía Alpha Vantage (25 llamadas/día — solo si TwelveData
    falla). QQQ×ratio a escala NQ. compact = últimas 100 velas (suficiente)."""
    if not ALPHA_VANTAGE_KEY or not budget_ok("alphavantage", 1):
        return None
    ratio = get_px_ratio()
    # Sin ratio REAL no se convierte nada: mejor sin velas que velas en la escala
    # equivocada. Antes aquí caía a la constante 41.51 y pintaba un chart cuyo
    # precio no cuadraba con los niveles de GEX (que sí vienen en puntos reales).
    if not ratio:
        print("[candles] sin ratio real (spot FlashAlpha ni SPX/SPY) — no se convierte")
        return None
    try:
        url = ("https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY"
               f"&symbol=QQQ&interval=5min&outputsize=compact&apikey={ALPHA_VANTAGE_KEY}")
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url)
        budget_charge("alphavantage", 1)
        if r.status_code != 200:
            return None
        d = r.json()
        series = d.get("Time Series (5min)")
        if not series:
            print(f"[candles] AlphaVantage sin data: {str(d)[:100]}")
            return None
        import datetime as _dt
        out = []
        for dt_str, v in series.items():
            try:
                t = _dt.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                ts = int(t.replace(tzinfo=NY).timestamp())
                out.append({"time": ts,
                            "open": round(float(v["1. open"])*ratio, 2),
                            "high": round(float(v["2. high"])*ratio, 2),
                            "low":  round(float(v["3. low"])*ratio, 2),
                            "close":round(float(v["4. close"])*ratio, 2)})
            except (KeyError, ValueError):
                continue
        out.sort(key=lambda c: c["time"])
        if out:
            print(f"[candles] base 5m {FA_PROXY_ETF}×{ratio} vía AlphaVantage ({len(out)} velas)")
            return {"status": "ok", "symbol": FA_PROXY_ETF, "interval": "5min",
                    "candles": out, "source": f"alphavantage-{FA_PROXY_ETF}-x{ratio}", "converted": True}
    except Exception as e:
        print(f"[candles] AlphaVantage error: {e}")
    return None

# ═══════════════════════════════════════════════════════════════════════════
#  HISTORIAL DE GEX — cada llamada real a FlashAlpha se archiva
# ═══════════════════════════════════════════════════════════════════════════
#  Los créditos de FlashAlpha son el recurso más escaso del sistema (100/día) y
#  cada refresh es un snapshot IRREPETIBLE del mercado: si no se archiva, se
#  pierde para siempre. Con 28 refreshes/día son ~140 puntos reales por semana
#  para estudiar migración del flip, cambios de régimen y patrones horarios.
#
#  Formato JSONL (una línea por snapshot): append barato, resistente a
#  corrupción (una línea rota no invalida el archivo) y leíble en streaming.
#  Vive en el Volume de Railway (/data), que sobrevive a los redeploys.
#
#  Regla #1: SOLO se archivan snapshots con dato real de FlashAlpha. Si los
#  niveles vienen vacíos no se escribe nada — un historial con huecos es útil;
#  uno con datos inventados no vale nada.
_GEX_HISTORY = os.getenv("GEX_HISTORY_PATH", "/data/lbc_gex_history.jsonl")
_GEX_HIST_MAX_MB = float(os.getenv("GEX_HISTORY_MAX_MB", "50"))

def append_gex_history(asset, snap):
    """Archiva un snapshot de GEX. Se llama tras CADA refresh real."""
    try:
        cw, pw, gf = snap.get("call_wall"), snap.get("put_wall"), snap.get("gamma_flip")
        if cw is None and pw is None and gf is None:
            return  # sin niveles reales no se archiva (Regla #1)
        now = datetime.now(NY)
        row = {
            "ts": now.isoformat(), "date": now.strftime("%Y-%m-%d"),
            "time_et": now.strftime("%H:%M:%S"), "asset": asset,
            "ticker": snap.get("ticker"),
            "spot": snap.get("underlying_price"),
            "call_wall": cw, "put_wall": pw, "gamma_flip": gf,
            "max_pain": snap.get("max_pain"), "net_gex": snap.get("net_gex"),
            "regime": snap.get("regime"),
            "atm_iv": snap.get("atm_iv"), "expected_move": snap.get("expected_move"),
            "fear_score": snap.get("fear_score"), "vix": snap.get("vix"),
            "source": snap.get("source"),
            "per_strike_count": snap.get("per_strike_count"),
        }
        os.makedirs(os.path.dirname(_GEX_HISTORY) or ".", exist_ok=True)
        with open(_GEX_HISTORY, "a") as f:
            f.write(json.dumps(row) + "\n")
        # Corte por tamaño: conserva la mitad más reciente. ~200 B/línea → 50 MB
        # son ~250.000 snapshots (unos 24 años a 28/día); el corte es un seguro,
        # no algo que vaya a dispararse en la práctica.
        try:
            if os.path.getsize(_GEX_HISTORY) > _GEX_HIST_MAX_MB * 1024 * 1024:
                with open(_GEX_HISTORY) as f:
                    lines = f.readlines()
                with open(_GEX_HISTORY, "w") as f:
                    f.writelines(lines[len(lines)//2:])
                print(f"[gex-hist] rotado: {len(lines)} → {len(lines)//2} líneas")
        except Exception:
            pass
    except Exception as e:
        print(f"[gex-hist] no se pudo archivar (no crítico): {e}")

_CANDLES_PERSIST = os.getenv("CANDLES_PATH", "/data/lbc_candles.json")
def _persist_candles(base):
    """Guarda la base 5m en el Volume para sobrevivir redeploys sin re-descargar.
    Se sella con el instrumento: sin el sello no hay forma de saber si las velas
    guardadas son del ES o de otro futuro (ver _load_persisted_candles)."""
    try:
        with open(_CANDLES_PERSIST, "w") as f:
            json.dump({"ts": time.time(), "asset": FA_ASSET, "data": base}, f)
    except Exception:
        pass
def _load_persisted_candles():
    """Carga la última base 5m real del Volume (si existe y es del instrumento
    que operamos AHORA).

    El Volume sobrevive a los redeploys: tras migrar NQ→ES el snapshot traía
    velas en escala NASDAQ (~29.800) y se servían tal cual en /candles/ES, o
    sea un chart del Nasdaq etiquetado como ES (Regla #1). Los snapshots
    anteriores a la migración no llevan 'asset': se descartan por seguridad."""
    try:
        with open(_CANDLES_PERSIST) as f:
            snap = json.load(f)
        if snap.get("asset") != FA_ASSET:
            print(f"[candles] base persistida descartada: asset={snap.get('asset')} "
                  f"(operamos {FA_ASSET})")
            return None
        if snap.get("data", {}).get("candles"):
            return {**snap["data"], "note": "base-5m-persistida"}
    except Exception:
        pass
    return None

async def _fetch_td_5m_base():
    """Base 5m de NQ vía TwelveData REST. Plan free NO tiene futuros (NQ) ni el
    índice (NDX) — SÍ tiene QQQ. Pedimos QQQ y convertimos con el ratio a escala
    NQ. Una sola llamada cada vez (el caché de 90s la comparten todos los
    clientes), ~150-250 créditos/día, muy bajo el límite de 800. Sin WebSocket."""
    if not TWELVEDATA_KEY or not td_budget_ok(1):
        if not td_budget_ok(1):
            print(f"[candles] presupuesto TwelveData agotado ({_td_credits['used']}/{TD_DAILY_LIMIT})")
        return None
    ratio = get_px_ratio()
    # Sin ratio REAL no se convierte nada: mejor sin velas que velas en la escala
    # equivocada. Antes aquí caía a la constante 41.51 y pintaba un chart cuyo
    # precio no cuadraba con los niveles de GEX (que sí vienen en puntos reales).
    if not ratio:
        print(f"[candles] sin ratio real ({FA_PROXY_ETF}) — no se convierte")
        return None
    try:
        url = (f"https://api.twelvedata.com/time_series?symbol={FA_PROXY_ETF}"
               f"&interval=5min&outputsize=390&timezone=America/New_York"
               f"&apikey={TWELVEDATA_KEY}")
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url)
        td_charge(1)
        if r.status_code != 200:
            print(f"[candles] TwelveData HTTP {r.status_code}: {r.text[:120]}")
            return None
        d = r.json()
        vals = d.get("values")
        if not vals:
            print(f"[candles] TwelveData sin values: {str(d)[:120]}")
            return None
        out = []
        for v in reversed(vals):
            try:
                import datetime as _dt
                t = _dt.datetime.strptime(v["datetime"], "%Y-%m-%d %H:%M:%S")
                ts = int(t.replace(tzinfo=NY).timestamp())
                out.append({"time": ts,
                            "open": round(float(v["open"])*ratio, 2),
                            "high": round(float(v["high"])*ratio, 2),
                            "low":  round(float(v["low"])*ratio, 2),
                            "close":round(float(v["close"])*ratio, 2)})
            except (KeyError, ValueError):
                continue
        if not out:
            return None
        # Derivar precio NQ actual + refrescar timestamp del ratio (sin WebSocket)
        last_qqq = float(vals[0]["close"])
        cache["px_ratio"]["etf_price"] = last_qqq
        cache["px_ratio"]["spot"]  = round(last_qqq * ratio, 2)
        cache["px_ratio"]["ts"] = datetime.now(NY).isoformat()
        print(f"[candles] base 5m {FA_PROXY_ETF}×{ratio} ok ({len(out)} velas) — escala {FA_ASSET}")
        return {"status": "ok", "symbol": FA_PROXY_ETF, "interval": "5min",
                "candles": out, "source": f"twelvedata-{FA_PROXY_ETF}-x{ratio}", "converted": True}
    except Exception as e:
        print(f"[candles] TwelveData error: {e}")
        return None

async def _market_candles_impl(tf: str = "5"):
    # (Sin key de TwelveData NO abortamos: Yahoo cubre las velas sin key alguna.)
    # Cache de 90s por timeframe
    cached = _candles_cache.get(tf)
    if cached and (time.time() - cached["ts"]) < 300:
        return cached["data"]
    # ── ARQUITECTURA: la base 5m se intenta por TwelveData y, si no, por Yahoo
    # (gratis, sin key). 15m y 30m se DERIVAN agregando esas velas reales.
    base = None
    c5 = _candles_cache.get("5")
    if c5 and (time.time() - c5["ts"]) < 300 and c5["data"].get("candles"):
        base = c5["data"]
    else:
        if TWELVEDATA_KEY:
            base = await _fetch_td_5m_base()      # 1) TwelveData (800/día, principal)
        if not (base and base.get("candles")):
            base = await _fetch_alphavantage_5m_base()  # 2) Alpha Vantage (25/día, respaldo)
        if not (base and base.get("candles")):
            base = await _fetch_yahoo_5m_base()   # 3) Yahoo (último recurso)
        if base and base.get("candles"):
            _candles_cache["5"] = {"ts": time.time(), "data": base}
            _persist_candles(base)                # guardar en Volume (sobrevive redeploy)
        elif c5 and c5["data"].get("candles"):
            base = {**c5["data"], "note": "base-5m-desde-cache"}  # 4) último real en RAM
        elif _load_persisted_candles():
            base = _load_persisted_candles()      # 5) último real del Volume
    if not base or not base.get("candles"):
        cached = _candles_cache.get(tf)
        if cached:
            return {**cached["data"], "note": "sirviendo-cache"}
        return {"status":"no-data","candles":[]}
    if tf == "5":
        return base
    candles = _resample_candles(base["candles"], int(tf))
    result = {"status":"ok","symbol":base.get("symbol"),
              "interval": f"{tf}min", "candles": candles,
              "source": (base.get("source","") + "-resampled"),
              "converted": base.get("converted", False)}
    _candles_cache[tf] = {"ts": time.time(), "data": result}
    return result


@app.get("/api/market/gamma-levels/NQ")
@app.get("/api/market/gamma-levels/ES")   # ambas → FA_ASSET (ver nota en candles)
async def gamma_levels():
    """GEX desde cache. FlashAlpha se llama en 4 ventanas: 19:00, 9:00, 9:15, 9:45 ET.
    Expone timestamp exacto + próxima actualización programada para que el usuario
    valide si los niveles son de hoy y a qué hora se obtuvieron."""
    gex = cache["gex"].get(FA_ASSET)
    if not gex:
        # Cache frío — típico tras un redeploy de Railway (borra el cache en memoria).
        # Disparar UN refresh en background para repoblar fear/vix/expected_move/GEX.
        # Self-guarded: refresh_gex ya verifica presupuesto FlashAlpha y bloqueo 429.
        # Debounce de 5 min para no spamear ni quemar créditos si el frontend poletea.
        global _gex_ondemand_ts
        _now = time.time()
        if _now - _gex_ondemand_ts > 300:
            _gex_ondemand_ts = _now
            asyncio.create_task(refresh_gex())
            print("[gex] cache frío → refresh on-demand disparado (repuebla fear/vix/em)")
        return {"status": "loading", "message": "GEX cargando — refresca en unos segundos",
                "last_call_ts": None, "next_update": _next_gex_window()}
    etf_px = gex.get("underlying_price")   # precio del ETF (SPY) que devuelve FlashAlpha
    # Ratio: 1) spot real de FlashAlpha, 2) SPX/SPY real, 3) precios del heatmap.
    # NUNCA una constante: si no hay dato real → None → la UI muestra "—".
    ratio = get_px_ratio()
    if not ratio:
        # Respaldo: precios reales del heatmap (índice / ETF)
        try:
            hm = cache["heatmap"]["data"]
            idx_p = (hm.get(FA_CASH_INDEX, {}) or {}).get("price")   # índice cash real
            etf_p = (hm.get(FA_PROXY_ETF, {}) or {}).get("price")    # ETF real
            if idx_p and etf_p and etf_p > 10:
                ratio = round(idx_p / etf_p, 6)
                print(f"[ratio] del heatmap {FA_CASH_INDEX}/{FA_PROXY_ETF}: {ratio}")
        except Exception:
            pass
    # Nota: se eliminó el respaldo que usaba cache["nq_price"] con el umbral
    # `> 10000` — era específico del NQ (~20.000). El ES cotiza ~6.000, así que
    # esa condición JAMÁS se cumpliría y el respaldo era código muerto.
    # NQ=F (futuro CME) YA llega en puntos del índice: NO convertir NUNCA.
    # Solo el ETF (plan free, escala ~700) se convierte con ratio.
    is_direct = str(gex.get("source") or "").endswith("-direct")
    # El PRECIO a mostrar depende del modo:
    #  · directo (Basic): underlying_price ES el spot del futuro (ej. 29.285 en NQ)
    #    → se muestra tal cual. Multiplicarlo por el ratio daría ~1,2M (bug).
    #  · ETF (free): underlying_price es el precio del ETF (~708) → ETF×ratio.
    # Además, si es directo y hay spot + ETF en el heatmap, se deriva el ratio de
    # respaldo (spot/ETF) para el resto del sistema, sin depender de NDX (que
    # Finnhub no da y Yahoo bloquea desde Railway).
    if is_direct:
        px = round(etf_px, 2) if isinstance(etf_px, (int, float)) else None
        if px and not ratio:
            try:
                _etf = (cache["heatmap"]["data"].get(FA_PROXY_ETF, {}) or {}).get("price")
                if _etf and _etf > 10:
                    ratio = round(px / _etf, 6)
                    cache["px_ratio"].update({"value": ratio, "spot": px,
                        "etf_price": float(_etf), "source": "spot-directo/etf",
                        "ts": datetime.now(NY).isoformat()})
                    print(f"[ratio] derivado del spot directo {FA_ASSET}/{FA_PROXY_ETF}: {ratio}")
            except Exception:
                pass
    else:
        px = round(etf_px*ratio, 2) if (etf_px and ratio) else None
    def _to_px(v):
        if is_direct:
            return v  # ya en escala del futuro (futures-direct), sin conversión
        if not isinstance(v, (int, float)):
            return v
        if not ratio:
            return None   # sin ratio real → "—". Antes: v*None → TypeError → 500.
        return round(v*ratio, 2)
    gex_nq = dict(gex)
    gex_nq["call_wall"]  = _to_px(gex.get("call_wall"))
    gex_nq["put_wall"]   = _to_px(gex.get("put_wall"))
    gex_nq["gamma_flip"] = _to_px(gex.get("gamma_flip"))
    if gex.get("max_pain") is not None:
        gex_nq["max_pain"] = _to_px(gex.get("max_pain"))
    gex_nq["conversion"] = ("none-direct" if is_direct
                            else (f"{FA_PROXY_ETF.lower()}-ratio-{ratio}" if ratio else "sin-ratio"))
    # Timestamp: preferir as_of de FlashAlpha (cuándo se CALCULÓ el dato).
    # FlashAlpha da as_of en ISO UTC; si no, usar _ts (cuándo llamamos).
    as_of = gex.get("as_of")
    ts = None
    if as_of:
        try:
            # as_of viene como "2026-06-26T14:30:00Z" → convertir a timestamp
            ts = datetime.fromisoformat(as_of.replace("Z","+00:00")).timestamp()
        except Exception:
            ts = None
    if not ts:
        ts = gex.get("_ts")
    if not ts:
        try:
            if os.path.exists(_PERSIST):
                ts = os.path.getmtime(_PERSIST)
        except Exception:
            ts = None
    last_call_iso = None
    last_call_is_today = False
    age_seconds = None
    if ts:
        dt_et = datetime.fromtimestamp(ts, NY)
        last_call_iso = dt_et.isoformat()
        last_call_is_today = (dt_et.date() == datetime.now(NY).date())
        age_seconds = int((datetime.now(NY) - dt_et).total_seconds())
    # `price` es la clave nueva (agnóstica del instrumento). Se mantiene `nq_price`
    # como alias para no romper el frontend desplegado durante la migración a ES;
    # se puede quitar cuando el front que consume `price` esté en producción.
    return {**gex_nq, "asset":FA_ASSET, "price":px, "nq_price":px,
            "ratio":get_px_ratio(), "credits_used":0,
            "last_call_ts": last_call_iso,
            "last_call_is_today": last_call_is_today,
            "age_seconds": age_seconds,
            "next_update": _next_gex_window()}

@app.get("/api/admin/diag-yahoo")
async def diag_yahoo(key: str = ""):
    """¿Alcanza Railway a Yahoo? Yahoo bloquea IPs de datacenter con frecuencia.
    Gratis: Yahoo no consume créditos de ninguna API nuestra."""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    out = {}
    for label, sym in ((FA_CASH_INDEX, FA_YAHOO_INDEX),
                       (f"{FA_ASSET}_futuro", quote(FA_INDEX_SYMBOL, safe="")),
                       (FA_PROXY_ETF, FA_PROXY_ETF)):
        try:
            url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
                   "?range=1d&interval=5m")
            async with httpx.AsyncClient(timeout=10, headers=_YAHOO_UA) as c:
                r = await c.get(url)
            body = r.text[:150]
            px = None
            try:
                res = ((r.json() or {}).get("chart", {}).get("result") or [None])[0]
                px = (res or {}).get("meta", {}).get("regularMarketPrice")
            except Exception:
                pass
            out[label] = {"http": r.status_code, "precio": px, "cuerpo": body}
        except Exception as e:
            out[label] = {"error": f"{type(e).__name__}: {e}"}
    out["veredicto"] = ("✅ Railway alcanza Yahoo" if any(v.get("precio") for v in out.values()
                        if isinstance(v, dict))
                        else "❌ Railway NO alcanza Yahoo (IP de datacenter bloqueada)")
    out[f"{FA_CASH_INDEX.lower()}_en_heatmap"] = FA_CASH_INDEX in cache["heatmap"]["data"]
    return out

@app.get("/api/admin/api-audit")
async def api_audit(key: str = ""):
    """CONTABILIDAD de todas las APIs: límite, uso real, presupuesto teórico del
    cron y estado. No gasta NI UN crédito: solo lee contadores y cache.
    Uso: /api/admin/api-audit?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    hm = cache["heatmap"]["data"]
    # Presupuesto TEÓRICO derivado del cron real (no de comentarios).
    plan = {
        "flashalpha": {
            "consumidores": ["refresh_gex (28x/día)", "diags (bajo budget_ok)"],
            "por_dia_teorico": 5 + (GEX_REFRESHES_PER_DAY - 1) * 3,
            "detalle": "5 créd el 1º del día + 3 los demás. 28 refreshes: 08:30, "
                       "09:15, 09:30-54 c/6min, 10:00-11:54 c/6min, 12:30",
            "reset": "00:00 UTC (lo dice el proveedor)",
        },
        "twelvedata": {
            "consumidores": ["_warm_candles (SPY 5m)"],
            "por_dia_teorico": 12 * 7 + 4,
            "detalle": "1 llamada/5min, 9-16h L-V (84) + premarket 7-8h (4)",
            "reset": "medianoche UTC",
        },
        "finnhub": {
            "consumidores": [f"heatmap ({len(REST_SYMBOLS)} símbolos/min, 7-16h L-V)",
                             f"índices ({len(_FH_INDICES)} c/4min)", "movers (1 c/45s)"],
            "pico_por_minuto": len(REST_SYMBOLS) + len(_FH_INDICES) + 2,
            "detalle": "el límite de Finnhub es POR MINUTO, no por día",
        },
        "groq": {"consumidores": ["refresh_institutional (4x/día)"],
                 "por_dia_teorico": 4,
                 "detalle": "09:00, 09:30, 09:45, 16:00 ET L-V"},
        "rapidapi": {
            "consumidores": ["calendario (TradingEconomics)"],
            "por_dia_teorico": _rapidapi_day_count,
            "detalle": "2 llamadas/ciclo, tope propio 85/día (contador SEPARADO de "
                       "_api_usage: no pasa por budget_ok)",
        },
        "alphavantage": {"consumidores": ["respaldo de velas"],
                         "por_dia_teorico": 0,
                         "detalle": "solo si TwelveData falla"},
    }
    out = {"generado": datetime.now(NY).isoformat(), "asset": FA_ASSET, "apis": {}}
    for name, cfg in API_BUDGETS.items():
        st = _api_usage[name]
        out["apis"][name] = {
            "key_configurada": bool({
                "flashalpha": FLASHALPHA_KEY, "twelvedata": TWELVEDATA_KEY,
                "finnhub": FINNHUB_KEY, "groq": GROQ_KEY,
                "alphavantage": ALPHA_VANTAGE_KEY, "fmp": FMP_KEY,
            }.get(name)),
            "limite_seguro": cfg["limit"], "ventana": cfg["window"],
            "usado_ahora": st["used"], "ventana_actual": st["window_key"],
            "restante": max(0, cfg["limit"] - st["used"]),
            "pct": round(st["used"] / cfg["limit"] * 100, 1) if cfg["limit"] else None,
            "plan": plan.get(name, {}),
        }
    out["apis"]["rapidapi"] = {
        "key_configurada": bool(RAPIDAPI_KEY), "limite_seguro": 85, "ventana": "day",
        "usado_ahora": _rapidapi_day_count, "restante": max(0, 85 - _rapidapi_day_count),
        "plan": plan["rapidapi"],
        "aviso": "contador propio, NO integrado en _api_usage → no se persiste",
    }
    # Salud observable: ¿el dato llega de verdad?
    out["salud_datos"] = {
        "heatmap_simbolos_con_precio": len([k for k, v in hm.items()
                                            if (v or {}).get("price") is not None]),
        f"{FA_CASH_INDEX}_presente": FA_CASH_INDEX in hm,
        f"{FA_PROXY_ETF}_presente": FA_PROXY_ETF in hm,
        f"{FA_ASSET}_presente": FA_ASSET in hm,
        "ratio_actual": get_px_ratio(),
        "ratio_fuente": cache["px_ratio"].get("source"),
        "gex_cache_warm": bool(cache["gex"].get(FA_ASSET)),
        "calendario_eventos": len(cache["calendar"]["data"] or []),
        "velas_en_cache": bool(_candles_cache.get("5")),
    }
    return out

@app.get("/api/gex/history")
async def gex_history(days: int = 7, limit: int = 2000, fmt: str = "json"):
    """Historial REAL de GEX archivado en el Volume (uno por refresh de FlashAlpha).

    days: cuántos días atrás (por fecha ET). limit: máximo de filas (más recientes).
    fmt='csv' devuelve CSV para analizarlo fuera. Público: son niveles, no secretos.
    """
    try:
        if not os.path.exists(_GEX_HISTORY):
            return {"status": "empty", "rows": [], "count": 0,
                    "note": "aún no hay snapshots archivados"}
        cutoff = (datetime.now(NY) - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = []
        with open(_GEX_HISTORY) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except Exception:
                    continue   # una línea corrupta no invalida el archivo
                if r.get("date", "") >= cutoff:
                    rows.append(r)
        rows = rows[-limit:]
        if fmt == "csv":
            cols = ["ts","date","time_et","asset","ticker","spot","call_wall","put_wall",
                    "gamma_flip","max_pain","net_gex","regime","atm_iv","expected_move",
                    "fear_score","vix","source","per_strike_count"]
            out = ",".join(cols) + "\n"
            for r in rows:
                out += ",".join("" if r.get(c) is None else str(r.get(c)) for c in cols) + "\n"
            return Response(content=out, media_type="text/csv")
        return {"status": "ok", "count": len(rows), "days": days,
                "asset": FA_ASSET, "rows": rows}
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.get("/api/heatmap")
async def get_heatmap():
    """22 activos: 8 vía WebSocket real-time + 14 vía REST batch cada 15min."""
    # Índices reales (Yahoo) — throttle interno 4 min; con el frontend pidiendo
    # cada 30s, los niveles reales llegan solos sin gastar créditos de nadie.
    asyncio.create_task(refresh_real_indices())
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
        "px_ratio":     cache["px_ratio"],
    }

@app.get("/api/version")
async def get_version():
    """Confirma qué versión del backend está desplegada + diagnóstico de estado."""
    try:
        fa_usage = _api_usage.get("flashalpha") if "_api_usage" in globals() else None
    except Exception:
        fa_usage = None
    return {
        "version": "v2026.07.09-session",
        "ws_symbols": WS_SYMBOLS,
        "has_nq1": "NQ1!" in WS_SYMBOLS,
        "has_dynamic_ratio": True,
        "px_ratio_current": get_px_ratio(),
        "flashalpha_usage": fa_usage,
        "gex_cache_warm": bool(cache["gex"].get(FA_ASSET)),
        "calendar_status": cache["calendar"].get("status"),
        "movers_status": cache["movers"].get("status"),
        "heatmap_status": cache["heatmap"].get("status"),
        "build": "session-2026-07-09",
    }

@app.get("/api/calendar")
async def get_calendar():
    """Devuelve caché INMEDIATAMENTE. Refresco en segundo plano (no bloquea).
    Incluye el precio NQ actual para que el frontend calcule el impacto inmediato."""
    last = cache["calendar"]["last_update"]
    is_stale = not last or (datetime.now(NY) - datetime.fromisoformat(last)).total_seconds() > 120
    if is_stale:
        asyncio.create_task(refresh_calendar())
    # FIX: "próximo" = Upcoming Y con fecha/hora futura (descarta eventos viejos
    # que quedaron como Upcoming porque nunca recibieron su 'actual').
    _now_et = datetime.now(NY)
    def _ev_is_future(e):
        try:
            _d = e.get("time") or e.get("date") or e.get("datetime") or ""
            if not _d:
                return True  # sin fecha, no lo descartamos
            _dt = datetime.fromisoformat(_d.replace("Z", "+00:00"))
            if _dt.tzinfo is None:
                _dt = _dt.replace(tzinfo=NY)
            return _dt >= _now_et - timedelta(hours=2)  # margen de 2h post-release
        except Exception:
            return True
    upcoming = [e for e in cache["calendar"]["data"]
                if e.get("status")=="Upcoming" and _ev_is_future(e)]
    # Precio NQ actual — para cálculo de reacción del mercado post-publicación
    nq_now = (cache["heatmap"]["data"].get(FA_ASSET, {}) or {}).get("price")
    # ── MOTOR DE REACCIÓN NQ (1 vela de 5 min tras la noticia) ──
    # Al detectar un evento recién Released: registra el precio NQ (p0).
    # Pasados ≥5 min: registra p5 y calcula la digestión = p5 - p0 en puntos.
    # Regla #1: solo con precios reales del heatmap; si faltan, no se inventa.
    global _event_reactions
    try:
        _now_ts = datetime.now(NY).timestamp()
        for e in cache["calendar"]["data"]:
            if e.get("status") != "Released":
                continue
            _k = f"{e.get('title','')}|{e.get('time','') or e.get('date','')}"
            _r = _event_reactions.get(_k)
            if _r is None and nq_now:
                _event_reactions[_k] = {"t0": _now_ts, "p0": nq_now, "p5": None}
            elif _r and _r.get("p5") is None and nq_now and (_now_ts - _r["t0"]) >= 300:
                _r["p5"] = nq_now
            _r = _event_reactions.get(_k)
            if _r and _r.get("p5") is not None:
                e["nq_reaction_pts"] = round(_r["p5"] - _r["p0"], 2)
                e["nq_reaction_window"] = "5min"
            elif _r:
                e["nq_reaction_pts"] = None  # aún midiendo (ventana de 5 min)
    except Exception as _e:
        print(f"[calendar] motor de reacción falló (no crítico): {_e}")
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
            if _earn_impact(sym) in ("extreme","high") and cache["gex"].get(FA_ASSET):
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
    # FIX: "próximo" = Upcoming Y con fecha/hora futura (descarta eventos viejos
    # que quedaron como Upcoming porque nunca recibieron su 'actual').
    _now_et = datetime.now(NY)
    def _ev_is_future(e):
        try:
            _d = e.get("time") or e.get("date") or e.get("datetime") or ""
            if not _d:
                return True  # sin fecha, no lo descartamos
            _dt = datetime.fromisoformat(_d.replace("Z", "+00:00"))
            if _dt.tzinfo is None:
                _dt = _dt.replace(tzinfo=NY)
            return _dt >= _now_et - timedelta(hours=2)  # margen de 2h post-release
        except Exception:
            return True
    upcoming = [e for e in cache["calendar"]["data"]
                if e.get("status")=="Upcoming" and _ev_is_future(e)]
    movers   = cache["movers"]["data"]
    breaking = next((m for m in movers if m.get("score",0)>=95), None)
    gex = cache["gex"].get(FA_ASSET,{})
    _up = gex.get("underlying_price")
    _r = get_px_ratio()   # puede ser None: sin dato real no se inventa (Regla #1)
    # directo: underlying_price ES el spot del futuro → tal cual. ETF: ×ratio.
    _is_direct = str(gex.get("source") or "").endswith("-direct")
    if _is_direct:
        _px = round(_up, 2) if isinstance(_up, (int, float)) else None
    else:
        _px = round(_up*_r, 2) if (_up and _r) else None
    return {
        "gamma_levels":        {**gex,"price":_px,"nq_price":_px} if gex else None,
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
    # ⚠️ WebSocket de TwelveData DESACTIVADO — en plan gratis consume créditos por
    # CADA tick (cientos/min → 10,000+/día, agotaba los 800 en horas y mataba el
    # chart). El precio NQ y el ratio NQ/QQQ ahora se derivan del REST de velas
    # (QQQ×ratio), sin desangre. Para reactivar con plan pago: quitar el guard.
    if os.getenv("TD_WEBSOCKET", "off").lower() == "on":
        asyncio.create_task(twelvedata_ws())

    # ── TwelveData REST: batch 13 símbolos macro cada 15min en RTH ────────
    scheduler.add_job(refresh_heatmap_finnhub,
                      CronTrigger(day_of_week="mon-fri", hour="7-16", minute="*"))  # cada 1 min vía Finnhub  # batch 13 símbolos c/10min, 8-17 ET (702 créd/día=88%)

    # ── Índices reales (Yahoo): SIEMPRE, incluso fuera de RTH y fines de semana ──
    # Cubre VIX/DXY/yields/Gold/WTI/BTC/SPX que Finnhub no tiene. Sin créditos.
    # Throttle interno de 4 min protege aunque el job corra cada 3.
    scheduler.add_job(refresh_real_indices, IntervalTrigger(minutes=3))
    # SPX vía Yahoo (gratis): Finnhub free no da ^GSPC. Sin SPX el ratio ES/SPY se
    # queda sin respaldo y el chart depende SOLO de FlashAlpha.
    scheduler.add_job(refresh_cash_index_yahoo, IntervalTrigger(minutes=3))
    # ── Velas del chart: warm SOLO cada 5 min en horario de mercado ─────────────
    # Arquitectura eficiente: 1 llamada cada 5 min (cuando cierra una vela nueva),
    # no en loop. El caché de 5 min sirve a todos los clientes. El precio en vivo
    # (Finnhub /quote, gratis) estira la última vela en el frontend entre llamadas.
    # Presupuesto: ~78 llamadas/día (6.5h × 12/h) de 800 → 10%. Imposible agotarlo.
    async def _warm_candles():
        try: await _market_candles_impl("5")
        except Exception as e: print(f"[candles] warm error: {e}")
    scheduler.add_job(_warm_candles,
                      CronTrigger(day_of_week="mon-fri", hour="9-16", minute="*/5"))
    # Una carga extra en premarket para que el chart no abra vacío
    scheduler.add_job(_warm_candles,
                      CronTrigger(day_of_week="mon-fri", hour="7-8", minute="0,30"))

    # ── FlashAlpha GEX: SOLO 9am + 7pm ET (2 créditos de 5/día) ──────────
    # FlashAlpha GEX: 5 horarios exactos — máx 5 créditos/día    # ── FlashAlpha GEX: 4 ventanas (límite 5/día, deja 1 para pruebas) ──
    # Estrategia para day trading: el estudiante analiza el gráfico ANTES de
    # operar, así que necesita niveles frescos en premarket, no tras la apertura.
    # ── FlashAlpha GEX (plan Basic, 100 llamadas/día) ────────────────
    # Con NDX directo cada refresh usa ~3 llamadas (levels+gex+maxpain).
    # Horario ampliado: premarket + apertura + media mañana + sesión.
    # ~14 ventanas × 3 = ~42 llamadas/día (bajo el límite de 100).
    # GEX cada 20 min · 7:00-13:00 ET (ventana operativa de los estudiantes).
    # ~19 refreshes/día × ~3 créditos c/u (options 1x/día, maxpain skip si falló)
    # = ~60 créditos, bajo el guard de 90. Después de la 1PM no hay llamadas.
    # ── FlashAlpha GEX — presupuesto concentrado en la ventana crítica ──────────
    # El GEX es la API más importante: define el market regime y todos los setups.
    # Un cambio de gamma flip (positivo↔negativo) invalida los setups al instante,
    # así que la máxima densidad de refresh va donde más se mueve el precio.
    #   · 08:30 y 09:15   → prep premarket (2 refreshes)
    #   · 09:30-11:54     → VENTANA CRÍTICA, cada 6 min (25 refreshes)
    #   · 12:30           → media sesión (1 refresh)
    # Total: 28 refreshes × ~3 créd = ~86 de 95 (margen 9). Un flip a las 10:15 se
    # ve máx a las 10:18-10:20, no a los 15 min.
    scheduler.add_job(refresh_gex,
                      CronTrigger(hour=8, minute=30, day_of_week="mon-fri"))   # prep
    scheduler.add_job(refresh_gex,
                      CronTrigger(hour=9, minute=15, day_of_week="mon-fri"))   # prep
    scheduler.add_job(refresh_gex,                                             # ventana crítica
                      CronTrigger(hour=9,  minute="30,36,42,48,54", day_of_week="mon-fri"))
    scheduler.add_job(refresh_gex,
                      CronTrigger(hour="10-11", minute="0,6,12,18,24,30,36,42,48,54", day_of_week="mon-fri"))
    scheduler.add_job(refresh_gex,
                      CronTrigger(hour=12, minute=30, day_of_week="mon-fri"))  # media sesión

    # ── Finnhub Calendar: cada 5 minutos ──────────────────────────────────
    scheduler.add_job(refresh_calendar, IntervalTrigger(seconds=30))  # latencia máx ~45s

    # ── Finnhub Movers: cada 60 segundos ──────────────────────────────────
    scheduler.add_job(refresh_movers, IntervalTrigger(seconds=45))

    # ── Finnhub Earnings: cada 6 horas ────────────────────────────────────
    scheduler.add_job(refresh_earnings, IntervalTrigger(hours=6))

    # ── Groq Institutional: 9:05 AM + 12:00 PM ET lun-vie ─────────────────
    # Resumen IA: cada 30min durante horario extendido (premarket→afterhours)
        # Groq — 4 eventos clave del mercado (4 llamadas/día en horario hábil):
    scheduler.add_job(refresh_institutional, CronTrigger(hour=9,  minute=0,  day_of_week="mon-fri"))  # contexto premarket
    scheduler.add_job(refresh_institutional, CronTrigger(hour=9,  minute=30, day_of_week="mon-fri"))  # apertura del mercado
    scheduler.add_job(refresh_institutional, CronTrigger(hour=9,  minute=45, day_of_week="mon-fri"))  # confirmación de flujo
    scheduler.add_job(refresh_institutional, CronTrigger(hour=16, minute=0,  day_of_week="mon-fri"))

    scheduler.start()

    # ── GEX al arrancar: NO se llama a FlashAlpha en cada redeploy ──────────────
    # ANTES: cada redeploy disparaba un refresh_gex() → con muchos redeploys en un
    # día, FlashAlpha respondía 429 y bloqueaba el GEX 24h. Ahora el GEX se carga
    # (a) on-demand cuando alguien abre el dashboard (/api/gamma-levels con cache
    # frío dispara un refresh, self-guarded por presupuesto + 429), y (b) en las
    # ventanas programadas (9:00/7:00 PM ET). Así los redeploys ya no queman crédito.
    async def _gex_boot():
        try:
            g = cache["gex"].get(FA_ASSET, {}) or {}
            if g:
                print(f"[startup] cache GEX presente — sin llamada")
            else:
                print(f"[startup] sin GEX en cache — cargará on-demand (primer visitante) o en ventana 9:00/19:00 ET")
        except Exception as e:
            print(f"[startup] gex boot falló: {e}")
    asyncio.create_task(_gex_boot())

    # ── BOOT inmediato de índices + velas (no esperar los ciclos) ───────────────
    # Así, al abrir el dashboard tras un deploy, el chart y los índices reales
    # ya tienen data en segundos en vez de esperar 3-4 min al primer job.
    async def _boot_data():
        try: await refresh_real_indices()
        except Exception as e: print(f"[indices] boot error: {e}")
        try: await _market_candles_impl("5")
        except Exception as e: print(f"[candles] boot error: {e}")
    asyncio.create_task(_boot_data())

    # ── Carga inicial: todo excepto FlashAlpha (ahorra créditos) ──────────
    print("="*60)
    print("🟢 LIBERATO BACKEND v2026.06.25-FIX11 — BUILD CORRECTO")
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
    if cache["gex"].get(FA_ASSET):
        print("[startup] GEX cargado desde disco ✓ (sin llamada a FlashAlpha)")
    else:
        print("[startup] Sin GEX en disco — cargará a las 9:00 AM ET (ahorra créditos)")
    # Generar resumen IA inmediatamente con los datos disponibles (con o sin GEX)
    asyncio.create_task(refresh_institutional())

    print("[startup] Liberato Backend v3.0 listo ✓")


# ═══════════════════════════════════════════════════════════════════════════
#  CONTACTO — recibe el formulario del home y envía correo a soporte
# ═══════════════════════════════════════════════════════════════════════════
_contact_rate = {}  # rate limiting simple anti-spam por IP

@app.post("/api/contact")
async def contact_form(request: Request):
    """
    Recibe {name, subject, description} del formulario de contacto y envía
    un correo a SUPPORT_EMAIL vía Gmail SMTP.
    Protección anti-spam: rate limiting + validación + honeypot (en frontend).
    """
    # Rate limiting: máximo 3 mensajes por IP cada 10 minutos
    ip = request.client.host if request.client else "unknown"
    now = time.time()
    bucket = [t for t in _contact_rate.get(ip, []) if now - t < 600]
    if len(bucket) >= 3:
        raise HTTPException(429, "Demasiados mensajes. Espera unos minutos.")
    bucket.append(now)
    _contact_rate[ip] = bucket

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "Datos inválidos")

    name = (body.get("name") or "").strip()
    subject = (body.get("subject") or "").strip()
    description = (body.get("description") or "").strip()

    # Validación de campos obligatorios
    if not name or not subject or not description:
        raise HTTPException(400, "Todos los campos son obligatorios")
    if len(name) > 120 or len(subject) > 200 or len(description) > 5000:
        raise HTTPException(400, "Contenido demasiado largo")
    if len(description) < 10:
        raise HTTPException(400, "La descripción es demasiado corta")

    # Construir y enviar el correo
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        print(f"[contact] ⚠️ Gmail no configurado — mensaje de {name} no enviado")
        print(f"[contact] Asunto: {subject} | {description[:80]}")
        # Devolver éxito igual para no romper la UX (el mensaje queda en logs)
        return {"success": True, "note": "logged"}

    html_body = f"""
    <div style="font-family:sans-serif;max-width:600px;margin:0 auto;">
      <h2 style="color:#C9A84C;">Nuevo mensaje de contacto · Liberato Community</h2>
      <p><strong>Nombre:</strong> {name}</p>
      <p><strong>Asunto:</strong> {subject}</p>
      <p><strong>Descripción:</strong></p>
      <p style="background:#f5f5f5;padding:14px;border-radius:8px;white-space:pre-wrap;">{description}</p>
      <hr>
      <p style="color:#888;font-size:12px;">Enviado desde el formulario de contacto de Liberato Community · IP: {ip}</p>
    </div>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"[Contacto Liberato] {subject}"
    msg["From"] = f"Liberato Community <{GMAIL_USER}>"
    msg["To"] = SUPPORT_EMAIL
    msg["Reply-To"] = GMAIL_USER  # respuestas van al emisor; el nombre va en el cuerpo
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_USER, SUPPORT_EMAIL, msg.as_string())
        print(f"[contact] ✓ Mensaje de {name} enviado a {SUPPORT_EMAIL}")
        return {"success": True}
    except Exception as e:
        print(f"[contact] ✗ Error enviando: {e}")
        raise HTTPException(500, "No se pudo enviar el mensaje. Intenta más tarde.")


# ═══════════════════════════════════════════════════════════════════════════
#  ENDPOINT DE PRUEBA MANUAL — dispara llamadas a FlashAlpha/Groq bajo demanda
#  Útil para probar sin esperar a las ventanas programadas.
#  Protegido con clave: agrega ?key=TU_CLAVE en la URL.
# ═══════════════════════════════════════════════════════════════════════════
ADMIN_KEY = os.getenv("ADMIN_KEY", "liberato2026")  # cámbiala en Railway si quieres

@app.get("/api/admin/refresh-gex")
async def manual_refresh_gex(key: str = ""):
    """Dispara una llamada manual a FlashAlpha (GEX). Uso: /api/admin/refresh-gex?key=TU_CLAVE"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    try:
        await refresh_gex()
        gex = cache["gex"].get(FA_ASSET)
        if gex:
            is_ndx = str(gex.get("source") or "").endswith("-direct")
            if is_ndx:
                # ES=F directo: los niveles YA están en puntos del índice. NO convertir.
                return {
                    "success": True,
                    "message": f"FlashAlpha {gex.get('ticker','?')} directo ✓ (sin conversión)",
                    "source": gex.get("source"),
                    "call_wall": gex.get("call_wall"),
                    "put_wall": gex.get("put_wall"),
                    "gamma_flip": gex.get("gamma_flip"),
                    "max_pain": gex.get("max_pain"),
                    "net_gex": gex.get("net_gex"),
                    "timestamp": gex.get("_ts"),
                }
            # Modo free (ETF): convertir a escala del futuro con ratio
            ratio = get_px_ratio()
            if not ratio:
                return {"success": False,
                        "message": f"FlashAlpha respondió, pero sin ratio real no se "
                                   f"convierte {FA_PROXY_ETF}→{FA_ASSET} (Regla #1)",
                        "source": gex.get("source"), "ratio": None,
                        "timestamp": gex.get("_ts")}
            def _cv(v): return round(v*ratio,2) if isinstance(v,(int,float)) else v
            return {
                "success": True,
                "message": f"FlashAlpha llamado manualmente ✓ ({FA_PROXY_ETF}→{FA_ASSET})",
                "source": "etf-converted",
                f"gamma_flip_{FA_PROXY_ETF}": gex.get("gamma_flip"),
                f"gamma_flip_{FA_ASSET}": _cv(gex.get("gamma_flip")),
                f"call_wall_{FA_ASSET}": _cv(gex.get("call_wall")),
                f"put_wall_{FA_ASSET}": _cv(gex.get("put_wall")),
                "ratio": ratio,
                "timestamp": gex.get("_ts"),
            }
        return {"success": False, "message": "FlashAlpha respondió pero sin datos GEX. Revisa la clave FLASHALPHA_KEY."}
    except Exception as e:
        return {"success": False, "error": str(e), "hint": "Revisa que FLASHALPHA_KEY esté configurada en Railway"}

@app.get("/api/admin/refresh-institutional")
async def manual_refresh_institutional(key: str = ""):
    """Dispara una llamada manual a Groq (resumen institucional). Uso: ?key=TU_CLAVE"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    try:
        await refresh_institutional()
        return {"success": True, "message": "Groq llamado manualmente ✓",
                "summary": cache.get("institutional", {}).get("text", "sin datos"),
                "status": cache.get("institutional", {}).get("status", "?")}
    except Exception as e:
        return {"success": False, "error": str(e), "hint": "Revisa que GROQ_KEY esté bien en Railway"}


# ═══════════════════════════════════════════════════════════════════════════
#  DIAGNÓSTICO DE SÍMBOLO — ¿el plan de FlashAlpha cubre este símbolo?
#  Prueba un símbolo ARBITRARIO sin tocar la config de producción, para poder
#  responder "¿nos da ES=F?" antes de migrar nada.
#  Uso: /api/admin/diag-symbol?sym=ES%3DF&key=liberato2026
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/diag-symbol")
async def diag_symbol(sym: str = "NQ=F", key: str = ""):
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    from urllib.parse import quote
    sym = (sym or "").strip().upper()
    sym_url = quote(sym, safe="")   # NQ=F → NQ%3DF (requerido por FlashAlpha)
    out = {"symbol": sym, "symbol_url": sym_url, "plan": FLASHALPHA_PLAN}
    if not FLASHALPHA_KEY:
        return {**out, "error": "no hay FLASHALPHA_KEY"}
    # Guardián de presupuesto: esta prueba gasta ~2 créditos de los 95/día.
    if not budget_ok("flashalpha", 2):
        st = _api_usage["flashalpha"]
        return {**out, "error": f"sin presupuesto FlashAlpha ({st['used']}/{API_BUDGETS['flashalpha']['limit']})"}
    try:
        async with httpx.AsyncClient(timeout=15,
                                     headers={"X-Api-Key": FLASHALPHA_KEY}) as client:
            r_lvl = await client.get(f"{FA_BASE}/v1/exposure/levels/{sym_url}")
            budget_charge("flashalpha", 1)
            out["levels_status"] = r_lvl.status_code
            if r_lvl.status_code == 200:
                lv = (r_lvl.json() or {}).get("levels", {}) or {}
                out["levels"] = {"call_wall": lv.get("call_wall"),
                                 "put_wall": lv.get("put_wall"),
                                 "gamma_flip": lv.get("gamma_flip"),
                                 "max_pain": lv.get("max_pain")}
                out["veredicto"] = f"✅ El plan CUBRE {sym} — niveles reales recibidos"
            elif r_lvl.status_code == 403:
                out["veredicto"] = f"❌ 403: el plan NO cubre {sym}"
                out["body"] = r_lvl.text[:200]
            elif r_lvl.status_code == 404:
                out["veredicto"] = f"❌ 404: FlashAlpha no conoce el símbolo {sym}"
                out["body"] = r_lvl.text[:200]
            elif r_lvl.status_code == 429:
                out["veredicto"] = "⚠️ 429: quota agotada (reset 00:00 UTC)"
            else:
                out["veredicto"] = f"⚠️ status inesperado {r_lvl.status_code}"
                out["body"] = r_lvl.text[:200]
            # Expiraciones: confirma que además del GEX hay cadena de opciones
            r_exp = await client.get(f"{FA_BASE}/v1/options/{sym_url}")
            budget_charge("flashalpha", 1)
            out["options_status"] = r_exp.status_code
            if r_exp.status_code == 200:
                ed = r_exp.json() or {}
                exps = ed.get("expirations") or []
                dates = [e if isinstance(e, str) else (e or {}).get("expiration")
                         for e in exps]
                out["expiraciones"] = [d for d in dates if d][:6]
    except Exception as e:
        out["error"] = repr(e)[:200]
    return out


# ═══════════════════════════════════════════════════════════════════════════
#  DIAGNÓSTICO FLASHALPHA — verifica plan, acceso a QQQ, y respuesta cruda
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/diag-ndx")
async def diag_ndx(key: str = ""):
    """Prueba el futuro DIRECTO del instrumento (plan Basic): confirma que los
    niveles reales llegan sin conversión.
    ⚠️ CUESTA ~3 créditos de los 100/día. Uso: ?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    # GUARDIÁN DE CRÉDITOS: este diag llama a FlashAlpha de verdad. Antes lo hacía
    # SIN comprobar ni registrar presupuesto → cada ejecución se comía ~3 créditos
    # invisibles de los 100/día. Los créditos de FlashAlpha son SOLO para el GEX;
    # un diagnóstico no puede robárselos a la sesión de trading.
    if not budget_ok("flashalpha", 3):
        st = _api_usage["flashalpha"]
        return {"status": "sin-presupuesto",
                "mensaje": f"Diag bloqueado: {st['used']}/{API_BUDGETS['flashalpha']['limit']} "
                           f"créditos usados hoy. Los créditos son para el GEX. "
                           f"Reset a las 00:00 UTC.",
                "usados": st["used"]}
    budget_charge("flashalpha", 3)
    sym = FA_INDEX_SYMBOL
    from urllib.parse import quote
    sym_url = quote(sym, safe="")  # NQ=F → NQ%3DF (requerido por FlashAlpha)
    out = {"symbol": sym, "plan_configurado": FLASHALPHA_PLAN,
           "key_present": bool(FLASHALPHA_KEY)}
    if not FLASHALPHA_KEY:
        return {**out, "error": "no hay FLASHALPHA_KEY"}
    try:
        async with httpx.AsyncClient(timeout=12,
                                      headers={"X-Api-Key": FLASHALPHA_KEY}) as client:
            r_lvl = await client.get(f"{FA_BASE}/v1/exposure/levels/{sym_url}")
            out["levels_status"] = r_lvl.status_code
            if r_lvl.status_code == 200:
                lv = (r_lvl.json() or {}).get("levels", {}) or {}
                out["levels"] = {
                    "call_wall": lv.get("call_wall"), "put_wall": lv.get("put_wall"),
                    "gamma_flip": lv.get("gamma_flip"), "max_pain": lv.get("max_pain"),
                }
                out["interpretacion"] = "✅ NDX directo FUNCIONA — Basic activo"
            elif r_lvl.status_code == 403:
                out["levels_body"] = r_lvl.text[:200]
                out["interpretacion"] = "❌ 403: el plan NO cubre índices. ¿Ya activaste Basic en FlashAlpha?"
            elif r_lvl.status_code == 429:
                out["interpretacion"] = "⚠️ 429: quota agotada. Espera al reset (00:00 UTC)."
            else:
                out["levels_body"] = r_lvl.text[:200]
            # Paso 1: obtener expiraciones REALES de NDX
            r_exp = await client.get(f"{FA_BASE}/v1/options/{sym_url}")
            out["options_status"] = r_exp.status_code
            exp = None
            if r_exp.status_code == 200:
                ed = r_exp.json() or {}
                exps = ed.get("expirations") or []
                exp_dates = []
                for e in exps:
                    if isinstance(e, str): exp_dates.append(e)
                    elif isinstance(e, dict) and e.get("expiration"): exp_dates.append(e["expiration"])
                out["expiraciones_disponibles"] = exp_dates[:6]
                today_str = _today_et_str()
                future = sorted([d for d in exp_dates if d > today_str])
                exp = future[0] if future else None
                out["expiracion_elegida"] = exp
            else:
                out["options_body"] = r_exp.text[:160]
            # Paso 2: probar VARIAS expiraciones hasta que una dé GEX
            future = sorted([d for d in exp_dates if d > _today_et_str()]) if exp_dates else ([exp] if exp else [])
            out["gex_intentos"] = []
            gex_ok = False
            for cand in future[:4]:
                r_gex = await client.get(f"{FA_BASE}/v1/exposure/gex/{sym}",
                                         params={"expiration": cand})
                out["gex_intentos"].append({"exp": cand, "status": r_gex.status_code})
                if r_gex.status_code == 200:
                    gd = r_gex.json() or {}
                    strikes = gd.get("strikes")
                    out["gex_status"] = 200
                    out["gex_expiracion_ok"] = cand
                    out["net_gex"] = gd.get("net_gex")
                    out["net_gex_label"] = gd.get("net_gex_label")
                    out["per_strike_count"] = len(strikes) if isinstance(strikes, list) else 0
                    out["gex_interpretacion"] = f"✅ /gex FUNCIONA con expiración {cand}"
                    gex_ok = True
                    break
            if not gex_ok:
                out["gex_status"] = "todos fallaron"
                out["gex_body_ultimo"] = r_gex.text[:200] if future else "sin expiraciones"
                out["gex_interpretacion"] = "⚠️ Ninguna expiración dio GEX. Los 3 niveles (levels) SÍ funcionan; net_gex es secundario."
    except Exception as e:
        out["error"] = str(e)
    return out


@app.get("/api/admin/diag-flashalpha")
async def diag_flashalpha(key: str = ""):
    """Diagnóstico completo de FlashAlpha: plan, quota, y qué devuelve.
    ⚠️ CUESTA ~3 créditos de los 100/día. Uso: ?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    # GUARDIÁN DE CRÉDITOS: este diag llama a FlashAlpha de verdad. Antes lo hacía
    # SIN comprobar ni registrar presupuesto → cada ejecución se comía ~3 créditos
    # invisibles de los 100/día. Los créditos de FlashAlpha son SOLO para el GEX;
    # un diagnóstico no puede robárselos a la sesión de trading.
    if not budget_ok("flashalpha", 3):
        st = _api_usage["flashalpha"]
        return {"status": "sin-presupuesto",
                "mensaje": f"Diag bloqueado: {st['used']}/{API_BUDGETS['flashalpha']['limit']} "
                           f"créditos usados hoy. Los créditos son para el GEX. "
                           f"Reset a las 00:00 UTC.",
                "usados": st["used"]}
    budget_charge("flashalpha", 3)
    out = {"flashalpha_key_present": bool(FLASHALPHA_KEY),
           "plan_configurado": FLASHALPHA_PLAN,
           "simbolo_indice": FA_INDEX_SYMBOL,
           "nota": "Si plan=basic usa NDX directo; si plan=free usa QQQ+conversión"}
    if not FLASHALPHA_KEY:
        return {**out, "error": "No hay FLASHALPHA_KEY configurada en Railway"}
    try:
        async with httpx.AsyncClient(timeout=15,
                                      headers={"X-Api-Key": FLASHALPHA_KEY}) as client:
            # 1. ¿Qué plan tengo?
            try:
                acc = await client.get(f"{FA_BASE}/v1/account")
                if acc.status_code == 200:
                    a = acc.json()
                    out["plan"] = a.get("plan")
                    out["daily_limit"] = a.get("daily_limit")
                    out["usage_today"] = a.get("usage_today")
                    out["remaining"] = a.get("remaining")
                else:
                    out["account_status"] = acc.status_code
            except Exception as e:
                out["account_error"] = str(e)
            # 2. ¿Qué devuelve QQQ summary? (lo que usa el dashboard)
            try:
                r = await client.get(f"{FA_BASE}/v1/stock/QQQ/summary")
                out["qqq_summary_status"] = r.status_code
                if r.status_code == 200:
                    d = r.json()
                    ex = d.get("exposure", {}) or {}
                    out["qqq_as_of"] = d.get("as_of")
                    out["qqq_market_open"] = d.get("market_open")
                    out["qqq_call_wall"] = ex.get("call_wall")
                    out["qqq_put_wall"] = ex.get("put_wall")
                    out["qqq_gamma_flip"] = ex.get("gamma_flip")
                    out["qqq_exposure_keys"] = list(ex.keys())
                else:
                    out["qqq_error_body"] = r.text[:300]
            except Exception as e:
                out["qqq_error"] = str(e)
            # 3. Probar el endpoint /v1/exposure/levels/QQQ (alternativa, Basic+)
            try:
                lv = await client.get(f"{FA_BASE}/v1/exposure/levels/QQQ")
                out["levels_qqq_status"] = lv.status_code
                if lv.status_code == 200:
                    out["levels_qqq"] = lv.json().get("levels")
                elif lv.status_code in (403, 429):
                    out["levels_qqq_error"] = lv.json()
            except Exception as e:
                out["levels_error"] = str(e)
    except Exception as e:
        out["fatal_error"] = str(e)
    return out


# ═══════════════════════════════════════════════════════════════════════════
#  DIAGNÓSTICO CALENDARIO — qué devuelve cada fuente para el "actual"
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/diag-calendar")
async def diag_calendar(key: str = ""):
    """Diagnóstico: muestra qué trae cada fuente del calendario (FF, Finnhub, RapidAPI).
    Uso: /api/admin/diag-calendar?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    out = {"sources": {}}
    async with httpx.AsyncClient(timeout=15) as client:
        # 1. ForexFactory
        try:
            r = await client.get(FF_URLS[0])
            ct = r.headers.get("content-type","")
            if "json" in ct.lower():
                data = r.json()
                # Buscar eventos US de hoy con actual
                today = datetime.now(NY).strftime("%Y-%m-%d")
                us_today = [e for e in data if e.get("country")=="USD" and today in str(e.get("date",""))]
                with_actual = [e for e in us_today if e.get("actual")]
                out["sources"]["forexfactory"] = {
                    "status": r.status_code, "blocked": False,
                    "total_events": len(data),
                    "us_today": len(us_today),
                    "us_today_with_actual": len(with_actual),
                    "sample": [{"title": e.get("title"), "actual": e.get("actual"),
                                "forecast": e.get("forecast"), "prev": e.get("previous")}
                               for e in us_today[:5]],
                }
            else:
                out["sources"]["forexfactory"] = {"status": r.status_code, "blocked": True,
                                                   "note": "Request Denied (rate limit)"}
        except Exception as e:
            out["sources"]["forexfactory"] = {"error": str(e)}
        # 2. Finnhub
        try:
            now_et = datetime.now(NY)
            r = await client.get(f"{FH_BASE}/calendar/economic",
                params={"from": now_et.strftime("%Y-%m-%d"),
                        "to": now_et.strftime("%Y-%m-%d"), "token": FINNHUB_KEY})
            if r.status_code == 200:
                body = r.json()
                cal = body.get("economicCalendar", []) if isinstance(body, dict) else []
                us = [e for e in cal if e.get("country","").upper()=="US"]
                with_actual = [e for e in us if e.get("actual") is not None]
                out["sources"]["finnhub"] = {
                    "status": 200, "total": len(cal), "us": len(us),
                    "us_with_actual": len(with_actual),
                    "raw_keys": list(body.keys()) if isinstance(body, dict) else "not-dict",
                    "sample": [{"event": e.get("event"), "actual": e.get("actual"),
                                "estimate": e.get("estimate"), "prev": e.get("prev")}
                               for e in us[:5]],
                }
            else:
                out["sources"]["finnhub"] = {"status": r.status_code, "body": r.text[:200]}
        except Exception as e:
            out["sources"]["finnhub"] = {"error": str(e)}
        # 3. RapidAPI — TradingEconomics /calendar (fuente del "actual" en vivo)
        try:
            if RAPIDAPI_KEY:
                headers = {"X-RapidAPI-Key": RAPIDAPI_KEY, "X-RapidAPI-Host": RAPIDAPI_HOST}
                # Diag: ventana ancha (5 días) para ver eventos US recientes reales
                # y poder validar el filtro. La PRODUCCIÓN usa daysBehind=2 (instant).
                r = await client.get(f"https://{RAPIDAPI_HOST}/calendar",
                    headers=headers, params={
                        "daysBehind": "5", "daysAhead": "0", "impact": "High",
                        "resolved": "true", "descriptions": "false", "limit": "80",
                        "tz": "America/New_York",
                        "fields": "id,date,eventName,country,impactLabel,actual,forecast,previous"})
                out["sources"]["rapidapi"] = {"status": r.status_code, "host": RAPIDAPI_HOST}
                if r.status_code == 200:
                    d = r.json()
                    raw = d.get("events") if isinstance(d, dict) else d
                    raw = raw if isinstance(raw, list) else []
                    us = [e for e in raw if (e.get("country","") or "").strip().lower()
                          not in _RT_NON_US_COUNTRIES and _rt_relevant(e.get("eventName",""))]
                    with_actual = [e for e in us if e.get("actual")]
                    out["sources"]["rapidapi"]["total"] = len(raw)
                    out["sources"]["rapidapi"]["us_relevant"] = len(us)
                    out["sources"]["rapidapi"]["us_with_actual"] = len(with_actual)
                    out["sources"]["rapidapi"]["daily_calls"] = _rapidapi_day_count
                    # RAW: TODOS los eventos tal cual (para validar país/nombre/actual)
                    out["sources"]["rapidapi"]["raw_events"] = [
                        {"country": e.get("country"), "eventName": e.get("eventName"),
                         "actual": e.get("actual"), "forecast": e.get("forecast")}
                        for e in raw[:20]]
                    out["sources"]["rapidapi"]["us_sample"] = [
                        {"eventName": e.get("eventName"), "country": e.get("country"),
                         "actual": e.get("actual"), "forecast": e.get("forecast"),
                         "previous": e.get("previous")}
                        for e in us[:6]]
                else:
                    out["sources"]["rapidapi"]["body"] = r.text[:200]
            else:
                out["sources"]["rapidapi"] = {"note": "No RAPIDAPI_KEY configurada"}
        except Exception as e:
            out["sources"]["rapidapi"] = {"error": str(e)}
        # 4. FMP (Financial Modeling Prep)
        try:
            if FMP_KEY:
                now_et = datetime.now(NY)
                frm = now_et.strftime("%Y-%m-%d")
                # Consultar desde AYER (los eventos pasados ya deben tener actual)
                frm_past = (now_et - timedelta(days=2)).strftime("%Y-%m-%d")
                to_diag = (now_et + timedelta(days=1)).strftime("%Y-%m-%d")
                r = await client.get(f"{FMP_BASE}/economic-calendar",
                    params={"from": frm_past, "to": to_diag, "apikey": FMP_KEY})
                out["sources"]["fmp"] = {"status": r.status_code}
                if r.status_code == 200:
                    d = r.json()
                    if isinstance(d, list):
                        us = [e for e in d if (e.get("country","") or "").upper() in ("US","USA","UNITED STATES")]
                        with_a = [e for e in us if e.get("actual") is not None]
                        high = [e for e in us if (e.get("impact","") or "").lower()=="high"]
                        out["sources"]["fmp"]["total"] = len(d)
                        out["sources"]["fmp"]["us"] = len(us)
                        out["sources"]["fmp"]["us_high_impact"] = len(high)
                        out["sources"]["fmp"]["us_with_actual"] = len(with_a)
                        # Mostrar eventos de ALTO impacto (los que importan)
                        out["sources"]["fmp"]["sample_high_impact"] = [
                            {"event": e.get("event"), "date": e.get("date"),
                             "actual": e.get("actual"), "estimate": e.get("estimate"),
                             "previous": e.get("previous")} for e in high[:8]]
                        # Mostrar los que SÍ tienen actual (si hay)
                        out["sources"]["fmp"]["sample_with_actual"] = [
                            {"event": e.get("event"), "actual": e.get("actual")}
                            for e in with_a[:5]]
                    else:
                        out["sources"]["fmp"]["note"] = "respuesta no es lista"
                        out["sources"]["fmp"]["body"] = str(d)[:200]
                else:
                    out["sources"]["fmp"]["body"] = r.text[:200]
            else:
                out["sources"]["fmp"] = {"note": "No FMP_KEY configurada"}
        except Exception as e:
            out["sources"]["fmp"] = {"error": str(e)}
    return out


# ═══════════════════════════════════════════════════════════════════════════
#  DIAGNÓSTICO DE VARIABLES — verifica qué keys están configuradas en Railway
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/diag-env")
async def diag_env(key: str = ""):
    """Muestra qué variables de entorno detecta el sistema (sin exponer las keys
    completas, solo si están presentes y sus primeros caracteres).
    Uso: /api/admin/diag-env?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    def mask(v):
        if not v: return None
        s = str(v)
        return f"{s[:4]}...{s[-3:]} ({len(s)} chars)" if len(s) > 8 else "***corta***"
    # Revisar TODOS los nombres posibles de cada variable
    return {
        "FMP": {
            "FMP_KEY": mask(os.getenv("FMP_KEY")),
            "FMP_API_KEY": mask(os.getenv("FMP_API_KEY")),
            "FINANCIAL_MODELING_PREP_KEY": mask(os.getenv("FINANCIAL_MODELING_PREP_KEY")),
            "mfp": mask(os.getenv("mfp")),
            "MFP": mask(os.getenv("MFP")),
            "FMP": mask(os.getenv("FMP")),
            "_detectada_por_codigo": mask(FMP_KEY),
        },
        "RAPIDAPI": {
            "RAPIDAPI_KEY": mask(os.getenv("RAPIDAPI_KEY")),
            "x-rapidapi-key": mask(os.getenv("x-rapidapi-key")),
            "X_RAPIDAPI_KEY": mask(os.getenv("X_RAPIDAPI_KEY")),
            "_detectada_por_codigo": mask(RAPIDAPI_KEY),
            "_host": RAPIDAPI_HOST,
        },
        "FLASHALPHA": {
            "_detectada": mask(FLASHALPHA_KEY),
        },
        "otras": {
            "FINNHUB": mask(FINNHUB_KEY),
            "TWELVEDATA": mask(TWELVEDATA_KEY),
            "GROQ": mask(GROQ_KEY),
        },
        "ayuda": "Si '_detectada_por_codigo' es null, el código NO está leyendo esa key. Revisa el nombre de la variable en Railway.",
        "ADVERTENCIAS": {
            "fmp": ("✅ FMP key detectada" if FMP_KEY else "❌ No hay FMP key (revisa la variable MFP en Railway)"),
            "rapidapi": ("✅ RapidAPI usando x-rapidapi-key (real)"
                         if (RAPIDAPI_KEY and "aqui-tu" not in RAPIDAPI_KEY and len(RAPIDAPI_KEY) > 20)
                         else "⚠️ RapidAPI key inválida o placeholder"),
            "rapidapi_host": f"Host activo: {RAPIDAPI_HOST}",
        }
    }


# ═══════════════════════════════════════════════════════════════════════════
#  DIAGNÓSTICO RAPIDAPI — prueba varios endpoints y muestra cuál funciona
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/diag-rapidapi")
async def diag_rapidapi(key: str = ""):
    """Prueba los endpoints de la Ultimate Economic Calendar para ver cuál responde.
    Uso: /api/admin/diag-rapidapi?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    if not RAPIDAPI_KEY:
        return {"error": "No hay RAPIDAPI_KEY"}
    headers = {"X-RapidAPI-Key": RAPIDAPI_KEY, "X-RapidAPI-Host": RAPIDAPI_HOST}
    base = {"impact": "High", "descriptions": "false", "sort": "asc",
            "limit": "80", "tz": "America/New_York",
            "fields": "id,date,eventName,country,impactLabel,actual,forecast,previous"}
    out = {"host": RAPIDAPI_HOST, "endpoint": "/calendar", "pruebas": {}}
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            for label, extra in [
                ("resueltos_7d", {"daysBehind": "7", "daysAhead": "0", "resolved": "true"}),
                ("proximos_3d",  {"daysBehind": "0", "daysAhead": "3", "resolved": "false"}),
            ]:
                r = await client.get(f"https://{RAPIDAPI_HOST}/calendar",
                                     headers=headers, params={**base, **extra})
                info = {"status": r.status_code}
                if r.status_code == 200:
                    d = r.json()
                    evs = d.get("events") if isinstance(d, dict) else d
                    evs = evs if isinstance(evs, list) else []
                    info["total_eventos"] = len(evs)
                    # Muestra los primeros 5 con sus campos clave para inspección
                    info["muestra"] = [{
                        "eventName": e.get("eventName"), "date": e.get("date"),
                        "country": e.get("country"), "actual": e.get("actual"),
                        "forecast": e.get("forecast"), "previous": e.get("previous"),
                    } for e in evs[:5]]
                    # ¿Aparece NFP / Payrolls?
                    info["tiene_nfp"] = any("payroll" in (e.get("eventName","") or "").lower()
                                            or "non-farm" in (e.get("eventName","") or "").lower()
                                            for e in evs)
                else:
                    info["body"] = r.text[:200]
                out["pruebas"][label] = info
    except Exception as e:
        out["error"] = str(e)
    out["contador_hoy"] = f"{_rapidapi_day_count}/85"
    return out
