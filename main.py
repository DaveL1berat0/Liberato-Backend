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
import os, time, asyncio, json, hashlib, hmac, base64, secrets
from urllib.parse import quote   # usado a nivel módulo (config del instrumento) y en varias funciones
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import httpx
from fastapi import FastAPI, HTTPException, Request, Response, Header
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import websockets

# ══ CREDENCIALES (solo Railway Variables, nunca en código) ════════════════════
FLASHALPHA_KEY   = os.getenv("FLASHALPHA_KEY",   "").strip()
# ── GexBot (fuente del gamma profile; permiso escrito 17-ago: atribución + educativo) ──
GEXBOT_API_KEY   = os.getenv("GEXBOT_API_KEY",   "").strip()
GEXBOT_SYMBOL    = os.getenv("GEXBOT_SYMBOL",   "NQ_NDX").strip()   # ticker GexBot v2 (futuro NQ)
GEXBOT_BASE      = "https://api.gex.bot/v2"                      # API v2 (Bearer auth)
def _gexbot_headers():
    return {"Authorization": f"Bearer {GEXBOT_API_KEY}",
            "Accept": "application/json", "User-Agent": "liberato-community/1.0"}
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
# ── TradeStation (journal automático, SOLO LECTURA) ──────────────────────────
# Se obtienen por email a ClientExperience@tradestation.com (no hay self-service).
# El scope pedido NO incluye "Trade": el sistema puede VER trades, nunca operar.
TRADESTATION_CLIENT_ID     = os.getenv("TRADESTATION_CLIENT_ID", "").strip()
TRADESTATION_CLIENT_SECRET = os.getenv("TRADESTATION_CLIENT_SECRET", "").strip()
TRADESTATION_REDIRECT_URI  = os.getenv("TRADESTATION_REDIRECT_URI",
    "https://web-production-33671.up.railway.app/api/broker/tradestation/callback").strip()
# API directa de TradeStation (trae FUTUROS, que SnapTrade no expone). LIVE por
# defecto; SIM si TRADESTATION_ENV=sim. Tokens OAuth persistidos en el snapshot.
TS_ENV        = os.getenv("TRADESTATION_ENV", "live").strip().lower()
TS_API_BASE   = "https://sim-api.tradestation.com/v3" if TS_ENV == "sim" else "https://api.tradestation.com/v3"
TS_TOKEN_URL  = "https://signin.tradestation.com/oauth/token"
TS_REDIRECT_AFTER = os.getenv("TS_REDIRECT_AFTER",
    "https://davel1berat0.github.io/Liberato-Backend/journal.html?tradestation=ok").strip()
_ts_tokens = {}   # {access_token, refresh_token, expires_at (epoch)}
# ── SnapTrade (agregador multi-broker, self-service, SOLO LECTURA) ────────────
# Credenciales self-service en snaptrade.com (no requiere email a ningún broker).
SNAPTRADE_CLIENT_ID   = os.getenv("SNAPTRADE_CLIENT_ID", "").strip()
SNAPTRADE_CONSUMER_KEY = os.getenv("SNAPTRADE_CONSUMER_KEY", "").strip()
# A dónde vuelve el estudiante tras conectar su broker en el portal de SnapTrade.
SNAPTRADE_REDIRECT_URI = os.getenv("SNAPTRADE_REDIRECT_URI",
    "https://davel1berat0.github.io/Liberato-Backend/journal.html?snaptrade=ok").strip()
# "personal" (key gratis = 1 usuario, tú) o "commercial" (multiusuario, de pago).
# En personal NO se registra usuario ni se envían userId/userSecret: la key ya
# identifica al usuario. Para pasar a multiusuario: SNAPTRADE_MODE=commercial.
SNAPTRADE_MODE = os.getenv("SNAPTRADE_MODE", "personal").strip().lower()
# Store por-usuario: {app_user_id: {snap_user_id, user_secret, accounts:[], ts}}.
# Persiste en el snapshot (_PERSIST) para sobrevivir redeploys — el user_secret
# es intransferible y re-registrar borra las conexiones del broker.
_snaptrade_users = {}
_st_client = None
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
    "flashalpha":  {"limit": int(os.getenv("FA_DAILY_LIMIT", "240")),  "window": "day"},   # real 250 (Basic subió de 100 el 26-jul)
    "fmp":         {"limit": int(os.getenv("FMP_DAILY_LIMIT", "230")), "window": "day"},   # real 250
    "alphavantage":{"limit": int(os.getenv("AV_DAILY_LIMIT", "22")),   "window": "day"},   # real 25
    "groq":        {"limit": int(os.getenv("GROQ_DAILY_LIMIT", "950")),"window": "day"},   # llamadas/día
}
# Estado de uso por API: {"window_key": str, "used": int}
_api_usage = {name: {"window_key": None, "used": 0} for name in API_BUDGETS}

# ── Tope de AI Coach por ESTUDIANTE/día ──────────────────────────────────────
# El briefing institucional es COMPARTIDO (1 para toda la plataforma, no escala).
# El AI Coach es POR ESTUDIANTE: este tope evita que un usuario agote la cuota
# compartida de Groq (950/día). Configurable en Railway.
COACH_DAILY_PER_USER = int(os.getenv("COACH_DAILY_PER_USER", "40"))
_coach_usage = {}  # {app_user_id: {"day": "YYYY-MM-DD", "count": n}}
def _coach_quota_ok(uid):
    from datetime import date
    day = date.today().isoformat()
    st = _coach_usage.get(uid)
    if not st or st.get("day") != day:
        st = {"day": day, "count": 0}; _coach_usage[uid] = st
    return st["count"] < COACH_DAILY_PER_USER
def _coach_charge(uid):
    st = _coach_usage.get(uid)
    if st: st["count"] += 1

# ── Fallback DORMIDO a Gemini ────────────────────────────────────────────────
# Solo se usa si Groq falla Y existe GEMINI_API_KEY en Railway. Sin la key es
# totalmente inerte (devuelve None y el flujo se comporta igual que hoy). Pensado
# para el día que Groq jubile un modelo: pegas la key y el coach sigue vivo.
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
# gemini-3.6-flash: verificado en la cuenta de Dave (gemini-2.5/2.0-flash ya no están
# para cuentas nuevas). Gemini 3.x RAZONA (~700 tok de thought) → se le da margen extra.
GEMINI_MODEL   = os.getenv("GEMINI_MODEL", "gemini-3.6-flash").strip()
async def _gemini_chat(sys_msg, usr_msg, max_tokens=400, temperature=0.5):
    if not GEMINI_API_KEY:
        return None
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}")
    body = {
        "systemInstruction": {"parts": [{"text": sys_msg}]},
        "contents": [{"role": "user", "parts": [{"text": usr_msg}]}],
        # +900 de colchón: Gemini 3.x gasta tokens de "thought" antes del texto.
        "generationConfig": {"maxOutputTokens": max_tokens + 900, "temperature": temperature},
    }
    try:
        async with httpx.AsyncClient(timeout=25) as c:
            r = await c.post(url, json=body)
        if r.status_code != 200:
            print(f"[gemini] {r.status_code}: {r.text[:160]}")
            return None
        j = r.json()
        cand = (j.get("candidates") or [{}])[0]
        parts = ((cand.get("content") or {}).get("parts") or [])
        txt = "".join(p.get("text", "") for p in parts).strip()
        return txt or None
    except Exception as e:
        print(f"[gemini] error: {e}")
        return None

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
#
#  🔴 CAMBIO 30-jul: FlashAlpha subió Basic a 250/día PERO restringió los FUTUROS
#  (NQ=F, ES=F) al plan Growth (403 tier_restricted). Basic ahora cubre ETFs e
#  índices: SPY, QQQ, IWM, SPX, VIX, RUT, NDX. Verificado con diag-symbol:
#  NQ=F→403, NDX→200, QQQ→200. Se pasa la fuente al ÍNDICE NDX (Nasdaq-100), que
#  es el SUBYACENTE REAL de las opciones — sus gamma levels son los verdaderos
#  del Nasdaq-100 y el futuro NQ los respeta (basis de cientos de pts, no miles:
#  NDX flip 28304 / put 27000 vs NQ ~27162, misma escala). Directo, sin ratio.
#  Para volver al ES: NDX→SPX, QQQ→SPY, NQ→ES, ^NDX→^GSPC (todos índices/ETF, no
#  futuros — el futuro requiere Growth).
FA_INDEX_SYMBOL = os.getenv("FA_INDEX_SYMBOL", "NDX")  # índice Nasdaq-100 directo (Basic no cubre NQ=F)
# Proxy para precio/velas: TwelveData free no da futuros, solo el ETF.
# NQ→QQQ. El ratio NO se hardcodea (antes vivía como 41.51): ver get_px_ratio.
FA_PROXY_ETF    = os.getenv("FA_PROXY_ETF", "QQQ").strip().upper()
# Símbolo para /exposure/gex (net_gex + régimen). NDX da /levels (walls) pero su
# /gex responde 404 "no options data" en Basic; QQQ (ETF) SÍ da net_gex. Como el
# net_gex es EXPOSICIÓN del dealer (lo que define el régimen es su SIGNO, no la
# escala del precio), se toma de QQQ. Los walls/flip siguen de NDX (escala real).
FA_GEX_SYMBOL   = os.getenv("FA_GEX_SYMBOL", FA_PROXY_ETF).strip().upper()  # QQQ
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
GEX_REFRESHES_PER_DAY = 56
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
        # _rapidapi_day/_count se leen aquí (solo lectura, pero se declaran para
        # que el linter no los confunda con locales al referenciarlos).
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
            # El contador de RapidAPI usa su propio mecanismo (_rapidapi_day_count),
            # separado de _api_usage. Tenía EL MISMO bug que FlashAlpha: vivía en
            # memoria y se reseteaba en cada redeploy → el guardián de 85/día creía
            # tener cuota tras un deploy. Se persiste su día + contador.
            "rapidapi_count": {"day": _rapidapi_day, "count": _rapidapi_day_count},
            "movers_seen": cache.get("_movers_seen", {}),
            "movers": {"data": cache["movers"]["data"],
                       "lu":   cache["movers"]["last_update"]},
            # Credenciales SnapTrade por usuario (user_secret intransferible).
            "snaptrade_users": _snaptrade_users,
            "ts_tokens": _ts_tokens,   # tokens OAuth de TradeStation (per-usuario)
            "users": _users,           # cuentas de estudiantes (auth: hash+salt+plan)
        }
        with open(_PERSIST, "w") as f:
            json.dump(snap, f)
    except Exception as e:
        print(f"[persist] error guardando: {e}")

def load_cache():
    global _rapidapi_day, _rapidapi_day_count
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
        # Contador de RapidAPI (separado de _api_usage). Solo si sigue siendo HOY;
        # si no, se queda en 0 y refresh_calendar lo reinicia al cambiar de día.
        _rc = snap.get("rapidapi_count")
        if isinstance(_rc, dict) and _rc.get("day") == _today_et_str():
            _rapidapi_day = _rc["day"]
            _rapidapi_day_count = int(_rc.get("count", 0))
            print(f"[persist] rapidapi restaurado: {_rapidapi_day_count}/85")
        _su = snap.get("snaptrade_users")
        if isinstance(_su, dict):
            _snaptrade_users.update(_su)
            print(f"[persist] snaptrade_users restaurados: {len(_snaptrade_users)}")
        _tst = snap.get("ts_tokens")
        if isinstance(_tst, dict):
            _ts_tokens.update(_tst)
            print(f"[persist] ts_tokens restaurados: {len(_ts_tokens)}")
        _usr = snap.get("users")
        if isinstance(_usr, dict):
            _users.update(_usr)
            print(f"[persist] usuarios restaurados: {len(_users)}")
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
    if now - _indices_last_ts < 10:   # precio más fresco: 20s→10s (feed gratis, self-guarded por presupuesto)
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
    if now - _indices_last_ts < 20:
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
    """GEX 100% de GexBot + sentiment (VIX/Fear&Greed/Expected Move) de fuentes reales.
    FlashAlpha ELIMINADO (Dave: ya no trabajamos con ellos)."""
    # ── 1) GexBot: gamma primaria (walls/flip/net/per-strike) ──
    if GEXBOT_API_KEY:
        try:
            await _refresh_gex_gexbot(asset)
        except Exception as e:
            print(f"[gexbot] overlay falló: {e}")
    # ── 2) Sentiment (VIX real + Fear&Greed CNN + Expected Move derivado) ──
    try:
        await _refresh_market_sentiment(asset)
    except Exception as e:
        print(f"[sentiment] falló: {e}")


async def _refresh_gex_gexbot(asset=FA_ASSET):
    """Capa GEX de GexBot — FUENTE PRIMARIA del gamma profile.
    Permiso escrito de GexBot (17-ago): atribución visible + uso educativo.
    Tier Classic → GET /{ticker}/classic/full (Bearer). Sobre-escribe SOLO los
    campos de gamma (walls/flip/net/per-strike/spot), preservando los auxiliares
    (fear/vix/expected_move/atm_iv/max_pain) que provee FlashAlpha. Escala NQ
    directa (spot ~29.500): NO se convierte con ratio."""
    if not GEXBOT_API_KEY:
        return False
    url = f"{GEXBOT_BASE}/{GEXBOT_SYMBOL}/classic/full"
    try:
        async with httpx.AsyncClient(timeout=12, headers=_gexbot_headers()) as c:
            r = await c.get(url)
        if r.status_code != 200:
            cache["health"]["gexbot"] = f"http-{r.status_code}"
            print(f"[gexbot] {GEXBOT_SYMBOL} classic/full HTTP {r.status_code}: {r.text[:120]}")
            return False
        j = r.json()
    except Exception as e:
        cache["health"]["gexbot"] = "error"
        print(f"[gexbot] excepción: {e}")
        return False

    spot = j.get("spot")
    gf   = j.get("zero_gamma")           # gamma flip (zero gamma)
    cw   = j.get("major_pos_oi")         # call wall  (mayor gamma positivo, por OI)
    pw   = j.get("major_neg_oi")         # put wall   (mayor gamma negativo, por OI)
    net  = j.get("sum_gex_oi")           # net gex (por OI)
    # ── EXTRA de Classic que no usábamos (sacar el 100% al plan) ──
    cw_v = j.get("major_pos_vol")        # call wall por VOLUMEN (posicionamiento intradía fresco)
    pw_v = j.get("major_neg_vol")        # put wall  por VOLUMEN
    net_v= j.get("sum_gex_vol")          # net gex por VOLUMEN
    skew = j.get("delta_risk_reversal")  # sesgo call/put (>0 alcista / calls más caros; <0 bajista)
    min_dte = j.get("min_dte")           # DTE del vencimiento más cercano (0 = hay 0DTE hoy)
    ts_src = j.get("timestamp")

    # strikes: filas [precio, gex_vol, gex_oi, [priors]] → {strike, gex} usando gex_oi.
    # Signo: negativo = put side (morado), positivo = call side (verde). Ya en NQ.
    per_strike = []
    for row in (j.get("strikes") or []):
        try:
            if not isinstance(row, (list, tuple)) or len(row) < 3:
                continue
            strike = float(row[0]); gex_oi = float(row[2])
            per_strike.append({"strike": strike, "gex": gex_oi})
        except (TypeError, ValueError):
            continue

    if cw is None and pw is None and gf is None and not per_strike:
        cache["health"]["gexbot"] = "online-no-levels"
        print(f"[gexbot] ⚠️ 200 sin niveles. keys={list(j.keys())}")
        return False

    g = cache["gex"].get(asset) or {}
    g.update({
        "underlying_price": spot,
        "call_wall": cw, "put_wall": pw, "gamma_flip": gf,
        "net_gex": net,
        # Capa por VOLUMEN (intradía fresco) + skew, todo de Classic:
        "call_wall_vol": cw_v, "put_wall_vol": pw_v, "net_gex_vol": net_v,
        "skew": skew, "min_dte": min_dte,
        "per_strike": per_strike,
        "per_strike_count": len(per_strike),
        "ticker": GEXBOT_SYMBOL,
        "source": "gexbot-direct",
        "gex_source": "GexBot",          # atribución obligatoria (permiso escrito)
        "regime": ("trending" if (isinstance(net, (int, float)) and net < 0)
                   else "pinning" if isinstance(net, (int, float)) else g.get("regime")),
        "_ts": time.time(),
    })
    if isinstance(ts_src, (int, float)) and ts_src > 0:
        try:
            g["as_of"] = datetime.fromtimestamp(ts_src, timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            pass
    # ── 2ª llamada: 0DTE (classic/zero) → capa filosa del scalper (toggle) ──
    # Mismos campos pero solo del vencimiento de HOY. Se guarda en g["zero"].
    try:
        async with httpx.AsyncClient(timeout=10, headers=_gexbot_headers()) as c0:
            r0 = await c0.get(f"{GEXBOT_BASE}/{GEXBOT_SYMBOL}/classic/zero")
        if r0.status_code == 200:
            j0 = r0.json() or {}
            _z_net = j0.get("sum_gex_oi")
            g["zero"] = {
                "gamma_flip":   j0.get("zero_gamma"),
                "call_wall":    j0.get("major_pos_oi"),
                "put_wall":     j0.get("major_neg_oi"),
                "call_wall_vol":j0.get("major_pos_vol"),
                "put_wall_vol": j0.get("major_neg_vol"),
                "net_gex":      _z_net,
                "net_gex_vol":  j0.get("sum_gex_vol"),
                "skew":         j0.get("delta_risk_reversal"),
                "regime": ("trending" if (isinstance(_z_net,(int,float)) and _z_net < 0)
                           else "pinning" if isinstance(_z_net,(int,float)) else None),
            }
        else:
            print(f"[gexbot] classic/zero HTTP {r0.status_code}")
    except Exception as _e0:
        print(f"[gexbot] 0DTE (zero) falló: {_e0}")
    cache["gex"][asset] = g
    if isinstance(spot, (int, float)) and spot > 0:
        _set_px_ratio_from_spot(spot)
    cache["health"]["gexbot"] = "online"
    print(f"[gexbot] ok {GEXBOT_SYMBOL}: flip={gf} call={cw} put={pw} net={net} strikes={len(per_strike)}")
    return True


import math as _math

async def _refresh_market_sentiment(asset=FA_ASSET):
    """Reemplazo de FlashAlpha para Fear&Greed, VIX y Expected Move — TODO de fuentes
    reales gratuitas (Regla #1): Fear&Greed de CNN, VIX de TwelveData, y el Expected
    Move DERIVADO del VIX real (no de FlashAlpha). Escribe los campos aux en el cache
    del GEX sin tocar los niveles de gamma (que vienen de GexBot)."""
    g = cache["gex"].setdefault(asset, {})
    # ── VIX real (TwelveData /quote) ──────────────────────────────────────────
    vix = None
    if TWELVEDATA_KEY and td_budget_ok(1):
        try:
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get("https://api.twelvedata.com/quote",
                                params={"symbol": "VIX", "apikey": TWELVEDATA_KEY})
            td_charge(1)  # contabilizar el crédito (antes ~220/día del VIX sin contar)
            if r.status_code == 200:
                j = r.json() or {}
                v = j.get("close") or j.get("price")
                if v not in (None, "", "0"):
                    vix = round(float(v), 2)
        except Exception as e:
            print(f"[sentiment] VIX TwelveData falló: {e}")
    if vix is not None:
        g["vix"] = vix
        cache["health"]["vix"] = "online-twelvedata"
    # ── Expected Move DERIVADO del VIX real: 1σ diario ≈ precio·(VIX/100)/√252 ──
    try:
        px = g.get("underlying_price") or (cache["heatmap"]["data"].get(asset, {}) or {}).get("price")
        _vix = g.get("vix")
        if px and _vix:
            g["expected_move"] = round(float(px) * (float(_vix) / 100.0) / _math.sqrt(252.0), 1)
            g["atm_iv"] = round(float(_vix), 1)   # proxy honesto: la IV del índice ≈ VIX
    except Exception:
        pass
    # ── Fear & Greed real (CNN, gratis, sin key) ──────────────────────────────
    try:
        async with httpx.AsyncClient(timeout=8, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15",
                "Accept": "application/json"}) as c:
            r = await c.get("https://production.dataviz.cnn.com/index/fearandgreed/graphdata")
        if r.status_code == 200:
            fg = (r.json() or {}).get("fear_and_greed", {})
            score = fg.get("score")
            if score is not None:
                g["fear_score"] = int(round(float(score)))
                g["fear_rating"] = str(fg.get("rating", "")).lower() or None
                cache["health"]["feargreed"] = "online-cnn"
    except Exception as e:
        print(f"[sentiment] CNN F&G falló: {e}")
    return {"vix": g.get("vix"), "fear_score": g.get("fear_score"),
            "expected_move": g.get("expected_move")}

@app.get("/api/admin/diag-gexbot-full")
async def diag_gexbot_full(key: str = ""):
    """Sonda TODAS las categorías de GexBot v2 para ver la FORMA REAL de cada respuesta
    (max pain / orderflow / 0DTE / vanna / charm) ANTES de cablearlas — así no mostramos
    números equivocados (Regla #1). Uso: ?key=ADMIN_KEY"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    if not GEXBOT_API_KEY:
        return {"error": "falta GEXBOT_API_KEY"}
    import urllib.parse as _up
    sym = GEXBOT_SYMBOL
    probes = [
        ("classic/full",     f"{GEXBOT_BASE}/{sym}/classic/full"),
        ("classic/zero",     f"{GEXBOT_BASE}/{sym}/classic/zero"),
        ("classic/one",      f"{GEXBOT_BASE}/{sym}/classic/one"),
        ("orderflow",        f"{GEXBOT_BASE}/{sym}/orderflow/orderflow"),
        ("state/gamma_zero", f"{GEXBOT_BASE}/{sym}/state/gamma_zero"),
        ("state/vanna_zero", f"{GEXBOT_BASE}/{sym}/state/vanna_zero"),
        ("state/charm_zero", f"{GEXBOT_BASE}/{sym}/state/charm_zero"),
        ("classic/categories", f"{GEXBOT_BASE}/classic/categories"),
        ("research/maxpain_a", f"{GEXBOT_BASE}/research/{sym}/{_up.quote('max pain')}?format=json"),
        ("research/maxpain_b", f"{GEXBOT_BASE}/research/{sym}/maxpain?format=json"),
        ("research/maxpain_c", f"{GEXBOT_BASE}/research/NDX/{_up.quote('max pain')}?format=json"),
    ]
    def _compact(j):
        if isinstance(j, dict):
            samp = {}
            for k, v in j.items():
                if isinstance(v, list):
                    samp[k] = {"list_len": len(v), "e0": v[0] if v else None}
                elif isinstance(v, dict):
                    samp[k] = {"keys": list(v.keys())}
                else:
                    samp[k] = v
            return {"keys": list(j.keys()), "sample": samp}
        if isinstance(j, list):
            return {"type": "list", "len": len(j), "e0": j[0] if j else None}
        return {"value": j}
    out = {}
    async with httpx.AsyncClient(timeout=12, headers=_gexbot_headers()) as c:
        for name, url in probes:
            try:
                r = await c.get(url)
                entry = {"http": r.status_code}
                if r.status_code == 200:
                    try:
                        entry.update(_compact(r.json()))
                    except Exception:
                        entry["body"] = r.text[:300]
                else:
                    entry["body"] = r.text[:200]
                out[name] = entry
            except Exception as e:
                out[name] = {"error": f"{type(e).__name__}: {str(e)[:150]}"}
    return {"symbol": sym, "probes": out}


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
    sym = FA_INDEX_SYMBOL  # "NDX" (índice Nasdaq-100 directo; Basic no cubre NQ=F)
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
        # NUNCA se leía para saltarse la llamada: se pedía en los 56 refreshes
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
        # El /gex se pide a FA_GEX_SYMBOL (QQQ), NO al índice (NDX), porque el /gex
        # de NDX responde 404 "no options data" en Basic mientras el de QQQ da
        # net_gex completo. Solo tomamos net_gex + label (régimen): son EXPOSICIÓN
        # del dealer, su signo vale independiente de la escala. Los walls y el flip
        # siguen de NDX /levels (escala real ~28.000); NO se tocan con datos de QQQ.
        gex_url = quote(FA_GEX_SYMBOL, safe="")
        _gex_es_proxy = (FA_GEX_SYMBOL != sym)   # QQQ ≠ NDX → escala distinta
        for cand_exp in future_list[:4]:   # máximo 4 intentos
            try:
                _fa_charge()   # cada intento de expiración es 1 request real
                r_gex = await client.get(f"{FA_BASE}/v1/exposure/gex/{gex_url}",
                                         params={"expiration": cand_exp})
                if r_gex.status_code == 200:
                    gd = r_gex.json() or {}
                    net_gex = gd.get("net_gex")
                    gex_label = gd.get("net_gex_label")   # "positive"/"negative" de FA
                    if not _gex_es_proxy:
                        # Mismo símbolo que /levels: sus strikes/flip SÍ están en
                        # escala real y sirven de respaldo. Si es proxy (QQQ), se
                        # ignoran para no mezclar escalas (~680 vs ~28.000).
                        per_strike = gd.get("strikes")
                        gex_flip = gd.get("gamma_flip")
                    exp = cand_exp
                    _gex_working_exp = cand_exp   # cachear la que funciona
                    print(f"[gex] /gex/{FA_GEX_SYMBOL}?expiration={cand_exp} OK net_gex={net_gex} label={gex_label}")
                    break
                else:
                    print(f"[gex] /gex/{FA_GEX_SYMBOL}?expiration={cand_exp} {r_gex.status_code}: {r_gex.text[:90]}")
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
    # 56 refreshes. Cacheado por día: 55 requests menos.
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
    "services pmi","manufacturing pmi","composite pmi","s&p global","flash pmi",
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
# ── Finnhub economic calendar: fuente de RESERVA encadenada del calendario ──
# Rellena el 'actual'/forecast/previous que a FF/FMP les falte. Se auto-limita
# con throttle propio (5 min) + budget_ok('finnhub'); entre llamadas sirve su
# último buen resultado cacheado, así nunca dispara créditos de más.
_fh_cal_last_fetch = 0
_fh_cal_cache = []

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
            # TradingEconomics = MÁXIMA prioridad del 'actual': su valor PISA al de
            # FF/Finnhub/FMP (es el más cercano al release oficial). Nunca inventa:
            # solo entra aquí si TE realmente trae 'actual'.
            ev["actual"] = rt["actual"]; ev["status"] = "Released"
            ev["_actual_from"] = "tradingeconomics"
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


# ── BLS / FRED: relleno de 'actual' de ÚLTIMO RECURSO (los grandes US, gratis) ──
# Se ejecutan al final del calendario SOLO sobre eventos ya vencidos que aún no
# tienen 'actual'. BLS no necesita key (fuente del gobierno, la más rápida). FRED
# se activa con FRED_API_KEY (gratis). Guardia de mes: el dato debe ser del mes
# ESPERADO (mes del release − 1) para no publicar el mes anterior como actual.
_bls_last_fetch = 0.0
_fred_last_fetch = 0.0
BLS_API_KEY = os.getenv("BLS_API_KEY", "").strip()
FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()

# ═══════════════════════ ENVIRONMENT ENGINE (Options / LEAPS) ═══════════════════════
# 9 motores macro ponderados → Environment Score 0-100. Cada motor sale de series
# REALES de FRED (Regla #1: si una serie falla, ese motor = "no disponible" y se
# EXCLUYE del score, renormalizando pesos — nunca se inventa un número). Score alto =
# entorno sano para asumir riesgo de largo plazo (LEAPS). Pesos configurables.
_env_cache = {"data": None, "ts": 0.0}
ENV_TTL = 6 * 3600   # el macro se mueve lento; refrescar cada 6h basta

async def _fred_obs(sid, limit=26):
    """Últimas `limit` observaciones (más reciente PRIMERO) de una serie FRED, como
    lista de (fecha, float). Descarta puntos '.' (sin dato). [] si falla/no hay key."""
    if not FRED_API_KEY:
        return []
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get("https://api.stlouisfed.org/fred/series/observations",
                            params={"series_id": sid, "api_key": FRED_API_KEY,
                                    "file_type": "json", "sort_order": "desc", "limit": limit})
        if r.status_code != 200:
            return []
        out = []
        for o in (r.json().get("observations") or []):
            v = o.get("value")
            if v in (None, ".", ""):
                continue
            try:
                out.append((o.get("date"), float(v)))
            except (TypeError, ValueError):
                continue
        return out
    except Exception as e:
        print(f"[env] FRED {sid}: {e}")
        return []

async def _yahoo_closes(ysym, rng="3mo"):
    """Cierres diarios de Yahoo (para el oro GC=F, etc.). [] si falla."""
    try:
        url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ysym.replace('=', '%3D')}"
               f"?interval=1d&range={rng}")
        async with httpx.AsyncClient(timeout=10, headers={"User-Agent": "Mozilla/5.0"}) as c:
            r = await c.get(url)
        if r.status_code != 200:
            return []
        res = ((r.json().get("chart") or {}).get("result") or [None])[0]
        if not res:
            return []
        cl = (((res.get("indicators") or {}).get("quote") or [{}])[0].get("close") or [])
        return [x for x in cl if x is not None]
    except Exception:
        return []

def _envclamp(v, lo=0.0, hi=100.0):
    return max(lo, min(hi, v))

def _envlin(x, x0, x1):
    """Score 0-100 lineal: x==x0 → 0, x==x1 → 100 (clamp)."""
    if x is None or x1 == x0:
        return None
    return round(_envclamp((x - x0) / (x1 - x0) * 100.0), 1)

def _env_status(score):
    if score is None:      return "n/a"
    if score >= 65:        return "Healthy"
    if score >= 45:        return "Caution"
    return "Risky"

def _env_trend(obs, invert=False):
    """Tendencia por el cambio reciente de la serie. invert=True cuando 'sube' es malo."""
    if not obs or len(obs) < 4:
        return "flat"
    recent, older = obs[0][1], obs[3][1]
    d = recent - older
    if abs(d) < abs(older) * 0.005 if older else abs(d) < 1e-9:
        return "flat"
    up = d > 0
    if invert:
        up = not up
    return "improving" if up else "deteriorating"

def _env_yoy(obs):
    """% interanual desde observaciones mensuales (más reciente primero): compara con
    el punto ~12 meses atrás."""
    if not obs or len(obs) < 13:
        return None
    a, b = obs[0][1], obs[12][1]
    return round((a - b) / b * 100.0, 2) if b else None

async def refresh_environment():
    """Calcula el Environment Score (9 motores FRED ponderados) y lo cachea."""
    now = time.time()
    # Series FRED por motor (todas gratis). Traer suficientes puntos para YoY/tendencia.
    ser = {}
    ids = ["T10Y2Y", "FEDFUNDS", "DGS10", "CPIAUCSL", "PCEPI", "TDSP",
           "MORTGAGE30US", "HOUST", "UNRATE", "INDPRO", "DCOILWTICO",
           "BAMLH0A0HYM2", "NFCI",
           # Curva de rendimientos real (para dibujarla): tramos por vencimiento
           "DGS3MO", "DGS2", "DGS5", "DGS30"]
    results = await asyncio.gather(*[_fred_obs(i, 26) for i in ids], return_exceptions=True)
    for i, sid in enumerate(ids):
        ser[sid] = results[i] if not isinstance(results[i], Exception) else []

    def last(sid):
        return ser.get(sid, [None])[0][1] if ser.get(sid) else None

    motors = []  # cada uno: {key,name,score,status,trend,weight,value,explain}
    def add(key, name, weight, score, obs, value, explain, invert=False):
        motors.append({"key": key, "name": name, "weight": weight,
                       "score": score, "status": _env_status(score),
                       "trend": _env_trend(obs, invert) if score is not None else "n/a",
                       "value": value, "explain": explain})

    # 1) YIELD CURVE (15%) — T10Y2Y: invertida (<0) = mala; empinada positiva = sana.
    yc = last("T10Y2Y")
    add("yield_curve", "Yield Curve", 15,
        _envlin(yc, -1.0, 1.5), ser["T10Y2Y"],
        (f"{yc:+.2f}%" if yc is not None else None),
        ("Curva invertida — históricamente antesala de recesión" if (yc is not None and yc < 0)
         else "Curva positiva/normal" if yc is not None else "Sin dato"))

    # 2) INTEREST RATES (10%) — FEDFUNDS: restrictivas/altas = caución.
    ff = last("FEDFUNDS")
    add("rates", "Interest Rates", 10,
        _envlin(ff, 6.0, 0.5), ser["FEDFUNDS"],
        (f"{ff:.2f}%" if ff is not None else None),
        ("Política restrictiva (tasas altas)" if (ff is not None and ff >= 4)
         else "Política acomodaticia" if ff is not None else "Sin dato"), invert=True)

    # 3) INFLATION (15%) — CPI YoY: 2% ideal, ≥6% mala; penaliza aceleración.
    cpi_yoy = _env_yoy(ser["CPIAUCSL"])
    add("inflation", "Inflation", 15,
        _envlin(cpi_yoy, 6.0, 2.0), ser["CPIAUCSL"],
        (f"{cpi_yoy:.1f}% YoY" if cpi_yoy is not None else None),
        ("Inflación elevada — presiona tasas y duración" if (cpi_yoy is not None and cpi_yoy > 3.5)
         else "Inflación contenida" if cpi_yoy is not None else "Sin dato"), invert=True)

    # 4) LEVERAGE / DEBT (15%) — TDSP (carga de deuda de hogares): más alta = peor.
    tdsp = last("TDSP")
    add("leverage", "Leverage / Debt", 15,
        _envlin(tdsp, 13.5, 9.0), ser["TDSP"],
        (f"{tdsp:.1f}%" if tdsp is not None else None),
        ("Carga de deuda de hogares elevada" if (tdsp is not None and tdsp > 12)
         else "Servicio de deuda manejable" if tdsp is not None else "Sin dato"), invert=True)

    # 5) REAL ESTATE (10%) — Mortgage 30y: >7% aprieta; combinar con housing starts.
    mtg = last("MORTGAGE30US")
    add("real_estate", "Real Estate", 10,
        _envlin(mtg, 8.0, 3.0), ser["MORTGAGE30US"],
        (f"{mtg:.2f}%" if mtg is not None else None),
        ("Hipotecas caras enfrían la vivienda" if (mtg is not None and mtg > 6.5)
         else "Financiación hipotecaria razonable" if mtg is not None else "Sin dato"), invert=True)

    # 6) BASE ECONOMY (15%) — Desempleo: 3.5% sano, ≥6% débil (+ tendencia INDPRO).
    ur = last("UNRATE")
    add("economy", "Base Economy", 15,
        _envlin(ur, 6.5, 3.3), ser["UNRATE"],
        (f"{ur:.1f}% paro" if ur is not None else None),
        ("Mercado laboral sólido" if (ur is not None and ur < 4.5)
         else "Empleo debilitándose" if ur is not None else "Sin dato"), invert=True)

    # 7) SUPPLY SHOCK (5%) — WTI: subida brusca (>25% en ~3m) = shock.
    oil = ser["DCOILWTICO"]
    oil_now = oil[0][1] if oil else None
    oil_3m = None
    if oil and len(oil) > 12:
        oil_3m = oil[min(12, len(oil) - 1)][1]
    oil_chg = ((oil_now - oil_3m) / oil_3m * 100.0) if (oil_now and oil_3m) else None
    add("supply", "Supply Shock", 5,
        (_envlin(oil_chg, 40.0, -10.0) if oil_chg is not None else None), oil,
        (f"WTI {oil_chg:+.0f}%/3m" if oil_chg is not None else None),
        ("Salto de energía — posible shock de oferta" if (oil_chg is not None and oil_chg > 25)
         else "Energía estable" if oil_chg is not None else "Sin dato"), invert=True)

    # 8) GOLD (5%) — oro subiendo fuerte = aversión al riesgo/estrés (contextual).
    gclose = await _yahoo_closes("GC=F", "6mo")
    gold_chg = None
    gold_obs = []
    if gclose and len(gclose) > 5:
        g_now = gclose[-1]
        g_idx = max(0, len(gclose) - 64)  # ~3 meses de sesiones atrás
        g_3m = gclose[g_idx]
        gold_chg = ((g_now - g_3m) / g_3m * 100.0) if g_3m else None
        tail = gclose[-16:][::-1]  # newest-first para la tendencia (submuestreo)
        gold_obs = [[i, v] for i, v in enumerate(tail)]
    add("gold", "Gold", 5,
        (_envlin(gold_chg, 25.0, -10.0) if gold_chg is not None else None), gold_obs,
        (f"Oro {gold_chg:+.0f}%/3m" if gold_chg is not None else None),
        ("Oro disparado — aversión al riesgo/estrés" if (gold_chg is not None and gold_chg > 15)
         else "Oro estable — sin señal de pánico" if gold_chg is not None else "Sin dato"),
        invert=True)

    # 9) FINANCIAL SYSTEM (10%) — HY OAS (spreads de crédito basura): anchos = estrés.
    hy = last("BAMLH0A0HYM2")
    add("financial", "Financial System", 10,
        _envlin(hy, 8.0, 3.0), ser["BAMLH0A0HYM2"],
        (f"HY OAS {hy:.2f}%" if hy is not None else None),
        ("Spreads de crédito anchos — estrés financiero" if (hy is not None and hy > 5)
         else "Condiciones de crédito calmadas" if hy is not None else "Sin dato"), invert=True)

    # ── Score ponderado, renormalizando sobre los motores CON dato (Regla #1) ──
    avail = [m for m in motors if m["score"] is not None]
    wsum = sum(m["weight"] for m in avail)
    total = None
    if wsum > 0:
        total = round(sum(m["score"] * m["weight"] for m in avail) / wsum, 1)
        for m in motors:
            m["contribution"] = (round(m["score"] * m["weight"] / wsum, 1)
                                 if m["score"] is not None else None)
    # Clasificación
    def _classify(s):
        if s is None: return "SIN DATOS"
        if s >= 80: return "HEALTHY"
        if s >= 65: return "CONSTRUCTIVE"
        if s >= 50: return "CAUTION"
        if s >= 35: return "HIGH RISK"
        return "DEFENSIVE"
    # Conflicting signals (economía fuerte pero inflación/tasas restrictivas, etc.)
    conflicts = []
    def _m(k): return next((x for x in motors if x["key"] == k), {})
    if (_m("economy").get("score") or 0) >= 65 and (_m("inflation").get("score") or 100) < 50:
        conflicts.append("Economía resiliente pero inflación acelerando")
    if (_m("economy").get("score") or 0) >= 65 and (_m("rates").get("score") or 100) < 45:
        conflicts.append("Crecimiento sólido con política monetaria restrictiva")
    if (_m("yield_curve").get("score") or 100) < 35 and (_m("economy").get("score") or 0) >= 60:
        conflicts.append("Curva invertida pese a datos de actividad aún firmes")

    # Curva de rendimientos REAL (para dibujarla) — tramos por vencimiento.
    curve = []
    for lbl, sid, yrs in [("3M", "DGS3MO", 0.25), ("2Y", "DGS2", 2),
                          ("5Y", "DGS5", 5), ("10Y", "DGS10", 10), ("30Y", "DGS30", 30)]:
        v = last(sid)
        if v is not None:
            curve.append({"label": lbl, "yrs": yrs, "yield": round(v, 2)})
    _yc_motor = next((m for m in motors if m["key"] == "yield_curve"), None)
    if _yc_motor is not None:
        _yc_motor["curve"] = curve

    data = {
        "score": total,
        "classification": _classify(total),
        "motors": motors,
        "conflicts": conflicts,
        "yield_curve": curve,
        "coverage": f"{len(avail)}/{len(motors)}",
        "as_of": datetime.now(NY).isoformat(),
        "source": "FRED",
    }
    _env_cache["data"] = data
    _env_cache["ts"] = now
    print(f"[env] score={total} class={data['classification']} motores={len(avail)}/{len(motors)}")
    return data

@app.get("/api/environment")
async def get_environment():
    """Environment Score (9 motores macro FRED ponderados) para la sección Options."""
    if not _env_cache["data"] or (time.time() - _env_cache["ts"] > ENV_TTL):
        try:
            await refresh_environment()
        except Exception as e:
            print(f"[env] refresh falló: {e}")
    return _env_cache["data"] or {"score": None, "classification": "SIN DATOS",
                                  "motors": [], "conflicts": [],
                                  "note": "FRED_API_KEY no configurada o sin datos aún"}

# ═══════════════════ LEAPS OPPORTUNITY SCANNER (fundamentals reales) ═══════════════════
_scan_cache = {"data": None, "ts": 0.0}
SCAN_TTL = 6 * 3600
# Watchlist de calidad (megacaps/compounders). Sector estático para no gastar una
# llamada extra de profile2 por ticker.
SCAN_LIST = [
    ("NVDA", "Tecnología"), ("AAPL", "Tecnología"), ("MSFT", "Tecnología"),
    ("GOOGL", "Comunicación"), ("AMZN", "Consumo Disc."), ("META", "Comunicación"),
    ("AVGO", "Tecnología"), ("TSLA", "Consumo Disc."), ("COST", "Consumo Básico"),
    ("NFLX", "Comunicación"), ("AMD", "Tecnología"), ("ADBE", "Tecnología"),
    ("QCOM", "Tecnología"), ("TXN", "Tecnología"), ("AMAT", "Tecnología"),
    ("INTU", "Tecnología"), ("ISRG", "Salud"), ("BKNG", "Consumo Disc."),
    ("AMGN", "Salud"), ("VRTX", "Salud"), ("PANW", "Tecnología"),
    ("GILD", "Salud"), ("LRCX", "Tecnología"), ("MU", "Tecnología"),
    ("KLAC", "Tecnología"), ("SNPS", "Tecnología"), ("CDNS", "Tecnología"),
    ("MRVL", "Tecnología"), ("CRWD", "Tecnología"), ("ORLY", "Consumo Disc."),
    ("ADP", "Industrial"), ("MELI", "Consumo Disc."), ("CTAS", "Industrial"),
    ("FTNT", "Tecnología"), ("ADI", "Tecnología"), ("NXPI", "Tecnología"),
]

def _pillar(v, lo, hi):
    """Score 0-100 lineal de un pilar (v==lo→0, v==hi→100), None si falta."""
    if v is None:
        return None
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    return max(0.0, min(100.0, (v - lo) / (hi - lo) * 100.0)) if hi != lo else 50.0

async def _yahoo_spot_c(client, sym):
    """Precio spot de Yahoo (gratis) con un cliente compartido. None si falla."""
    for host in ("query1", "query2"):
        try:
            r = await client.get(f"https://{host}.finance.yahoo.com/v8/finance/chart/{sym}"
                                 f"?interval=1d&range=1d")
            if r.status_code != 200:
                continue
            res = ((r.json().get("chart") or {}).get("result") or [None])[0]
            if res:
                p = (res.get("meta") or {}).get("regularMarketPrice")
                if p is not None:
                    return p
        except Exception:
            continue
    return None

async def refresh_scanner():
    """Escanea un universo amplio del Nasdaq (no solo las magníficas). SECUENCIAL con corte
    grácil: 1 llamada Finnhub (metric=all) por ticker + precio de Yahoo (gratis). Chequea el
    budget de Finnhub POR TICKER y se detiene cuando se agota (cacheando lo obtenido) — así
    NO se bloquea por pedir 36 slots de golpe (el budget es 55/min y es compartido). Guard:
    cachea si obtuvo ≥8 tickers o si aún no había caché (evita pisar una buena con una parcial)."""
    if not FINNHUB_KEY:
        return None
    n = len(SCAN_LIST)
    rows = []
    async with httpx.AsyncClient(timeout=12, headers={"User-Agent": "Mozilla/5.0"}) as client:
        for sym, sector in SCAN_LIST:
            if not fh_budget_ok(1):
                print(f"[scanner] budget Finnhub agotado tras {len(rows)} tickers")
                break
            fh_charge(1)
            try:
                rm = await client.get(f"{FH_BASE}/stock/metric",
                                     params={"symbol": sym, "metric": "all", "token": FINNHUB_KEY})
                m = (rm.json().get("metric") if rm.status_code == 200 else {}) or {}
                price = await _yahoo_spot_c(client, sym)
                hi52 = m.get("52WeekHigh")
                pillars = {
                    "revenue":   _pillar(m.get("revenueGrowthTTMYoy"), -5, 25),
                    "eps":       _pillar(m.get("epsGrowthTTMYoy"), -10, 40),
                    "margin":    _pillar(m.get("netProfitMarginTTM"), 0, 30),
                    "gross":     _pillar(m.get("grossMarginTTM"), 20, 70),
                    "roe":       _pillar(m.get("roeTTM"), 5, 40),
                    "balance":   _pillar(-(m.get("totalDebt/totalEquityQuarterly") or 0), -2.0, 0),
                    "fcf":       _pillar(m.get("currentRatioQuarterly"), 0.8, 3.0),
                    "valuation": _pillar(-(m.get("peTTM") or 0), -60, -10),
                }
                have = [v for v in pillars.values() if v is not None]
                fscore = round(sum(have) / len(have), 0) if have else None
                if fscore is None:
                    continue
                dist_high = round((price - hi52) / hi52 * 100, 1) if (price and hi52) else None
                cls = "—"
                if dist_high is not None:
                    if fscore >= 70 and dist_high <= -12:
                        cls = "HIGH QUALITY / PRICE DISLOCATION"
                    elif fscore >= 65 and dist_high <= -6:
                        cls = "QUALITY PULLBACK"
                    elif fscore < 45:
                        cls = "WEAK FUNDAMENTALS"
                    else:
                        cls = "NEUTRAL"
                rows.append({
                    "ticker": sym, "sector": sector, "price": round(price, 2) if price else None,
                    "high52": round(hi52, 2) if hi52 else None, "distHigh": dist_high,
                    "fscore": fscore, "class": cls, "pillars": pillars,
                    "revGrowth": m.get("revenueGrowthTTMYoy"), "epsGrowth": m.get("epsGrowthTTMYoy"),
                    "pe": m.get("peTTM"), "roe": m.get("roeTTM"),
                })
            except Exception as e:
                print(f"[scanner] {sym}: {e}")
                continue
    if rows and (len(rows) >= 8 or not _scan_cache["data"]):
        rows.sort(key=lambda r: (r.get("fscore") or 0), reverse=True)
        _scan_cache["data"] = {"rows": rows, "as_of": datetime.now(NY).isoformat(), "count": len(rows)}
        _scan_cache["ts"] = time.time()
        print(f"[scanner] ok: {len(rows)}/{n} tickers")
    return _scan_cache["data"]

@app.get("/api/leaps/scanner")
async def get_leaps_scanner():
    if not _scan_cache["data"] or (time.time() - _scan_cache["ts"] > SCAN_TTL):
        try:
            await refresh_scanner()
        except Exception as e:
            print(f"[scanner] refresh falló: {e}")
    return _scan_cache["data"] or {"rows": [], "note": "sin datos aún (Finnhub)"}

# ═══════════════════ LEAPS AI BRIEF (Groq narra el dato REAL — Regla #1) ═══════════════════
_brief_cache = {"text": None, "ts": 0.0, "score": None, "classification": None, "cooldown": 0.0}
BRIEF_TTL = 3 * 3600   # el Environment se mueve lento (FRED); 3h basta y protege el budget Groq

async def refresh_leaps_brief():
    """Genera un brief macro para LEAPS con Groq (qwen) narrando SOLO las cifras reales
    del Environment + Scanner. No inventa datos (Regla #1). Cachea y respeta el budget.
    Si Groq no está disponible, deja text=None y el frontend usa su brief determinista."""
    if not GROQ_KEY or not budget_ok("groq", 1):
        return
    env = _env_cache.get("data") or {}
    scan = _scan_cache.get("data") or {}
    if env.get("score") is None:
        return  # sin Environment real no hay brief (Regla #1)

    motors = env.get("motors") or []
    def mv(k):
        m = next((x for x in motors if x.get("key") == k), {})
        return f"{m.get('name')}: {m.get('value') or '—'} ({m.get('status') or 'n/a'}, score {m.get('score')})"
    curve = env.get("yield_curve") or []
    y2 = next((p for p in curve if p.get("label") == "2Y"), None)
    y10 = next((p for p in curve if p.get("label") == "10Y"), None)
    curve_txt = "sin datos"
    if y2 and y10:
        spr = y10["yield"] - y2["yield"]
        curve_txt = (f"10Y-2Y {spr:+.2f}%" + (" (INVERTIDA)" if spr < 0 else " (normal)"))
    disloc = [r for r in (scan.get("rows") or []) if "DISLOCATION" in (r.get("class") or "")]
    disloc_txt = ", ".join(f"{r['ticker']} (fscore {r.get('fscore')}, {r.get('distHigh')}% vs máx)"
                           for r in disloc[:5]) or "ninguna ahora mismo"

    ctx = (f"ENVIRONMENT SCORE: {round(env['score'],1)}/100 — {env.get('classification','—')}\n"
           f"Cobertura motores: {env.get('coverage','—')}\n"
           f"Motores clave:\n"
           f"- {mv('economy')}\n- {mv('inflation')}\n- {mv('rates')}\n- {mv('yield_curve')}\n"
           f"- {mv('financial')}\n- {mv('real_estate')}\n- {mv('leverage')}\n- {mv('gold')}\n"
           f"Curva de rendimientos: {curve_txt}\n"
           f"Conflictos macro: {', '.join(env.get('conflicts') or []) or 'ninguno'}\n"
           f"Dislocaciones de calidad (scanner LEAPS): {disloc_txt}")

    sys_msg = (
        "Eres el estratega macro de una mesa que invierte en LEAPS (opciones call de largo plazo, "
        "1-3 años) sobre acciones de calidad del Nasdaq. Escribes en ESPAÑOL, tono institucional y "
        "sobrio, para un inversor avanzado. REGLA ABSOLUTA: usa EXCLUSIVAMENTE las cifras que te doy; "
        "NUNCA inventes un número, precio, nivel ni dato que no esté en el contexto. Si algo no está, "
        "no lo menciones. Nada de disclaimers ni de 'no soy asesor'. Sin emojis.\n"
        "FORMATO EXACTO (markdown, usa **negrita** en el dato clave de cada frase):\n"
        "**Entorno:** <1-2 frases: qué dice el score y su clasificación, apoyado en 2-3 motores concretos con su valor>\n"
        "**Tensión:** <1 frase: el punto de fricción principal (inflación/tasas/curva/inmobiliario/crédito) con su cifra>\n"
        "**Implicación LEAPS:** <1-2 frases ACCIONABLES: si el entorno pide exposición selectiva de largo plazo, "
        "prudencia o postura defensiva, y qué tipo de nombre priorizar (calidad/valoración/margin of safety). "
        "Si hay dislocaciones de calidad en el scanner, nómbralas como candidatas a estudiar.>\n"
        "Total: 4 a 6 frases. Nada más.")
    usr_msg = f"Datos macro reales de ahora mismo:\n\n{ctx}\n\nEscribe el brief en el formato exacto."

    budget_charge("groq", 1)
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROQ_KEY}", "Content-Type": "application/json"},
                json={"model": "qwen/qwen3.6-27b", "max_tokens": 420, "temperature": 0.5,
                      "reasoning_effort": "none",
                      "messages": [{"role": "system", "content": sys_msg},
                                   {"role": "user", "content": usr_msg}]})
        if r.status_code == 200:
            _brief_cache["text"] = r.json()["choices"][0]["message"]["content"].strip()
            _brief_cache["ts"] = time.time()
            _brief_cache["score"] = env.get("score")
            _brief_cache["classification"] = env.get("classification")
            cache["health"]["groq"] = "online"
            _brief_cache["cooldown"] = 0.0
            print("[leaps-brief] ok")
        else:
            # 429 (cuota) u otro error → back-off para no martillear Groq (30 min si 429, 10 min resto)
            _brief_cache["cooldown"] = time.time() + (1800 if r.status_code == 429 else 600)
            print(f"[leaps-brief] groq {r.status_code} — cooldown")
    except Exception as e:
        _brief_cache["cooldown"] = time.time() + 600
        print(f"[leaps-brief] error: {e}")

@app.get("/api/leaps/brief")
async def get_leaps_brief():
    """Brief macro para LEAPS (Groq narrando el dato real). Asegura Environment+Scanner
    frescos, regenera si el brief caducó, y devuelve el texto (o null → fallback determinista)."""
    if not _env_cache["data"] or (time.time() - _env_cache["ts"] > ENV_TTL):
        try: await refresh_environment()
        except Exception as e: print(f"[leaps-brief] env: {e}")
    if not _scan_cache["data"] or (time.time() - _scan_cache["ts"] > SCAN_TTL):
        try: await refresh_scanner()
        except Exception as e: print(f"[leaps-brief] scan: {e}")
    stale = (not _brief_cache["text"]) or (time.time() - _brief_cache["ts"] > BRIEF_TTL) \
        or (_brief_cache.get("score") != (_env_cache.get("data") or {}).get("score"))
    if stale and time.time() > _brief_cache.get("cooldown", 0.0):
        try: await refresh_leaps_brief()
        except Exception as e: print(f"[leaps-brief] gen: {e}")
    env = _env_cache.get("data") or {}
    return {"brief": _brief_cache["text"], "model": "qwen/qwen3.6-27b (Groq)",
            "generated_at": (datetime.fromtimestamp(_brief_cache["ts"], NY).isoformat()
                             if _brief_cache["ts"] else None),
            "score": env.get("score"), "classification": env.get("classification")}

# ═══════════════════ LEAPS CHART — velas DIARIAS de cualquier ticker (Yahoo) ═══════════════════
_leaps_ohlc_cache = {}  # (ysym, rng, interval) → {"ts": epoch, "bars": [...]}
_LEAPS_RANGES = {"1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "max"}
_LEAPS_INTERVALS = {"1d", "1wk", "1mo"}

@app.get("/api/leaps/ohlc/{symbol}")
async def get_leaps_ohlc(symbol: str, rng: str = "1y", interval: str = "1d"):
    """Velas reales (Yahoo, lado servidor) de cualquier subyacente para el chart nativo de
    la sección Options — índices (NDX→^NDX, SPX→^GSPC), futuros (=F) o acciones. Soporta
    intervalo diario/semanal/mensual (D/W/M). Respuesta Lightweight-Charts-ready:
    {ok, symbol, ysym, rng, interval, bars:[{t:'YYYY-MM-DD',o,h,l,c}]}."""
    raw = (symbol or "").upper().strip()
    if not raw:
        raise HTTPException(400, "falta symbol")
    _MAP = {"NDX": "^NDX", "SPX": "^GSPC", "NASDAQ": "^NDX", "US100": "^NDX"}
    if raw in _MAP:
        ysym = _MAP[raw]
    elif raw.endswith("=F") or raw.startswith("^"):
        ysym = raw
    else:
        ysym = raw  # acción normal (NVDA, AAPL, …)
    if rng not in _LEAPS_RANGES:
        rng = "1y"
    if interval not in _LEAPS_INTERVALS:
        interval = "1d"
    ck = (ysym, rng, interval)
    cached = _leaps_ohlc_cache.get(ck)
    if cached and (time.time() - cached["ts"] < 900) and cached["bars"]:  # 15 min
        return {"ok": True, "symbol": raw, "ysym": ysym, "rng": rng, "interval": interval,
                "bars": cached["bars"], "cached": True, "source": f"yahoo:{ysym}"}
    for host in ("query1", "query2"):
        url = (f"https://{host}.finance.yahoo.com/v8/finance/chart/{ysym.replace('=', '%3D')}"
               f"?interval={interval}&range={rng}")
        try:
            async with httpx.AsyncClient(timeout=12, headers={"User-Agent": "Mozilla/5.0"}) as c:
                r = await c.get(url)
            if r.status_code != 200:
                continue
            res = ((r.json().get("chart") or {}).get("result") or [None])[0]
            if not res:
                continue
            ts = res.get("timestamp") or []
            q = ((res.get("indicators") or {}).get("quote") or [{}])[0]
            op, hi, lo, cl = q.get("open", []), q.get("high", []), q.get("low", []), q.get("close", [])
            bars = []
            for i, tt in enumerate(ts):
                try:
                    o, h, l, c2 = op[i], hi[i], lo[i], cl[i]
                except Exception:
                    continue
                if None in (o, h, l, c2):
                    continue
                d = datetime.fromtimestamp(tt, NY).strftime("%Y-%m-%d")
                bars.append({"t": d, "o": round(o, 2), "h": round(h, 2),
                             "l": round(l, 2), "c": round(c2, 2)})
            if bars:
                _leaps_ohlc_cache[ck] = {"ts": time.time(), "bars": bars}
                if len(_leaps_ohlc_cache) > 400:
                    for k in list(_leaps_ohlc_cache)[:120]:
                        _leaps_ohlc_cache.pop(k, None)
                return {"ok": True, "symbol": raw, "ysym": ysym, "rng": rng,
                        "interval": interval, "bars": bars, "source": f"yahoo:{ysym}"}
        except Exception as e:
            print(f"[leaps-ohlc] {host} {ysym} {rng} {interval}: {e}")
            continue
    return {"ok": False, "symbol": raw, "ysym": ysym, "reason": f"sin velas para {ysym}"}

# ═══════════════════ SECTOR ROTATION — 11 ETFs SPDR reales (Finnhub) ═══════════════════
_sectors_cache = {"data": None, "ts": 0.0}
SECTORS_TTL = 600   # 10 min
SECTOR_ETFS = [
    ("XLK", "Tecnología"), ("XLC", "Comunicación"), ("XLY", "Consumo Discr."),
    ("XLP", "Consumo Básico"), ("XLV", "Salud"), ("XLF", "Financiero"),
    ("XLI", "Industrial"), ("XLE", "Energía"), ("XLB", "Materiales"),
    ("XLU", "Utilities"), ("XLRE", "Inmobiliario"),
]

async def refresh_sectors():
    """% de cambio diario real de los 11 ETFs sectoriales SPDR desde YAHOO (gratis, NO
    consume el budget de Finnhub — antes lo estrangulaba con el scanner). Ordenado de
    líder a rezagado = rotación sectorial. Cacheado 10 min."""
    out = []
    async with httpx.AsyncClient(timeout=10, headers={"User-Agent": "Mozilla/5.0"}) as client:
        for sym, name in SECTOR_ETFS:
            chg = None
            price = None
            for host in ("query1", "query2"):
                try:
                    r = await client.get(
                        f"https://{host}.finance.yahoo.com/v8/finance/chart/{sym}"
                        f"?interval=1d&range=5d")
                    if r.status_code != 200:
                        continue
                    res = ((r.json().get("chart") or {}).get("result") or [None])[0]
                    if not res:
                        continue
                    meta = res.get("meta") or {}
                    price = meta.get("regularMarketPrice")
                    prev = meta.get("chartPreviousClose") or meta.get("previousClose")
                    if price is not None and prev:
                        chg = (price - prev) / prev * 100.0
                    else:
                        cl = [x for x in (((res.get("indicators") or {}).get("quote")
                              or [{}])[0].get("close") or []) if x is not None]
                        if len(cl) >= 2:
                            price = cl[-1]
                            chg = (cl[-1] - cl[-2]) / cl[-2] * 100.0
                    break
                except Exception as e:
                    print(f"[sectors] {host} {sym}: {e}")
                    continue
            if chg is not None:
                out.append({"sym": sym, "name": name, "chg": round(chg, 2),
                            "price": round(price, 2) if price else None})
    if out:
        out.sort(key=lambda x: x["chg"], reverse=True)
        _sectors_cache["data"] = {"sectors": out, "as_of": datetime.now(NY).isoformat(),
                                  "count": len(out)}
        _sectors_cache["ts"] = time.time()
        print(f"[sectors] ok: {len(out)} sectores")

@app.get("/api/leaps/sectors")
async def get_leaps_sectors():
    if not _sectors_cache["data"] or (time.time() - _sectors_cache["ts"] > SECTORS_TTL):
        try:
            await refresh_sectors()
        except Exception as e:
            print(f"[sectors] refresh falló: {e}")
    return _sectors_cache["data"] or {"sectors": [], "note": "sin datos aún (Finnhub)"}

# ═══════ SECTOR ROTATION MAP (RRG) — dónde se mueve el dinero entre sectores ═══════
# Estilo Relative Rotation Graph: cada sector se ubica por su FUERZA relativa (eje X, 3m
# vs SPY) y su MOMENTUM relativo (eje Y). Cuadrantes: LÍDER (fuerte+momentum), MEJORANDO
# (débil pero acelerando → entra dinero), REZAGADO (débil+cayendo), DEBILITÁNDOSE (fuerte
# pero perdiendo momentum → sale dinero). Todo desde Yahoo (gratis), cacheado 30 min.
_rotation_cache = {"data": None, "ts": 0.0}
ROTATION_TTL = 1800

@app.get("/api/leaps/rotation")
async def get_leaps_rotation():
    now = time.time()
    if _rotation_cache["data"] and (now - _rotation_cache["ts"] < ROTATION_TTL):
        return _rotation_cache["data"]
    bench = "SPY"
    syms = [bench] + [s for s, _ in SECTOR_ETFS]
    async with httpx.AsyncClient(timeout=12, headers={"User-Agent": "Mozilla/5.0"}) as client:
        async def closes(sym):
            for host in ("query1", "query2"):
                try:
                    r = await client.get(f"https://{host}.finance.yahoo.com/v8/finance/chart/{sym}"
                                        f"?interval=1d&range=6mo")
                    if r.status_code != 200:
                        continue
                    res = ((r.json().get("chart") or {}).get("result") or [None])[0]
                    if not res:
                        continue
                    return [x for x in (((res.get("indicators") or {}).get("quote")
                            or [{}])[0].get("close") or []) if x is not None]
                except Exception:
                    continue
            return []
        allc = await asyncio.gather(*[closes(s) for s in syms])
    data = dict(zip(syms, allc))
    spy = data.get(bench) or []
    if len(spy) < 64:
        return _rotation_cache["data"] or {"sectors": [], "note": "sin histórico suficiente"}
    out = []
    for sym, name in SECTOR_ETFS:
        c = data.get(sym) or []
        L = min(len(c), len(spy))
        if L < 64:
            continue
        cc, ss = c[-L:], spy[-L:]
        rs = [cc[i] / ss[i] for i in range(L) if ss[i]]
        if len(rs) < 64:
            continue
        def rel(days):
            return (rs[-1] / rs[-days] - 1) * 100.0 if len(rs) > days else 0.0
        x = rel(63)                      # fuerza relativa 3 meses vs SPY
        y = rel(21) - x * (21.0 / 63.0)  # momentum: 1m por encima/debajo de la tendencia 3m
        chg = ((cc[-1] - cc[-2]) / cc[-2] * 100.0) if (len(cc) >= 2 and cc[-2]) else None
        if x >= 0 and y >= 0:   q = "LIDER"
        elif x < 0 and y >= 0:  q = "MEJORANDO"
        elif x < 0 and y < 0:   q = "REZAGADO"
        else:                   q = "DEBILITANDOSE"
        out.append({"sym": sym, "name": name, "x": round(x, 2), "y": round(y, 2),
                    "quadrant": q, "chg": round(chg, 2) if chg is not None else None})
    if out:
        _rotation_cache["data"] = {"sectors": out, "benchmark": bench,
                                   "as_of": datetime.now(NY).isoformat(),
                                   "axes": {"x": "Fuerza relativa 3m vs SPY",
                                            "y": "Momentum relativo"}}
        _rotation_cache["ts"] = now
    return _rotation_cache["data"] or {"sectors": [], "note": "sin datos aún"}

# (frases_en_titulo [más específicas primero], series_id, transform)
BLS_SERIES = [
    (["core cpi m/m", "core cpi mom"],                         "CUSR0000SA0L1E", "mom"),
    (["core cpi y/y", "core cpi yoy"],                         "CUUR0000SA0L1E", "yoy"),
    (["cpi y/y", "cpi yoy"],                                   "CUUR0000SA0",    "yoy"),
    (["cpi m/m", "cpi mom"],                                   "CUSR0000SA0",    "mom"),
    (["ppi m/m", "ppi mom", "ppi final demand m/m"],          "WPSFD4",         "mom"),
    (["average hourly earnings m/m", "hourly earnings m/m"],  "CES0500000003",  "mom"),
    (["nonfarm payrolls", "non-farm payrolls", "nonfarm employment change"], "CES0000000001", "chg_k"),
    (["unemployment rate"],                                    "LNS14000000",    "value"),
]

def _bls_match(title):
    tl = (title or "").lower()
    for phrases, sid, tf in BLS_SERIES:
        if any(p in tl for p in phrases):
            return sid, tf
    return None

def _bls_fmt(arr, tf):
    """Computa el resultado desde la serie BLS (newest-first). El campo
    'calculations' de BLS viene vacío, así que calculamos el cambio nosotros.
    No inventa nada: si faltan puntos, devuelve None."""
    try:
        if not arr:
            return None
        a = float(arr[0]["value"])
        if tf == "value":
            return f"{a:.1f}%"
        if tf == "mom":
            if len(arr) < 2:
                return None
            b = float(arr[1]["value"])
            return f"{((a - b) / b * 100):.1f}%" if b else None
        if tf == "yoy":
            # buscar el punto de hace 12 meses (mismo period, año-1)
            y0, p0 = arr[0].get("year"), arr[0].get("period")
            prev = next((float(x["value"]) for x in arr
                         if x.get("period") == p0 and str(x.get("year")) == str(int(y0) - 1)), None)
            return f"{((a - prev) / prev * 100):.1f}%" if prev else None
        if tf == "chg_k":
            if len(arr) < 2:
                return None
            b = float(arr[1]["value"])
            return f"{int(round(a - b))}K"
    except Exception:
        return None
    return None

async def _bls_fetch(series_ids):
    if not series_ids:
        return {}
    yr = datetime.now(NY).year
    body = {"seriesid": list(series_ids), "startyear": str(yr - 1), "endyear": str(yr)}
    if BLS_API_KEY:
        body["registrationkey"] = BLS_API_KEY
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post("https://api.bls.gov/publicAPI/v2/timeseries/data/", json=body)
        if r.status_code != 200:
            print(f"[bls] status {r.status_code}"); return {}
        j = r.json()
        out = {}
        for s in j.get("Results", {}).get("series", []):
            data = s.get("data", [])
            if data:
                out[s.get("seriesID")] = data   # serie completa (newest-first)
        return out
    except Exception as e:
        print(f"[bls] {e}"); return {}

async def _fill_actuals_bls(events):
    """Rellena 'actual' de eventos US ya vencidos y sin dato, vía BLS (gratis)."""
    global _bls_last_fetch
    now_dt = datetime.now(NY)
    now_ts = now_dt.timestamp()
    need = {}   # sid -> [(event, tf, expected_month)]
    for e in events:
        if e.get("actual"):
            continue
        try:
            ev_dt = datetime.fromisoformat(str(e.get("time", "")).replace("Z", "+00:00"))
            ev_ts = ev_dt.timestamp()
        except Exception:
            continue
        if ev_ts > now_ts:           # nunca eventos futuros
            continue
        m = _bls_match(e.get("title", "") or "")
        if not m:
            continue
        sid, tf = m
        exp_month = ev_dt.month - 1 or 12   # dato del mes del release − 1
        need.setdefault(sid, []).append((e, tf, exp_month))
    if not need:
        return
    if now_ts - _bls_last_fetch < 120:   # throttle (respeta límite free sin key)
        return
    _bls_last_fetch = now_ts
    data = await _bls_fetch(need.keys())
    filled = 0
    for sid, lst in need.items():
        arr = data.get(sid)
        if not arr:
            continue
        try:
            bls_month = int(str(arr[0].get("period", "M00"))[1:])
        except Exception:
            bls_month = 0
        for e, tf, exp_month in lst:
            if bls_month and bls_month != exp_month:   # guardia: BLS aún no publicó el mes esperado
                continue
            val = _bls_fmt(arr, tf)
            if val:
                e["actual"] = val
                e["status"] = "Released"
                e["_from"] = ((e.get("_from", "") or "") + "+bls").lstrip("+")
                filled += 1
    if filled:
        print(f"[bls] rellenó {filled} actual(es) de último recurso")

# ── FRED: respaldo (requiere FRED_API_KEY gratis). Cubre lo que BLS no trae fácil
#    (GDP, retail sales) además de reforzar los de empleo/inflación. ──
FRED_SERIES = [
    (["gdp q/q", "gdp annualized", "gdp growth"],     "A191RL1Q225SBEA", "value"),   # GDP real % anualizado
    (["retail sales m/m", "retail sales mom"],        "RSAFS",           "mom"),
    (["cpi m/m", "cpi mom"],                          "CPIAUCSL",        "mom"),
    (["unemployment rate"],                           "UNRATE",          "value"),
    (["nonfarm payrolls", "non-farm payrolls"],       "PAYEMS",          "chg_k"),
]

def _fred_match(title):
    tl = (title or "").lower()
    for phrases, sid, tf in FRED_SERIES:
        if any(p in tl for p in phrases):
            return sid, tf
    return None

async def _fill_actuals_fred(events):
    """Relleno de respaldo vía FRED (solo si FRED_API_KEY está configurada)."""
    global _fred_last_fetch
    if not FRED_API_KEY:
        return
    now_ts = datetime.now(NY).timestamp()
    pend = [e for e in events if not e.get("actual") and _fred_match(e.get("title", "") or "")]
    # solo vencidos
    def _past(e):
        try:
            return datetime.fromisoformat(str(e.get("time", "")).replace("Z", "+00:00")).timestamp() <= now_ts
        except Exception:
            return False
    pend = [e for e in pend if _past(e)]
    if not pend:
        return
    if now_ts - _fred_last_fetch < 180:
        return
    _fred_last_fetch = now_ts
    filled = 0
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            for e in pend:
                sid, tf = _fred_match(e.get("title", ""))
                r = await c.get("https://api.stlouisfed.org/fred/series/observations",
                                params={"series_id": sid, "api_key": FRED_API_KEY,
                                        "file_type": "json", "sort_order": "desc", "limit": 13})
                if r.status_code != 200:
                    continue
                obs = [o for o in r.json().get("observations", []) if o.get("value") not in (".", None, "")]
                if not obs:
                    continue
                val = None
                try:
                    if tf == "value":
                        val = f"{float(obs[0]['value']):.1f}" + ("%" if sid in ("UNRATE", "A191RL1Q225SBEA") else "")
                    elif tf == "mom" and len(obs) >= 2:
                        a, b = float(obs[0]['value']), float(obs[1]['value'])
                        val = f"{((a - b) / b * 100):.1f}%"
                    elif tf == "chg_k" and len(obs) >= 2:
                        val = f"{int(round(float(obs[0]['value']) - float(obs[1]['value'])))}K"
                except Exception:
                    val = None
                if val:
                    e["actual"] = val
                    e["status"] = "Released"
                    e["_from"] = ((e.get("_from", "") or "") + "+fred").lstrip("+")
                    filled += 1
    except Exception as ex:
        print(f"[fred] {ex}")
    if filled:
        print(f"[fred] rellenó {filled} actual(es) de respaldo")

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
        """Finnhub economic calendar — eslabón de RESERVA de la cadena de proveedores.
        Aporta 'actual'/forecast/previous para rellenar lo que a ForexFactory/FMP
        les falte. Ventana from=-2d..+7d para capturar también los resultados que
        se acaban de publicar (no solo lo próximo). Nunca inventa datos.

        PRESUPUESTO: se auto-limita para no gastar créditos de más:
          · throttle propio de 5 min (el feed semanal cambia despacio) → entre
            llamadas devuelve su último buen resultado cacheado.
          · budget_ok('finnhub')/fh_charge(1): respeta el límite por minuto (55).
        Kill-switch: FINNHUB_CALENDAR_ENABLED=false lo apaga (por si el endpoint
        empieza a devolver 403 premium en tu plan). Sin key → []."""
        global _fh_cal_last_fetch, _fh_cal_cache
        if not FINNHUB_KEY: return []
        if os.getenv("FINNHUB_CALENDAR_ENABLED", "true").lower() == "false":
            return []
        nowts_fh = time.time()
        if nowts_fh - _fh_cal_last_fetch < 300:
            return list(_fh_cal_cache)          # throttle: usa cache reciente
        if not budget_ok("finnhub", 1):
            print("[calendar] presupuesto Finnhub agotado — usando cache Finnhub")
            return list(_fh_cal_cache)
        _fh_cal_last_fetch = nowts_fh
        try:
            now_et = datetime.now(NY)
            # from=-2d capta los 'actual' recién publicados; to=+7d los próximos.
            from_dt = (now_et - __import__('datetime').timedelta(days=2)).strftime("%Y-%m-%d")
            to_dt   = (now_et + __import__('datetime').timedelta(days=7)).strftime("%Y-%m-%d")
            fh_charge(1)  # contabilizar la llamada real (una por ciclo, con throttle)
            r = await client.get(f"{FH_BASE}/calendar/economic",
                params={"from": from_dt, "to": to_dt, "token": FINNHUB_KEY}, timeout=8)
            if r.status_code != 200:
                print(f"[calendar] Finnhub status {r.status_code} — usando cache Finnhub")
                return list(_fh_cal_cache)
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
                    "type": "macro", "source": "finnhub", "_from": "finnhub",
                })
            if events: _fh_cal_cache = events   # cachear último bueno
            with_a = sum(1 for e in events if e["actual"])
            print(f"[calendar] Finnhub: {len(events)} eventos US, {with_a} con actual")
            return events
        except Exception as e:
            print(f"[calendar] Finnhub fallback error: {e}")
            return list(_fh_cal_cache)

    async def _fetch_fmp(client):
        """FMP economic calendar. ⚠️ El endpoint /economic-calendar es de PAGO
        (devuelve 402 en plan free — no es límite de cuota, es muro de suscripción).
        Apagado por defecto. Si algún día pagas FMP, pon FMP_ENABLED=true en Railway."""
        if not FMP_KEY: return []
        # /api/health/feeds confirmó que este endpoint devuelve 200 con esta key
        # (no premium-locked) → habilitado por defecto. Si algún día empieza a dar
        # 402, poner FMP_ENABLED=false en Railway y el merge sigue con FF+RapidAPI.
        if os.getenv("FMP_ENABLED", "true").lower() != "true":
            return []
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
                # PROMOCIÓN: FMP etiqueta algunos movers reales del NQ como "medium"
                # (p.ej. los Flash PMI de S&P Global a las 9:45 ET). Si el evento está
                # en HIGH_KW, lo subimos a "high" para que aparezca en High Impact News
                # y en el Institutional Context, no solo enterrado en el calendario.
                if any(k in name.lower() for k in HIGH_KW):
                    impact = "high"
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

        # ── Finnhub: eslabón de RESERVA de la cadena (throttle + budget internos).
        # Aporta actual/forecast/previous que a FF/FMP les falte. Su propio fetch
        # se auto-limita (5 min + budget_ok('finnhub')), así que lanzarlo cada
        # ciclo es barato: la mayoría de las veces devuelve su cache sin llamar.
        fh_task = [_fetch_finnhub_fallback(client)]

        # ── RapidAPI: reserva para eventos enormes (guard interno ya lo limita) ──
        rapid_task = [_fetch_rapidapi_actuals(client)]

        all_tasks = ff_tasks + fmp_task + fh_task + rapid_task
        all_results = await asyncio.gather(*all_tasks, return_exceptions=True)

        # Separar resultados por posición conocida
        n_ff = len(ff_tasks)
        n_fmp = len(fmp_task)
        n_fh = len(fh_task)
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
        # Finnhub (su función ya cachea y respeta budget/throttle internamente)
        fh_raw = all_results[n_ff + n_fmp] if len(all_results) > n_ff + n_fmp else []
        fh_events = [e for e in fh_raw if e] if isinstance(fh_raw, list) else []
        rt_raw = all_results[n_ff + n_fmp + n_fh] if len(all_results) > n_ff + n_fmp + n_fh else []
        rt_actuals = [e for e in rt_raw if e] if isinstance(rt_raw, list) else []

        # Cachear FF
        if ff_fresh:
            _ff_cache = ff_fresh
        ff_events = list(_ff_cache)

        # ══ MERGE ENCADENADO CON PRIORIDAD DE PROVEEDORES ════════════════════
        # OBJETIVO: el 'actual' (y forecast/previous) NUNCA falta mientras ALGÚN
        # proveedor lo tenga. Cada evento se indexa por (título normalizado, fecha)
        # y se rellena/actualiza siguiendo esta jerarquía del 'actual':
        #   1º TradingEconomics (RapidAPI) — el más cercano al release oficial: PISA
        #   2º ForexFactory — base gratuita fiable (aporta el RECORD canónico)
        #   3º Finnhub — reserva; rellena lo que FF no trae
        #   4º FMP — premium (normalmente apagado); rellena lo que aún falte
        # REGLA: nunca se INVENTA un dato. Si NINGÚN proveedor tiene 'actual', el
        # evento queda 'Upcoming' sin actual (correcto). FF es el record base para
        # conservar su metadata (type/impact/holiday); los secundarios solo aportan
        # los campos numéricos que falten (fill-only), y TradingEconomics puede pisar.
        # NO-VACÍO: si FF cae, los secundarios AÑADEN sus eventos (merged nunca queda
        # vacío mientras responda al menos un proveedor); si todos caen → stale_backup.
        def norm_key(e):
            title = (e.get("title","") or "").lower().strip()
            # Normalizar título (quitar variaciones comunes)
            title = title.replace(" m/m","").replace(" y/y","").replace(" q/q","").strip()
            date = (e.get("time","") or "")[:10]
            return (title, date)

        def _fill(existing, e, source, override_actual=False):
            """Rellena en 'existing' los campos numéricos que le falten con los de
            'e' (proveedor 'source'). Con override_actual=True un 'actual' de mayor
            prioridad PISA al previo. Deja traza en '_actual_from'. No inventa: solo
            copia lo que 'e' realmente trae."""
            if e.get("actual") and (not existing.get("actual") or override_actual):
                existing["actual"] = e["actual"]
                existing["status"] = "Released"
                existing["_actual_from"] = source
            if e.get("forecast") and not existing.get("forecast"):
                existing["forecast"] = e["forecast"]
            if e.get("previous") and not existing.get("previous"):
                existing["previous"] = e["previous"]

        merged = {}
        # (2) ForexFactory = record base (metadata canónica del evento).
        for e in ff_events:
            e.setdefault("_from", "forexfactory")
            merged[norm_key(e)] = e
        # (3) Finnhub y (4) FMP: rellenan (fill-only) lo que a FF le falte; si el
        #     evento no existía en FF, se AÑADE (así nunca queda vacío si FF cae).
        #     Orden Finnhub→FMP = prioridad Finnhub > FMP entre los secundarios:
        #     el primero que aporte un campo gana, el segundo ya no lo pisa.
        for src_name, src_events in (("finnhub", fh_events), ("fmp", fmp_events)):
            for e in src_events:
                k = norm_key(e)
                if k in merged:
                    _fill(merged[k], e, src_name, override_actual=False)
                else:
                    e.setdefault("_from", src_name)
                    merged[k] = e

        out = list(merged.values())

        # (1) CAPA TIEMPO REAL: TradingEconomics (RapidAPI) = MÁXIMA prioridad del
        # 'actual'. Su valor PISA a FF/Finnhub/FMP (más cercano al release oficial)
        # y además INYECTA eventos US que las otras fuentes ya no listan (NFP de la
        # semana pasada, etc.). Alias-aware (Unemployment Claims ≡ Initial Jobless
        # Claims). Reserva para eventos enormes (NFP/CPI/FOMC); su guard interno lo limita.
        if rt_actuals:
            out = _merge_rapidapi(out, rt_actuals)

        # Si NINGÚN proveedor devolvió eventos → servir caché stale (nunca vacío).
        if not out:
            if stale_backup:
                print(f"[calendar] ningún proveedor respondió — sirviendo stale ({len(stale_backup)} eventos)")
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

    # ── Dedup + FUSIÓN de casillas entre fuentes ──────────────────────────────
    # Agrupa por (título, fecha/hora). Para cada grupo produce UN evento con el
    # máximo de casillas llenas: rellena forecast/previous/actual desde cualquier
    # fuente del grupo que SÍ las tenga, SOLO si faltan (nunca pisa un valor ya
    # presente) y NUNCA inventa. Preferencia de fuente para el relleno:
    # tradingeconomics > fmp > forexfactory > finnhub > bls > fred.
    _SRC_PRIO = {"tradingeconomics": 0, "fmp": 1, "forexfactory": 2,
                 "finnhub": 3, "bls": 4, "fred": 5}
    def _src_rank(ev):
        # '_from' puede venir compuesto (p.ej. 'forexfactory+bls'): usa la primera.
        base_src = ((ev.get("_from") or "").split("+")[0]).strip().lower()
        return _SRC_PRIO.get(base_src, 9)
    def _is_empty(v):
        return v is None or (isinstance(v, str) and v.strip() == "")

    groups, order = {}, []
    for e in out:
        k = ((e.get("title","") or "").lower().strip(), (e.get("time","") or "")[:16])
        if k not in groups:
            groups[k] = []; order.append(k)
        groups[k].append(e)

    _FILL_FIELDS = ("forecast", "previous", "actual", "impact")
    deduped = []
    for k in order:
        grp = groups[k]
        base = grp[0]                      # preserva metadata/orden del primer evento
        cands = sorted(grp, key=_src_rank)  # mejor fuente primero
        for f in _FILL_FIELDS:
            if _is_empty(base.get(f)):
                for c in cands:
                    if not _is_empty(c.get(f)):
                        base[f] = c[f]
                        if f == "actual":
                            base["_actual_from"] = (c.get("_from") or "").split("+")[0]
                        break
        # Coherencia de estado: si hay actual, el evento está 'Released'.
        if not _is_empty(base.get("actual")):
            base["status"] = "Released"
        elif _is_empty(base.get("status")):
            base["status"] = "Upcoming"
        # Recalcular sorpresa/clasificación si ganó casillas y aún no las tenía.
        if base.get("surprise") is None:
            _actual = _parse_econ_num(base.get("actual"))
            _forecast = _parse_econ_num(base.get("forecast"))
            if _actual is not None and _forecast is not None:
                _sp = _actual - _forecast
                base["surprise"] = round(_sp, 2)
                base["surprise_pct"] = round((_sp/abs(_forecast)*100), 1) if _forecast != 0 else None
                _name = (base.get("title","") or "").lower()
                _hb = any(x in _name for x in ["cpi","ppi","inflation","claims","unemployment","jobless"])
                if abs(_sp) < 0.001:
                    base["classification"] = "Neutral"
                elif _hb:
                    base["classification"] = "Bearish" if _sp > 0 else "Bullish"
                else:
                    base["classification"] = "Bullish" if _sp > 0 else "Bearish"
        deduped.append(base)
    deduped.sort(key=lambda e: e.get("time",""))

    # ── RELLENO DE ÚLTIMO RECURSO: BLS (gratis) → FRED (con key) ──
    # Para eventos US ya vencidos que ninguna fuente marcó con 'actual' todavía.
    # Así garantizamos el resultado lo más rápido posible desde el gobierno.
    try:
        await _fill_actuals_bls(deduped)
        await _fill_actuals_fred(deduped)
    except Exception as _e:
        print(f"[calendar] relleno BLS/FRED: {_e}")

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
    "fed minutes":     (9.2,"Entire Market","Central Bank","bearish"),
    "fomc minutes":    (9.4,"Entire Market","Central Bank","bearish"),
    "meeting minutes": (8.2,"Entire Market","Central Bank","bearish"),
    "interest rate":   (9.0,"Entire Market","Monetary Policy","bearish"),
    "interest rates":  (9.0,"Entire Market","Monetary Policy","bearish"),
    "hawkish":         (8.6,"Entire Market","Monetary Policy","bearish"),
    "dovish":          (8.6,"Entire Market","Monetary Policy","bullish"),
    "higher for longer":(8.8,"Entire Market","Monetary Policy","bearish"),
    "basis points":    (8.0,"Entire Market","Monetary Policy","bearish"),
    # Rates & Bonds (yields → sube el descuento → presiona múltiplos del NQ)
    "treasury yield":  (9.0,"Entire Market","Rates & Bonds","bearish"),
    "treasury yields": (9.0,"Entire Market","Rates & Bonds","bearish"),
    "bond yield":      (8.8,"Entire Market","Rates & Bonds","bearish"),
    "bond yields":     (8.8,"Entire Market","Rates & Bonds","bearish"),
    "10-year":         (8.6,"Entire Market","Rates & Bonds","bearish"),
    "10 year":         (8.6,"Entire Market","Rates & Bonds","bearish"),
    "2-year":          (8.3,"Entire Market","Rates & Bonds","bearish"),
    "yield curve":     (8.6,"Entire Market","Rates & Bonds","bearish"),
    "bond market":     (8.2,"Entire Market","Rates & Bonds","bearish"),
    "treasury buyback":(8.8,"Entire Market","Rates & Bonds","bullish"),
    "bond buyback":    (8.6,"Entire Market","Rates & Bonds","bullish"),
    "debt buyback":    (8.6,"Entire Market","Rates & Bonds","bullish"),
    "treasury auction":(8.0,"Entire Market","Rates & Bonds","bearish"),
    # Semiconductores — sector que mueve el NQ en bloque (no solo NVDA)
    "semiconductor":   (7.6,"Technology","Sector","bearish"),
    "semiconductors":  (7.6,"Technology","Sector","bearish"),
    "chip stocks":     (7.6,"Technology","Sector","bearish"),
    "chipmaker":       (7.4,"Technology","Sector","bearish"),
    "chip export":     (8.2,"Technology","Sector/Trade","bearish"),
    "chip curb":       (8.2,"Technology","Sector/Trade","bearish"),
    "philadelphia semiconductor":(8.0,"Technology","Sector","bearish"),
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

    # Umbral mínimo. Antes 8.5 filtraba TODO (el panel quedaba vacío → caía al mock,
    # "mismas noticias por días"). 7.0 mantiene lo sistémico (Fed/CPI/NFP/geopolítica/
    # tariffs) y AÑADE megacaps que sí mueven el NQ (NVDA/AAPL/MSFT 7.0-7.5).
    if best_score < 7.0: return None

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

def _macro_news_from_calendar():
    """PUENTE calendario → High Impact News. Convierte cada release macro de alto
    impacto que YA publicó su 'actual' en un titular de último minuto con su
    dirección para el NQ. Así el resultado (ej. 'Philly Fed 47.4 vs 25 esp') aparece
    en High Impact News al instante, sin depender de un proveedor de noticias."""
    out = []
    now_ts = time.time()
    def _num(v):
        try:
            return float(str(v).replace("%", "").replace(",", "").replace("K", "").replace("k", "").strip())
        except Exception:
            return None
    for e in (cache["calendar"]["data"] or []):
        if e.get("status") != "Released" or e.get("type") == "holiday":
            continue
        actual = e.get("actual")
        if not actual or not str(actual).strip():
            continue
        if str(e.get("impact", "")).lower() not in ("high", "extreme"):
            continue
        title = e.get("title", "") or ""
        tl0 = title.lower()
        # Solo macro US (el NQ es US). Si el país es explícito, exigir US; si no,
        # excluir por marcadores extranjeros en el título.
        ctry = str(e.get("country", "") or "").upper()
        if ctry and ctry not in ("US", "USD", "UNITED STATES"):
            continue
        _FOREIGN = ("japan", "china", "chinese", "euro", "ecb", "germany", "german",
                    "france", "french", "u.k", "uk ", "britain", "british", "australia",
                    "australian", "canada", "canadian", "spain", "italy", "mexico",
                    "brazil", "india", "new zealand", "swiss", "switzerland", "boj",
                    "pboc", "boe", "gbp", "eur ", "jpy", "aud", "cad", "cny")
        if any(m in tl0 for m in _FOREIGN):
            continue
        # Mantener si es de HOY (ET) — para que el release de la mañana se vea toda
        # la sesión — o de las últimas 6h.
        try:
            ev_ts = datetime.fromisoformat(str(e.get("time", "")).replace("Z", "+00:00")).timestamp()
        except Exception:
            ev_ts = now_ts
        ev_date = str(e.get("time", ""))[:10]
        today_et = datetime.now(NY).strftime("%Y-%m-%d")
        if ev_date != today_et and ev_ts and (now_ts - ev_ts) > 6 * 3600:
            continue
        forecast = e.get("forecast")
        a, f = _num(actual), _num(forecast)
        tl = title.lower()
        higher_bearish = any(k in tl for k in ("cpi", "ppi", "pce", "inflation", "claims", "unemployment"))
        sentiment = "Neutral"
        if a is not None and f is not None and abs(a - f) > 1e-9:
            beat = a > f
            sentiment = ("Bearish" if beat else "Bullish") if higher_bearish else ("Bullish" if beat else "Bearish")
        score = 9.2 if str(e.get("impact", "")).lower() == "extreme" else 8.5
        hl = f"{title}: {actual}" + (f" (esp {forecast})" if forecast else "")
        out.append({
            "headline": hl, "impact_score": score, "scope": "Entire Market",
            "category": "Macro Data", "sentiment": sentiment, "source": "Calendario macro",
            "source_confidence": "High", "alert_level": "CRITICAL" if score >= 9.0 else "HIGH",
            "ts": ev_ts or now_ts, "type": "ultra_impact", "url": "",
        })
    return out

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
        # PUENTE: añadir los resultados macro del calendario (último minuto) a la
        # misma lista de noticias de alto impacto.
        classified.extend(_macro_news_from_calendar())

        store = cache.setdefault("_movers_seen", {})
        now_ts = time.time()
        for it in classified:
            # BUG HISTÓRICO: se leía "title" pero _classify_impact_news devuelve
            # "headline" → key vacío → todas las noticias se descartaban (panel
            # vacío → caía al mock, "mismas noticias por días").
            key = (it.get("headline") or it.get("title") or "")[:80].lower().strip()
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
        # Poda por TTL: fuera noticias con timestamp (o primera vista) > 24h.
        # Dave: High Impact solo muestra noticias de MÁXIMO 24 horas de antigüedad.
        cutoff = now_ts - 24 * 3600
        for k in list(store):
            v = store[k]
            news_ts = v.get("ts") or 0
            if (news_ts and news_ts < cutoff) or (not news_ts and v.get("_first_seen", now_ts) < cutoff):
                del store[k]
        # RANKING con BONUS DE FRESCURA. Antes ordenaba SOLO por impact_score (la
        # hora era desempate), así que una noticia nueva quedaba enterrada 10-20 min
        # detrás de otras más viejas pero de mayor impacto. Ahora una noticia
        # reciente recibe un bonus (+4 recién salida, decae a 0 en 3h) que la sube
        # al panel de inmediato; las viejas se desvanecen y las de impacto genuino
        # aún permanecen un rato. Así el daytrading actualiza igual de rápido que el home.
        def _eff(x):
            t = x.get("ts") or x.get("_first_seen") or now_ts
            age_h = max(0.0, (now_ts - t) / 3600.0)
            recency = max(0.0, 4.0 * (1.0 - age_h / 3.0))
            return (round(x.get("impact_score", 0) + recency, 3), t)
        ranked = sorted(store.values(), key=_eff, reverse=True)
        out = [{k: v for k, v in it.items() if k != "_first_seen"} for it in ranked[:8]]
        # HORA DE PUBLICACIÓN (campo "Time" del panel High Impact News). El front
        # muestra it.time_et; lo derivamos del timestamp real de la noticia (ts, unix
        # UTC de Finnhub / hora del release del calendario) formateado a ET "HH:MM ET".
        for it in out:
            _ts = it.get("ts") or 0
            if _ts and not it.get("time_et"):
                try:
                    it["time_et"] = datetime.fromtimestamp(float(_ts), NY).strftime("%H:%M ET")
                except Exception:
                    pass

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

@app.get("/api/admin/diag-news")
async def diag_news(key: str = ""):
    """Sondea Finnhub /news crudo: cuántas noticias trae y cuántas pasan el
    clasificador (y con qué score). Uso: ?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    out = {"finnhub_key": bool(FINNHUB_KEY), "movers_status": cache["movers"].get("status"),
           "categorias": {}}
    if not FINNHUB_KEY:
        out["veredicto"] = "❌ Falta FINNHUB_KEY"; return out
    cal_titles = [e.get("title", "") for e in cache["calendar"]["data"]]
    async with httpx.AsyncClient(timeout=10) as client:
        for cat in ("general", "forex"):
            try:
                r = await client.get(f"{FH_BASE}/news", params={"category": cat, "token": FINNHUB_KEY})
                entry = {"http": r.status_code}
                if r.status_code == 200:
                    items = r.json()
                    entry["crudas"] = len(items)
                    passed, samples = 0, []
                    for it in items[:60]:
                        res = _classify_impact_news(it.get("headline", ""), it.get("source", ""),
                                                    it.get("datetime", 0), cal_titles)
                        if res:
                            passed += 1
                            if len(samples) < 4:
                                samples.append({"score": res.get("impact_score"),
                                                "h": (it.get("headline", "") or "")[:70]})
                    entry["pasan_clasificador"] = passed
                    entry["muestras_top"] = samples
                    entry["muestra_cruda"] = [(it.get("headline", "") or "")[:70] for it in items[:4]]
                else:
                    entry["cuerpo"] = r.text[:160]
                out["categorias"][cat] = entry
            except Exception as e:
                out["categorias"][cat] = {"error": f"{type(e).__name__}: {str(e)[:120]}"}
    return out


EARN_EXTREME = {"AAPL","MSFT","NVDA","AMZN","META","GOOGL","GOOG","TSLA","AVGO","NFLX"}
EARN_HIGH    = {
    "AMD","INTC","QCOM","MU","TSM","ORCL","CRM","ADBE","CSCO","TXN","AMAT",
    "LRCX","PANW","CRWD","SNOW","PLTR","SMCI","MRVL","ARM","DELL","NOW","INTU",
    "UBER","SHOP","COIN","PYPL","COST","TMUS","ADP","ADI","KLAC","MCHP",
    "WDAY","FTNT","DDOG","ZS","NXPI",
}

_sym_names = {}            # sym -> nombre real de la empresa (persistente en app_config)
_sym_logos = {}            # sym -> URL del logo real (persistente en app_config)
_sym_names_loaded = False

async def _sym_names_load():
    global _sym_names_loaded
    if _sym_names_loaded:
        return
    _sym_names_loaded = True
    try:
        v = await _sb_get_config("sym_names")
        if isinstance(v, dict):
            _sym_names.update(v)
    except Exception:
        pass
    try:
        vl = await _sb_get_config("sym_logos")
        if isinstance(vl, dict):
            _sym_logos.update(vl)
    except Exception:
        pass

async def _fill_earn_names(events, cap=50):
    """Rellena e['name'] (nombre real) y e['logo'] (URL del logo real) de cada empresa,
    para el tooltip y las fichas del calendario. El feed de Finnhub /calendar/earnings
    solo trae el ticker; aquí lo resolvemos: cache-first (nombres/logos ya vistos +
    compañías ya abiertas en el drawer), y si falta, Finnhub /stock/profile2 UNA sola vez
    por símbolo (con presupuesto y tope por refresh), persistido en app_config. Cada
    símbolo se paga una vez en la vida (nombre+logo en la misma llamada). Se resuelven
    PRIMERO las empresas de mayor impacto (extreme/high) para que las importantes tengan
    nombre/logo cuanto antes."""
    await _sym_names_load()
    new, dirty = 0, False
    _prio = {"extreme": 0, "high": 1, "medium": 2}
    for e in sorted(events, key=lambda x: _prio.get(x.get("impact"), 9)):
        sym = e.get("symbol")
        if not sym:
            continue
        nm = _sym_names.get(sym)
        lg = _sym_logos.get(sym)
        if not nm or lg is None:
            c = ((cache.get("company") or {}).get(sym) or {}).get("data") or {}
            if not nm and c.get("name"):
                nm = c["name"]; _sym_names[sym] = nm; dirty = True
            if lg is None and c.get("logo") is not None:
                lg = c["logo"]; _sym_logos[sym] = lg; dirty = True
        if (not nm or lg is None) and new < cap and FINNHUB_KEY and fh_budget_ok(1):
            try:
                async with httpx.AsyncClient(timeout=6) as client:
                    r = await client.get(f"{FH_BASE}/stock/profile2",
                                         params={"symbol": sym, "token": FINNHUB_KEY})
                fh_charge(1); new += 1
                if r.status_code == 200:
                    p = r.json() or {}
                    if not nm and p.get("name"):
                        nm = p["name"]; _sym_names[sym] = nm; dirty = True
                    # Guardamos el logo aunque venga vacío ("") para no re-consultar
                    # eternamente a las empresas que Finnhub no tiene con logo.
                    lg = p.get("logo", "") or ""
                    _sym_logos[sym] = lg; dirty = True
            except Exception as ex:
                print(f"[earn-names] {sym}: {ex}")
        if nm:
            e["name"] = nm
        if lg:
            e["logo"] = lg
    if dirty:
        try:
            await _sb_set_config("sym_names", _sym_names)
            await _sb_set_config("sym_logos", _sym_logos)
        except Exception:
            pass

def _earn_impact(sym):
    s = (sym or "").upper()
    if s in EARN_EXTREME: return "extreme"
    if s in EARN_HIGH:    return "high"
    return "medium"

async def refresh_earnings(days=45):
    if not FINNHUB_KEY: return
    if not fh_budget_ok(1):
        print("[earnings] presupuesto Finnhub agotado — se omite"); return
    today = datetime.now(NY).date()
    frm   = today.isoformat()
    to    = (today + timedelta(days=days)).isoformat()
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{FH_BASE}/calendar/earnings",
                                  params={"from":frm,"to":to,"token":FINNHUB_KEY})
        fh_charge(1)  # contabilizar la llamada (antes sin contar)
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
        # Nombre real de cada empresa (para el tooltip del calendario al pasar el cursor).
        try:
            await _fill_earn_names(out)
        except Exception as _e:
            print(f"[earnings] fill names: {_e}")
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
        # Sin niveles GEX en cache (típico en cold-start). GexBot los publica en RTH.
        ctx.append("- Gamma (GEX): esperando cálculo de la sesión RTH — aún no publicados por GexBot")

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

    # ── MOVERS DE ULTRA IMPACTO (noticias market-moving en vivo, score >=7/10) ──
    # Para que el briefing avise de un titular de última hora que mueve el mercado
    # AHORA (Fed, geopolítica, shock macro) — no solo lo programado.
    movers = cache["movers"]["data"] or []
    if movers:
        top = sorted(movers, key=lambda m: (m.get("impact_score", 0), m.get("ts", 0)), reverse=True)[:3]
        mv = []
        for m in top:
            hl = (m.get("headline") or "").strip()[:120]
            if hl:
                mv.append(f"[{m.get('impact_score','?')}/10 {m.get('sentiment','')}] {hl}")
        if mv:
            ctx.append("- Movers ultra-impacto (en vivo): " + " || ".join(mv))

    # ── CICLO MACRO: releases recientes clave (del calendario, actual vs esperado) ──
    MACRO_KEYS = ["gdp", "cpi", "inflation", "ppi", "nonfarm", "payroll",
                  "unemployment", "pce", "retail sales", "interest rate",
                  "fed funds", "jobless", "michigan", "adp", "employment",
                  "ism", "consumer confidence", "durable goods", "housing"]
    macro_rel = {}
    for e in cal:
        if e.get("status") != "Released":
            continue
        t = (e.get("title", "") or "").lower()
        for mk in MACRO_KEYS:
            if mk in t and mk not in macro_rel:
                act = e.get("actual"); fc = e.get("forecast")
                if act:
                    macro_rel[mk] = f"{e.get('title','')} {act}" + (f" (esp {fc})" if fc else "")
                break
    if macro_rel:
        ctx.append("- Datos macro recientes (actual vs esperado): " + " | ".join(list(macro_rel.values())[:6]))

    # Rendimientos de bonos (dirección) — clave para múltiplos tech
    yields = []
    for k, lbl in [("US2Y", "2Y"), ("US10Y", "10Y"), ("US30Y", "30Y")]:
        d = hm.get(k, {})
        if d.get("chg_pct") is not None:
            yields.append(f"{lbl} {d['chg_pct']:+.2f}%")
    if yields:
        ctx.append(f"- Bonos (variación rendimiento hoy): {' | '.join(yields)}")

    # Sector tecnológico: promedio de 7 megacaps
    tech = [hm.get(s, {}).get("chg_pct") for s in ["NVDA", "AAPL", "MSFT", "META", "AMZN", "GOOGL", "AVGO"]]
    tech = [x for x in tech if x is not None]
    if tech:
        ctx.append(f"- Sector tecnológico (7 megacaps): promedio {sum(tech)/len(tech):+.2f}%")

    # Gap de apertura (del inventario overnight ya calculado)
    try:
        if nq_chg is not None:
            gapdir = "alcista" if nq_chg > 0.15 else "bajista" if nq_chg < -0.15 else "plano (sin gap)"
            ctx.append(f"- Gap de apertura: {gapdir} ({nq_chg:+.2f}%)")
    except Exception:
        pass

    # Próximo evento macro
    upcoming = [e for e in cal if e.get("status") == "Upcoming"]
    if upcoming:
        ctx.append(f"- Próximo catalizador: {upcoming[0].get('title','')}")
    # Eventos ya publicados hoy con resultado
    today_str = now_et.strftime("%Y-%m-%d")
    # Eventos de HOY programados de alto impacto CON HORA (ej. "FOMC 14:00 ET") —
    # para que el briefing avise: "hoy la Fed habla a las 2pm".
    def _ev_hora(tm):
        try:
            return datetime.fromisoformat(str(tm).replace("Z", "+00:00")).astimezone(NY).strftime("%H:%M")
        except Exception:
            s = str(tm or "")
            return s[11:16] if len(s) >= 16 else ""
    sched_today = []
    for e in cal:
        tm = e.get("time", "") or ""
        if str(tm).startswith(today_str) and e.get("status") == "Upcoming" \
           and str(e.get("impact", "")).lower() in ("high", "extreme"):
            hh = _ev_hora(tm)
            sched_today.append(f"{e.get('title','')}{(' ' + hh + ' ET') if hh else ''}")
    if sched_today:
        ctx.append(f"- Eventos programados hoy (alto impacto, con hora): {' | '.join(sched_today[:4])}")
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
    sys_msg = (f"Eres el analista jefe de mesa de Liberato Community para {FA_ASSET} Futures, "
               "razonando con Auction Market Theory (AMT) y posicionamiento dealer. "
               "Tu lector lo tiene que entender en 20 SEGUNDOS. Escribes SOLO en español, ULTRA-CONCISO, "
               "TELEGRÁFICO, cero relleno. Devuelves EXACTAMENTE 7 líneas con este formato (una línea cada una, "
               "nada antes ni después, sin títulos de sección, sin listas con guiones):\n"
               "**Macro:** <1 oración: ciclo macro con los datos reales — GDP (crecimiento), inflación (CPI/PCE/PPI), "
               "empleo (NFP/desempleo/claims), bonos y rendimientos (2Y/10Y/30Y), tasa de interés y trayectoria (senda "
               "de la Fed). Nombra la FASE del ciclo si se infiere (expansión/desaceleración/estanflación). Menciona COT SOLO si está>\n"
               "**Hoy:** <1 oración: catalizadores de HOY de alto impacto (con su hora si la hay, ej. FOMC 14:00 ET) + "
               "geopolítica relevante. Si en los datos hay 'Movers ultra-impacto (en vivo)', menciona el más fuerte "
               "como titular de última hora que mueve el mercado ahora>\n"
               "**Earnings:** <1 oración: earnings que impacten directamente el Nasdaq hoy; si no hay, di 'sin earnings "
               "relevantes para el NQ hoy'>\n"
               "**Técnico:** <1 oración: niveles GEX (Call/Put Wall, Flip, Max Pain) + Market Regime (gamma +/−) + "
               "VAH/VAL + cambio de precio + % del sector tecnológico + gap (alcista/bajista)>\n"
               "**Volatilidad:** <1 oración: VIX (nivel + si sube/baja) + movimiento esperado (±pts) + régimen de vol "
               "(compresión si gamma+ / expansión si gamma−) + Fear&Greed si está>\n"
               "**Gestión:** <1 oración ACCIONABLE de gestión de riesgo para el scalper: tamaño (reducir/normal), "
               "apalancamiento (evitar agresivo si hay riesgo), y stops (ajustados/normales) — JUSTIFICADO por lo que "
               "manda hoy (guerra/geopolítica, evento de alto impacto y su hora, volatilidad/expansión, o posicionamiento "
               "extremo SI está en los datos). Ej: 'Reduce tamaño y stops ajustados: geopolítica Irán + subasta 30Y 13:00 "
               "ET son los gatillos de gap; evita apalancamiento agresivo'. NO inventes cifras de posicionamiento>\n"
               "**Claridad:** <n>/10 hacia <alza / baja / sin dirección clara> · Vol <alta/media/baja>\n"
               "FORMATO: 5 a 7 oraciones en total (una por etiqueta que tenga datos), SIN EMOJIS. Usa las etiquetas en "
               "negrita **Macro:**, **Hoy:**, **Earnings:**, **Técnico:**, **Volatilidad:**, **Gestión:**, **Claridad:** tal cual, y pon en **negrita** "
               "el dato/nivel más importante de cada oración. Nada de iconos ni símbolos decorativos.\n"
               "IMPORTANTE: NO marques un sesgo alcista/bajista duro. En su lugar, la línea **Claridad:** da un SCORE "
               "1-10 de qué tan claro/limpio está el día y hacia qué lado se inclina (alza/baja/sin dirección). "
               "REGLAS: usa números EXACTOS de los datos. En gamma negativo piensa momentum/expansión (dealers "
               "persiguen); en gamma positivo reversión/compresión (dealers absorben). NUNCA inventes un dato: si algo "
               "no está en los datos, omite esa etiqueta (no la escribas). Cada oración es corta. Prohibida la prosa larga.")

    if has_gamma:
        usr_msg = (f"Datos de mesa ahora mismo:\n\n{ctx_str}\n\n"
                   "Escribe el briefing en el formato exacto (4-6 oraciones, SIN emojis, etiquetas en negrita). "
                   "Cubre Macro (GDP/inflación/empleo/bonos/tasas + fase del ciclo), catalizadores de HOY + geopolítica, "
                   "earnings del Nasdaq, el técnico (GEX/regime/VAH-VAL/precio/tech%/gap), una línea propia de "
                   "**Volatilidad:** (VIX nivel+dirección, movimiento esperado ±pts, régimen de vol, Fear&Greed), y una "
                   "línea **Gestión:** con la acción de riesgo (tamaño/apalancamiento/stops justificados por lo que manda hoy). "
                   "Cierra con **Claridad:** score 1-10 hacia alza/baja/sin dirección — NO un sesgo duro. Usa los "
                   "números exactos; omite la etiqueta de cualquier tema sin datos.")
    else:
        usr_msg = (f"Datos de mesa ahora mismo (sin GEX disponible aún):\n\n{ctx_str}\n\n"
                   "Escribe el briefing en el formato exacto (3-5 oraciones, etiquetas en negrita) con lo disponible "
                   "(macro, catalizadores de hoy, earnings, inventario/gap, sentimiento, sector tech). En **Técnico:** "
                   "como aún no hay niveles GEX de RTH, di literalmente 'GEX: esperando niveles de la sesión RTH' y "
                   "apóyate en inventario/gap/vol/sector tech. Cierra con **Claridad:** score 1-10 hacia alza/baja/sin "
                   "dirección. No inventes datos.")

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization":f"Bearer {GROQ_KEY}","Content-Type":"application/json"},
                json={"model":"qwen/qwen3.6-27b","max_tokens":460,"temperature":0.55,
                      "reasoning_effort":"none",   # texto: sin razonamiento -> respuesta directa y corta (verificado app fitness)
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
                "model":            "qwen/qwen3.6-27b (Groq)",
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
#  pierde para siempre. Con 56 refreshes/día son ~280 puntos reales por semana
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
    # Walls por VOLUMEN (mismos precios → misma conversión). skew/net_vol/min_dte NO son precios.
    gex_nq["call_wall_vol"] = _to_px(gex.get("call_wall_vol"))
    gex_nq["put_wall_vol"]  = _to_px(gex.get("put_wall_vol"))
    # Niveles 0DTE (capa scalper): escalar sus precios igual que los del full.
    _z = gex.get("zero")
    if isinstance(_z, dict):
        gex_nq["zero"] = {**_z,
            "gamma_flip":    _to_px(_z.get("gamma_flip")),
            "call_wall":     _to_px(_z.get("call_wall")),
            "put_wall":      _to_px(_z.get("put_wall")),
            "call_wall_vol": _to_px(_z.get("call_wall_vol")),
            "put_wall_vol":  _to_px(_z.get("put_wall_vol")),
        }
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

@app.get("/api/admin/diag-sentiment")
async def diag_sentiment(key: str = ""):
    """Verifica las fuentes que reemplazan a FlashAlpha: VIX (TwelveData) y
    Fear&Greed (CNN). Uso: ?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    res = await _refresh_market_sentiment()
    g = cache["gex"].get(FA_ASSET, {})
    return {
        "resultado": res,
        "en_cache": {"vix": g.get("vix"), "fear_score": g.get("fear_score"),
                     "fear_rating": g.get("fear_rating"), "expected_move": g.get("expected_move"),
                     "atm_iv": g.get("atm_iv")},
        "health": {"vix": cache["health"].get("vix"), "feargreed": cache["health"].get("feargreed")},
        "twelvedata_key": bool(TWELVEDATA_KEY),
    }


@app.get("/api/admin/diag-gexbot")
async def diag_gexbot(key: str = ""):
    """Sondea la API de GexBot con la key REAL (de Railway) y muestra la forma
    exacta del JSON, SIN exponer la key. Prueba varias combinaciones state/tipo
    para saber qué habilita tu tier. Uso: /api/admin/diag-gexbot?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    out = {
        "key_presente": bool(GEXBOT_API_KEY),
        "key_len": len(GEXBOT_API_KEY),
        "symbol": GEXBOT_SYMBOL,
        "base": GEXBOT_BASE,
        "pruebas": {},
    }
    if not GEXBOT_API_KEY:
        out["veredicto"] = "❌ Falta GEXBOT_API_KEY en Railway (servicio web)"
        return out

    def _shape(v, depth=0):
        """Describe la forma de un valor sin volcar todo (arrays: len + 1ª fila)."""
        if isinstance(v, dict):
            return {k: _shape(val, depth+1) for k, val in list(v.items())[:40]}
        if isinstance(v, list):
            return {"_array_len": len(v), "_ejemplo": (_shape(v[0], depth+1) if v else None)}
        return type(v).__name__ + (f"={v}" if isinstance(v, (int, float, bool)) and depth < 2 else "")

    combos = [
        ("tickers", None, None),          # descubre tickers válidos para tu tier
        (GEXBOT_SYMBOL, "classic", "full"),
        (GEXBOT_SYMBOL, "classic", "zero"),
        (GEXBOT_SYMBOL, "state",   "full"),
    ]
    async with httpx.AsyncClient(timeout=15, headers=_gexbot_headers()) as c:
        for sym, state, tipo in combos:
            if state is None:
                label = f"/{sym}"; url = f"{GEXBOT_BASE}/{sym}"
            else:
                label = f"{sym}/{state}/{tipo}"; url = f"{GEXBOT_BASE}/{sym}/{state}/{tipo}"
            try:
                r = await c.get(url)
                entry = {"http": r.status_code}
                if r.status_code == 200:
                    try:
                        j = r.json()
                        entry["forma"] = _shape(j)
                        if label == "/tickers" and isinstance(j, dict):
                            entry["lista"] = {k: j.get(k) for k in ("indexes", "futures", "stocks")}
                        # muestra cruda de la 1ª fila de strikes/mini_contracts
                        for arrk in ("strikes", "mini_contracts", "levels"):
                            arr = j.get(arrk) if isinstance(j, dict) else None
                            if isinstance(arr, list) and arr:
                                entry[f"{arrk}_muestra"] = arr[:3]
                                entry[f"{arrk}_total"] = len(arr)
                    except Exception as e:
                        entry["parse_error"] = f"{type(e).__name__}: {e}"
                        entry["cuerpo"] = r.text[:200]
                else:
                    entry["cuerpo"] = r.text[:200]
                out["pruebas"][label] = entry
            except Exception as e:
                out["pruebas"][label] = {"error": f"{type(e).__name__}: {e}"}
    ok = [k for k, v in out["pruebas"].items() if v.get("http") == 200]
    out["veredicto"] = (f"✅ Responden 200: {ok}" if ok
                        else "❌ Ninguna combinación devolvió 200 (¿tier/símbolo/key?)")
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
            "consumidores": ["refresh_gex (56x/día)", "diags (bajo budget_ok)"],
            "por_dia_teorico": 5 + (GEX_REFRESHES_PER_DAY - 1) * 3,
            "detalle": "5 créd el 1º del día + 3 los demás. 56 refreshes: 08:30, "
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
    # Semáforo de agotamiento: verde <70%, amarillo 70-90%, rojo >90%.
    # Es la medida "nunca agotar créditos" hecha visible: la rutina de auditoría
    # (lq-6-web-auditor) puede alertar solo cuando algo pasa a amarillo/rojo.
    def _semaforo(used, limit):
        if not limit:
            return "verde", None
        p = round(used / limit * 100, 1)
        return ("rojo" if p >= 90 else "amarillo" if p >= 70 else "verde"), p

    # USO EFECTIVO: el reset del contador es perezoso (solo al llamar budget_ok).
    # Si la ventana guardada ya venció (fin de semana sin GEX, o cambio de día
    # UTC), el `used` crudo es un residuo obsoleto: el próximo budget_ok lo pondrá
    # a 0. El reporte debe mostrar 0, no el residuo, o daría una falsa alarma de
    # "casi agotado" el lunes por la mañana antes del primer refresh.
    def _uso_efectivo(name, cfg):
        st = _api_usage[name]
        if st["window_key"] != _window_key(cfg["window"]):
            return 0, True   # ventana vencida → arranca en 0
        return st["used"], False

    out = {"generado": datetime.now(NY).isoformat(), "asset": FA_ASSET, "apis": {}}
    _peor = "verde"
    _orden = {"verde": 0, "amarillo": 1, "rojo": 2}
    for name, cfg in API_BUDGETS.items():
        st = _api_usage[name]
        used_ef, vencida = _uso_efectivo(name, cfg)
        estado, pct = _semaforo(used_ef, cfg["limit"])
        if _orden[estado] > _orden[_peor]:
            _peor = estado
        out["apis"][name] = {
            "key_configurada": bool({
                "flashalpha": FLASHALPHA_KEY, "twelvedata": TWELVEDATA_KEY,
                "finnhub": FINNHUB_KEY, "groq": GROQ_KEY,
                "alphavantage": ALPHA_VANTAGE_KEY, "fmp": FMP_KEY,
            }.get(name)),
            "estado": estado,
            "limite_seguro": cfg["limit"], "ventana": cfg["window"],
            "usado_ahora": used_ef, "ventana_actual": st["window_key"],
            "ventana_vencida": vencida,   # True = el used crudo es residuo, se reseteará
            "restante": max(0, cfg["limit"] - used_ef),
            "pct": pct,
            "plan": plan.get(name, {}),
        }
    # RapidAPI tiene su propio contador; su reset también es lazy (por día ET).
    _ra_ef = _rapidapi_day_count if _rapidapi_day == _today_et_str() else 0
    _ra_estado, _ra_pct = _semaforo(_ra_ef, 85)
    if _orden[_ra_estado] > _orden[_peor]:
        _peor = _ra_estado
    out["apis"]["rapidapi"] = {
        "key_configurada": bool(RAPIDAPI_KEY), "estado": _ra_estado,
        "limite_seguro": 85, "ventana": "day",
        "usado_ahora": _ra_ef, "restante": max(0, 85 - _ra_ef),
        "ventana_vencida": _rapidapi_day != _today_et_str(),
        "pct": _ra_pct, "plan": plan["rapidapi"],
    }
    out["estado_global"] = _peor   # verde = ninguna API en riesgo en la ventana actual
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

@app.get("/api/admin/diag-snaptrade")
async def diag_snaptrade(key: str = ""):
    """Muestra QUÉ variables de entorno relacionadas con SnapTrade existen y con
    qué nombre exacto (valores enmascarados). Sirve para depurar sin que nadie
    tenga que pegar credenciales en un chat."""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    def mask(v):
        if not v: return None
        v = str(v)
        return (v[:4] + "..." + v[-3:] + f" ({len(v)} chars)") if len(v) > 8 else f"({len(v)} chars)"
    encontradas = {k: mask(v) for k, v in os.environ.items()
                   if "SNAP" in k.upper() or "TRADESTATION" in k.upper()}
    # Nombres de TODAS las variables configuradas (SOLO nombres, sin valores) para
    # ver si el nombre se escribió distinto. Se filtran las internas de Railway.
    _internas = ("RAILWAY_", "NIXPACKS_", "PATH", "HOME", "PORT", "PYTHON", "LANG",
                 "PWD", "SHLVL", "_", "HOSTNAME", "SSL_", "GPG_", "LD_")
    otras = sorted([k for k in os.environ
                    if not any(k.upper().startswith(i) for i in _internas)])
    return {
        "nombres_que_el_codigo_busca": ["SNAPTRADE_CLIENT_ID", "SNAPTRADE_CONSUMER_KEY"],
        "variables_encontradas_en_este_servicio": encontradas or "(ninguna)",
        "leidas_por_el_codigo": {
            "SNAPTRADE_CLIENT_ID": mask(SNAPTRADE_CLIENT_ID),
            "SNAPTRADE_CONSUMER_KEY": mask(SNAPTRADE_CONSUMER_KEY),
        },
        "nombres_de_variables_configuradas": otras,
        "total_variables": len(otras),
        "claves_conocidas_presentes": {
            "FLASHALPHA_KEY": bool(FLASHALPHA_KEY), "GROQ_KEY": bool(GROQ_KEY),
            "FINNHUB_KEY": bool(FINNHUB_KEY),
        },
        "servicio": "Liberato-Backend (dashboard) — web-production-33671",
        "ayuda": "Si 'variables_encontradas' está vacío, las variables se pusieron "
                 "en OTRO servicio de Railway (p.ej. el de usuarios) o con otro nombre. "
                 "Deben ir en el servicio 'web' del proyecto del dashboard.",
    }


@app.get("/api/broker/snaptrade/status")
async def snaptrade_status():
    """Estado de la integración SnapTrade (multi-broker, solo lectura). El flujo
    real (registrar usuario + generar login portal) se monta cuando Dave ponga
    sus credenciales self-service de snaptrade.com en Railway. Por ahora informa
    si ya están, para que el botón exista desde ya sin fingir que conecta."""
    if not (SNAPTRADE_CLIENT_ID and SNAPTRADE_CONSUMER_KEY):
        return {"configured": False,
                "message": "SnapTrade aún no está activado. Falta el clientId y "
                           "consumerKey (self-service en snaptrade.com, minutos). "
                           "En cuanto los pongas en Railway, este botón conecta "
                           "cualquier broker — TradeStation incluido."}
    return {"configured": True,
            "message": "SnapTrade activado. Listo para conectar tu broker."}


def _snaptrade():
    """Cliente SnapTrade (SDK 13.0.3, firma automática). Lazy singleton."""
    global _st_client
    if _st_client is None:
        from snaptrade_client import SnapTrade, SnapTradeAuth
        mk = (SnapTradeAuth.personal_api_key if SNAPTRADE_MODE == "personal"
              else SnapTradeAuth.commercial_api_key)
        _st_client = SnapTrade(auth=mk(
            consumer_key=SNAPTRADE_CONSUMER_KEY, client_id=SNAPTRADE_CLIENT_ID))
    return _st_client


async def _st_user_kwargs(app_user_id, register=True):
    """Devuelve los kwargs de identidad para las llamadas SnapTrade.
    Personal → {} (la key identifica al usuario). Commercial → user_id/user_secret
    (registrando si hace falta)."""
    if SNAPTRADE_MODE == "personal":
        return {}
    app_user_id = _require_snaptrade(app_user_id)
    rec = _snaptrade_users.get(app_user_id)
    if not rec and register:
        rec = await _st_register(app_user_id)
    if not rec:
        raise HTTPException(404, "usuario no registrado — conecta un broker primero")
    return {"user_id": rec["snap_user_id"], "user_secret": rec["user_secret"]}


def _st_plain(x):
    """Convierte los tipos-schema del SDK (frozendict/tuple/str/int) a JSON puro."""
    if isinstance(x, bool):
        return bool(x)
    if isinstance(x, dict) or hasattr(x, "items"):
        return {str(k): _st_plain(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_st_plain(v) for v in x]
    if isinstance(x, (int, float)):
        return x.real if isinstance(x, complex) else (int(x) if isinstance(x, int) else float(x))
    if isinstance(x, str):
        return str(x)
    if x is None:
        return None
    return str(x)


async def _st_register(app_user_id: str):
    """Registra (idempotente) un usuario SnapTrade y guarda su user_secret.
    Devuelve el record. Si SnapTrade dice que ya existe pero perdimos el secret,
    borra y re-registra (se perderían conexiones previas — caso raro)."""
    rec = _snaptrade_users.get(app_user_id)
    if rec and rec.get("user_secret"):
        return rec
    snap_user_id = f"lbc_{app_user_id}"
    st = _snaptrade()
    try:
        resp = await st.authentication.aregister_snap_trade_user(user_id=snap_user_id)
        body = _st_plain(resp.body)
        secret = body.get("userSecret")
    except Exception as e:
        ebody = str(getattr(e, "body", "") or getattr(e, "reason", ""))
        msg = (ebody + " || " + str(e))
        if "already" in msg.lower() or "exist" in msg.lower():
            # perdimos el secret: borrar y re-registrar
            try:
                await st.authentication.adelete_snap_trade_user(user_id=snap_user_id)
                resp = await st.authentication.aregister_snap_trade_user(user_id=snap_user_id)
                body = _st_plain(resp.body); secret = body.get("userSecret")
            except Exception as e2:
                eb2 = str(getattr(e2, "body", "") or e2)
                raise HTTPException(502, f"SnapTrade re-registro falló: {eb2[:280]}")
        else:
            raise HTTPException(502, f"SnapTrade registro falló [{type(e).__name__}]: {ebody[:280] or str(e)[:280]}")
    if not secret:
        raise HTTPException(502, "SnapTrade no devolvió userSecret")
    rec = {"snap_user_id": snap_user_id, "user_secret": str(secret), "accounts": [], "ts": time.time()}
    _snaptrade_users[app_user_id] = rec
    save_cache()
    return rec


def _require_snaptrade(app_user_id: str):
    if not (SNAPTRADE_CLIENT_ID and SNAPTRADE_CONSUMER_KEY):
        raise HTTPException(400, "SnapTrade no configurado en el servidor")
    app_user_id = (app_user_id or "").strip()
    if not app_user_id:
        raise HTTPException(400, "Falta app_user_id")
    return app_user_id


@app.post("/api/broker/snaptrade/portal")
async def snaptrade_portal(request: Request):
    """Devuelve la URL del portal de SnapTrade para conectar un broker (por
    defecto TradeStation). Registra al usuario si hace falta. La URL expira en
    5 min. body: {app_user_id, broker?}"""
    if not (SNAPTRADE_CLIENT_ID and SNAPTRADE_CONSUMER_KEY):
        raise HTTPException(400, "SnapTrade no configurado en el servidor")
    data = await request.json()
    # Sin broker → el portal de SnapTrade muestra TODOS los brokers (Tradovate, IBKR,
    # tastytrade, etc.) para que el estudiante elija el suyo. Antes forzaba TradeStation.
    broker = (data.get("broker") or "").strip().upper() or None
    ukw = await _st_user_kwargs(data.get("app_user_id"))
    try:
        resp = await _snaptrade().authentication.alogin_snap_trade_user(
            broker=broker, custom_redirect=SNAPTRADE_REDIRECT_URI or None,
            dark_mode=True, **ukw)
        body = _st_plain(resp.body)
    except Exception as e:
        eb = str(getattr(e, "body", "") or e)
        raise HTTPException(502, f"SnapTrade portal falló: {eb[:280]}")
    url = body.get("redirectURI") or body.get("redirect_uri")
    if not url:
        raise HTTPException(502, f"SnapTrade no devolvió redirectURI: {body}")
    return {"ok": True, "url": str(url)}


@app.get("/api/broker/snaptrade/accounts")
async def snaptrade_accounts(app_user_id: str = ""):
    """Lista las cuentas de broker conectadas por el usuario."""
    if not (SNAPTRADE_CLIENT_ID and SNAPTRADE_CONSUMER_KEY):
        raise HTTPException(400, "SnapTrade no configurado en el servidor")
    ukw = await _st_user_kwargs(app_user_id, register=False)
    try:
        resp = await _snaptrade().account_information.alist_user_accounts(**ukw)
        rows = _st_plain(resp.body) or []
    except Exception as e:
        eb = str(getattr(e, "body", "") or e)
        raise HTTPException(502, f"SnapTrade accounts falló: {eb[:280]}")
    accts = [{"id": str(a.get("id")), "name": a.get("name"),
              "number": a.get("number"),
              "institution": a.get("institution_name") or a.get("brokerage_authorization")}
             for a in rows if isinstance(a, dict)]
    return {"ok": True, "count": len(accts), "accounts": accts}


@app.get("/api/broker/snaptrade/fills")
async def snaptrade_fills(app_user_id: str = "", days: int = 90, raw: int = 0, refresh: int = 0):
    """Trae las órdenes (fills) de todas las cuentas conectadas y las mapea a
    trades del journal. ?raw=1 devuelve la forma cruda (para calibrar el mapeo).
    ?refresh=1 FUERZA a SnapTrade a re-sincronizar con el broker ANTES de leer (para
    el botón RESYNC): sin esto, SnapTrade devuelve lo último que cacheó y un trade
    recién hecho puede no aparecer. El re-sync del broker es asíncrono, así que un
    fill muy reciente puede tardar unos segundos aún tras forzar el refresh."""
    if not (SNAPTRADE_CLIENT_ID and SNAPTRADE_CONSUMER_KEY):
        raise HTTPException(400, "SnapTrade no configurado en el servidor")
    ukw = await _st_user_kwargs(app_user_id, register=False)
    st = _snaptrade()
    refreshed = []
    if refresh:
        # Best-effort: empuja a SnapTrade a re-sincronizar cada conexión antes de leer.
        try:
            resp = await st.connections.alist_brokerage_authorizations(**ukw)
            for a in (_st_plain(resp.body) or []):
                aid = a.get("id") if isinstance(a, dict) else None
                if not aid:
                    continue
                try:
                    await st.connections.arefresh_brokerage_authorization(authorization_id=aid, **ukw)
                    refreshed.append(aid)
                except Exception as e:
                    print(f"[snaptrade] refresh auth {aid} falló: {str(getattr(e,'body','') or e)[:160]}")
        except Exception as e:
            print(f"[snaptrade] listar auths (refresh) falló: {str(getattr(e,'body','') or e)[:160]}")
    accts = []
    try:
        resp = await st.account_information.alist_user_accounts(**ukw)
        for a in (_st_plain(resp.body) or []):
            if isinstance(a, dict):
                accts.append({"id": str(a.get("id")), "name": a.get("name"),
                              "number": a.get("number")})
    except Exception as e:
        eb = str(getattr(e, "body", "") or e)
        raise HTTPException(502, f"SnapTrade accounts falló: {eb[:280]}")
    raw_orders = []
    for a in accts:
        try:
            resp = await st.account_information.aget_user_account_orders(
                account_id=a["id"], state="all", days=int(days), **ukw)
            for o in (_st_plain(resp.body) or []):
                if isinstance(o, dict):
                    o["_account"] = {"id": a["id"], "name": a.get("name"), "number": a.get("number")}
                    raw_orders.append(o)
        except Exception as e:
            print(f"[snaptrade] orders {a.get('id')} falló: {e}")
    if raw:
        return {"ok": True, "count": len(raw_orders), "orders": raw_orders[:50]}
    trades = [_st_map_order(o) for o in raw_orders]
    trades = [t for t in trades if t]
    return {"ok": True, "count": len(trades), "trades": trades, "refreshed": len(refreshed)}


@app.get("/api/admin/snaptrade-refresh")
async def snaptrade_refresh(key: str = ""):
    """Fuerza a SnapTrade a re-sincronizar todas las conexiones (por si una cuenta
    recién conectada —futuros 210EKW34— no apareció). Uso: ?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    ukw = await _st_user_kwargs("", register=False)
    st = _snaptrade()
    out = {"refrescadas": [], "cuentas_antes": 0, "cuentas_despues": 0}
    try:
        resp = await st.connections.alist_brokerage_authorizations(**ukw)
        auths = [a.get("id") for a in (_st_plain(resp.body) or []) if isinstance(a, dict)]
    except Exception as e:
        raise HTTPException(502, f"authorizations: {str(getattr(e,'body','') or e)[:200]}")
    ra = await st.account_information.alist_user_accounts(**ukw)
    out["cuentas_antes"] = len(_st_plain(ra.body) or [])
    for aid in auths:
        try:
            await st.connections.arefresh_brokerage_authorization(authorization_id=aid, **ukw)
            out["refrescadas"].append(aid)
        except Exception as e:
            out["refrescadas"].append({"id": aid, "error": str(getattr(e, "body", "") or e)[:160]})
    return out


@app.get("/api/admin/diag-snaptrade-connections")
async def diag_snaptrade_connections(key: str = ""):
    """Lista las CONEXIONES (brokerage authorizations) de SnapTrade con su estado y
    las cuentas de cada una. Sirve para ver si una cuenta recién conectada (ej. la de
    futuros 210EKW34) quedó enganchada o pendiente. Uso: ?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    ukw = await _st_user_kwargs("", register=False)
    st = _snaptrade()
    out = {"conexiones": []}
    try:
        resp = await st.connections.alist_brokerage_authorizations(**ukw)
        auths = _st_plain(resp.body) or []
    except Exception as e:
        raise HTTPException(502, f"authorizations: {str(getattr(e,'body','') or e)[:200]}")
    for a in auths:
        if not isinstance(a, dict):
            continue
        entry = {
            "id": a.get("id"),
            "broker": ((a.get("brokerage") or {}) if isinstance(a.get("brokerage"), dict) else {}).get("name") or a.get("brokerage"),
            "disabled": a.get("disabled"),
            "created": a.get("created_date"),
            "updated": a.get("updated_date"),
            "type": a.get("type"),
            "cuentas": [],
        }
        try:
            r2 = await st.connections.alist_brokerage_authorization_accounts(authorization_id=a.get("id"), **ukw) \
                 if hasattr(st.connections, "alist_brokerage_authorization_accounts") else None
        except Exception:
            r2 = None
        out["conexiones"].append(entry)
    # además, las cuentas planas con su número (para cruzar con 210EKW34)
    try:
        ra = await st.account_information.alist_user_accounts(**ukw)
        out["cuentas_planas"] = [{"name": x.get("name"), "number": x.get("number"),
                                  "institution": x.get("institution_name")}
                                 for x in (_st_plain(ra.body) or []) if isinstance(x, dict)]
    except Exception as e:
        out["cuentas_planas_error"] = str(getattr(e, "body", "") or e)[:160]
    return out


@app.get("/api/admin/diag-snaptrade-activities")
async def diag_snaptrade_activities(key: str = "", days: int = 150):
    """Sondea las 'activities' de SnapTrade para ver la forma de los eventos de
    EXPIRACIÓN/ASIGNACIÓN de opciones (que las órdenes no traen). Resumen de tipos
    + muestras. Uso: /api/admin/diag-snaptrade-activities?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "Clave incorrecta")
    from datetime import date, timedelta
    ukw = await _st_user_kwargs("", register=False)
    st = _snaptrade()
    end = date.today(); start = end - timedelta(days=int(days))
    out = {"tipos": {}, "muestras": {}, "cuentas": 0, "total": 0}
    try:
        resp = await st.account_information.alist_user_accounts(**ukw)
        accts = [str(a.get("id")) for a in (_st_plain(resp.body) or []) if isinstance(a, dict)]
    except Exception as e:
        raise HTTPException(502, f"accounts: {str(getattr(e,'body','') or e)[:200]}")
    out["cuentas"] = len(accts)
    for aid in accts:
        try:
            r = await st.account_information.aget_account_activities(
                account_id=aid, start_date=start, end_date=end, limit=1000, **ukw)
            body = _st_plain(r.body)
            rows = body.get("data") if isinstance(body, dict) else body
            for act in (rows or []):
                if not isinstance(act, dict):
                    continue
                out["total"] += 1
                ty = str(act.get("type") or act.get("activity_type") or "?")
                out["tipos"][ty] = out["tipos"].get(ty, 0) + 1
                # guardar 1 muestra por tipo, priorizando los que huelan a opción/expiración
                if ty not in out["muestras"]:
                    out["muestras"][ty] = {k: act.get(k) for k in
                        ("type","description","amount","units","price","trade_date","settlement_date","symbol","option_symbol","option_type","instrument") if k in act}
        except Exception as e:
            out["muestras"]["_error_"+aid[:6]] = str(getattr(e, "body", "") or e)[:200]
    return out


def _sym_of(d):
    """Extrae un símbolo legible de un universal_symbol/underlying dict."""
    if isinstance(d, dict):
        return d.get("symbol") or d.get("raw_symbol") or d.get("description")
    return d

def _st_map_order(o):
    """Mapea una orden ejecutada de SnapTrade a un fill del journal. Calibrado con
    datos reales: `symbol` es un UUID interno; el ticker legible vive en
    option_symbol.ticker (opciones) o universal_symbol.symbol (acciones/futuros)."""
    if not isinstance(o, dict):
        return None
    status = str(o.get("status") or "").upper()
    if status not in ("EXECUTED", "FILLED", "COMPLETE", "COMPLETED", "PARTIAL", "PARTIALLY_FILLED"):
        return None  # descarta REJECTED/CANCELED/etc.
    opt = o.get("option_symbol") if isinstance(o.get("option_symbol"), dict) else None
    uni = o.get("universal_symbol") if isinstance(o.get("universal_symbol"), dict) else None
    qu  = o.get("quote_universal_symbol") if isinstance(o.get("quote_universal_symbol"), dict) else None
    if opt:
        underlying = _sym_of(opt.get("underlying_symbol"))
        display = opt.get("ticker") or underlying
        instrument = "option"
        option_meta = {"underlying": underlying, "strike": opt.get("strike_price"),
                       "expiry": opt.get("expiration_date"),
                       "type": opt.get("option_type") or opt.get("type")}
    else:
        display = _sym_of(uni or qu or {})
        instrument = "equity"
        option_meta = None
    def num(v):
        try:
            return round(float(v), 6)
        except (TypeError, ValueError):
            return v
    return {
        "broker_order_id": o.get("brokerage_order_id"),
        "symbol": display,
        "instrument": instrument,
        "option": option_meta,
        "side": o.get("action"),
        "qty": num(o.get("filled_quantity") or o.get("total_quantity")),
        "price": num(o.get("execution_price")),
        "order_type": o.get("order_type"),
        "time": o.get("time_executed") or o.get("time_placed"),
        "status": status,
        "account": (o.get("_account") or {}).get("number") or (o.get("_account") or {}).get("name"),
        "source": "snaptrade",
    }


# Redirect que TradeStation ya acepta POR DEFECTO (localhost pre-aprobados) — permite
# probar SIN esperar a que Client Experience registre la URL de Railway. Flujo manual:
# el usuario copia el ?code= de la URL de localhost y lo pega en el journal.
TS_MANUAL_REDIRECT = os.getenv("TS_MANUAL_REDIRECT", "http://localhost:3000").strip()

@app.get("/api/broker/tradestation/connect")
async def tradestation_connect(app_user_id: str = "", manual: int = 0):
    """Inicia el OAuth de TradeStation en modo SOLO LECTURA (scope sin 'Trade').
    manual=1 usa el redirect localhost pre-aprobado (para probar sin registrar la URL
    de Railway); el usuario pega el código a mano en /exchange."""
    if not (TRADESTATION_CLIENT_ID and TRADESTATION_CLIENT_SECRET):
        return {"configured": False,
                "message": "TradeStation aún no activado: faltan TRADESTATION_CLIENT_ID/SECRET en Railway."}
    from urllib.parse import urlencode
    uid = (app_user_id or "dave").strip() or "dave"
    redirect = TS_MANUAL_REDIRECT if manual else TRADESTATION_REDIRECT_URI
    params = {
        "response_type": "code",
        "client_id": TRADESTATION_CLIENT_ID,
        "redirect_uri": redirect,
        "audience": "https://api.tradestation.com",
        "scope": "openid offline_access MarketData ReadAccount",
        "state": f"lbc::{uid}",
    }
    return {"configured": True, "manual": bool(manual), "redirect_uri": redirect,
            "auth_url": "https://signin.tradestation.com/authorize?" + urlencode(params)}


@app.post("/api/broker/tradestation/exchange")
async def tradestation_exchange(request: Request):
    """Flujo MANUAL: recibe el `code` que el usuario copió de la URL de localhost tras
    autorizar, lo canjea por tokens (redirect = TS_MANUAL_REDIRECT) y los guarda."""
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "JSON inválido")
    uid = (data.get("app_user_id") or "dave").strip() or "dave"
    code = (data.get("code") or "").strip()
    # aceptar que pegue la URL completa o solo el code
    if "code=" in code:
        from urllib.parse import urlparse, parse_qs
        code = (parse_qs(urlparse(code).query).get("code") or [code])[0]
    if not code:
        raise HTTPException(400, "Falta el código")
    try:
        tok = await _ts_token_request({"grant_type": "authorization_code", "code": code,
                                       "redirect_uri": TS_MANUAL_REDIRECT})
        _ts_store(uid, tok)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"No se pudo canjear el código: {str(e)[:160]}")
    return {"ok": True}


# ══════════════════════════════════════════════════════════════════════════
#  AUTH del backend (cuentas de estudiantes) — pbkdf2 + JWT HS256, stdlib.
#  Reemplaza el login 100% cliente (localStorage + base64 + admin hardcodeado).
#  Durabilidad: hoy _users va en el snapshot (persist.json). En Railway el disco
#  es efímero -> para producción mover a un store durable (Upstash/Supabase).
#  POR AHORA requiere AUTH_SECRET en Railway para que las sesiones sobrevivan al
#  reinicio (sin él, se usa una clave dev aleatoria por arranque).
# ══════════════════════════════════════════════════════════════════════════
_users = {}  # fallback en memoria/snapshot si NO hay Supabase
AUTH_SECRET = os.getenv("AUTH_SECRET", "").strip() or ("dev-" + secrets.token_hex(32))

# ── STORE DURABLE: Supabase (Postgres via REST). Si SUPABASE_URL+KEY están en Railway,
#    los usuarios y el AUTH_SECRET viven en Supabase (sobreviven redeploys). Si no,
#    fallback al snapshot (efímero en Railway). ──
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
if SUPABASE_URL and not SUPABASE_URL.startswith("http"):
    SUPABASE_URL = "https://" + SUPABASE_URL   # tolera que pongan la URL sin https://
if SUPABASE_URL:
    # tolera que peguen la URL con ruta de más (p.ej. .../rest/v1): dejamos solo el origen
    _m = re.match(r"^(https?://[^/]+)", SUPABASE_URL)
    if _m:
        SUPABASE_URL = _m.group(1)
SUPABASE_KEY = (os.getenv("SUPABASE_SERVICE_KEY", "") or os.getenv("SUPABASE_KEY", "")).strip()
def _sb_on(): return bool(SUPABASE_URL and SUPABASE_KEY)
def _sb_h(extra=None):
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json"}
    if extra: h.update(extra)
    return h
async def _sb_get_user(email):
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(f"{SUPABASE_URL}/rest/v1/app_users",
                            params={"email": f"eq.{email}", "select": "*"}, headers=_sb_h())
        if r.status_code == 200:
            rows = r.json(); return rows[0] if rows else None
    except Exception as e:
        print(f"[supabase] get_user: {e}")
    return None
async def _sb_put_user(u):
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(f"{SUPABASE_URL}/rest/v1/app_users", json=u,
                         headers=_sb_h({"Prefer": "resolution=merge-duplicates"}))
    if r.status_code >= 300:
        raise Exception(f"supabase {r.status_code}: {r.text[:120]}")
async def _sb_get_config(k):
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(f"{SUPABASE_URL}/rest/v1/app_config",
                            params={"k": f"eq.{k}", "select": "v"}, headers=_sb_h())
        if r.status_code == 200:
            rows = r.json(); return rows[0]["v"] if rows else None
    except Exception as e:
        print(f"[supabase] get_config: {e}")
    return None
async def _sb_set_config(k, v):
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            await c.post(f"{SUPABASE_URL}/rest/v1/app_config", json={"k": k, "v": v},
                         headers=_sb_h({"Prefer": "resolution=merge-duplicates"}))
    except Exception as e:
        print(f"[supabase] set_config: {e}")

async def user_get(email):
    if _sb_on():
        return await _sb_get_user(email)
    return _users.get(email)
async def user_put(email, u):
    rec = {**u, "email": email}
    if _sb_on():
        await _sb_put_user(rec)
    else:
        _users[email] = u
        try: save_cache()
        except Exception: pass

async def users_list(plan=None):
    """Lista usuarios (opcionalmente filtrados por plan). Supabase o memoria."""
    if _sb_on():
        try:
            params = {"select": "email,name,plan,created"}
            if plan and plan != "all":
                params["plan"] = f"eq.{plan}"
            async with httpx.AsyncClient(timeout=12) as c:
                r = await c.get(f"{SUPABASE_URL}/rest/v1/app_users", params=params, headers=_sb_h())
            if r.status_code == 200:
                rows = r.json()
                return [x for x in rows if not str(x.get("email", "")).endswith("@example.com")]
        except Exception as e:
            print(f"[users_list] {e}")
        return []
    out = [{"email": k, **v} for k, v in _users.items()]
    if plan and plan != "all":
        out = [x for x in out if (x.get("plan") or "free") == plan]
    return [x for x in out if not str(x.get("email", "")).endswith("@example.com")]

async def _load_auth_secret():
    """Carga/genera el AUTH_SECRET de forma DURABLE en Supabase (así no depende de una
    env var de Railway). env AUTH_SECRET tiene prioridad si existe."""
    global AUTH_SECRET
    if os.getenv("AUTH_SECRET", "").strip():
        return  # env manda
    if _sb_on():
        s = await _sb_get_config("auth_secret")
        if not s:
            s = secrets.token_hex(32)
            await _sb_set_config("auth_secret", s)
            print("[auth] AUTH_SECRET generado y guardado en Supabase")
        AUTH_SECRET = s

def _b64u(b): return base64.urlsafe_b64encode(b).rstrip(b"=").decode()
def _b64u_dec(s): return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))
def _hash_pw(pw, salt): return _b64u(hashlib.pbkdf2_hmac("sha256", (pw or "").encode(), salt, 200_000))

def _make_jwt(payload, days=30):
    hdr = {"alg": "HS256", "typ": "JWT"}
    p = dict(payload)
    now = int(datetime.now(timezone.utc).timestamp())
    p["iat"] = now; p["exp"] = now + days * 86400
    seg = _b64u(json.dumps(hdr, separators=(",", ":")).encode()) + "." + _b64u(json.dumps(p, separators=(",", ":")).encode())
    sig = _b64u(hmac.new(AUTH_SECRET.encode(), seg.encode(), hashlib.sha256).digest())
    return seg + "." + sig

def _verify_jwt(token):
    try:
        parts = (token or "").split(".")
        if len(parts) != 3:
            return None
        seg = parts[0] + "." + parts[1]
        exp_sig = _b64u(hmac.new(AUTH_SECRET.encode(), seg.encode(), hashlib.sha256).digest())
        if not hmac.compare_digest(exp_sig, parts[2]):
            return None
        payload = json.loads(_b64u_dec(parts[1]))
        if int(payload.get("exp", 0)) < int(datetime.now(timezone.utc).timestamp()):
            return None
        return payload
    except Exception:
        return None

def _pub_user(email):
    u = _users.get(email, {})
    return {"id": u.get("id"), "email": email, "name": u.get("name"), "plan": u.get("plan", "free")}

_HEALTH_FEEDS_CACHE = {"ts": 0.0, "data": None}
_HEALTH_FEEDS_TTL = 60  # segundos: evita que múltiples requests públicos disparen sondas repetidas

@app.get("/api/health/feeds")
async def health_feeds():
    """Salud de los feeds macro (calendario + noticias) SIN exponer secretos.
    Sonda en vivo ForexFactory y Finnhub para ver quién está caído en producción.
    Solo devuelve booleanos de presencia de clave y estados/conteos — nunca valores.
    Público a propósito (diagnóstico), pero cacheado 60s para no quemar cuota."""
    _now = time.time()
    if _HEALTH_FEEDS_CACHE["data"] is not None and (_now - _HEALTH_FEEDS_CACHE["ts"]) < _HEALTH_FEEDS_TTL:
        cached = dict(_HEALTH_FEEDS_CACHE["data"])
        cached["cached"] = True
        cached["cache_age_s"] = round(_now - _HEALTH_FEEDS_CACHE["ts"], 1)
        return cached
    out = {
        "keys_present": {
            "finnhub": bool(FINNHUB_KEY),
            "fmp": bool(FMP_KEY),
            "rapidapi": bool(RAPIDAPI_KEY),
        },
        "cache": {
            "calendar_n": len(cache["calendar"]["data"]),
            "calendar_status": cache["calendar"].get("status"),
            "calendar_with_actual": sum(1 for e in cache["calendar"]["data"] if e.get("actual")),
            "movers_n": len(cache["movers"]["data"]),
            "movers_status": cache["movers"].get("status"),
        },
        "probes": {},
    }
    # Sonda ForexFactory (fuente gratis del calendario) — ¿bloquea la IP de Railway?
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json",
                            headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        ct = r.headers.get("content-type", "")
        is_json = "json" in ct.lower()
        n = len(r.json()) if is_json else 0
        out["probes"]["forexfactory"] = {"status": r.status_code, "is_json": is_json,
                                          "events": n, "blocked": (not is_json)}
    except Exception as e:
        out["probes"]["forexfactory"] = {"error": str(e)[:120]}
    # Sonda Finnhub /news (fuente de movers)
    if FINNHUB_KEY:
        try:
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get(f"{FH_BASE}/news", params={"category": "general", "token": FINNHUB_KEY})
            n = len(r.json()) if r.status_code == 200 else 0
            out["probes"]["finnhub_news"] = {"status": r.status_code, "items": n}
        except Exception as e:
            out["probes"]["finnhub_news"] = {"error": str(e)[:120]}
        # Sonda Finnhub calendario económico (el que trae 'actual')
        try:
            now_et = datetime.now(NY)
            frm = (now_et - timedelta(days=2)).strftime("%Y-%m-%d")
            to = (now_et + timedelta(days=7)).strftime("%Y-%m-%d")
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get(f"{FH_BASE}/calendar/economic",
                                params={"from": frm, "to": to, "token": FINNHUB_KEY})
            body = r.json() if r.status_code == 200 else {}
            evs = body.get("economicCalendar", []) if isinstance(body, dict) else []
            with_actual = sum(1 for e in evs if e.get("actual") not in (None, "", 0))
            out["probes"]["finnhub_calendar"] = {"status": r.status_code, "events": len(evs),
                                                 "with_actual": with_actual}
        except Exception as e:
            out["probes"]["finnhub_calendar"] = {"error": str(e)[:120]}
    else:
        out["probes"]["finnhub_news"] = {"note": "sin FINNHUB_KEY"}
        out["probes"]["finnhub_calendar"] = {"note": "sin FINNHUB_KEY"}
    # Sonda TradingEconomics vía RapidAPI (nuestra fuente RÁPIDA de 'actual', free 100/día)
    if RAPIDAPI_KEY:
        try:
            async with httpx.AsyncClient(follow_redirects=True) as c:
                items = await _fetch_rapidapi_actuals(c)
            with_actual = sum(1 for e in (items or []) if e.get("actual"))
            out["probes"]["rapidapi_tradingeconomics"] = {"ok": True, "events": len(items or []),
                                                          "with_actual": with_actual}
        except Exception as e:
            out["probes"]["rapidapi_tradingeconomics"] = {"error": str(e)[:150]}
    else:
        out["probes"]["rapidapi_tradingeconomics"] = {"note": "sin RAPIDAPI_KEY"}
    # Sonda FMP economic calendar (premium: 402/403 en free) — para saber si vale la pena
    if FMP_KEY:
        try:
            now_et = datetime.now(NY)
            frm = now_et.strftime("%Y-%m-%d")
            to = (now_et + timedelta(days=2)).strftime("%Y-%m-%d")
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get(f"{FMP_BASE}/economic-calendar",
                                params={"from": frm, "to": to, "apikey": FMP_KEY})
            out["probes"]["fmp_calendar"] = {"status": r.status_code,
                                             "premium_locked": r.status_code in (401, 402, 403)}
        except Exception as e:
            out["probes"]["fmp_calendar"] = {"error": str(e)[:120]}
    # Sonda BLS (gratis, sin key) — última capa del relleno de 'actual'
    try:
        arrs = await _bls_fetch(["CUSR0000SA0", "LNS14000000", "CES0000000001"])
        out["probes"]["bls"] = {
            "ok": bool(arrs),
            "cpi_mom": _bls_fmt(arrs.get("CUSR0000SA0"), "mom"),
            "unemployment": _bls_fmt(arrs.get("LNS14000000"), "value"),
            "nfp_chg": _bls_fmt(arrs.get("CES0000000001"), "chg_k"),
            "key": bool(BLS_API_KEY),
        }
    except Exception as e:
        out["probes"]["bls"] = {"error": str(e)[:120]}
    # Sonda FRED en vivo (verifica que la key autentica de verdad)
    if FRED_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.get("https://api.stlouisfed.org/fred/series/observations",
                                params={"series_id": "UNRATE", "api_key": FRED_API_KEY,
                                        "file_type": "json", "sort_order": "desc", "limit": 1})
            body = r.json() if r.status_code == 200 else {}
            obs = body.get("observations", []) if isinstance(body, dict) else []
            out["probes"]["fred"] = {"enabled": True, "status": r.status_code,
                                     "ok": (r.status_code == 200 and bool(obs)),
                                     "sample_unrate": (obs[0].get("value") if obs else None)}
        except Exception as e:
            out["probes"]["fred"] = {"enabled": True, "error": str(e)[:120]}
    else:
        out["probes"]["fred"] = {"enabled": False}
    out["cached"] = False
    _HEALTH_FEEDS_CACHE["ts"] = time.time()
    _HEALTH_FEEDS_CACHE["data"] = out
    return out

@app.get("/api/auth/health")
async def auth_health():
    """Estado del store de auth (sin exponer secretos). Confirma si Supabase conectó."""
    reachable = False
    detail = ""
    if _sb_on():
        try:
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get(f"{SUPABASE_URL}/rest/v1/app_config",
                                params={"select": "k", "limit": "1"}, headers=_sb_h())
            reachable = (r.status_code == 200)
            if not reachable:
                detail = f"http {r.status_code}: {r.text[:100]}"
        except Exception as e:
            detail = str(e)[:100]
    return {"store": "supabase" if _sb_on() else "snapshot",
            "supabase_configured": _sb_on(), "supabase_reachable": reachable,
            "auth_secret_durable": bool(_sb_on()) or bool(os.getenv("AUTH_SECRET", "").strip()),
            "detail": detail}


# ── Correos transaccionales (segmentados por plan free/premium) ──────────────
EMAIL_ON = bool(GMAIL_USER and GMAIL_APP_PASSWORD)
DISCORD_FREE_INVITE = os.getenv("DISCORD_FREE_INVITE", "https://discord.gg/rBDXT5uDH").strip()   # Discord gratis
WHOP_HUB_URL = os.getenv("WHOP_HUB_URL", "https://whop.com/dave-liberato-group/live-day-trading-52/").strip()
# Base de la web (para links en correos). Cambiar a https://liberatocommunity.com
# cuando el dominio quede apuntando a GitHub Pages.
SITE_URL = os.getenv("SITE_URL", "https://davel1berat0.github.io/Liberato-Backend").strip().rstrip("/")
# Railway BLOQUEA los puertos SMTP salientes (465/587) → "Network is unreachable".
# Por eso el correo se envía por la API HTTP de Brevo (puerto 443). SMTP queda de
# fallback (funciona en local u otros hosts que lo permitan).
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "").strip()
BREVO_API_KEY = os.getenv("BREVO_API_KEY", "").strip()
MAIL_FROM = (os.getenv("MAIL_FROM") or GMAIL_USER or "").strip()
# Resend exige un dominio VERIFICADO como remitente (no acepta @gmail.com). Si
# MAIL_FROM es un gmail o no está, usa el dominio de pruebas onboarding@resend.dev
# (que SOLO puede enviar al correo dueño de la cuenta Resend hasta verificar dominio).
def _resend_from():
    f = MAIL_FROM
    if not f or f.lower().endswith("@gmail.com"):
        return "onboarding@resend.dev"
    return f
EMAIL_READY = bool(RESEND_API_KEY) or bool(BREVO_API_KEY and MAIL_FROM) or EMAIL_ON

def _email_shell(titulo, cuerpo_html, cta_text=None, cta_url=None):
    cta = ""
    if cta_text and cta_url:
        cta = (f'<a href="{cta_url}" style="display:inline-block;margin-top:22px;background:#CCA94F;'
               f'color:#05060C;text-decoration:none;font-weight:700;padding:13px 26px;border-radius:10px;'
               f'font-family:Arial,sans-serif;font-size:15px;">{cta_text}</a>')
    return (f'<div style="background:#0B0B12;padding:32px 0;font-family:Arial,Helvetica,sans-serif;">'
            f'<div style="max-width:520px;margin:0 auto;background:#12121C;border:1px solid rgba(204,169,79,0.25);'
            f'border-radius:16px;padding:32px;">'
            f'<div style="font-family:Georgia,serif;font-size:24px;color:#CCA94F;margin-bottom:6px;">Liberato Community</div>'
            f'<h1 style="color:#F2EFE8;font-size:20px;margin:14px 0;">{titulo}</h1>'
            f'<div style="color:#C9C6BE;font-size:15px;line-height:1.6;">{cuerpo_html}</div>{cta}'
            f'<div style="margin-top:28px;border-top:1px solid rgba(255,255,255,0.08);padding-top:16px;'
            f'color:#7A7870;font-size:12px;">Recibiste este correo porque creaste una cuenta en Liberato Community.</div>'
            f'</div></div>')

async def _send_email(to, subject, html, reply_to=None):
    """Envía un correo transaccional (no bloqueante). reply_to = correo al que
    responderá el destinatario (ej. quien escribió el contacto)."""
    if not to:
        return False
    # 1) Resend por HTTP (puerto 443 → funciona en Railway)
    if RESEND_API_KEY:
        try:
            _payload = {"from": f"Liberato Community <{_resend_from()}>",
                        "to": [to], "subject": subject, "html": html}
            if reply_to:
                _payload["reply_to"] = reply_to
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.post("https://api.resend.com/emails",
                    headers={"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"},
                    json=_payload)
            if r.status_code in (200, 201):
                print(f"[email] ✓ (resend) {subject} → {to}")
                return True
            print(f"[email] ✗ resend {r.status_code}: {r.text[:200]}")
            return False
        except Exception as e:
            print(f"[email] ✗ resend: {e}")
            return False
    # 2) Brevo por HTTP (puerto 443 → funciona en Railway)
    if BREVO_API_KEY and MAIL_FROM:
        try:
            _bp = {"sender": {"name": "Liberato Community", "email": MAIL_FROM},
                   "to": [{"email": to}], "subject": subject, "htmlContent": html}
            if reply_to:
                _bp["replyTo"] = {"email": reply_to}
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.post("https://api.brevo.com/v3/smtp/email",
                    headers={"api-key": BREVO_API_KEY, "Content-Type": "application/json",
                             "accept": "application/json"},
                    json=_bp)
            if r.status_code in (200, 201):
                print(f"[email] ✓ (brevo) {subject} → {to}")
                return True
            print(f"[email] ✗ brevo {r.status_code}: {r.text[:180]}")
            return False
        except Exception as e:
            print(f"[email] ✗ brevo: {e}")
            return False
    # 2) SMTP de fallback
    if not EMAIL_ON:
        return False
    def _send():
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"Liberato Community <{GMAIL_USER}>"
        msg["To"] = to
        if reply_to:
            msg["Reply-To"] = reply_to
        msg.attach(MIMEText(html, "html"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=15) as s:
            s.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            s.sendmail(GMAIL_USER, to, msg.as_string())
    try:
        await asyncio.to_thread(_send)
        print(f"[email] ✓ {subject} → {to}")
        return True
    except Exception as e:
        print(f"[email] ✗ {to}: {e}")
        return False

async def send_welcome_free(email, name):
    body = (f"<p>Hola, {name or ''}.</p>"
            f"<p>Tu cuenta en <b>Liberato Community</b> ha sido <b>confirmada</b>. Ya puedes entrar a "
            f"nuestro Discord y disfrutar de <b>Daily Bias</b>, noticias de alto impacto en vivo, "
            f"resultados de nuestros estudiantes, contenido educativo gratuito y mucho más.</p>"
            f"<p>¡Bienvenido a Liberato Community! 🚀</p>")
    cta_t, cta_u = ("Entrar al Discord gratuito →", DISCORD_FREE_INVITE) if DISCORD_FREE_INVITE else (None, None)
    return await _send_email(email, "Bienvenido a Liberato Community ✅",
                             _email_shell("Tu cuenta está confirmada", body, cta_t, cta_u))

async def send_welcome_premium(email, name):
    body = (f"<p>Hola, {name or ''}.</p>"
            f"<p>¡Bienvenido al <b>acceso completo</b> de Liberato Community! Con tu membresía ya tienes el "
            f"<b>Dashboard Institucional</b>, niveles de <b>GEX</b>, Earnings, noticias de alto impacto en vivo "
            f"y todo el contenido premium.</p>"
            f"<p>Entra a la plataforma con el botón de abajo (inicia sesión con este mismo correo) y accederás "
            f"directo al dashboard con todo abierto.</p>"
            f"<p>Y no te pierdas los <b>livestreams en vivo</b> desde Whop: "
            f"<a href='{WHOP_HUB_URL}' style='color:#CCA94F;'>ir al Livestream →</a></p>"
            f"<p>¡Nos vemos adentro! 🚀</p>")
    return await _send_email(email, "Acceso completo activado — Liberato Community ⭐",
                             _email_shell("Tu acceso premium está activo", body,
                                          "Entrar al Dashboard →", f"{SITE_URL}/auth.html"))

async def send_verify_code(email, name, code):
    body = (f"<p>Hola, {name or ''}.</p>"
            f"<p>Para activar tu cuenta en <b>Liberato Community</b>, ingresa este código de verificación:</p>"
            f"<div style='margin:18px 0;text-align:center;'>"
            f"<span style='display:inline-block;font-family:monospace;font-size:34px;font-weight:800;"
            f"letter-spacing:10px;color:#E7CC74;background:#0B0B12;border:1px solid rgba(204,169,79,0.3);"
            f"border-radius:12px;padding:14px 22px;'>{code}</span></div>"
            f"<p style='color:#9a9aa2;font-size:13px;'>El código vence en 10 minutos. Si no creaste esta "
            f"cuenta, ignora este correo.</p>")
    return await _send_email(email, "Tu código de verificación · Liberato Community",
                             _email_shell("Verifica tu correo", body))

async def send_reset_code(email, name, code):
    body = (f"<p>Hola, {name or ''}.</p>"
            f"<p>Recibimos una solicitud para <b>restablecer tu contraseña</b> en Liberato Community. "
            f"Usa este código:</p>"
            f"<div style='margin:18px 0;text-align:center;'>"
            f"<span style='display:inline-block;font-family:monospace;font-size:34px;font-weight:800;"
            f"letter-spacing:10px;color:#E7CC74;background:#0B0B12;border:1px solid rgba(204,169,79,0.3);"
            f"border-radius:12px;padding:14px 22px;'>{code}</span></div>"
            f"<p style='color:#9a9aa2;font-size:13px;'>Vence en 10 minutos. Si no lo pediste, ignora este "
            f"correo — tu contraseña no cambia.</p>")
    return await _send_email(email, "Restablecer contraseña · Liberato Community",
                             _email_shell("Restablecer contraseña", body))

@app.get("/api/email/health")
async def email_health():
    """Estado de correos e integraciones (solo booleanos, sin exponer secretos)."""
    _g = globals()
    return {"email_configured": EMAIL_READY,
            "email_via": ("resend" if RESEND_API_KEY else ("brevo" if BREVO_API_KEY and MAIL_FROM else ("smtp" if EMAIL_ON else None))),
            "resend_set": bool(RESEND_API_KEY), "resend_from": _resend_from() if RESEND_API_KEY else None,
            "brevo_set": bool(BREVO_API_KEY), "mail_from_set": bool(MAIL_FROM),
            "discord_free_link_set": bool(DISCORD_FREE_INVITE),
            "whop_hub": WHOP_HUB_URL, "sender": GMAIL_USER[:3] + "…" if GMAIL_USER else None,
            "discord_webhook_set": bool(_g.get("DISCORD_WEBHOOK_URL")),
            "discord_premium_webhook_set": bool(_g.get("DISCORD_PREMIUM_WEBHOOK_URL")),
            "whop_secret_set": bool(_g.get("WHOP_WEBHOOK_SECRET")),
            "admin_key_set": not str(_g.get("ADMIN_KEY", "")).startswith("disabled-")}

# ═══════════════════════════════════════════════════════════════════════════
#  CRM: usuarios segmentados (free/premium) + correos por grupo + brief diario
#  a Discord (free = sin GEX) y Discord premium/Whop (premium = con GEX).
#  Endpoints admin con ?key=ADMIN_KEY.
# ═══════════════════════════════════════════════════════════════════════════
# Discord = UN solo canal, el GRATUITO. El brief free (SIN niveles GEX) va aquí.
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
# Canal PREMIUM (Whop) = se automatiza MÁS ADELANTE (con niveles GEX). Queda listo:
# si algún día se pone un webhook premium/Whop-bridge, el brief con GEX se publica ahí.
DISCORD_PREMIUM_WEBHOOK_URL = os.getenv("DISCORD_PREMIUM_WEBHOOK_URL", "").strip()

def _is_premium(u):
    return (u.get("plan") or "free") in ("premium", "pro", "admin")

@app.get("/api/admin/users")
async def admin_users(key: str = "", authorization: str = Header("")):
    """Lista de usuarios segmentada por plan (free vs premium)."""
    if not _is_admin(key, authorization):
        raise HTTPException(403, "acceso denegado")
    users = await users_list("all")
    free = [u for u in users if not _is_premium(u)]
    premium = [u for u in users if _is_premium(u)]
    return {"counts": {"total": len(users), "free": len(free), "premium": len(premium)},
            "free": free, "premium": premium}

@app.post("/api/admin/email/broadcast")
async def admin_email_broadcast(request: Request, key: str = "", authorization: str = Header("")):
    """Envía un correo a un grupo (free | premium | all). Throttle anti-spam.
    OJO: Gmail limita ~500/día; para listas grandes usar un ESP (MailerLite/Brevo)."""
    if not _is_admin(key, authorization):
        raise HTTPException(403, "acceso denegado")
    if not EMAIL_READY:
        raise HTTPException(400, "Correo no configurado (pon BREVO_API_KEY + MAIL_FROM)")
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "JSON inválido")
    segment = (data.get("segment") or "free").strip().lower()
    subject = (data.get("subject") or "").strip()
    html = (data.get("html") or data.get("body") or "").strip()
    if not subject or not html:
        raise HTTPException(400, "Faltan 'subject' y 'html'")
    users = await users_list("all")
    if segment == "free":
        users = [u for u in users if not _is_premium(u)]
    elif segment in ("premium", "pro"):
        users = [u for u in users if _is_premium(u)]
    wrapped = _email_shell(subject, html)
    sent, failed = 0, 0
    for u in users[:500]:
        ok = await _send_email(u.get("email"), subject, wrapped)
        sent += 1 if ok else 0
        failed += 0 if ok else 1
        await asyncio.sleep(1.1)   # throttle
    return {"segment": segment, "recipients": len(users), "sent": sent, "failed": failed}

# ── Brief diario a Discord ──────────────────────────────────────────────────
async def _discord_post(url, content):
    if not url or not content:
        return False
    try:
        async with httpx.AsyncClient(timeout=12) as c:
            r = await c.post(url, json={"content": content[:1950]})
        return r.status_code in (200, 204)
    except Exception as e:
        print(f"[discord] {e}"); return False

def _daily_brief_text(include_gex):
    """Construye el mensaje diario desde el contexto institucional cacheado.
    Para el grupo FREE se OMITE la línea Técnico (los niveles GEX). El grupo
    PREMIUM la incluye."""
    txt = (cache.get("institutional", {}) or {}).get("text") or ""
    if not txt:
        return None
    lines = [l for l in txt.split("\n") if l.strip()]
    if not include_gex:
        lines = [l for l in lines if not l.strip().lower().startswith("**técnico")]
    fecha = datetime.now(NY).strftime("%d-%b-%Y")
    header = ("📊 **Contexto del Mercado — NQ**  ·  " + fecha +
              ("  ·  ⭐ Premium" if include_gex else ""))
    footer = ("\n\n_— Liberato Community" +
              (" · Acceso completo (livestreams + niveles GEX)_" if include_gex
               else " · Comunidad libre — sube a Premium para niveles GEX y livestreams_"))
    return header + "\n\n" + "\n".join(lines) + footer

async def send_daily_briefs():
    """Publica el brief diario: canal FREE (sin GEX) y canal PREMIUM (con GEX)."""
    free_txt = _daily_brief_text(False)
    prem_txt = _daily_brief_text(True)
    out = {}
    if DISCORD_WEBHOOK_URL and free_txt:
        out["free"] = await _discord_post(DISCORD_WEBHOOK_URL, free_txt)
    if DISCORD_PREMIUM_WEBHOOK_URL and prem_txt:
        out["premium"] = await _discord_post(DISCORD_PREMIUM_WEBHOOK_URL, prem_txt)
    print(f"[daily-brief] {out}")
    return out

@app.get("/api/admin/daily-brief/preview")
async def admin_daily_preview(key: str = "", authorization: str = Header("")):
    """Previsualiza los dos mensajes (free sin GEX / premium con GEX) sin enviar."""
    if not _is_admin(key, authorization):
        raise HTTPException(403, "acceso denegado")
    return {"free": _daily_brief_text(False), "premium": _daily_brief_text(True),
            "discord_free_configured": bool(DISCORD_WEBHOOK_URL),
            "discord_premium_configured": bool(DISCORD_PREMIUM_WEBHOOK_URL)}

@app.post("/api/admin/daily-brief/send")
async def admin_daily_send(key: str = "", authorization: str = Header("")):
    """Dispara el envío del brief diario AHORA (prueba manual)."""
    if not _is_admin(key, authorization):
        raise HTTPException(403, "acceso denegado")
    return await send_daily_briefs()

# ── Verificación de correo por código (evita cuentas falsas/bots) ────────────
# Activa cuando podemos enviar correo (EMAIL_READY). El registro NO crea la
# cuenta hasta que el usuario ingresa el código; el "pendiente" vive en config.
AUTH_VERIFY = os.getenv("AUTH_VERIFY", "true").lower() != "false"
_pending_reg = {}   # fallback en memoria si no hay Supabase
_avatars = {}       # fallback en memoria para fotos de perfil (si no hay Supabase)

async def _avatar_get(email):
    if _sb_on():
        return await _sb_get_config(f"avatar::{email}")
    return _avatars.get(email)

async def _avatar_set(email, val):
    if _sb_on():
        await _sb_set_config(f"avatar::{email}", val)
    else:
        _avatars[email] = val

def _gen_code():
    return f"{secrets.randbelow(900000) + 100000:06d}"   # 6 dígitos

async def _pending_get(email):
    if _sb_on():
        return await _sb_get_config(f"pending_reg::{email}")
    return _pending_reg.get(email)

async def _pending_set(email, val):
    if _sb_on():
        await _sb_set_config(f"pending_reg::{email}", val)
    else:
        _pending_reg[email] = val

async def _plan_from_whop_pending(email):
    plan0 = "free"
    if _sb_on():
        pend = await _sb_get_config(f"whop_plan::{email}")
        if pend in ("premium", "pro"):
            plan0 = pend
    return plan0

@app.post("/api/auth/register")
async def auth_register(request: Request):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "JSON inválido")
    email = (data.get("email") or "").strip().lower()
    name = (data.get("name") or "").strip()[:80]
    pw = data.get("password") or ""
    if not email or "@" not in email or "." not in email.split("@")[-1]:
        raise HTTPException(400, "Email inválido")
    if len(pw) < 8:
        raise HTTPException(400, "La contraseña debe tener al menos 8 caracteres")
    if await user_get(email):
        raise HTTPException(409, "Ya existe una cuenta con ese email")
    salt = secrets.token_bytes(16)
    uid = "u_" + secrets.token_hex(9)
    plan0 = await _plan_from_whop_pending(email)
    lang = (data.get("language") or "").strip().lower()
    rec = {"id": uid, "name": name or email.split("@")[0],
           "salt": base64.b64encode(salt).decode(), "pass_hash": _hash_pw(pw, salt),
           "plan": plan0, "created": int(time.time())}
    if lang in ("es", "en"):
        rec["language"] = lang

    # ── Con verificación por correo activa: NO se crea la cuenta todavía ──
    if AUTH_VERIFY and EMAIL_READY:
        code = _gen_code()
        pend = dict(rec)
        pend.update({"code": code, "expires": int(time.time()) + 10 * 60, "email": email})
        await _pending_set(email, pend)
        try:
            asyncio.create_task(send_verify_code(email, rec["name"], code))
        except Exception:
            pass
        return {"ok": True, "needs_verification": True, "email": email}

    # ── Sin correo configurado: auto-verifica (comportamiento anterior) ──
    await user_put(email, rec)
    try:
        if plan0 in ("premium", "pro"):
            asyncio.create_task(send_welcome_premium(email, rec["name"]))
        else:
            asyncio.create_task(send_welcome_free(email, rec["name"]))
    except Exception:
        pass
    token = _make_jwt({"sub": uid, "email": email, "plan": plan0, "name": rec["name"]})
    return {"ok": True, "token": token, "user": {"id": uid, "email": email, "name": rec["name"], "plan": plan0}}

@app.post("/api/auth/verify")
async def auth_verify(request: Request):
    """Valida el código de 6 dígitos y crea la cuenta (devuelve token)."""
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "JSON inválido")
    email = (data.get("email") or "").strip().lower()
    code = (str(data.get("code") or "")).strip()
    if await user_get(email):
        raise HTTPException(409, "Esta cuenta ya está verificada. Inicia sesión.")
    pend = await _pending_get(email)
    if not pend or not isinstance(pend, dict) or not pend.get("code"):
        raise HTTPException(400, "No hay un registro pendiente para ese correo. Regístrate de nuevo.")
    if int(pend.get("expires", 0)) < int(time.time()):
        raise HTTPException(400, "El código venció. Pide uno nuevo.")
    if not hmac.compare_digest(str(pend.get("code")), code):
        raise HTTPException(400, "Código incorrecto")
    # crear la cuenta real (verificada por construcción)
    plan0 = pend.get("plan", "free")
    rec = {k: pend[k] for k in ("id", "name", "salt", "pass_hash", "plan", "created") if k in pend}
    if pend.get("language"):
        rec["language"] = pend["language"]
    await user_put(email, rec)
    await _pending_set(email, {"used": True})   # invalida el pendiente
    try:
        if plan0 in ("premium", "pro"):
            asyncio.create_task(send_welcome_premium(email, rec.get("name")))
        else:
            asyncio.create_task(send_welcome_free(email, rec.get("name")))
    except Exception:
        pass
    token = _make_jwt({"sub": rec["id"], "email": email, "plan": plan0, "name": rec.get("name")})
    return {"ok": True, "token": token,
            "user": {"id": rec["id"], "email": email, "name": rec.get("name"), "plan": plan0}}

@app.post("/api/auth/resend-code")
async def auth_resend_code(request: Request):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "JSON inválido")
    email = (data.get("email") or "").strip().lower()
    pend = await _pending_get(email)
    if not pend or not isinstance(pend, dict) or pend.get("used"):
        raise HTTPException(400, "No hay un registro pendiente para ese correo.")
    code = _gen_code()
    pend["code"] = code
    pend["expires"] = int(time.time()) + 10 * 60
    await _pending_set(email, pend)
    try:
        asyncio.create_task(send_verify_code(email, pend.get("name"), code))
    except Exception:
        pass
    return {"ok": True, "resent": True, "email": email}

# ── Olvidé mi contraseña (código por correo) ─────────────────────────────────
_reset_codes = {}
async def _reset_get(email):
    if _sb_on():
        return await _sb_get_config(f"reset::{email}")
    return _reset_codes.get(email)
async def _reset_set(email, val):
    if _sb_on():
        await _sb_set_config(f"reset::{email}", val)
    else:
        _reset_codes[email] = val

@app.post("/api/auth/forgot-password")
async def auth_forgot(request: Request):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "JSON inválido")
    email = (data.get("email") or "").strip().lower()
    u = await user_get(email)
    if u and EMAIL_READY:
        code = _gen_code()
        await _reset_set(email, {"code": code, "expires": int(time.time()) + 10 * 60})
        try:
            asyncio.create_task(send_reset_code(email, u.get("name"), code))
        except Exception:
            pass
    # Siempre ok (no revelamos si el correo existe o no).
    return {"ok": True}

@app.post("/api/auth/reset-password")
async def auth_reset(request: Request):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "JSON inválido")
    email = (data.get("email") or "").strip().lower()
    code = str(data.get("code") or "").strip()
    npw = data.get("new_password") or data.get("password") or ""
    if len(npw) < 8:
        raise HTTPException(400, "La contraseña debe tener al menos 8 caracteres")
    r = await _reset_get(email)
    if not r or not isinstance(r, dict) or not r.get("code"):
        raise HTTPException(400, "No hay una solicitud de restablecimiento para ese correo.")
    if int(r.get("expires", 0)) < int(time.time()):
        raise HTTPException(400, "El código venció. Pide uno nuevo.")
    if not hmac.compare_digest(str(r.get("code")), code):
        raise HTTPException(400, "Código incorrecto")
    u = await user_get(email)
    if not u:
        raise HTTPException(404, "Cuenta no encontrada")
    salt = secrets.token_bytes(16)
    u["salt"] = base64.b64encode(salt).decode()
    u["pass_hash"] = _hash_pw(npw, salt)
    await user_put(email, u)
    await _reset_set(email, {"used": True})
    return {"ok": True}

@app.post("/api/admin/preview-email")
async def admin_preview_email(request: Request):
    """Envía un template de EJEMPLO — SOLO a un correo de ADMIN_EMAILS (no se puede
    usar para spam a terceros). tpl: free | premium | verify."""
    try:
        data = await request.json()
    except Exception:
        data = {}
    to = (data.get("to") or "").strip().lower()
    tpl = (data.get("tpl") or "free").strip().lower()
    if to not in ADMIN_EMAILS:
        raise HTTPException(403, "Solo se puede enviar a un correo de administrador")
    if tpl == "premium":
        ok = await send_welcome_premium(to, "Dave")
    elif tpl == "verify":
        ok = await send_verify_code(to, "Dave", _gen_code())
    else:
        ok = await send_welcome_free(to, "Dave")
    return {"ok": ok, "tpl": tpl, "to": to}

@app.post("/api/auth/login")
async def auth_login(request: Request):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "JSON inválido")
    email = (data.get("email") or "").strip().lower()
    pw = data.get("password") or ""
    u = await user_get(email)
    if not u:
        # ¿registro pendiente sin verificar? damos una pista clara
        pend = await _pending_get(email)
        if pend and isinstance(pend, dict) and pend.get("code") and not pend.get("used"):
            raise HTTPException(403, "Verifica tu correo: te enviamos un código para activar la cuenta.")
        raise HTTPException(401, "Email o contraseña incorrectos")
    try:
        salt = base64.b64decode(u["salt"])
    except Exception:
        raise HTTPException(500, "cuenta corrupta")
    if not hmac.compare_digest(_hash_pw(pw, salt), u.get("pass_hash", "")):
        raise HTTPException(401, "Email o contraseña incorrectos")
    token = _make_jwt({"sub": u["id"], "email": email, "plan": u.get("plan", "free"), "name": u.get("name")})
    return {"ok": True, "token": token,
            "user": {"id": u["id"], "email": email, "name": u.get("name"), "plan": u.get("plan", "free")}}

@app.get("/api/auth/me")
async def auth_me(authorization: str = Header("")):
    token = (authorization or "").replace("Bearer ", "").strip()
    p = _verify_jwt(token)
    if not p:
        raise HTTPException(401, "Sesión inválida o expirada")
    email = (p.get("email") or "").lower()
    u = await user_get(email) or {}
    uid = u.get("id") or p.get("sub")
    name = u.get("name") or p.get("name")
    plan = u.get("plan") or p.get("plan", "free")
    # Los correos admin (ADMIN_EMAILS) pasan como premium para no toparse con el paywall.
    is_premium = plan in ("premium", "pro", "admin") or email in ADMIN_EMAILS
    avatar = await _avatar_get(email)
    # Devolvemos tanto la forma anidada (user{}) como campos planos, para que
    # tanto la homepage como auth.html (que leen distinto) funcionen igual.
    return {"ok": True,
            "user": {"id": uid, "email": email, "name": name, "plan": plan, "avatar": avatar},
            "id": uid, "email": email, "name": name, "plan": plan, "avatar": avatar,
            "is_premium": is_premium, "language": u.get("language", "es"),
            "plan_expires": u.get("plan_expires")}

# ── Perfil: cambiar contraseña / idioma / email ─────────────────────────────
async def _require_user(authorization):
    token = (authorization or "").replace("Bearer ", "").strip()
    p = _verify_jwt(token)
    if not p:
        raise HTTPException(401, "Sesión inválida o expirada")
    email = (p.get("email") or "").lower()
    u = await user_get(email)
    if not u:
        raise HTTPException(404, "Cuenta no encontrada")
    return email, u

async def _del_user(email):
    if _sb_on():
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                await c.delete(f"{SUPABASE_URL}/rest/v1/app_users",
                               params={"email": f"eq.{email}"}, headers=_sb_h())
        except Exception as e:
            print(f"[del_user] {e}")
    else:
        _users.pop(email, None)

@app.post("/api/auth/change-password")
async def auth_change_password(request: Request, authorization: str = Header("")):
    email, u = await _require_user(authorization)
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "JSON inválido")
    cur = data.get("current_password") or ""
    npw = data.get("new_password") or ""
    if len(npw) < 8:
        raise HTTPException(400, "La nueva contraseña debe tener al menos 8 caracteres")
    try:
        salt = base64.b64decode(u["salt"])
    except Exception:
        raise HTTPException(500, "cuenta corrupta")
    if not hmac.compare_digest(_hash_pw(cur, salt), u.get("pass_hash", "")):
        raise HTTPException(401, "La contraseña actual es incorrecta")
    nsalt = secrets.token_bytes(16)
    u["salt"] = base64.b64encode(nsalt).decode()
    u["pass_hash"] = _hash_pw(npw, nsalt)
    await user_put(email, u)
    return {"ok": True}

@app.post("/api/auth/set-language")
async def auth_set_language(request: Request, authorization: str = Header("")):
    email, u = await _require_user(authorization)
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "JSON inválido")
    lang = (data.get("language") or "").strip().lower()
    if lang not in ("es", "en"):
        raise HTTPException(400, "Idioma inválido")
    u["language"] = lang
    await user_put(email, u)
    return {"ok": True, "language": lang}

@app.post("/api/auth/set-avatar")
async def auth_set_avatar(request: Request, authorization: str = Header("")):
    """Foto de perfil. Recibe una data URL (imagen ya redimensionada en el cliente
    a un cuadrado pequeño). Se guarda en app_config (avatar::<email>), NO en una
    columna nueva de app_users, para no depender de un cambio de esquema."""
    email, u = await _require_user(authorization)
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "JSON inválido")
    av = (data.get("avatar") or "").strip()
    if not av.startswith("data:image/"):
        raise HTTPException(400, "Imagen inválida (se espera una data URL de imagen)")
    # Límite de tamaño: el cliente debe redimensionar antes de enviar. ~350KB de
    # data URL ≈ 256KB de imagen — suficiente para un avatar cuadrado.
    if len(av) > 350_000:
        raise HTTPException(413, "La imagen es muy grande; redúcela antes de subir")
    await _avatar_set(email, av)
    return {"ok": True, "avatar": av}

@app.post("/api/auth/remove-avatar")
async def auth_remove_avatar(authorization: str = Header("")):
    email, u = await _require_user(authorization)
    await _avatar_set(email, None)
    return {"ok": True}

@app.post("/api/auth/change-email")
async def auth_change_email(request: Request, authorization: str = Header("")):
    email, u = await _require_user(authorization)
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "JSON inválido")
    pw = data.get("password") or ""
    new_email = (data.get("new_email") or "").strip().lower()
    if "@" not in new_email or "." not in new_email.split("@")[-1]:
        raise HTTPException(400, "Correo inválido")
    if new_email == email:
        raise HTTPException(400, "Es el mismo correo")
    try:
        salt = base64.b64decode(u["salt"])
    except Exception:
        raise HTTPException(500, "cuenta corrupta")
    if not hmac.compare_digest(_hash_pw(pw, salt), u.get("pass_hash", "")):
        raise HTTPException(401, "Contraseña incorrecta")
    if await user_get(new_email):
        raise HTTPException(409, "Ese correo ya está en uso")
    # Migrar la cuenta al nuevo correo (crear con nuevo email, borrar el viejo)
    await user_put(new_email, dict(u))
    # Migrar la foto de perfil (avatar) al nuevo correo, si existe.
    try:
        _av = await _avatar_get(email)
        if _av:
            await _avatar_set(new_email, _av)
            await _avatar_set(email, None)
    except Exception as _e:
        print(f"[change-email] avatar migrate: {_e}")
    await _del_user(email)
    token = _make_jwt({"sub": u.get("id"), "email": new_email,
                       "plan": u.get("plan", "free"), "name": u.get("name")})
    return {"ok": True, "token": token, "email": new_email}

@app.get("/api/community/access")
async def community_access(authorization: str = Header("")):
    """Acceso a la comunidad: 401 sin sesión, 403 si no es premium, 200 si premium."""
    token = (authorization or "").replace("Bearer ", "").strip()
    p = _verify_jwt(token)
    if not p:
        raise HTTPException(401, "Inicia sesión")
    email = (p.get("email") or "").lower()
    u = await user_get(email) or {}
    plan = u.get("plan") or p.get("plan", "free")
    if plan not in ("premium", "pro", "admin"):
        raise HTTPException(403, "Requiere acceso premium")
    return {"access": True, "plan": plan, "email": email,
            "name": u.get("name") or p.get("name")}

@app.post("/api/auth/logout")
async def auth_logout():
    """Logout: el JWT es sin estado; el cliente simplemente descarta el token."""
    return {"ok": True}

@app.post("/api/auth/set-plan")
async def auth_set_plan(request: Request, key: str = ""):
    """Marca el plan de un usuario (admin, o desde un webhook de pago Whop/Stripe)."""
    if key != ADMIN_KEY:
        raise HTTPException(403, "clave incorrecta")
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "JSON inválido")
    email = (data.get("email") or "").strip().lower()
    plan = (data.get("plan") or "free").strip()
    u = await user_get(email)
    if not u:
        raise HTTPException(404, "usuario no encontrado")
    u["plan"] = plan
    await user_put(email, u)
    return {"ok": True, "user": {"id": u.get("id"), "email": email, "name": u.get("name"), "plan": plan}}


# ── Whop: webhook de pagos ──────────────────────────────────────────────────
WHOP_WEBHOOK_SECRET = os.getenv("WHOP_WEBHOOK_SECRET", "").strip()

def _whop_extract_email(d):
    """Busca el email del comprador en las variantes de payload de Whop."""
    if not isinstance(d, dict):
        return ""
    data = d.get("data") if isinstance(d.get("data"), dict) else d
    for path in (("email",), ("user", "email"), ("user_email",),
                 ("member", "email"), ("membership", "email"),
                 ("customer", "email"), ("metadata", "email")):
        cur = data
        ok = True
        for k in path:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                ok = False; break
        if ok and isinstance(cur, str) and "@" in cur:
            return cur.strip().lower()
    return ""

@app.post("/api/whop/webhook")
async def whop_webhook(request: Request):
    """Recibe eventos de Whop y marca el plan del usuario (premium/free).
    SEGURIDAD (fail-closed): exige SIEMPRE firma HMAC válida (X-Whop-Signature).
    Sin WHOP_WEBHOOK_SECRET configurado NO se acepta ningún webhook — antes se
    aceptaba sin firma y cualquiera podía auto-concederse premium."""
    raw = await request.body()
    if not WHOP_WEBHOOK_SECRET:
        raise HTTPException(503, "webhook no configurado (falta WHOP_WEBHOOK_SECRET)")
    sig = (request.headers.get("x-whop-signature") or
           request.headers.get("whop-signature") or "").strip()
    expected = hmac.new(WHOP_WEBHOOK_SECRET.encode(), raw, hashlib.sha256).hexdigest()
    sig_ok = bool(sig) and hmac.compare_digest(sig.split(",")[-1].replace("sha256=", ""), expected)
    if not sig_ok:
        raise HTTPException(403, "firma inválida")
    try:
        d = json.loads(raw.decode() or "{}")
    except Exception:
        raise HTTPException(400, "JSON inválido")
    evt = (d.get("action") or d.get("type") or d.get("event") or "").lower()
    email = _whop_extract_email(d)
    if not email:
        print(f"[whop] evento sin email: {evt} :: {str(d)[:200]}")
        return {"ok": True, "note": "sin email; ignorado"}
    # eventos que QUITAN acceso vs los que lo CONCEDEN.
    # OJO: "invalid" contiene "valid" y "deactivated" contiene "activated" →
    # hay que evaluar revoke PRIMERO (con "deactiv") para no confundirlos.
    # Nombres reales de Whop: membership.activated / membership.deactivated /
    # payment.succeeded (NO existen went_valid/went_invalid en este panel).
    revoke = any(w in evt for w in ("invalid", "deactiv", "cancel", "expire",
                                    "refund", "deleted", "failed"))
    grant = (not revoke) and any(w in evt for w in ("valid", "activ", "created",
                                                    "completed", "succeeded", "paid"))
    plan = "premium" if grant else ("free" if revoke else None)
    if plan is None:
        print(f"[whop] evento no accionable: {evt} ({email})")
        return {"ok": True, "note": f"evento {evt} ignorado"}
    u = await user_get(email)
    if u:
        was = u.get("plan")
        u["plan"] = plan
        await user_put(email, u)
        print(f"[whop] {email} -> {plan} ({evt})")
        # al CONCEDER premium (transición), enviar el correo de bienvenida premium
        if plan == "premium" and was != "premium":
            try:
                asyncio.create_task(send_welcome_premium(email, u.get("name")))
            except Exception:
                pass
    else:
        # aún no tiene cuenta: guardamos la titularidad para aplicarla al registrarse
        await _sb_set_config(f"whop_plan::{email}", plan)
        print(f"[whop] {email} sin cuenta; plan {plan} guardado como pendiente")
    return {"ok": True, "email": email, "plan": plan, "event": evt}


async def _ts_token_request(data):
    """POST al endpoint de token de TradeStation (code exchange o refresh)."""
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(TS_TOKEN_URL, data={**data,
                         "client_id": TRADESTATION_CLIENT_ID,
                         "client_secret": TRADESTATION_CLIENT_SECRET})
    if r.status_code != 200:
        raise HTTPException(502, f"TradeStation token {r.status_code}: {r.text[:200]}")
    return r.json() or {}


def _ts_store(uid, tok):
    _ts_tokens[uid] = {
        "access_token": tok.get("access_token"),
        "refresh_token": tok.get("refresh_token") or (_ts_tokens.get(uid) or {}).get("refresh_token"),
        "expires_at": time.time() + int(tok.get("expires_in", 1200)) - 60,
    }
    save_cache()


async def _ts_access(uid):
    """Devuelve un access_token válido para el usuario, refrescando si expiró."""
    rec = _ts_tokens.get(uid)
    if not rec or not rec.get("refresh_token") and not rec.get("access_token"):
        raise HTTPException(404, "TradeStation no conectado para este usuario")
    if rec.get("access_token") and time.time() < rec.get("expires_at", 0):
        return rec["access_token"]
    # refrescar
    if rec.get("refresh_token"):
        tok = await _ts_token_request({"grant_type": "refresh_token",
                                       "refresh_token": rec["refresh_token"]})
        _ts_store(uid, tok)
        return _ts_tokens[uid]["access_token"]
    raise HTTPException(401, "TradeStation: token expirado y sin refresh; reconecta")


@app.get("/api/broker/tradestation/callback")
async def tradestation_callback(code: str = "", state: str = "", error: str = ""):
    """Recibe el code del OAuth, lo canjea por tokens y guarda por usuario. Redirige
    de vuelta al journal."""
    if error:
        return RedirectResponse(TS_REDIRECT_AFTER + "&ts_error=" + quote(error, safe=""))
    if not code:
        raise HTTPException(400, "Falta 'code'")
    uid = state.split("::", 1)[1] if state.startswith("lbc::") else "dave"
    try:
        tok = await _ts_token_request({"grant_type": "authorization_code", "code": code,
                                       "redirect_uri": TRADESTATION_REDIRECT_URI})
        _ts_store(uid, tok)
    except HTTPException:
        raise
    except Exception as e:
        return RedirectResponse(TS_REDIRECT_AFTER + "&ts_error=" + quote(str(e)[:80], safe=""))
    return RedirectResponse(TS_REDIRECT_AFTER)


@app.get("/api/broker/tradestation/accounts")
async def tradestation_accounts(app_user_id: str = "dave"):
    """Lista las cuentas de TradeStation del usuario (incluye FUTUROS)."""
    uid = (app_user_id or "dave").strip() or "dave"
    tokn = await _ts_access(uid)
    async with httpx.AsyncClient(timeout=12, headers={"Authorization": f"Bearer {tokn}"}) as c:
        r = await c.get(f"{TS_API_BASE}/brokerage/accounts")
    if r.status_code != 200:
        raise HTTPException(502, f"TS accounts {r.status_code}: {r.text[:200]}")
    accts = (r.json() or {}).get("Accounts", [])
    return {"ok": True, "count": len(accts),
            "accounts": [{"id": a.get("AccountID"), "type": a.get("AccountType"),
                          "detail": a.get("AccountDetail")} for a in accts]}


def _ts_map_order(o, acct):
    """Mapea una orden ejecutada de TradeStation a fills del journal (por leg)."""
    status = str(o.get("StatusDescription") or o.get("Status") or "").upper()
    if status not in ("FILLED", "FLL", "PARTIALLY FILLED", "FPR"):
        return []
    opened = o.get("OpenedDateTime") or o.get("ClosedDateTime") or ""
    out = []
    for leg in (o.get("Legs") or []):
        exq = leg.get("ExecQuantity") or leg.get("QuantityOrdered")
        try:
            q = float(exq)
        except (TypeError, ValueError):
            q = None
        if not q:
            continue
        sym = leg.get("Symbol", "")
        # instrumento: futuros (ej. NQU25 / @NQ), opción o equity
        asset_type = str(leg.get("AssetType") or "").upper()
        instrument = ("future" if asset_type in ("FUTURE", "FUTURES") else
                      "option" if asset_type in ("STOCKOPTION", "OPTION", "INDEXOPTION") else "equity")
        out.append({
            "broker_order_id": str(o.get("OrderID") or ""),
            "symbol": sym, "instrument": instrument, "option": None,
            "side": str(leg.get("BuyOrSell") or "").upper(),
            "qty": q,
            "price": (lambda v: float(v) if v not in (None, "") else None)(leg.get("ExecutionPrice") or o.get("FilledPrice")),
            "order_type": o.get("OrderType"),
            "time": opened, "status": "EXECUTED",
            "account": acct, "source": "tradestation",
        })
    return out


@app.get("/api/broker/tradestation/fills")
async def tradestation_fills(app_user_id: str = "dave", days: int = 90, raw: int = 0):
    """Trae las órdenes históricas (fills) de TradeStation — incluye FUTUROS — y las
    mapea al formato del journal. ?raw=1 devuelve la forma cruda para calibrar."""
    uid = (app_user_id or "dave").strip() or "dave"
    tokn = await _ts_access(uid)
    from datetime import date, timedelta
    since = (date.today() - timedelta(days=int(days))).isoformat()
    async with httpx.AsyncClient(timeout=15, headers={"Authorization": f"Bearer {tokn}"}) as c:
        ra = await c.get(f"{TS_API_BASE}/brokerage/accounts")
        if ra.status_code != 200:
            raise HTTPException(502, f"TS accounts {ra.status_code}: {ra.text[:200]}")
        acct_ids = [a.get("AccountID") for a in (ra.json() or {}).get("Accounts", []) if a.get("AccountID")]
        raw_orders = []
        for aid in acct_ids:
            r = await c.get(f"{TS_API_BASE}/brokerage/accounts/{aid}/historicalorders",
                            params={"since": since})
            if r.status_code == 200:
                for o in (r.json() or {}).get("Orders", []):
                    o["_acct"] = aid
                    raw_orders.append(o)
            else:
                print(f"[ts] orders {aid} {r.status_code}: {r.text[:120]}")
    if raw:
        return {"ok": True, "count": len(raw_orders), "orders": raw_orders[:40]}
    trades = []
    for o in raw_orders:
        trades.extend(_ts_map_order(o, o.get("_acct")))
    return {"ok": True, "count": len(trades), "trades": trades}


@app.get("/api/broker/tradestation/status")
async def tradestation_status(app_user_id: str = "dave"):
    """Dice si este usuario ya tiene TradeStation conectado (para que el frontend
    muestre 'Desconectar' en vez de 'Conectar')."""
    uid = (app_user_id or "dave").strip() or "dave"
    rec = _ts_tokens.get(uid) or {}
    return {"connected": bool(rec.get("refresh_token") or rec.get("access_token")),
            "configured": bool(TRADESTATION_CLIENT_ID and TRADESTATION_CLIENT_SECRET)}


@app.post("/api/broker/tradestation/disconnect")
async def tradestation_disconnect(app_user_id: str = "dave"):
    """Borra los tokens OAuth de este usuario — deslogueo del broker. El próximo
    'Conectar' vuelve a pedir autorización."""
    uid = (app_user_id or "dave").strip() or "dave"
    existed = uid in _ts_tokens
    _ts_tokens.pop(uid, None)
    try:
        save_cache()
    except Exception:
        pass
    return {"ok": True, "disconnected": existed}


@app.get("/api/admin/costs")
async def admin_costs(key: str = ""):
    """Panel de GASTOS mensuales de la web. Montos configurables en Railway (COST_*).
    Uso: /api/admin/costs?key=TU_ADMIN_KEY  (renderiza un panel; añade &json=1 para JSON)."""
    if key != ADMIN_KEY:
        raise HTTPException(403, "clave incorrecta")
    def _f(name, d):
        try:
            return float(os.getenv(name, str(d)))
        except Exception:
            return float(d)
    gexbot   = _f("COST_GEXBOT", 40)      # GexBot Classic
    railway  = _f("COST_RAILWAY", 5)      # Railway hobby (backend)
    dom_year = _f("COST_DOMAIN_YEAR", 22) # GoDaddy .com ~$22/año
    twelve   = _f("COST_TWELVEDATA", 0)   # TwelveData (free)
    groq     = _f("COST_GROQ", 0)         # Groq (free tier)
    gemini   = _f("COST_GEMINI", 0)       # Gemini fallback (free tier)
    snap     = _f("COST_SNAPTRADE", 0)    # SnapTrade personal (0) / commercial ~1-2/usuario
    otros    = _f("COST_OTROS", 0)
    domain_m = round(dom_year / 12, 2)
    items = [
        ("GexBot (niveles GEX)", gexbot, "mensual"),
        ("Railway (backend)", railway, "mensual"),
        ("Dominio GoDaddy", domain_m, f"${dom_year:.0f}/año ÷ 12"),
        ("TwelveData (precio/VIX)", twelve, "gratis"),
        ("Groq (IA institucional + coach)", groq, "gratis"),
        ("Gemini (respaldo IA)", gemini, "gratis, dormido"),
        ("SnapTrade (brokers)", snap, "personal $0 · commercial ~$1-2/usuario"),
        ("Otros", otros, ""),
    ]
    total = round(sum(v for _, v, _ in items), 2)
    rows = ""
    for name, val, note in items:
        col = "#7a7f8a" if val == 0 else "#E8E4D9"
        rows += (f'<tr><td style="padding:11px 8px;border-bottom:0.5px solid rgba(255,255,255,0.06);color:#c9cdd6;">{name}'
                 f'{f"<span style=\"color:#7a7f8a;font-size:11px;\"> · {note}</span>" if note else ""}</td>'
                 f'<td style="padding:11px 8px;border-bottom:0.5px solid rgba(255,255,255,0.06);text-align:right;font-family:monospace;color:{col};">${val:,.2f}</td></tr>')
    html = f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Gastos · Liberato</title></head>
<body style="margin:0;background:#06080D;color:#E8E4D9;font-family:-apple-system,Segoe UI,Roboto,sans-serif;padding:28px 16px;">
<div style="max-width:520px;margin:0 auto;">
  <div style="font-family:Georgia,serif;font-style:italic;font-size:20px;color:#C9A84C;margin-bottom:4px;">Liberato · Gastos de la web</div>
  <div style="font-size:12px;color:#7a7f8a;margin-bottom:20px;">Coste mensual para sostener la plataforma</div>
  <div style="border:0.5px solid rgba(201,168,76,0.25);border-radius:16px;padding:8px 14px;background:linear-gradient(180deg,rgba(201,168,76,0.05),transparent);">
    <table style="width:100%;border-collapse:collapse;font-size:13.5px;">{rows}
    <tr><td style="padding:14px 8px;font-weight:700;color:#E8E4D9;">TOTAL MENSUAL</td>
        <td style="padding:14px 8px;text-align:right;font-family:monospace;font-weight:800;font-size:20px;color:#2EE8A4;">${total:,.2f}</td></tr>
    </table>
  </div>
  <div style="font-size:11px;color:#7a7f8a;margin-top:14px;line-height:1.6;">Anual ≈ <b style="color:#c9cdd6;">${total*12:,.2f}</b>. Ajusta los montos en Railway con variables <code style="color:#C9A84C;">COST_GEXBOT, COST_RAILWAY, COST_DOMAIN_YEAR, COST_SNAPTRADE…</code>. Groq/Gemini/TwelveData/Yahoo van en tier gratis (si crecen los alumnos, sube el tope o pasa a pago — céntimos).</div>
</div></body></html>"""
    return HTMLResponse(html)


_ohlc_cache = {}  # (ysym, date, interval) → {"ts": epoch, "bars": [...]}

def _ohlc_interval_for_age(age_days):
    """Yahoo retiene intradía por límite de intervalo: 1m ~7d, 2-30m ~60d, 60m ~730d.
    Elegimos el intervalo MÁS FINO que Yahoo servirá para esa antigüedad."""
    if age_days <= 6:   return "1m"
    if age_days <= 58:  return "5m"
    if age_days <= 720: return "60m"
    return "1d"

@app.get("/api/ohlc/{symbol}")
async def get_ohlc(symbol: str, date: str = ""):
    """Velas REALES del futuro (NQ=F, ES=F) para una fecha, vía Yahoo del lado SERVIDOR
    (sin CORS; el navegador no puede pegarle a Yahoo directo). Antes servía el ETF proxy
    (QQQ×ratio); ahora sirve el NQ/ES REAL — precio exacto del futuro, cubre también la
    sesión Globex/overnight. GRATIS (Yahoo). Selecciona el intervalo más fino disponible
    según la antigüedad (1m ~7d, 5m ~60d, 60m ~730d) y cachea los días pasados (inmutables).
    Respuesta: {ok, symbol, instrument, date, interval, real:true, source, bars:[{hhmm,o,h,l,c}]}."""
    sym = (symbol or "").upper()
    if ("NQ" in sym or "NDX" in sym):
        ysym, instrument = "NQ=F", "NQ"
    elif ("ES" in sym or "SPX" in sym):
        ysym, instrument = "ES=F", "ES"
    else:
        return {"ok": False, "reason": f"sin velas para {sym} (solo NQ/ES)"}
    if not date:
        raise HTTPException(400, "falta ?date=YYYY-MM-DD")
    from datetime import datetime, timezone, timedelta
    try:
        d0 = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        raise HTTPException(400, "date inválida (YYYY-MM-DD)")
    today_ny = datetime.now(NY).strftime("%Y-%m-%d")
    age_days = (datetime.now(NY).date() - d0.date()).days
    if age_days < 0:
        return {"ok": False, "reason": "fecha futura"}
    # Intervalos a intentar, del más fino disponible hacia atrás (fallback si Yahoo no da).
    order = ["1m", "5m", "60m", "1d"]
    start = _ohlc_interval_for_age(age_days)
    intervals = order[order.index(start):]
    p1 = int((d0 - timedelta(hours=6)).timestamp())   # ventana amplia; luego se filtra a la fecha NY exacta (incluye su sesión overnight, no la tarde del día previo)
    p2 = int((d0 + timedelta(hours=34)).timestamp())
    for interval in intervals:
        # Caché: días pasados son inmutables (TTL largo); HOY caduca a 60s.
        ck = (ysym, date, interval)
        cached = _ohlc_cache.get(ck)
        if cached:
            fresh = (date != today_ny) or (time.time() - cached["ts"] < 60)
            if fresh and cached["bars"]:
                return {"ok": True, "symbol": ysym, "instrument": instrument, "date": date,
                        "interval": interval, "real": True, "source": f"yahoo:{ysym}",
                        "bars": cached["bars"], "cached": True}
        for host in ("query1", "query2"):
            url = (f"https://{host}.finance.yahoo.com/v8/finance/chart/{ysym.replace('=', '%3D')}"
                   f"?interval={interval}&period1={p1}&period2={p2}")
            try:
                async with httpx.AsyncClient(timeout=12, headers={"User-Agent": "Mozilla/5.0"}) as c:
                    r = await c.get(url)
                if r.status_code != 200:
                    continue
                j = r.json()
                res = ((j.get("chart") or {}).get("result") or [None])[0]
                if not res:
                    continue
                ts = res.get("timestamp") or []
                q = ((res.get("indicators") or {}).get("quote") or [{}])[0]
                op, hi, lo, cl = q.get("open", []), q.get("high", []), q.get("low", []), q.get("close", [])
                bars = []
                for i, tt in enumerate(ts):
                    try:
                        o, h, l, c = op[i], hi[i], lo[i], cl[i]
                    except Exception:
                        continue
                    if None in (o, h, l, c):
                        continue
                    dt = datetime.fromtimestamp(tt, NY)
                    if dt.strftime("%Y-%m-%d") != date:
                        continue
                    bars.append({"hhmm": dt.strftime("%H:%M"), "o": o, "h": h, "l": l, "c": c})
                if bars:
                    _ohlc_cache[ck] = {"ts": time.time(), "bars": bars}
                    if len(_ohlc_cache) > 4000:   # poda simple anti-crecimiento
                        for k in list(_ohlc_cache)[:1000]:
                            _ohlc_cache.pop(k, None)
                    return {"ok": True, "symbol": ysym, "instrument": instrument, "date": date,
                            "interval": interval, "real": True, "source": f"yahoo:{ysym}",
                            "bars": bars}
            except Exception as e:
                print(f"[ohlc] {host} {ysym} {date} {interval}: {e}")
                continue
    return {"ok": False, "reason": f"sin velas reales de {instrument} para {date} "
            "(Yahoo limita el histórico intradía; prueba una fecha más reciente)"}


@app.post("/api/journal/coach")
async def journal_coach(request: Request):
    """AI Coach del journal: analiza la data REAL del trader (stats, rendimiento por
    playbook/setup, hora, riesgo) y responde CONCISO como un mentor de daytrading.
    La key de Groq vive en el servidor (nunca en el frontend). Modelo qwen (vivo)."""
    if not GROQ_KEY:
        raise HTTPException(400, "Coach no disponible (falta GROQ_KEY en el servidor)")
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "JSON inválido")
    # Tope por estudiante/día + presupuesto global de Groq (contabilización real).
    uid = str(data.get("app_user_id") or "anon").strip() or "anon"
    if not _coach_quota_ok(uid):
        return {"ok": True, "answer": f"Llegaste a tu límite diario de {COACH_DAILY_PER_USER} consultas al Coach. "
                "Vuelve mañana — mientras tanto, tus estadísticas y patrones ya están en el dashboard."}
    if not budget_ok("groq", 1):
        return {"ok": True, "answer": "El Coach está muy solicitado ahora mismo. Inténtalo en unos minutos."}
    question = str(data.get("question") or "").strip()[:600]
    # Contexto compacto que manda el frontend (ya agregado, sin PII).
    ctx = data.get("context") or {}
    ctx_str = json.dumps(ctx, ensure_ascii=False)[:6000]
    sys_msg = (
        "Eres el AI Coach de Liberato Community: un mentor de daytrading de NQ/Nasdaq "
        "y opciones 0DTE, experto en Auction Market Theory, order flow, gestión de "
        "riesgo y disciplina de playbooks. Hablas en español, directo y cercano, como "
        "un coach que conoce el mercado. REGLAS: (1) responde CONCISO — 2 a 4 frases, "
        "sin relleno ni listas largas; (2) basa TODO en los datos reales del trader que "
        "recibes (no inventes cifras: si un dato no está, dilo); (3) da 1-2 acciones "
        "concretas y accionables. Ejemplos de tono: 'Tu stop se ve muy ajustado, te saca "
        "mucho — dale más aire.' · 'Tu playbook D es el que más rinde, priorízalo salvo "
        "que no aparezca ese día.' · 'Operas peor por la tarde: concentra tu tamaño en la "
        "primera hora.' Nunca des consejo financiero personalizado de inversión; céntrate "
        "en su ejecución, disciplina y patrones."
    )
    usr_msg = (
        f"Datos del trader (JSON agregado de su journal):\n{ctx_str}\n\n"
        f"Pregunta del trader: {question or '¿Qué es lo más importante que ves en mi data ahora mismo?'}\n\n"
        "Responde en 2-4 frases, concreto y basado en sus números."
    )
    answer = None
    try:
        async with httpx.AsyncClient(timeout=25) as client:
            r = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROQ_KEY}", "Content-Type": "application/json"},
                json={"model": "qwen/qwen3.6-27b", "max_tokens": 350, "temperature": 0.5,
                      "reasoning_effort": "none",
                      "messages": [{"role": "system", "content": sys_msg},
                                   {"role": "user", "content": usr_msg}]}
            )
        if r.status_code == 200:
            j = r.json()
            answer = (((j.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
        else:
            print(f"[coach] groq {r.status_code}: {r.text[:160]}")
    except Exception as e:
        print(f"[coach] groq error: {type(e).__name__}: {str(e)[:160]}")
    if answer:
        budget_charge("groq", 1); _coach_charge(uid)   # contabiliza tras respuesta OK
        return {"ok": True, "answer": answer}
    # Groq falló → fallback DORMIDO a Gemini (solo si hay GEMINI_API_KEY)
    g = await _gemini_chat(sys_msg, usr_msg, max_tokens=350, temperature=0.5)
    if g:
        _coach_charge(uid)   # respeta el tope por usuario; no toca la cuota de Groq
        return {"ok": True, "answer": g}
    raise HTTPException(502, "Coach no disponible ahora mismo, inténtalo en unos minutos")


@app.get("/api/admin/diag-ai")
async def diag_ai(key: str = ""):
    """Monitoreo de consumo de IA (Groq): cuota global del día + uso del AI Coach por
    estudiante. Uso: ?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "clave incorrecta")
    from datetime import date
    day = date.today().isoformat()
    active = {u: v.get("count", 0) for u, v in _coach_usage.items() if v.get("day") == day}
    g = _api_usage.get("groq", {})
    return {
        "groq_dia": {"usadas": g.get("used", 0), "limite": API_BUDGETS["groq"]["limit"]},
        "coach_tope_por_estudiante_dia": COACH_DAILY_PER_USER,
        "coach_uso_por_estudiante_hoy": active,
        "estudiantes_activos_coach_hoy": len(active),
        "briefing_institucional": "COMPARTIDO — 1 para toda la plataforma, no escala con estudiantes",
        "gemini_fallback": ("armado (" + GEMINI_MODEL + ")") if GEMINI_API_KEY else "dormido (sin GEMINI_API_KEY)",
    }


@app.get("/api/admin/test-gemini")
async def test_gemini(key: str = ""):
    """Prueba el fallback de Gemini con la MISMA personalidad del AI Coach, sin esperar
    a que Groq falle. Uso: ?key=liberato2026"""
    if key != ADMIN_KEY:
        raise HTTPException(403, "clave incorrecta")
    if not GEMINI_API_KEY:
        return {"ok": False, "estado": "dormido", "detalle": "falta GEMINI_API_KEY en Railway"}
    sys_msg = (
        "Eres el AI Coach de Liberato Community: un mentor de daytrading de NQ/Nasdaq "
        "y opciones 0DTE, experto en Auction Market Theory, order flow, gestión de "
        "riesgo y disciplina de playbooks. Hablas en español, directo y cercano. "
        "Responde CONCISO, 2 a 4 frases."
    )
    ans = await _gemini_chat(sys_msg, "Preséntate en una frase como mi coach de trading.", max_tokens=120)
    return {"ok": bool(ans), "modelo": GEMINI_MODEL,
            "respuesta": ans or "(sin respuesta — revisa la key o el modelo)"}


@app.post("/api/journal/parse-csv")
async def journal_parse_csv(request: Request):
    """Parsea un CSV de broker con IA (Groq) → trades del journal.

    El journal (journal.html) llamaba DIRECTAMENTE a api.anthropic.com desde el
    navegador SIN api-key → 401 + CORS bloqueado → "error al subir CSV". Una key
    en el frontend seria un agujero de seguridad. Ahora el navegador llama a este
    endpoint y la IA se ejecuta en el servidor con la key de Groq (ya configurada).
    """
    try:
        body = await request.json()
    except Exception:
        return {"error": "cuerpo de la petición inválido"}
    sample = (body.get("sample") or "").strip()
    total_rows = body.get("totalRows", 0)
    if not GROQ_KEY:
        return {"error": "IA no configurada en el servidor (falta GROQ_KEY en Railway)"}
    if not sample:
        return {"error": "El archivo está vacío o no tiene datos."}
    if not budget_ok("groq", 1):
        return {"error": "Límite de IA alcanzado por hoy. Intenta más tarde."}
    sys_msg = (
        "Eres un parser experto de CSVs de brokers de trading (futuros, acciones, "
        "forex): NinjaTrader, Tradovate, TradeStation, ThinkOrSwim, Interactive "
        "Brokers, etc. Cada broker usa nombres de columna distintos. Identifica qué "
        "columna corresponde a cada campo y devuelve TODOS los trades. Responde "
        "SOLO con un objeto JSON válido, sin texto extra ni markdown."
    )
    usr_msg = (
        "CAMPOS DEL JOURNAL (destino):\n"
        "- date: fecha YYYY-MM-DD\n- time: hora HH:MM (24h)\n"
        "- asset: símbolo (NQ, ES...). 'NQ 03-26'/'NQH6' -> 'NQ'. Acción -> ticker.\n"
        "- direction: 'long' o 'short' (compra/buy=long, venta/sell=short)\n"
        "- entry: precio de entrada (número)\n- exit: precio de salida (número)\n"
        "- stop: precio de stop o 0\n- contracts: cantidad (número, default 1)\n"
        "- setup: columna de estrategia o 'Importado'\n- note: comentario o ''\n\n"
        "REGLAS: ignora filas de resumen/total; si no puedes determinar entry/exit "
        "omite ese trade; convierte cualquier formato de fecha a YYYY-MM-DD; separa "
        "fecha y hora si vienen juntas.\n\n"
        "FORMATO JSON de respuesta:\n"
        '{"detected":{"broker":"nombre o desconocido","columnsFound":[...]},'
        '"trades":[{"date":"2026-06-15","time":"09:42","asset":"NQ","direction":'
        '"long","entry":21720,"exit":21790,"stop":21700,"contracts":1,"setup":'
        '"Importado","note":""}]}\n\n'
        f"CSV (primeras filas, total {total_rows} filas de datos):\n{sample}"
    )
    try:
        budget_charge("groq", 1)
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROQ_KEY}", "Content-Type": "application/json"},
                json={"model": "qwen/qwen3.6-27b", "max_tokens": 6000,
                      "temperature": 0.1, "response_format": {"type": "json_object"},
                      "reasoning_effort": "none",   # llama-3.3 decomisionado; qwen es el modelo vivo (ver institutional)
                      "messages": [{"role": "system", "content": sys_msg},
                                   {"role": "user", "content": usr_msg}]}
            )
        if r.status_code != 200:
            return {"error": f"La IA respondió {r.status_code}. Intenta de nuevo."}
        txt = r.json()["choices"][0]["message"]["content"]
        data = json.loads(txt)
        if not data.get("trades"):
            return {"error": "La IA no encontró trades válidos. Revisa que el CSV "
                             "tenga columnas de entrada y salida.", "detected": data.get("detected")}
        return data
    except Exception as e:
        return {"error": f"No se pudo procesar el CSV: {str(e)[:120]}"}


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

async def _seed_auditor():
    """Cuenta de AUDITOR con acceso PREMIUM sin pago (supervisión de la web),
    pre-creada y pre-verificada. Solo el HASH de la contraseña vive en el código
    (el texto plano se entrega aparte). No es admin: solo premium (ve todo, sin
    poderes de administración)."""
    email = "auditor@liberatocommunity.com"
    try:
        if await user_get(email):
            return   # ya existe → no sobrescribir
        await user_put(email, {
            "id": "aud_1096260d7bf1",
            "name": "Auditor",
            "salt": "Ji0DFYMA3JV5k2nFQgIcdA==",
            "pass_hash": "fhn4R2xGMIgZh5oaD0I1X52wQO0LMjRVL14880kr6Zo",
            "plan": "premium",
            "created": int(time.time()),
            "language": "es",
        })
        print("[seed] cuenta auditor creada (premium)")
    except Exception as e:
        print(f"[seed] auditor: {e}")

@app.on_event("startup")
async def startup():
    load_cache()
    try:
        await _load_auth_secret()   # AUTH_SECRET durable desde Supabase (si está configurado)
        print(f"[auth] store: {'Supabase' if _sb_on() else 'snapshot (efímero)'}")
    except Exception as e:
        print(f"[auth] _load_auth_secret: {e}")
    try:
        await _seed_auditor()
    except Exception as e:
        print(f"[seed] auditor: {e}")
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
    scheduler.add_job(refresh_real_indices, IntervalTrigger(seconds=10))  # precio del índice ~10s (throttle interno 10s)
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
    #   · 09:30-11:57     → VENTANA CRÍTICA, cada 3 min (50 refreshes)
    #   · 12:30-15:30     → tarde, 1 por hora (4 refreshes)
    # Total: 56 refreshes → ~170 créd de 240 seguro (colchón 70; de 250 real, 80).
    # Historia del límite: Basic era 100/día → ventana de 6 min (86 créd), luego
    # 7 min (80 créd) por poco margen. El 26-jul FlashAlpha subió Basic a 250/día
    # (mismo precio): con 2,5× de headroom se baja a 3 min → un gamma flip se ve
    # en máx 3 min (antes 7) y se añade cobertura de tarde. Sigue con 70 de colchón.
    scheduler.add_job(refresh_gex,
                      CronTrigger(hour=8, minute=30, day_of_week="mon-fri"))   # prep
    scheduler.add_job(refresh_gex,
                      CronTrigger(hour=9, minute=15, day_of_week="mon-fri"))   # prep
    scheduler.add_job(refresh_gex,                                             # ventana crítica
                      CronTrigger(hour=9,  minute="30,33,36,39,42,45,48,51,54,57", day_of_week="mon-fri"))
    scheduler.add_job(refresh_gex,
                      CronTrigger(hour="10-11", minute="*/3", day_of_week="mon-fri"))
    scheduler.add_job(refresh_gex,                                             # tarde: 1/hora
                      CronTrigger(hour="12-15", minute=30, day_of_week="mon-fri"))
    # ── GexBot: capa de gamma frecuente (Classic tier, sin coste por llamada).
    #    Cada 2 min de 8:00 a 16:15 ET para que el volume profile se sienta vivo,
    #    independiente de las ventanas de FlashAlpha.
    if GEXBOT_API_KEY:
        scheduler.add_job(_refresh_gex_gexbot,
                          CronTrigger(hour="7-16", minute="*", day_of_week="mon-fri"))  # GexBot LIVE: cada 1 min en RTH
    # Sentiment (VIX real + Fear&Greed CNN + Expected Move derivado) — reemplaza a
    # FlashAlpha. Cada 3 min (VIX/F&G cambian lento; CNN sin key, VIX 1 crédito TD).
    scheduler.add_job(_refresh_market_sentiment,
                      CronTrigger(hour="7-17", minute="*/3", day_of_week="mon-fri"))

    # ── Finnhub Calendar: cada 5 minutos ──────────────────────────────────
    scheduler.add_job(refresh_calendar, IntervalTrigger(seconds=30))  # latencia máx ~45s

    # ── Finnhub Movers: cada 60 segundos ──────────────────────────────────
    scheduler.add_job(refresh_movers, IntervalTrigger(seconds=45))

    # ── Finnhub Earnings: cada 6 horas ────────────────────────────────────
    scheduler.add_job(refresh_earnings, IntervalTrigger(hours=6))

    # ── Groq Institutional: 9:05 AM + 12:00 PM ET lun-vie ─────────────────
    # Resumen IA: cada 30min durante horario extendido (premarket→afterhours)
        # Groq — 4 eventos clave del mercado (4 llamadas/día en horario hábil):
    # Institutional: refrescos frecuentes para tener SIEMPRE lo más reciente y captar
    # los GEX de RTH en cuanto GexBot los publica (Dave: pre-market, apertura, +30min
    # como mínimo). Groq/qwen es barato → refresco cada 15 min de 8:00 a 12:00 + tarde.
    # CADA 5 MIN en pre-market + RTH (7:00-16:00 ET): el briefing habla de gamma/flip,
    # niveles GEX y movers en vivo — datos que envejecen en minutos, no en horas. A ~120
    # llamadas/día cabe de sobra en el tope Groq (950/día). Fuera de RTH: 1/hora.
    scheduler.add_job(refresh_institutional, CronTrigger(hour="7-16", minute="*/5", day_of_week="mon-fri"))
    scheduler.add_job(refresh_institutional, CronTrigger(hour="17-23,0-6", minute=0, day_of_week="mon-fri"))  # after-hours/overnight: contexto macro 1/hora
    scheduler.add_job(refresh_institutional, CronTrigger(hour="*/3"))  # fines de semana: se mantiene vivo

    # ── Brief diario a Discord (free sin GEX / premium con GEX) ──
    # 8:45 ET lun-vie (antes de la apertura). No hace nada si no hay webhooks configurados.
    scheduler.add_job(send_daily_briefs, CronTrigger(hour=8, minute=45, day_of_week="mon-fri"))

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
    sender_email = (body.get("email") or "").strip().lower()
    subject = (body.get("subject") or "").strip()
    description = (body.get("description") or "").strip()

    # Validación de campos obligatorios
    if not name or not sender_email or not subject or not description:
        raise HTTPException(400, "Todos los campos son obligatorios")
    if "@" not in sender_email or "." not in sender_email.split("@")[-1]:
        raise HTTPException(400, "Correo inválido")
    if len(name) > 120 or len(subject) > 200 or len(description) > 5000:
        raise HTTPException(400, "Contenido demasiado largo")
    if len(description) < 10:
        raise HTTPException(400, "La descripción es demasiado corta")

    if not EMAIL_READY:
        print(f"[contact] ⚠️ correo no configurado — mensaje de {name} <{sender_email}> solo en logs")
        return {"success": True, "note": "logged"}

    html_body = f"""
    <div style="font-family:sans-serif;max-width:600px;margin:0 auto;">
      <h2 style="color:#C9A84C;">Nuevo mensaje de contacto · Liberato Community</h2>
      <p><strong>Nombre:</strong> {name}</p>
      <p><strong>Correo:</strong> <a href="mailto:{sender_email}">{sender_email}</a></p>
      <p><strong>Asunto:</strong> {subject}</p>
      <p><strong>Descripción:</strong></p>
      <p style="background:#f5f5f5;padding:14px;border-radius:8px;white-space:pre-wrap;">{description}</p>
      <hr>
      <p style="color:#888;font-size:12px;">Responde este correo para contestarle directamente a {sender_email} · IP: {ip}</p>
    </div>
    """

    # Enviado vía _send_email; reply_to = el correo de quien escribió (para responderle directo).
    ok = await _send_email(SUPPORT_EMAIL, f"[Contacto Liberato] {subject}", html_body, reply_to=sender_email)
    if ok:
        print(f"[contact] ✓ Mensaje de {name} enviado a {SUPPORT_EMAIL}")
        return {"success": True}
    raise HTTPException(500, "No se pudo enviar el mensaje. Intenta más tarde.")


# ═══════════════════════════════════════════════════════════════════════════
#  ENDPOINT DE PRUEBA MANUAL — dispara llamadas a FlashAlpha/Groq bajo demanda
#  Útil para probar sin esperar a las ventanas programadas.
#  Protegido con clave: agrega ?key=TU_CLAVE en la URL.
# ═══════════════════════════════════════════════════════════════════════════
# SEGURIDAD: sin default en el código. Si no hay ADMIN_KEY en Railway, se usa una clave
# ALEATORIA por arranque = los endpoints /api/admin/* quedan deshabilitados (nadie la
# adivina). Antes el default "liberato2026" estaba en el código = agujero. Pon ADMIN_KEY
# en Railway para reactivar los diagnósticos.
import secrets as _secrets
ADMIN_KEY = os.getenv("ADMIN_KEY", "").strip() or ("disabled-" + _secrets.token_hex(12))
# Admins por EMAIL: quien inicie sesión con uno de estos correos tiene acceso admin
# SIN necesidad de ADMIN_KEY (usa su propio JWT). Dave por defecto.
# El correo del dueño (liberatoceo@gmail.com) SIEMPRE es admin, aunque en Railway
# se sobrescriba ADMIN_EMAILS (una sobrescritura reemplaza el valor por defecto y
# podría dejar fuera al dueño → is_premium=false y candados cerrados para él).
# Por eso hacemos la UNIÓN del env con el correo del dueño garantizado.
_OWNER_EMAIL = "liberatoceo@gmail.com"
ADMIN_EMAILS = {e.strip().lower() for e in os.getenv("ADMIN_EMAILS", _OWNER_EMAIL).split(",") if e.strip()} | {_OWNER_EMAIL}
def _is_admin(key="", authorization=""):
    """True si trae la ADMIN_KEY correcta O un JWT válido de un email admin."""
    if key and ADMIN_KEY and not ADMIN_KEY.startswith("disabled-") and key == ADMIN_KEY:
        return True
    tok = (authorization or "").replace("Bearer ", "").strip()
    if tok:
        p = _verify_jwt(tok)
        if p and (p.get("email") or "").lower() in ADMIN_EMAILS:
            return True
    return False

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
async def diag_symbol(sym: str = "NDX", key: str = ""):
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
                # Probar /exposure/gex con la 1ª expiración futura y mostrar las
                # keys CRUDAS: net_gex sale None si FlashAlpha lo nombra distinto
                # para NDX (net_gamma, total_gex, netGex...).
                _fut = sorted([d for d in dates if d and d > _today_et_str()])
                if _fut and budget_ok("flashalpha", 1):
                    budget_charge("flashalpha", 1)
                    r_gex = await client.get(f"{FA_BASE}/v1/exposure/gex/{sym_url}",
                                             params={"expiration": _fut[0]})
                    out["gex_status"] = r_gex.status_code
                    if r_gex.status_code == 200:
                        gd = r_gex.json() or {}
                        out["gex_keys"] = list(gd.keys())
                        out["gex_net_gex"] = gd.get("net_gex")
                        out["gex_net_gex_label"] = gd.get("net_gex_label")
                        out["gex_expiration_probada"] = _fut[0]
                    else:
                        out["gex_body"] = r_gex.text[:150]
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
