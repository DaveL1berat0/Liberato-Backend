"""
═══════════════════════════════════════════════════════════════════════════
  LIBERATO COMMUNITY — Calendario Económico en Tiempo Real
═══════════════════════════════════════════════════════════════════════════

  Arquitectura de 3 capas (para datos casi en tiempo real):

    Capa 1 · ForexFactory  → estructura del calendario, forecast, previous
                             (ya integrado en main.py del dashboard)
    Capa 2 · RapidAPI      → el "actual" a los minutos del release  ← ESTE MÓDULO
             Economic Calendar  (descarta BLS/BEA/Census por su retraso de 1 día)
    Capa 3 · Finnhub       → respaldo final
                             (ya integrado en main.py del dashboard)

  Este módulo provee la CAPA 2: consulta RapidAPI Economic Calendar y devuelve
  los valores "actual" reales tan pronto se publican, para fusionarlos con
  ForexFactory en el backend del dashboard.

  PARA CONECTAR:
    RAPIDAPI_KEY  → tu clave de RapidAPI
    RAPIDAPI_HOST → el host del Economic Calendar que te suscribas
                    (ej: "economic-calendar.p.rapidapi.com")

  Cómo integrarlo en main.py del dashboard:
    from realtime_calendar import fetch_realtime_actuals
    actuals = await fetch_realtime_actuals(client)
    # luego fusionar 'actuals' con los eventos de ForexFactory por (título, fecha)
═══════════════════════════════════════════════════════════════════════════
"""

import os
import re
from datetime import datetime, timezone, timedelta

RAPIDAPI_KEY  = os.getenv("RAPIDAPI_KEY", "")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "economic-calendar.p.rapidapi.com")

# Eventos sistémicos que nos importan (alineado con el dashboard)
RELEVANT_EVENTS = [
    "non-farm", "nonfarm", "nfp", "cpi", "core cpi", "ppi", "core ppi",
    "pce", "fomc", "federal funds", "interest rate", "fed", "powell",
    "gdp", "retail sales", "ism", "jolts", "adp", "jobless claims",
    "unemployment", "michigan", "consumer confidence", "durable goods",
    "building permits", "housing starts", "trade balance",
]


def _is_relevant(name: str) -> bool:
    n = (name or "").lower()
    return any(k in n for k in RELEVANT_EVENTS)


def _classify_nq_impact(name: str, actual, consensus) -> dict:
    """
    Calcula el impacto cualitativo en el NQ a partir de la sorpresa.
    Inflación/desempleo alto = bearish; crecimiento alto = bullish.
    """
    def parse_num(v):
        if v is None:
            return None
        try:
            return float(re.sub(r"[^0-9.\-]", "", str(v)))
        except (ValueError, AttributeError):
            return None

    a = parse_num(actual)
    c = parse_num(consensus)
    if a is None or c is None:
        return {"surprise": None, "classification": None, "nq_impact": None}

    surprise = round(a - c, 2)
    name_l = (name or "").lower()
    higher_bearish = any(k in name_l for k in
                         ["cpi", "ppi", "inflation", "claims", "unemployment", "jobless", "pce"])
    if abs(surprise) < 0.001:
        cls = "Neutral"
    elif higher_bearish:
        cls = "Bearish" if surprise > 0 else "Bullish"
    else:
        cls = "Bullish" if surprise > 0 else "Bearish"

    nq = {"Bullish": "Alcista", "Bearish": "Bajista", "Neutral": "Neutral"}[cls]
    return {"surprise": surprise, "classification": cls, "nq_impact": nq}


async def fetch_realtime_actuals(client) -> list:
    """
    Consulta RapidAPI Economic Calendar y devuelve los eventos US relevantes
    con su 'actual' real (cuando ya se publicó).

    Devuelve lista de dicts:
      { "title", "date", "actual", "consensus", "previous",
        "surprise", "classification", "nq_impact", "is_better" }

    Si RapidAPI no está configurado o falla, devuelve [] (el dashboard usa
    sus otras capas — ForexFactory y Finnhub — como respaldo).
    """
    if not RAPIDAPI_KEY:
        print("[rt-calendar] RapidAPI no configurado — usando solo ForexFactory + Finnhub")
        return []

    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": RAPIDAPI_HOST,
    }
    # Ventana: hoy + 1 día (para capturar eventos recientes)
    today = datetime.now(timezone.utc)
    params = {
        "from": today.strftime("%Y-%m-%d"),
        "to": (today + timedelta(days=1)).strftime("%Y-%m-%d"),
        "countries": "US",
    }

    try:
        url = f"https://{RAPIDAPI_HOST}/economic-events"
        r = await client.get(url, headers=headers, params=params, timeout=10)
        if r.status_code != 200:
            print(f"[rt-calendar] RapidAPI status {r.status_code}")
            return []

        data = r.json()
        # La respuesta puede venir como lista o dict con 'data'/'events'
        events_raw = data if isinstance(data, list) else (
            data.get("data") or data.get("events") or data.get("result") or []
        )

        out = []
        for ev in events_raw:
            name = ev.get("name") or ev.get("event") or ev.get("title", "")
            country = (ev.get("countryCode") or ev.get("country") or "").upper()
            if country not in ("US", "USA", "UNITED STATES"):
                continue
            if not _is_relevant(name):
                continue

            actual = ev.get("actual")
            consensus = ev.get("consensus") or ev.get("estimate") or ev.get("forecast")
            previous = ev.get("previous") or ev.get("prev")
            date = ev.get("dateUtc") or ev.get("date") or ev.get("time", "")

            impact = _classify_nq_impact(name, actual, consensus)

            out.append({
                "title": name,
                "date": date,
                "actual": str(actual) if actual is not None else None,
                "consensus": str(consensus) if consensus is not None else None,
                "previous": str(previous) if previous is not None else None,
                "surprise": impact["surprise"],
                "classification": impact["classification"],
                "nq_impact": impact["nq_impact"],
                "is_better": ev.get("isBetterThanExpected"),
            })

        released = sum(1 for e in out if e["actual"])
        print(f"[rt-calendar] RapidAPI: {len(out)} eventos US relevantes ({released} con actual)")
        return out

    except Exception as e:
        print(f"[rt-calendar] Error: {e}")
        return []


def merge_actuals_into_calendar(ff_events: list, rt_actuals: list) -> list:
    """
    Fusiona los 'actual' de RapidAPI dentro de los eventos de ForexFactory.
    Indexa por (título normalizado, fecha) y rellena actual/surprise/clasificación
    cuando ForexFactory aún no los tiene.

    Esto resuelve el caso ADP/Building Permits: si ForexFactory no tiene el
    actual todavía, RapidAPI lo provee en tiempo casi real.
    """
    def norm(title):
        t = (title or "").lower().strip()
        t = t.replace(" m/m", "").replace(" y/y", "").replace(" q/q", "")
        return re.sub(r"\s+", " ", t).strip()

    # Indexar RapidAPI por (título, fecha)
    rt_index = {}
    for e in rt_actuals:
        date = (e.get("date", "") or "")[:10]
        rt_index[(norm(e["title"]), date)] = e

    for ev in ff_events:
        date = (ev.get("time", "") or ev.get("date", "") or "")[:10]
        key = (norm(ev.get("title", "") or ev.get("name", "")), date)
        rt = rt_index.get(key)
        if rt and rt.get("actual"):
            # ForexFactory no tiene actual pero RapidAPI sí → rellenar
            if not ev.get("actual"):
                ev["actual"] = rt["actual"]
                ev["status"] = "Released"
            if not ev.get("forecast") and rt.get("consensus"):
                ev["forecast"] = rt["consensus"]
            if not ev.get("previous") and rt.get("previous"):
                ev["previous"] = rt["previous"]
            # Siempre actualizar la clasificación con el dato en tiempo real
            if rt.get("surprise") is not None:
                ev["surprise"] = rt["surprise"]
                ev["classification"] = rt["classification"]

    return ff_events
