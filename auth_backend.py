"""
═══════════════════════════════════════════════════════════════════════════
  LIBERATO COMMUNITY — Sistema de Autenticación y Gestión de Usuarios
  v1.0 · FastAPI + SQLite + Stripe + Gmail SMTP
═══════════════════════════════════════════════════════════════════════════

  REGLA #1: Datos reales en producción. Sin mocks.

  Este backend gestiona:
    • Registro de usuarios (email, nombre, contraseña)
    • Verificación por código de 6 dígitos (expira 10 min)
    • Login con tokens JWT
    • Base de datos SQLite (lista para migrar a PostgreSQL)
    • Gestión de planes: 'free' (Comunidad Libre) vs 'premium' (Acceso Completo)
    • Integración Stripe (checkout + webhooks para pagos y reembolsos)
    • Emails: verificación + bienvenida diferenciada (libre/premium)

  PARA CONECTAR (variables de entorno):
    JWT_SECRET           → clave secreta para firmar tokens (genera una larga y aleatoria)
    GMAIL_USER           → tu correo Gmail (ej: notificaciones@liberato.com)
    GMAIL_APP_PASSWORD   → App Password de Gmail (16 caracteres, NO tu contraseña normal)
    STRIPE_SECRET_KEY    → sk_live_... de tu cuenta Stripe
    STRIPE_WEBHOOK_SECRET→ whsec_... del webhook configurado en Stripe
    STRIPE_PRICE_ID      → price_... del producto de $199/mes en Stripe
    RAPIDAPI_KEY         → tu clave de RapidAPI (Economic Calendar)
    BASE_URL             → URL pública del frontend (ej: https://liberato.com)
═══════════════════════════════════════════════════════════════════════════
"""

import os
import re
import sqlite3
import secrets
import hashlib
import hmac
import time
import json
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager

import jwt  # PyJWT
from fastapi import FastAPI, HTTPException, Request, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr, field_validator

# ─────────────────────────────────────────────────────────────────────────
#  CONFIGURACIÓN (desde variables de entorno — nunca hardcodear secretos)
# ─────────────────────────────────────────────────────────────────────────
JWT_SECRET          = os.getenv("JWT_SECRET", "CAMBIAR-ESTA-CLAVE-EN-PRODUCCION")
JWT_ALGORITHM       = "HS256"
JWT_EXPIRE_HOURS    = 24 * 7  # token válido 7 días
DB_PATH             = os.getenv("DB_PATH", "liberato_users.db")
CODE_EXPIRE_MINUTES = 10
BASE_URL            = os.getenv("BASE_URL", "https://liberato.community")

# Stripe (la cuenta ya existe — solo conectar claves)
STRIPE_SECRET_KEY     = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
STRIPE_PRICE_ID       = os.getenv("STRIPE_PRICE_ID", "")

# ─────────────────────────────────────────────────────────────────────────
#  HASHING DE CONTRASEÑAS (PBKDF2 — incluido en Python, sin dependencias extra)
#  Nota: en producción de alto volumen, bcrypt/argon2 son preferibles, pero
#  PBKDF2-HMAC-SHA256 con 200k iteraciones es seguro y no requiere compilar nada.
# ─────────────────────────────────────────────────────────────────────────
def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 200_000)
    return f"pbkdf2_sha256$200000${salt}${dk.hex()}"

def verify_password(password: str, stored: str) -> bool:
    try:
        algo, iters, salt, hexhash = stored.split("$")
        dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), int(iters))
        return hmac.compare_digest(dk.hex(), hexhash)
    except Exception:
        return False

# ─────────────────────────────────────────────────────────────────────────
#  BASE DE DATOS (SQLite — un archivo, cero configuración)
# ─────────────────────────────────────────────────────────────────────────
@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

def init_db():
    """Crea las tablas si no existen."""
    with get_db() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                email           TEXT UNIQUE NOT NULL,
                name            TEXT NOT NULL,
                password_hash   TEXT NOT NULL,
                plan            TEXT NOT NULL DEFAULT 'free',     -- 'free' | 'premium'
                verified        INTEGER NOT NULL DEFAULT 0,       -- 0 | 1
                language        TEXT NOT NULL DEFAULT 'es',       -- 'es' | 'en'
                verify_code     TEXT,                             -- código 6 dígitos
                verify_expires  TEXT,                             -- ISO timestamp
                stripe_customer TEXT,                             -- cus_... de Stripe
                stripe_sub      TEXT,                             -- sub_... de Stripe
                plan_expires    TEXT,                             -- ISO timestamp (renovación)
                created_at      TEXT NOT NULL,
                last_login      TEXT
            )
        """)
        # Índice para búsquedas rápidas por email
        db.execute("CREATE INDEX IF NOT EXISTS idx_email ON users(email)")
        # Tabla de eventos procesados de Stripe (idempotencia — evita duplicados)
        db.execute("""
            CREATE TABLE IF NOT EXISTS stripe_events (
                event_id    TEXT PRIMARY KEY,
                processed_at TEXT NOT NULL
            )
        """)
    print("[db] Base de datos inicializada ✓")

# ─────────────────────────────────────────────────────────────────────────
#  JWT (tokens de sesión)
# ─────────────────────────────────────────────────────────────────────────
def create_token(user_id: int, email: str) -> str:
    payload = {
        "sub": str(user_id),
        "email": email,
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRE_HOURS),
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "Sesión expirada — inicia sesión de nuevo")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Token inválido")

async def get_current_user(authorization: str = Header(None)) -> dict:
    """Dependency: extrae el usuario del token Bearer."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "No autenticado")
    token = authorization.split(" ", 1)[1]
    payload = decode_token(token)
    with get_db() as db:
        row = db.execute("SELECT * FROM users WHERE id = ?", (payload["sub"],)).fetchone()
    if not row:
        raise HTTPException(401, "Usuario no encontrado")
    return dict(row)

# ─────────────────────────────────────────────────────────────────────────
#  RATE LIMITING (simple, en memoria — evita abuso de registro/verificación)
# ─────────────────────────────────────────────────────────────────────────
_rate_buckets: dict = {}

def rate_limit(key: str, max_attempts: int, window_seconds: int):
    """Lanza 429 si se exceden los intentos en la ventana."""
    now = time.time()
    bucket = _rate_buckets.get(key, [])
    bucket = [t for t in bucket if now - t < window_seconds]
    if len(bucket) >= max_attempts:
        raise HTTPException(429, "Demasiados intentos. Espera unos minutos.")
    bucket.append(now)
    _rate_buckets[key] = bucket

# ─────────────────────────────────────────────────────────────────────────
#  MODELOS (validación de entrada)
# ─────────────────────────────────────────────────────────────────────────
class RegisterInput(BaseModel):
    email: EmailStr
    name: str
    password: str
    language: str = "es"

    @field_validator("name")
    @classmethod
    def name_valid(cls, v):
        v = v.strip()
        if len(v) < 2:
            raise ValueError("El nombre debe tener al menos 2 caracteres")
        if len(v) > 80:
            raise ValueError("El nombre es demasiado largo")
        return v

    @field_validator("password")
    @classmethod
    def password_strong(cls, v):
        if len(v) < 8:
            raise ValueError("La contraseña debe tener al menos 8 caracteres")
        if not re.search(r"[A-Za-z]", v) or not re.search(r"\d", v):
            raise ValueError("La contraseña debe incluir letras y números")
        return v

    @field_validator("language")
    @classmethod
    def lang_valid(cls, v):
        return v if v in ("es", "en") else "es"

class VerifyInput(BaseModel):
    email: EmailStr
    code: str

class LoginInput(BaseModel):
    email: EmailStr
    password: str

class ResendInput(BaseModel):
    email: EmailStr

# ─────────────────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────────────────
def generate_code() -> str:
    """Código de 6 dígitos aleatorio y seguro."""
    return f"{secrets.randbelow(1_000_000):06d}"

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# ─────────────────────────────────────────────────────────────────────────
#  APP
# ─────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Liberato Community — Auth API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # en producción, restringir a tu dominio
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    init_db()
    print("[auth] Liberato Auth API iniciada ✓")

@app.get("/")
async def root():
    return {"service": "Liberato Community Auth", "version": "1.0", "status": "online"}

@app.get("/api/health")
async def health():
    return {
        "status": "online",
        "stripe_configured": bool(STRIPE_SECRET_KEY),
        "email_configured": bool(os.getenv("GMAIL_USER")),
    }

# ═══════════════════════════════════════════════════════════════════════════
#  ENDPOINTS DE AUTENTICACIÓN
# ═══════════════════════════════════════════════════════════════════════════

@app.post("/api/auth/register")
async def register(data: RegisterInput, request: Request):
    """
    Registra un nuevo usuario, genera código de verificación y envía email.
    El usuario empieza como 'free' y no verificado.
    """
    rate_limit(f"register:{request.client.host}", max_attempts=5, window_seconds=600)

    email = data.email.lower().strip()

    with get_db() as db:
        existing = db.execute("SELECT id, verified FROM users WHERE email = ?", (email,)).fetchone()
        if existing:
            if existing["verified"]:
                raise HTTPException(409, "Este correo ya está registrado. Inicia sesión.")
            # Existe pero no verificado → regenerar código y reenviar
            code = generate_code()
            expires = (datetime.now(timezone.utc) + timedelta(minutes=CODE_EXPIRE_MINUTES)).isoformat()
            db.execute(
                "UPDATE users SET verify_code = ?, verify_expires = ?, name = ?, password_hash = ?, language = ? WHERE email = ?",
                (code, expires, data.name, hash_password(data.password), data.language, email)
            )
        else:
            code = generate_code()
            expires = (datetime.now(timezone.utc) + timedelta(minutes=CODE_EXPIRE_MINUTES)).isoformat()
            db.execute(
                """INSERT INTO users (email, name, password_hash, plan, verified, language, verify_code, verify_expires, created_at)
                   VALUES (?, ?, ?, 'free', 0, ?, ?, ?, ?)""",
                (email, data.name, hash_password(data.password), data.language, code, expires, now_iso())
            )

    # Enviar email de verificación (import diferido para evitar dependencia circular)
    try:
        from emails import send_verification_email
        send_verification_email(email, data.name, code, data.language)
    except Exception as e:
        print(f"[register] Error enviando email: {e}")
        # No bloqueamos el registro; el usuario puede pedir reenvío

    return {
        "success": True,
        "message": "Código de verificación enviado a tu correo",
        "email": email,
        "expires_in_minutes": CODE_EXPIRE_MINUTES,
    }


@app.post("/api/auth/verify")
async def verify(data: VerifyInput, request: Request):
    """
    Valida el código de 6 dígitos. Si es correcto y no expiró, marca al
    usuario como verificado y envía el email de bienvenida (libre).
    """
    rate_limit(f"verify:{request.client.host}", max_attempts=10, window_seconds=600)

    email = data.email.lower().strip()
    code = data.code.strip()

    with get_db() as db:
        user = db.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if not user:
            raise HTTPException(404, "Usuario no encontrado")
        if user["verified"]:
            return {"success": True, "message": "Cuenta ya verificada", "already_verified": True}
        if not user["verify_code"] or user["verify_code"] != code:
            raise HTTPException(400, "Código incorrecto")
        # Verificar expiración
        expires = datetime.fromisoformat(user["verify_expires"])
        if datetime.now(timezone.utc) > expires:
            raise HTTPException(400, "El código expiró. Solicita uno nuevo.")

        # Marcar como verificado, limpiar el código
        db.execute(
            "UPDATE users SET verified = 1, verify_code = NULL, verify_expires = NULL, last_login = ? WHERE email = ?",
            (now_iso(), email)
        )

    # Email de bienvenida — Comunidad Libre (todos empiezan en free)
    try:
        from emails import send_welcome_free_email
        send_welcome_free_email(email, user["name"], user["language"])
    except Exception as e:
        print(f"[verify] Error enviando bienvenida: {e}")

    # Generar token de sesión
    token = create_token(user["id"], email)
    return {
        "success": True,
        "message": "Cuenta verificada correctamente",
        "token": token,
        "user": {"email": email, "name": user["name"], "plan": "free", "language": user["language"]},
    }


@app.post("/api/auth/resend-code")
async def resend_code(data: ResendInput, request: Request):
    """Reenvía el código de verificación si expiró o se perdió."""
    rate_limit(f"resend:{request.client.host}", max_attempts=3, window_seconds=600)

    email = data.email.lower().strip()
    with get_db() as db:
        user = db.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if not user:
            raise HTTPException(404, "Usuario no encontrado")
        if user["verified"]:
            raise HTTPException(400, "Esta cuenta ya está verificada")
        code = generate_code()
        expires = (datetime.now(timezone.utc) + timedelta(minutes=CODE_EXPIRE_MINUTES)).isoformat()
        db.execute("UPDATE users SET verify_code = ?, verify_expires = ? WHERE email = ?", (code, expires, email))

    try:
        from emails import send_verification_email
        send_verification_email(email, user["name"], code, user["language"])
    except Exception as e:
        print(f"[resend] Error: {e}")

    return {"success": True, "message": "Nuevo código enviado", "expires_in_minutes": CODE_EXPIRE_MINUTES}


@app.post("/api/auth/login")
async def login(data: LoginInput, request: Request):
    """Autentica con email + contraseña. Devuelve token JWT."""
    rate_limit(f"login:{request.client.host}", max_attempts=10, window_seconds=300)

    email = data.email.lower().strip()
    with get_db() as db:
        user = db.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if not user or not verify_password(data.password, user["password_hash"]):
            raise HTTPException(401, "Correo o contraseña incorrectos")
        if not user["verified"]:
            raise HTTPException(403, "Verifica tu correo antes de iniciar sesión")
        db.execute("UPDATE users SET last_login = ? WHERE email = ?", (now_iso(), email))

    token = create_token(user["id"], email)
    return {
        "success": True,
        "token": token,
        "user": {
            "email": email,
            "name": user["name"],
            "plan": user["plan"],
            "language": user["language"],
            "plan_expires": user["plan_expires"],
        },
    }


@app.get("/api/auth/me")
async def me(user: dict = Depends(get_current_user)):
    """Devuelve los datos del usuario autenticado y su plan."""
    return {
        "email": user["email"],
        "name": user["name"],
        "plan": user["plan"],
        "verified": bool(user["verified"]),
        "language": user["language"],
        "plan_expires": user["plan_expires"],
        "created_at": user["created_at"],
        "is_premium": user["plan"] == "premium",
    }

# ═══════════════════════════════════════════════════════════════════════════
#  STRIPE — Checkout + Webhooks (pagos y reembolsos)
# ═══════════════════════════════════════════════════════════════════════════
#  Flujo:
#   1. Usuario verificado pulsa "Hacerse Premium" → /api/stripe/create-checkout
#   2. Se le redirige a Stripe Checkout (página segura de Stripe)
#   3. Paga → Stripe envía webhook 'checkout.session.completed'
#   4. El webhook marca al usuario como 'premium' → grupo de paga
#   5. Recibe email de bienvenida premium
#   6. Si pide reembolso/cancela → webhook lo devuelve a 'free'
# ═══════════════════════════════════════════════════════════════════════════

def _verify_stripe_signature(payload: bytes, sig_header: str) -> dict:
    """
    Verifica la firma del webhook de Stripe SIN la librería stripe
    (implementación manual del esquema de firma de Stripe).
    Esto evita una dependencia pesada y funciona idéntico.
    """
    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(500, "Webhook secret no configurado")
    try:
        # El header tiene formato: t=timestamp,v1=signature
        parts = dict(p.split("=", 1) for p in sig_header.split(","))
        timestamp = parts.get("t")
        signature = parts.get("v1")
        if not timestamp or not signature:
            raise ValueError("Header de firma malformado")
        # Stripe firma: timestamp + "." + payload
        signed_payload = f"{timestamp}.{payload.decode()}"
        expected = hmac.new(
            STRIPE_WEBHOOK_SECRET.encode(),
            signed_payload.encode(),
            hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(expected, signature):
            raise ValueError("Firma inválida")
        # Tolerancia de tiempo: rechazar eventos de más de 5 minutos (anti-replay)
        if abs(time.time() - int(timestamp)) > 300:
            raise ValueError("Timestamp fuera de rango")
        return json.loads(payload.decode())
    except Exception as e:
        raise HTTPException(400, f"Firma de webhook inválida: {e}")


@app.post("/api/stripe/create-checkout")
async def create_checkout(user: dict = Depends(get_current_user)):
    """
    Crea una sesión de Stripe Checkout para el plan premium ($199/mes).
    Devuelve la URL a la que redirigir al usuario.
    Requiere la librería 'stripe' (pip install stripe).
    """
    if not STRIPE_SECRET_KEY or not STRIPE_PRICE_ID:
        raise HTTPException(503, "Stripe no está configurado aún")
    if user["plan"] == "premium":
        raise HTTPException(400, "Ya tienes el plan premium activo")

    try:
        import stripe
        stripe.api_key = STRIPE_SECRET_KEY
        session = stripe.checkout.Session.create(
            mode="subscription",
            line_items=[{"price": STRIPE_PRICE_ID, "quantity": 1}],
            customer_email=user["email"],
            # metadata: vincula la sesión con nuestro usuario (clave para el webhook)
            metadata={"user_id": str(user["id"]), "email": user["email"]},
            subscription_data={"metadata": {"user_id": str(user["id"]), "email": user["email"]}},
            success_url=f"{BASE_URL}/cuenta?stripe=success",
            cancel_url=f"{BASE_URL}/cuenta?stripe=cancel",
        )
        return {"checkout_url": session.url}
    except Exception as e:
        print(f"[stripe] Error creando checkout: {e}")
        raise HTTPException(500, f"Error al crear sesión de pago: {e}")


@app.post("/api/stripe/webhook")
async def stripe_webhook(request: Request):
    """
    Recibe eventos de Stripe. Verifica la firma, procesa el evento de forma
    idempotente (evita duplicados), y actualiza el plan del usuario.

    Eventos manejados:
      • checkout.session.completed  → usuario pagó → marcar premium + bienvenida
      • customer.subscription.deleted → canceló → volver a free
      • charge.refunded             → reembolso → volver a free
      • invoice.payment_failed      → pago falló → (log, opcional notificar)
    """
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")
    event = _verify_stripe_signature(payload, sig_header)

    event_id = event.get("id")
    event_type = event.get("type")

    # IDEMPOTENCIA: si ya procesamos este evento, no repetir
    with get_db() as db:
        seen = db.execute("SELECT 1 FROM stripe_events WHERE event_id = ?", (event_id,)).fetchone()
        if seen:
            return {"status": "already_processed"}
        db.execute("INSERT INTO stripe_events (event_id, processed_at) VALUES (?, ?)", (event_id, now_iso()))

    obj = event.get("data", {}).get("object", {})

    # ── Usuario completó el pago → PREMIUM ──────────────────────────────────
    if event_type == "checkout.session.completed":
        email = (obj.get("customer_email") or obj.get("metadata", {}).get("email", "")).lower()
        customer = obj.get("customer")
        subscription = obj.get("subscription")
        # Renovación: +31 días desde hoy (mensual)
        plan_expires = (datetime.now(timezone.utc) + timedelta(days=31)).isoformat()
        if email:
            with get_db() as db:
                u = db.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
                if u:
                    db.execute(
                        "UPDATE users SET plan = 'premium', stripe_customer = ?, stripe_sub = ?, plan_expires = ? WHERE email = ?",
                        (customer, subscription, plan_expires, email)
                    )
                    print(f"[stripe] {email} → PREMIUM (grupo de paga)")
                    # Email de bienvenida premium
                    try:
                        from emails import send_welcome_premium_email
                        send_welcome_premium_email(email, u["name"], u["language"])
                    except Exception as e:
                        print(f"[stripe] Error bienvenida premium: {e}")

    # ── Renovación mensual exitosa → extender expiración ───────────────────
    elif event_type == "invoice.payment_succeeded":
        # Solo en renovaciones recurrentes (no la primera, ya manejada arriba)
        if obj.get("billing_reason") == "subscription_cycle":
            customer = obj.get("customer")
            plan_expires = (datetime.now(timezone.utc) + timedelta(days=31)).isoformat()
            with get_db() as db:
                db.execute(
                    "UPDATE users SET plan = 'premium', plan_expires = ? WHERE stripe_customer = ?",
                    (plan_expires, customer)
                )
                print(f"[stripe] Renovación: customer {customer} extendido")

    # ── Suscripción cancelada → volver a FREE ──────────────────────────────
    elif event_type == "customer.subscription.deleted":
        customer = obj.get("customer")
        with get_db() as db:
            u = db.execute("SELECT email FROM users WHERE stripe_customer = ?", (customer,)).fetchone()
            db.execute(
                "UPDATE users SET plan = 'free', stripe_sub = NULL, plan_expires = NULL WHERE stripe_customer = ?",
                (customer,)
            )
            if u:
                print(f"[stripe] {u['email']} → FREE (suscripción cancelada)")

    # ── Reembolso → volver a FREE ──────────────────────────────────────────
    elif event_type == "charge.refunded":
        customer = obj.get("customer")
        if customer:
            with get_db() as db:
                u = db.execute("SELECT email FROM users WHERE stripe_customer = ?", (customer,)).fetchone()
                db.execute(
                    "UPDATE users SET plan = 'free', stripe_sub = NULL, plan_expires = NULL WHERE stripe_customer = ?",
                    (customer,)
                )
                if u:
                    print(f"[stripe] {u['email']} → FREE (reembolso procesado)")

    # ── Pago fallido → log (opcional: notificar al usuario) ────────────────
    elif event_type == "invoice.payment_failed":
        customer = obj.get("customer")
        print(f"[stripe] ⚠️ Pago fallido para customer {customer}")

    # Siempre devolver 200 para que Stripe no reintente innecesariamente
    return {"status": "processed", "event": event_type}
