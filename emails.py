"""
═══════════════════════════════════════════════════════════════════════════
  LIBERATO COMMUNITY — Módulo de Emails (Gmail SMTP)
═══════════════════════════════════════════════════════════════════════════

  Tres plantillas, bilingües (español/inglés), con branding Liberato:
    1. Verificación (código 6 dígitos)
    2. Bienvenida Comunidad Libre (+ llamado sutil a upgrade)
    3. Bienvenida Acceso Completo (premium)

  Branding: fondo oscuro #06070D, dorado #CCA94F / #E7CC74 (NO el azul de GEX),
  fuentes serif para títulos, mono para acentos.

  PARA CONECTAR:
    GMAIL_USER          → tu correo Gmail
    GMAIL_APP_PASSWORD  → App Password de Google (16 chars, Configuración →
                          Seguridad → Verificación en 2 pasos → Contraseñas de aplicación)

  NOTA: Gmail SMTP soporta ~500 emails/día. Suficiente para verificación y
  bienvenida. Para envíos masivos de noticias a futuro, migrar a Resend/Brevo.
═══════════════════════════════════════════════════════════════════════════
"""

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

GMAIL_USER         = os.getenv("GMAIL_USER", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
BASE_URL           = os.getenv("BASE_URL", "https://liberato.community")
DISCORD_URL        = os.getenv("DISCORD_URL", "https://discord.gg/liberato")
YOUTUBE_URL        = os.getenv("YOUTUBE_URL", "https://youtube.com/@liberato")

# ─────────────────────────────────────────────────────────────────────────
#  ENVÍO BASE
# ─────────────────────────────────────────────────────────────────────────
def _send(to_email: str, subject: str, html_body: str):
    """Envía un email HTML vía Gmail SMTP."""
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        print(f"[email] ⚠️ Gmail no configurado — no se envió a {to_email}")
        print(f"[email] (Asunto era: {subject})")
        return False
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"Liberato Community <{GMAIL_USER}>"
    msg["To"] = to_email
    msg.attach(MIMEText(html_body, "html"))
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_USER, to_email, msg.as_string())
        print(f"[email] ✓ Enviado a {to_email}: {subject}")
        return True
    except Exception as e:
        print(f"[email] ✗ Error enviando a {to_email}: {e}")
        return False

# ─────────────────────────────────────────────────────────────────────────
#  PLANTILLA BASE (estética Liberato — dark + dorado)
# ─────────────────────────────────────────────────────────────────────────
def _wrap(inner_html: str) -> str:
    """Envuelve el contenido en el layout Liberato."""
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#06070D;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#06070D;padding:40px 16px;">
    <tr><td align="center">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:520px;background:linear-gradient(160deg,#0B0E17,#08090F);border:1px solid rgba(204,169,79,0.18);border-radius:16px;overflow:hidden;">
        <!-- Header con logo -->
        <tr><td style="padding:36px 40px 24px;text-align:center;border-bottom:1px solid rgba(255,255,255,0.04);">
          <div style="font-size:26px;font-weight:700;letter-spacing:0.08em;color:#E7CC74;font-family:Georgia,serif;">LIBERATO</div>
          <div style="font-size:10px;font-weight:600;letter-spacing:0.28em;color:#6B7280;margin-top:4px;text-transform:uppercase;">Community</div>
        </td></tr>
        <!-- Contenido -->
        <tr><td style="padding:36px 40px;">
          {inner_html}
        </td></tr>
        <!-- Footer -->
        <tr><td style="padding:24px 40px 32px;text-align:center;border-top:1px solid rgba(255,255,255,0.04);">
          <div style="font-size:11px;color:#4B5563;line-height:1.6;">
            Freedom through discipline and skill.<br>
            Liberato Community © 2026 · Todos los derechos reservados
          </div>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

# ═══════════════════════════════════════════════════════════════════════════
#  1 · EMAIL DE VERIFICACIÓN
# ═══════════════════════════════════════════════════════════════════════════
def send_verification_email(to_email: str, name: str, code: str, lang: str = "es"):
    if lang == "en":
        subject = "Verify your account · Liberato Community"
        inner = f"""
          <div style="font-size:13px;color:#9CA3AF;margin-bottom:8px;">Hi {name},</div>
          <div style="font-size:22px;font-weight:600;color:#F3F0E7;margin-bottom:20px;font-family:Georgia,serif;">Verify your email</div>
          <div style="font-size:14px;color:#9CA3AF;line-height:1.7;margin-bottom:28px;">
            Use the following code to confirm your email address and activate your account:
          </div>
          <div style="text-align:center;margin:0 0 28px;">
            <div style="display:inline-block;background:rgba(204,169,79,0.08);border:1px solid rgba(204,169,79,0.35);border-radius:12px;padding:20px 36px;">
              <div style="font-size:38px;font-weight:700;letter-spacing:0.18em;color:#E7CC74;font-family:'Courier New',monospace;">{code}</div>
            </div>
          </div>
          <div style="font-size:12px;color:#6B7280;text-align:center;">This code expires in <strong style="color:#9CA3AF;">10 minutes</strong>.</div>
        """
    else:
        subject = "Verifica tu cuenta · Liberato Community"
        inner = f"""
          <div style="font-size:13px;color:#9CA3AF;margin-bottom:8px;">Hola {name},</div>
          <div style="font-size:22px;font-weight:600;color:#F3F0E7;margin-bottom:20px;font-family:Georgia,serif;">Verifica tu correo</div>
          <div style="font-size:14px;color:#9CA3AF;line-height:1.7;margin-bottom:28px;">
            Usa el siguiente código para confirmar tu correo electrónico y activar tu cuenta:
          </div>
          <div style="text-align:center;margin:0 0 28px;">
            <div style="display:inline-block;background:rgba(204,169,79,0.08);border:1px solid rgba(204,169,79,0.35);border-radius:12px;padding:20px 36px;">
              <div style="font-size:38px;font-weight:700;letter-spacing:0.18em;color:#E7CC74;font-family:'Courier New',monospace;">{code}</div>
            </div>
          </div>
          <div style="font-size:12px;color:#6B7280;text-align:center;">Este código expira en <strong style="color:#9CA3AF;">10 minutos</strong>.</div>
        """
    return _send(to_email, subject, _wrap(inner))

# ═══════════════════════════════════════════════════════════════════════════
#  2 · BIENVENIDA — COMUNIDAD LIBRE (+ llamado a upgrade)
# ═══════════════════════════════════════════════════════════════════════════
def send_welcome_free_email(to_email: str, name: str, lang: str = "es"):
    if lang == "en":
        subject = "Welcome to Liberato Community 🎯"
        inner = f"""
          <div style="font-size:13px;color:#9CA3AF;margin-bottom:8px;">Hi {name},</div>
          <div style="font-size:24px;font-weight:600;color:#F3F0E7;margin-bottom:16px;font-family:Georgia,serif;">Welcome to the community</div>
          <div style="font-size:14px;color:#9CA3AF;line-height:1.7;margin-bottom:24px;">
            Your free account is active. Here's what you have access to:
          </div>
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
            {_benefit_row_en("Discord with active traders")}
            {_benefit_row_en("Daily market bias ideas")}
            {_benefit_row_en("High-impact market news")}
            {_benefit_row_en("Shared trading results")}
            {_benefit_row_en("Free educational content on YouTube")}
          </table>
          <div style="text-align:center;margin-bottom:28px;">
            <a href="{DISCORD_URL}" style="display:inline-block;background:#5865F2;color:#fff;text-decoration:none;font-size:13px;font-weight:600;padding:12px 28px;border-radius:8px;">Join the Discord →</a>
          </div>
          <!-- Llamado a upgrade -->
          <div style="background:rgba(204,169,79,0.06);border:1px solid rgba(204,169,79,0.2);border-radius:12px;padding:22px;margin-top:8px;">
            <div style="font-size:15px;font-weight:600;color:#E7CC74;margin-bottom:8px;font-family:Georgia,serif;">Ready to trade like a professional?</div>
            <div style="font-size:13px;color:#9CA3AF;line-height:1.6;margin-bottom:16px;">
              Full Access unlocks live trading sessions, institutional Gamma Exposure levels, real-time order flow, the AI trading journal, and the complete platform.
            </div>
            <a href="{BASE_URL}/cuenta" style="display:inline-block;background:linear-gradient(135deg,#CCA94F,#E7CC74);color:#06070D;text-decoration:none;font-size:13px;font-weight:700;padding:11px 26px;border-radius:8px;">Upgrade to Full Access →</a>
          </div>
        """
    else:
        subject = "Bienvenido a Liberato Community 🎯"
        inner = f"""
          <div style="font-size:13px;color:#9CA3AF;margin-bottom:8px;">Hola {name},</div>
          <div style="font-size:24px;font-weight:600;color:#F3F0E7;margin-bottom:16px;font-family:Georgia,serif;">Bienvenido a la comunidad</div>
          <div style="font-size:14px;color:#9CA3AF;line-height:1.7;margin-bottom:24px;">
            Tu cuenta gratuita ya está activa. Esto es lo que tienes disponible:
          </div>
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
            {_benefit_row_es("Discord con traders activos")}
            {_benefit_row_es("Ideas de bias del día")}
            {_benefit_row_es("Noticias de alto impacto")}
            {_benefit_row_es("Resultados compartidos")}
            {_benefit_row_es("Contenido educativo gratuito en YouTube")}
          </table>
          <div style="text-align:center;margin-bottom:28px;">
            <a href="{DISCORD_URL}" style="display:inline-block;background:#5865F2;color:#fff;text-decoration:none;font-size:13px;font-weight:600;padding:12px 28px;border-radius:8px;">Entrar al Discord →</a>
          </div>
          <!-- Llamado a upgrade -->
          <div style="background:rgba(204,169,79,0.06);border:1px solid rgba(204,169,79,0.2);border-radius:12px;padding:22px;margin-top:8px;">
            <div style="font-size:15px;font-weight:600;color:#E7CC74;margin-bottom:8px;font-family:Georgia,serif;">¿Listo para operar como profesional?</div>
            <div style="font-size:13px;color:#9CA3AF;line-height:1.6;margin-bottom:16px;">
              El Acceso Completo desbloquea las sesiones de trading en vivo, los niveles institucionales de Gamma Exposure, el order flow en tiempo real, el journal con IA y la plataforma completa.
            </div>
            <a href="{BASE_URL}/cuenta" style="display:inline-block;background:linear-gradient(135deg,#CCA94F,#E7CC74);color:#06070D;text-decoration:none;font-size:13px;font-weight:700;padding:11px 26px;border-radius:8px;">Mejorar a Acceso Completo →</a>
          </div>
        """
    return _send(to_email, subject, _wrap(inner))

# ═══════════════════════════════════════════════════════════════════════════
#  3 · BIENVENIDA — ACCESO COMPLETO (premium)
# ═══════════════════════════════════════════════════════════════════════════
def send_welcome_premium_email(to_email: str, name: str, lang: str = "es"):
    if lang == "en":
        subject = "Welcome to Full Access · Liberato Community ⭐"
        inner = f"""
          <div style="font-size:13px;color:#9CA3AF;margin-bottom:8px;">Hi {name},</div>
          <div style="font-size:24px;font-weight:600;color:#F3F0E7;margin-bottom:16px;font-family:Georgia,serif;">Welcome to Full Access</div>
          <div style="font-size:14px;color:#9CA3AF;line-height:1.7;margin-bottom:24px;">
            Your subscription is active. You now have everything you need to operate like a professional:
          </div>
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
            {_benefit_row_en("Full access to Liberato Community Web", gold=True)}
            {_benefit_row_en("AI-powered trading journal", gold=True)}
            {_benefit_row_en("Institutional Gamma Exposure levels", gold=True)}
            {_benefit_row_en("Real-time order flow & market reads", gold=True)}
            {_benefit_row_en("Earnings calendar with high-impact news", gold=True)}
            {_benefit_row_en("Live Day Trading — New York Market Open", gold=True)}
            {_benefit_row_en("Premium Discord with real-time signals", gold=True)}
            {_benefit_row_en("Institutional strategies & playbooks", gold=True)}
          </table>
          <div style="text-align:center;">
            <a href="{BASE_URL}/dashboard" style="display:inline-block;background:linear-gradient(135deg,#CCA94F,#E7CC74);color:#06070D;text-decoration:none;font-size:14px;font-weight:700;padding:13px 32px;border-radius:8px;">Enter the platform →</a>
          </div>
        """
    else:
        subject = "Bienvenido al Acceso Completo · Liberato Community ⭐"
        inner = f"""
          <div style="font-size:13px;color:#9CA3AF;margin-bottom:8px;">Hola {name},</div>
          <div style="font-size:24px;font-weight:600;color:#F3F0E7;margin-bottom:16px;font-family:Georgia,serif;">Bienvenido al Acceso Completo</div>
          <div style="font-size:14px;color:#9CA3AF;line-height:1.7;margin-bottom:24px;">
            Tu suscripción está activa. Ahora tienes todo lo que necesitas para operar como un profesional:
          </div>
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
            {_benefit_row_es("Acceso completo a Liberato Community Web", gold=True)}
            {_benefit_row_es("Journal de trading con Inteligencia Artificial", gold=True)}
            {_benefit_row_es("Niveles institucionales de Gamma Exposure", gold=True)}
            {_benefit_row_es("Order Flow y lecturas de mercado en tiempo real", gold=True)}
            {_benefit_row_es("Earnings Calendar con noticias de alto impacto", gold=True)}
            {_benefit_row_es("Day Trading en vivo — New York Market Open", gold=True)}
            {_benefit_row_es("Discord Premium con señales en tiempo real", gold=True)}
            {_benefit_row_es("Estrategias institucionales y playbooks", gold=True)}
          </table>
          <div style="text-align:center;">
            <a href="{BASE_URL}/dashboard" style="display:inline-block;background:linear-gradient(135deg,#CCA94F,#E7CC74);color:#06070D;text-decoration:none;font-size:14px;font-weight:700;padding:13px 32px;border-radius:8px;">Entrar a la plataforma →</a>
          </div>
        """
    return _send(to_email, subject, _wrap(inner))

# ─────────────────────────────────────────────────────────────────────────
#  HELPERS de filas de beneficios
# ─────────────────────────────────────────────────────────────────────────
def _benefit_row_es(text: str, gold: bool = False) -> str:
    check = "#E7CC74" if gold else "#2EE8A4"
    return f"""<tr><td style="padding:7px 0;">
      <span style="color:{check};font-size:14px;margin-right:10px;">✓</span>
      <span style="color:#D1D5DB;font-size:13px;">{text}</span>
    </td></tr>"""

def _benefit_row_en(text: str, gold: bool = False) -> str:
    return _benefit_row_es(text, gold)
