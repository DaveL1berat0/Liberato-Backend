/* ═══════════════════════════════════════════════════════════════════════════
   opening_bell.js — Campana de apertura del NYSE (9:30 ET), COMPARTIDA en TODAS
   las secciones (Day Trading, Journal, Earnings, Options, Home).
   · Autónomo: inyecta su propio CSS, sintetiza la campana (Web Audio API, sin
     archivos y sin material con copyright) y muestra un popup "Market opened".
   · Suena UNA sola vez al cruzar las 9:30 ET en día de mercado (L-V, no feriado
     NYSE). El guard vive en localStorage ('lbc_openbell'), así que sólo suena una
     vez al día aunque el usuario cambie de sección.
   · Respeta el toggle de sonido de la barra (localStorage 'lbc_sound'): si está
     en 'off' NO suena, pero el popup visual SÍ aparece.
   Inclúyelo en cada página con:  <script src="opening_bell.js" defer></script>
   ═══════════════════════════════════════════════════════════════════════════ */
(function () {
  if (window.__lbcOpeningBellLoaded) return;      // evita doble carga
  window.__lbcOpeningBellLoaded = true;

  /* ── CSS del popup (auto-inyectado) ───────────────────────────────────── */
  var css =
    '.mkt-open-pop{position:fixed;top:50%;left:50%;transform:translate(-50%,-50%) scale(.9);z-index:2147483000;display:flex;flex-direction:column;align-items:center;gap:11px;padding:26px 44px;border-radius:18px;background:linear-gradient(145deg,rgba(11,14,23,.98),rgba(8,10,18,.98));border:1px solid rgba(201,168,76,.55);box-shadow:0 20px 60px rgba(0,0,0,.7),0 0 44px rgba(201,168,76,.28);opacity:0;transition:opacity .35s cubic-bezier(.16,1,.3,1),transform .35s cubic-bezier(.16,1,.3,1);pointer-events:none;font-family:ui-sans-serif,system-ui,-apple-system,sans-serif;}' +
    '.mkt-open-pop.show{opacity:1;transform:translate(-50%,-50%) scale(1);}' +
    '.mkt-open-bell{font-size:46px;line-height:1;animation:lbcBellRing .9s ease-in-out;}' +
    '.mkt-open-txt{font-family:ui-monospace,"DM Mono",SFMono-Regular,Menlo,monospace;font-size:15px;font-weight:700;letter-spacing:.15em;text-transform:uppercase;color:#C9A84C;}' +
    '@keyframes lbcBellRing{0%,100%{transform:rotate(0)}12%{transform:rotate(17deg)}26%{transform:rotate(-14deg)}40%{transform:rotate(10deg)}54%{transform:rotate(-7deg)}68%{transform:rotate(4deg)}82%{transform:rotate(-2deg)}}';
  try {
    var st = document.createElement('style');
    st.textContent = css;
    (document.head || document.documentElement).appendChild(st);
  } catch (e) {}

  /* ── Sonido (compartido con el toggle 🔔 de la barra) ─────────────────── */
  function soundOn() { try { return localStorage.getItem('lbc_sound') !== 'off'; } catch (e) { return true; } }
  var _ac = null;
  function ensureAudio() {
    try {
      if (!_ac) _ac = new (window.AudioContext || window.webkitAudioContext)();
      if (_ac.state === 'suspended') _ac.resume();
      return _ac;
    } catch (e) { return null; }
  }
  // Los navegadores bloquean el audio hasta el primer gesto → lo desbloqueamos.
  ['pointerdown', 'keydown', 'touchstart'].forEach(function (ev) {
    window.addEventListener(ev, function u() { ensureAudio(); window.removeEventListener(ev, u); }, { once: true });
  });

  /* ── Campana de LATÓN: parciales INARMÓNICOS de campana real + transitorio
        de golpe (clapper) + decaimiento largo. 3 toques (ding-ding-ding),
        como el bell del NYSE. Todo sintetizado, sin archivos. ──────────── */
  function playBell() {
    if (!soundOn()) return;
    var ac = ensureAudio(); if (!ac) return;
    var t0 = ac.currentTime;
    var base = 620; // Hz — latón BRILLANTE (el bell del NYSE es agudo/clangoroso)
    // Parciales inarmónicos de campana de latón con ÉNFASIS en agudos (brillo):
    var P = [
      { r: 1.00, a: 0.50, d: 1.7 }, { r: 2.00, a: 0.50, d: 1.4 },
      { r: 2.40, a: 0.36, d: 1.1 }, { r: 3.00, a: 0.30, d: 0.9 },
      { r: 4.20, a: 0.28, d: 0.7 }, { r: 5.40, a: 0.22, d: 0.55 },
      { r: 6.80, a: 0.16, d: 0.42 }, { r: 8.20, a: 0.11, d: 0.32 }
    ];
    // Limitador de volumen global para que el repique no sature
    var master = ac.createGain(); master.gain.value = 0.85; master.connect(ac.destination);
    function strike(delay, mul) {
      var s = t0 + delay;
      // Transitorio del clapper: ráfaga corta de ruido muy agudo (metal al golpe)
      try {
        var len = Math.floor(ac.sampleRate * 0.03);
        var nb = ac.createBuffer(1, len, ac.sampleRate), ch = nb.getChannelData(0);
        for (var i = 0; i < len; i++) ch[i] = (Math.random() * 2 - 1) * Math.pow(1 - i / len, 3);
        var ns = ac.createBufferSource(); ns.buffer = nb;
        var hp = ac.createBiquadFilter(); hp.type = 'highpass'; hp.frequency.value = 3500;
        var ng = ac.createGain(); ng.gain.value = 0.09 * mul;
        ns.connect(hp); hp.connect(ng); ng.connect(master);
        ns.start(s); ns.stop(s + 0.04);
      } catch (e) {}
      P.forEach(function (p) {
        var o = ac.createOscillator(), g = ac.createGain();
        o.type = 'sine'; o.frequency.value = base * p.r;
        var peak = p.a * 0.14 * mul;
        g.gain.setValueAtTime(0.0001, s);
        g.gain.exponentialRampToValueAtTime(peak, s + 0.003);
        g.gain.exponentialRampToValueAtTime(0.0001, s + p.d);
        o.connect(g); g.connect(master);
        o.start(s); o.stop(s + p.d + 0.05);
      });
    }
    // Repique RÁPIDO y continuo, como el bell del NYSE tocado a mano (~12 golpes
    // en ~2,3 s que se solapan en un timbre sostenido y brillante).
    var N = 12, iv = 0.19;
    for (var k = 0; k < N; k++) strike(k * iv, k === 0 ? 1.0 : 0.72);
  }

  /* ── Popup "Market opened" (~2.4s, auto-cierre) ───────────────────────── */
  function popup() {
    try {
      if (document.querySelector('.mkt-open-pop')) return;
      var el = document.createElement('div');
      el.className = 'mkt-open-pop';
      el.innerHTML = '<span class="mkt-open-bell">🔔</span><span class="mkt-open-txt">Market opened</span>';
      (document.body || document.documentElement).appendChild(el);
      setTimeout(function () { el.classList.add('show'); }, 20);
      setTimeout(function () {
        el.classList.remove('show');
        setTimeout(function () { if (el.parentNode) el.parentNode.removeChild(el); }, 420);
      }, 2400);
    } catch (e) {}
  }

  /* ── Tiempo ET + feriados NYSE ────────────────────────────────────────── */
  function etNow() {
    try {
      var parts = new Intl.DateTimeFormat('en-US', {
        timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
      }).formatToParts(new Date());
      var o = {}; parts.forEach(function (x) { o[x.type] = x.value; });
      var h = o.hour === '24' ? '00' : o.hour;
      return new Date(+o.year, +o.month - 1, +o.day, +h, +o.minute, +o.second);
    } catch (e) { return new Date(); }
  }
  var HOL = {
    '2025-01-01':1,'2025-01-20':1,'2025-02-17':1,'2025-04-18':1,'2025-05-26':1,
    '2025-06-19':1,'2025-07-04':1,'2025-09-01':1,'2025-11-27':1,'2025-12-25':1,
    '2026-01-01':1,'2026-01-19':1,'2026-02-16':1,'2026-04-03':1,'2026-05-25':1,
    '2026-06-19':1,'2026-07-03':1,'2026-09-07':1,'2026-11-26':1,'2026-12-25':1
  };
  function pad(n) { return (n < 10 ? '0' : '') + n; }
  var _rangDay = (function () { try { return localStorage.getItem('lbc_openbell') || null; } catch (e) { return null; } })();

  function check() {
    try {
      var et = etNow(), dow = et.getDay();
      if (dow < 1 || dow > 5) return;                 // sólo L-V
      var key = et.getFullYear() + '-' + pad(et.getMonth() + 1) + '-' + pad(et.getDate());
      if (HOL[key]) return;                           // no en feriados NYSE
      var mins = et.getHours() * 60 + et.getMinutes();
      if (mins !== 570) return;                       // sólo durante el minuto 9:30 ET (570)
      if (_rangDay === key) return;                   // ya sonó hoy (en cualquier sección)
      _rangDay = key;
      try { localStorage.setItem('lbc_openbell', key); } catch (e) {}
      playBell(); popup();
    } catch (e) {}
  }
  setInterval(check, 5000);                           // chequeo cada 5s → captura el minuto 9:30
  window.lbcTestOpeningBell = function () { playBell(); popup(); }; // prueba manual desde consola
})();

// ── IDIOMA ES/EN compartido ──────────────────────────────────────────────────
// profileSetLang() llama a window.toggleLang(). Antes NO existía en las subpáginas de
// plataforma → el toggle "English" decía "Language updated" pero no traducía (éxito falso).
// Ahora traduce todos los elementos con data-es/data-en de la página, persiste la elección
// y la aplica al cargar. (El contenido dinámico —charts/datos— permanece en ES por diseño.)
(function () {
  function applyLang(lang) {
    try {
      var els = document.querySelectorAll('[data-es][data-en]');
      for (var i = 0; i < els.length; i++) {
        var v = els[i].getAttribute(lang === 'en' ? 'data-en' : 'data-es');
        if (v != null) els[i].textContent = v;
      }
      document.documentElement.setAttribute('lang', lang);
    } catch (e) {}
  }
  window.lbcApplyLang = applyLang;
  if (typeof window.toggleLang !== 'function') {   // no pisar el de la homepage
    window.toggleLang = function () {
      var cur = 'es'; try { cur = localStorage.getItem('lbc_lang') || 'es'; } catch (e) {}
      var next = cur === 'en' ? 'es' : 'en';
      try { localStorage.setItem('lbc_lang', next); } catch (e) {}
      applyLang(next);
      return next;
    };
  }
  function initLang() { var l = 'es'; try { l = localStorage.getItem('lbc_lang') || 'es'; } catch (e) {} if (l === 'en') applyLang('en'); }
  if (document.readyState !== 'loading') initLang();
  else document.addEventListener('DOMContentLoaded', initLang);
})();
