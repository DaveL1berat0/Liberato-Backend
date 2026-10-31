/* Liberato Community — AUTH GATE (frontend).
   1) Adjunta Authorization: Bearer <lbc_token> a TODA llamada a /api/* (para que la
      data premium — ahora cerrada en el servidor — llegue al que sí pagó).
   2) Si el servidor responde 401 (sin sesión) o 402 (sin premium): muestra el paywall
      si la página lo tiene, si no redirige al home (regla: un no-pagador solo ve el home).
      Caso especial 401 'session_invalid' = la cuenta se abrió en OTRO dispositivo (límite
      de 1 sesión) → se muestra un aviso claro y se limpia el token para no quedar en bucle.
   Debe cargarse LO MÁS TEMPRANO posible (antes que los fetch de datos de la página). */
(function(){
  if(window.__lbcAuthGate) return; window.__lbcAuthGate = true;
  var _fetch = window.fetch;
  var _bounced = false;
  function _token(){ try{ return localStorage.getItem('lbc_token') || ''; }catch(e){ return ''; } }
  function _bounce(){
    if(_bounced) return; _bounced = true;
    try{
      var pw = document.getElementById('lbc-paywall');
      if(pw){ pw.style.display = 'flex'; }
      else { location.href = 'homepage.html'; }
    }catch(e){ try{ location.href = 'homepage.html'; }catch(_){} }
  }
  // Aviso específico cuando la sesión fue expulsada por abrirse en otro dispositivo.
  function _kicked(){
    if(_bounced) return; _bounced = true;
    try{ localStorage.removeItem('lbc_token'); }catch(e){}
    function paint(){
      try{
        if(document.getElementById('lbc-kick')) return;
        var o = document.createElement('div');
        o.id = 'lbc-kick';
        o.style.cssText = 'position:fixed;inset:0;z-index:2147483647;background:rgba(8,8,14,0.92);display:flex;align-items:center;justify-content:center;padding:24px;font-family:system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;';
        o.innerHTML = '<div style="max-width:420px;width:100%;text-align:center;background:#12121C;border:1px solid rgba(201,168,76,0.35);border-radius:16px;padding:32px 26px;color:#E8E4D9;box-shadow:0 20px 60px rgba(0,0,0,0.5);">'
          + '<div style="font-size:30px;margin-bottom:12px;">🔒</div>'
          + '<div style="font-size:17px;font-weight:700;margin-bottom:10px;color:#C9A84C;">Sesión abierta en otro dispositivo</div>'
          + '<div style="font-size:13.5px;line-height:1.6;color:rgba(232,228,217,0.75);margin-bottom:22px;">Por seguridad solo permitimos <strong>una sesión activa</strong> por cuenta. Tu acceso aquí se cerró porque iniciaste sesión en otro lugar. Vuelve a entrar para continuar.</div>'
          + '<a href="auth.html" style="display:inline-block;background:#C9A84C;color:#0B0B12;font-weight:700;text-decoration:none;padding:12px 26px;border-radius:10px;font-size:14px;">Volver a iniciar sesión →</a>'
          + '</div>';
        (document.body || document.documentElement).appendChild(o);
      }catch(e){ try{ location.href = 'auth.html'; }catch(_){} }
    }
    if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', paint); else paint();
  }
  window.fetch = function(input, init){
    init = init || {};
    var url = (typeof input === 'string') ? input : ((input && input.url) || '');
    var isApi = url.indexOf('/api/') > -1;
    if(isApi){
      var t = _token();
      if(t){
        try{
          var h = new Headers(init.headers || (typeof input !== 'string' && input && input.headers) || {});
          if(!h.has('Authorization')) h.set('Authorization', 'Bearer ' + t);
          init.headers = h;
        }catch(e){}
      }
    }
    return _fetch.call(this, input, init).then(function(r){
      if(isApi && r.status === 401){
        // Distinguir "expulsado por otro dispositivo" de un 401 normal. Clonamos (barato,
        // solo en 401) para leer el code sin consumir el cuerpo que usa la página.
        try{
          r.clone().json().then(function(d){
            if(d && d.code === 'session_invalid') _kicked(); else _bounce();
          }).catch(function(){ _bounce(); });
        }catch(e){ _bounce(); }
      } else if(isApi && r.status === 402){
        _bounce();
      }
      return r;
    });
  };
})();
