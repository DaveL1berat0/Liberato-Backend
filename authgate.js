/* Liberato Community — AUTH GATE (frontend).
   1) Adjunta Authorization: Bearer <lbc_token> a TODA llamada a /api/* (para que la
      data premium — ahora cerrada en el servidor — llegue al que sí pagó).
   2) Si el servidor responde 401 (sin sesión) o 402 (sin premium): muestra el paywall
      si la página lo tiene, si no redirige al home (regla: un no-pagador solo ve el home).
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
      if(isApi && (r.status === 401 || r.status === 402)) _bounce();
      return r;
    });
  };
})();
