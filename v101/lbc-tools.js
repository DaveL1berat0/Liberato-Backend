/* Liberato Community · capa de herramientas de miembros (lbc-tools.js · v101).
   Solo interfaz: en móvil deja a la vista la herramienta activa de la navegación
   y difumina el borde de la fila solo por el lado donde queda algo por ver.
   No hace llamadas de red ni toca datos. (Al final: bloque de opiniones de Trustpilot,
   inactivo hasta que se escriban los identificadores del widget; ver su comentario.) */
(function(){
  if(window.__lbcTools) return; window.__lbcTools = true;
  function init(){
    var fila = document.querySelector('.lbcn-links');
    if(!fila) return;
    function bordes(){
      var max = fila.scrollWidth - fila.clientWidth;
      fila.classList.toggle('lbcn-mas-izq', fila.scrollLeft > 4);
      fila.classList.toggle('lbcn-mas-der', max - fila.scrollLeft > 4);
    }
    function centrar(){
      var act = fila.querySelector('.lbcn-active');
      if(act && fila.scrollWidth > fila.clientWidth + 1){
        var a = act.getBoundingClientRect(), f = fila.getBoundingClientRect();
        var destino = fila.scrollLeft + (a.left - f.left) - (f.width - a.width) / 2;
        fila.scrollLeft = Math.max(0, destino);
      }
      bordes();
    }
    fila.addEventListener('scroll', bordes, {passive:true});
    window.addEventListener('resize', bordes, {passive:true});
    centrar();
    setTimeout(centrar, 400);   // tras cargar fuentes y el interruptor de tema, que cambian anchos
  }
  if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();

/* ── Opiniones · widget gratuito «Review Collector» de Trustpilot (v101 · cierre) ──────────────
   Un bloque discreto «¿Qué te parece Liberato Community?» al final del panel «Mi perfil»,
   una sola vez por página y solo en las herramientas de miembros (no en la portada).
   · Se monta la primera vez que se abre el panel: nada de Trustpilot se pide antes.
   · Consentimiento (mismo criterio que consent.js): con las cookies aceptadas el widget se carga
     al abrir el panel; si no, se muestra un botón y el script de Trustpilot solo se pide al pulsarlo.
   · Sin estrellas, notas ni logotipos escritos a mano: solo el widget oficial.
   PARA ACTIVARLO: copiar del código que da Trustpilot Business (Share & promote → Website widgets →
   Review Collector) los valores data-businessunit-id y data-template-id (y, si cambian, data-locale,
   data-style-height y el href del enlace). Mientras businessUnitId o templateId estén vacíos,
   el bloque no existe y no se carga nada de terceros. */
(function(){
  var CFG = window.LBC_TRUSTPILOT || {
    businessUnitId: '',          // data-businessunit-id del código de Trustpilot
    templateId: '',              // data-template-id del código de Trustpilot (Review Collector)
    locale: 'es-ES',             // data-locale
    height: '52px',              // data-style-height
    reviewUrl: 'https://www.trustpilot.com/review/liberatocommunity.com'   // href del enlace de reserva del widget
  };
  var SRC = 'https://widget.trustpilot.com/bootstrap/v5/tp.widget.bootstrap.min.js';
  var montado = false;
  function idioma(){ try{ return localStorage.getItem('lbc_lang') === 'en' ? 'en' : 'es'; }catch(e){ return 'es'; } }
  function consentido(){ try{ return localStorage.getItem('lbc_cookie_consent') === 'accepted'; }catch(e){ return false; } }
  function texto(el, es, en){ el.setAttribute('data-es', es); el.setAttribute('data-en', en); el.textContent = idioma() === 'en' ? en : es; }
  function cargar(caja, hueco){
    var w = document.createElement('div');
    w.className = 'trustpilot-widget';
    w.setAttribute('data-locale', CFG.locale || 'es-ES');
    w.setAttribute('data-template-id', CFG.templateId);
    w.setAttribute('data-businessunit-id', CFG.businessUnitId);
    w.setAttribute('data-style-height', CFG.height || '52px');
    w.setAttribute('data-style-width', '100%');
    if(CFG.reviewUrl){ var a = document.createElement('a'); a.href = CFG.reviewUrl; a.target = '_blank'; a.rel = 'noopener'; a.textContent = 'Trustpilot'; w.appendChild(a); }
    hueco.innerHTML = ''; hueco.appendChild(w); caja.classList.add('is-cargado');
    function pintar(){ try{ if(window.Trustpilot && window.Trustpilot.loadFromElement) window.Trustpilot.loadFromElement(w, true); }catch(e){} }
    if(window.Trustpilot){ pintar(); return; }
    var s = document.createElement('script'); s.src = SRC; s.async = true; s.onload = pintar;
    document.head.appendChild(s);
  }
  function montar(){
    if(montado) return;
    var cfg = window.LBC_TRUSTPILOT || CFG; CFG = cfg;
    if(!cfg.businessUnitId || !cfg.templateId) return;
    var panel = document.querySelector('#profile-drawer .pd-scroll'); if(!panel) return;
    montado = true;
    var caja = document.createElement('div'); caja.className = 'pd-sec lbc-opina'; caja.id = 'lbc-opina';
    var t = document.createElement('div'); t.className = 'pd-sec-t'; texto(t, '¿Qué te parece Liberato Community?', 'What do you think of Liberato Community?');
    var hueco = document.createElement('div'); hueco.className = 'lbc-opina__widget';
    caja.appendChild(t); caja.appendChild(hueco);
    var salir = panel.querySelector('.pd-logout');
    if(salir) panel.insertBefore(caja, salir); else panel.appendChild(caja);
    if(consentido()){ cargar(caja, hueco); return; }
    var b = document.createElement('button'); b.type = 'button'; b.className = 'lbc-opina__btn';
    texto(b, 'Dejar mi opinión en Trustpilot', 'Leave my review on Trustpilot');
    var n = document.createElement('p'); n.className = 'lbc-opina__nota';
    texto(n, 'Al pulsar se carga el formulario de Trustpilot, un servicio externo.', 'Pressing it loads the Trustpilot form, an external service.');
    b.addEventListener('click', function(){ cargar(caja, hueco); });
    hueco.appendChild(b); hueco.appendChild(n);
  }
  function init(){
    var panel = document.getElementById('profile-drawer'); if(!panel) return;
    if(panel.classList.contains('open')){ montar(); return; }
    try{
      var ob = new MutationObserver(function(){ if(panel.classList.contains('open')){ montar(); if(montado) ob.disconnect(); } });
      ob.observe(panel, {attributes:true, attributeFilter:['class']});
    }catch(e){}
  }
  if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();
