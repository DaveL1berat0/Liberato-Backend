/* Liberato Community — consentimiento de cookies / analítica.
   Carga Metricool SOLO tras aceptar. Las cookies esenciales (sesión) no requieren consentimiento.
   Reemplaza la inyección inline de Metricool que antes cargaba sin consentir (auditoría 29-oct). */
(function(){
  var KEY='lbc_cookie_consent';                       // 'accepted' | 'rejected'
  var MC_HASH='70c4ae0378b29f36cc2348fb8b674472';
  function loadMetricool(){
    if(window.__lbcMC)return; window.__lbcMC=true;
    try{
      var h=document.getElementsByTagName('head')[0], c=document.createElement('script');
      c.type='text/javascript'; c.src='https://tracker.metricool.com/resources/be.js';
      c.onreadystatechange=c.onload=function(){ try{ beTracker.t({hash:MC_HASH}); }catch(e){} };
      h.appendChild(c);
    }catch(e){}
  }
  var choice=null; try{choice=localStorage.getItem(KEY);}catch(e){}
  if(choice==='accepted'){ loadMetricool(); return; }
  if(choice==='rejected'){ return; }
  function set(v){ try{localStorage.setItem(KEY,v);}catch(e){} }
  function banner(){
    if(document.getElementById('lbc-cc'))return;
    var d=document.createElement('div');
    d.id='lbc-cc'; d.setAttribute('role','dialog'); d.setAttribute('aria-live','polite'); d.setAttribute('aria-label','Aviso de cookies');
    // v101: mismo lenguaje que el resto del sitio (tokens de lbc-ui.css, con valor de reserva donde no esté cargado);
    // los dos botones miden 44 px y lo mismo de ancho en móvil. El texto y la lógica no cambian.
    d.style.cssText='position:fixed;left:12px;right:12px;bottom:calc(12px + env(safe-area-inset-bottom,0px));z-index:2147483000;max-width:720px;margin:0 auto;box-sizing:border-box;'
      +'background:var(--lbc-bg-3,#10141F);border:1px solid var(--lbc-line-2,rgba(255,255,255,0.14));border-radius:16px;'
      +'box-shadow:var(--lbc-sh-3,0 28px 64px -24px rgba(0,0,0,0.8));padding:16px 18px;'
      +'font-family:var(--lbc-font-sans,-apple-system,system-ui,"DM Sans",sans-serif);color:var(--lbc-ink,#F3F0E7);'
      +'display:flex;flex-wrap:wrap;align-items:center;gap:12px 16px;';
    var BTN='cursor:pointer;font-family:inherit;font-size:14px;min-height:44px;padding:0 18px;border-radius:12px;flex:1 1 0;white-space:nowrap;-webkit-appearance:none;appearance:none;';
    d.innerHTML=
      '<div style="flex:1 1 300px;min-width:230px;font-size:14px;line-height:1.5;color:var(--lbc-ink-2,rgba(243,240,231,0.74));">'
      +'Usamos cookies esenciales para tu sesión y, con tu permiso, analítica para mejorar el sitio. '
      +'<a href="politicas.html#privacidad" style="color:var(--lbc-gold-2,#E7CC74);text-decoration:underline;text-underline-offset:3px;">Más información</a>.'
      +'</div>'
      +'<div style="display:flex;gap:8px;flex:1 1 220px;max-width:340px;margin-left:auto;">'
      +'<button type="button" id="lbc-cc-no" style="'+BTN+'font-weight:600;border:1px solid var(--lbc-line-2,rgba(255,255,255,0.14));background:var(--lbc-fill,rgba(255,255,255,0.04));color:var(--lbc-ink-2,rgba(243,240,231,0.74));">Rechazar</button>'
      +'<button type="button" id="lbc-cc-yes" style="'+BTN+'font-weight:700;border:1px solid rgba(255,255,255,0.10);background:var(--lbc-primary-bg,linear-gradient(180deg,#E7CC74 0%,#CCA94F 58%,#B8933A 130%));color:var(--lbc-on-gold,#0B0B12);">Aceptar</button>'
      +'</div>';
    document.body.appendChild(d);
    document.getElementById('lbc-cc-yes').onclick=function(){ set('accepted'); if(d.parentNode)d.parentNode.removeChild(d); loadMetricool(); };
    document.getElementById('lbc-cc-no').onclick=function(){ set('rejected'); if(d.parentNode)d.parentNode.removeChild(d); };
  }
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded', banner); else banner();
})();
