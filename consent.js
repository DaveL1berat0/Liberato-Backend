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
    d.style.cssText='position:fixed;left:12px;right:12px;bottom:12px;z-index:2147483000;max-width:720px;margin:0 auto;'
      +'background:#0C0F17;border:0.5px solid rgba(201,168,76,0.30);border-radius:14px;'
      +'box-shadow:0 10px 40px -8px rgba(0,0,0,0.6);padding:16px 18px;'
      +'font-family:-apple-system,system-ui,"DM Sans",sans-serif;color:#EFECE3;'
      +'display:flex;flex-wrap:wrap;align-items:center;gap:12px;';
    d.innerHTML=
      '<div style="flex:1 1 300px;min-width:230px;font-size:13px;line-height:1.5;color:rgba(239,236,227,0.82);">'
      +'Usamos cookies esenciales para tu sesión y, con tu permiso, analítica para mejorar el sitio. '
      +'<a href="politicas.html#privacidad" style="color:#E7CC74;text-decoration:underline;">Más información</a>.'
      +'</div>'
      +'<div style="display:flex;gap:8px;flex:0 0 auto;">'
      +'<button type="button" id="lbc-cc-no" style="cursor:pointer;font-family:inherit;font-size:12.5px;font-weight:600;padding:9px 16px;border-radius:9px;border:0.5px solid rgba(255,255,255,0.14);background:transparent;color:rgba(239,236,227,0.7);">Rechazar</button>'
      +'<button type="button" id="lbc-cc-yes" style="cursor:pointer;font-family:inherit;font-size:12.5px;font-weight:700;padding:9px 18px;border-radius:9px;border:none;background:#C9A84C;color:#0C0F17;">Aceptar</button>'
      +'</div>';
    document.body.appendChild(d);
    document.getElementById('lbc-cc-yes').onclick=function(){ set('accepted'); if(d.parentNode)d.parentNode.removeChild(d); loadMetricool(); };
    document.getElementById('lbc-cc-no').onclick=function(){ set('rejected'); if(d.parentNode)d.parentNode.removeChild(d); };
  }
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded', banner); else banner();
})();
