/* Liberato Community — sistema de tema claro/oscuro COMPARTIDO.
   - Persistencia en localStorage 'lbc_theme' (compartido entre páginas del mismo origen).
   - Aplica data-theme="light" en <html> (documentElement) antes del paint => sin flash.
   - Inyecta el toggle en #lbcn-right (member nav) o como FAB flotante si no hay nav.
   Se auto-protege para no chocar con páginas que ya definan su propio toggle (p.ej. journal). */
(function(){
  if(window.__lbcThemeInit) return; window.__lbcThemeInit=true;

  function apply(theme){
    var d=document.documentElement;
    if(theme==='light') d.setAttribute('data-theme','light'); else d.removeAttribute('data-theme');
    // sincroniza cualquier etiqueta/ícono opcional del toggle
    try{
      var ic=document.getElementById('tt-icon'), lb=document.getElementById('tt-label');
      if(ic)ic.textContent = theme==='light'?'☀️':'🌙';
      if(lb)lb.textContent = theme==='light'?'Modo Claro':'Modo Oscuro';
    }catch(e){}
    // avisa a la página (p.ej. para re-tematizar charts que pintan colores por JS)
    try{ window.dispatchEvent(new CustomEvent('lbc:theme',{detail:{theme:theme}})); }catch(e){}
  }
  if(typeof window.applyTheme!=='function') window.applyTheme=apply;
  if(typeof window.toggleTheme!=='function') window.toggleTheme=function(){
    var cur=document.documentElement.getAttribute('data-theme');
    var next = cur==='light' ? 'dark' : 'light';
    (window.applyTheme||apply)(next);
    try{localStorage.setItem('lbc_theme',next);}catch(e){}
  };

  // aplica el tema guardado lo antes posible (este script se carga en <head>)
  var saved; try{saved=localStorage.getItem('lbc_theme');}catch(e){}
  (window.applyTheme||apply)(saved||'dark');

  // inyecta el botón del toggle si la página no trae uno propio
  function mountFab(){
    if(document.querySelector('.theme-fab')) return; // journal ya trae el suyo
    var b=document.createElement('button');
    b.className='theme-fab'; b.type='button';
    b.setAttribute('aria-label','Cambiar tema claro/oscuro'); b.title='Cambiar tema';
    b.addEventListener('click',function(){ window.toggleTheme(); });
    b.innerHTML='<span class="theme-fab-track"><span class="theme-fab-knob"></span></span>'
      +'<span class="theme-fab-icons"><span class="tfi tfi-moon">🌙</span><span class="tfi tfi-sun">☀️</span></span>';
    var tail=document.getElementById('lbcn-right');
    if(tail){ b.classList.add('theme-fab-innav'); tail.insertBefore(b, tail.firstChild); }
    else if(document.body){ document.body.appendChild(b); }
  }
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded',mountFab);
  else mountFab();
})();
