/* Liberato Community · comportamiento mínimo del sistema de diseño (lbc-ui.js · v101).
   Opcional: lbc-ui.css funciona sin este archivo. Sin dependencias; no toca datos ni llamadas a /api/*.
   API pública (window.LBCUI):
     LBCUI.busy(boton, true|false)      → estado «cargando» de un .lbc-btn (bloquea el doble envío)
     LBCUI.openModal(id) / closeModal(id) → abre/cierra un .lbc-modal-backdrop (Esc, clic fuera, foco, bloqueo de scroll)
     LBCUI.toast('texto', ms)           → aviso breve
     LBCUI.fieldError(input, 'texto')   → marca un .lbc-input con error y escribe la pista; '' lo limpia */
(function(){
  if(window.LBCUI) return;
  var d=document, root=d.documentElement;

  // iOS solo aplica :active al tocar si hay un listener táctil en el documento: así los botones «responden».
  try{ d.addEventListener('touchstart',function(){},{passive:true}); }catch(e){}

  // Cabecera: línea inferior solo cuando hay contenido por debajo (clase .is-scrolled en .lbc-nav).
  function navState(){
    var y=window.scrollY||root.scrollTop||0, navs=d.querySelectorAll('.lbc-nav');
    for(var i=0;i<navs.length;i++) navs[i].classList.toggle('is-scrolled', y>4);
  }
  var ticking=false;
  window.addEventListener('scroll',function(){ if(ticking) return; ticking=true; requestAnimationFrame(function(){ ticking=false; navState(); }); },{passive:true});
  if(d.readyState==='loading') d.addEventListener('DOMContentLoaded',navState); else navState();

  function el(x){ return typeof x==='string' ? d.getElementById(x) : x; }

  function busy(btn,on){
    btn=el(btn); if(!btn) return;
    if(on){ btn.setAttribute('aria-busy','true'); btn.classList.add('is-loading'); }
    else{ btn.removeAttribute('aria-busy'); btn.classList.remove('is-loading'); }
  }

  var lastFocus=null, openCount=0;
  function focusables(box){ return box.querySelectorAll('a[href],button:not([disabled]),input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex]:not([tabindex="-1"])'); }
  function openModal(id){
    var b=el(id); if(!b||b.classList.contains('is-open')) return;
    lastFocus=d.activeElement; b.classList.add('is-open'); b.setAttribute('aria-hidden','false');
    openCount++; root.classList.add('lbc-lock');
    // El foco entra en el propio diálogo (no en la X: evitaría un anillo de foco que nadie pidió); [autofocus] manda si existe.
    var box=b.querySelector('.lbc-modal')||b, af=b.querySelector('[autofocus]');
    if(!box.hasAttribute('tabindex')) box.setAttribute('tabindex','-1');
    setTimeout(function(){ try{ (af||box).focus({preventScroll:true}); }catch(e){} },60);
  }
  function closeModal(id){
    var b=el(id); if(!b||!b.classList.contains('is-open')) return;
    b.classList.remove('is-open'); b.setAttribute('aria-hidden','true');
    openCount=Math.max(0,openCount-1); if(!openCount) root.classList.remove('lbc-lock');
    try{ if(lastFocus&&lastFocus.focus) lastFocus.focus({preventScroll:true}); }catch(e){}
  }
  d.addEventListener('click',function(e){
    var t=e.target;
    if(t&&t.classList&&t.classList.contains('lbc-modal-backdrop')&&t.classList.contains('is-open')&&!t.hasAttribute('data-lbc-static')) closeModal(t);
    var c=t&&t.closest?t.closest('[data-lbc-close]'):null;
    if(c){ var b=c.closest('.lbc-modal-backdrop'); if(b) closeModal(b); }
    var o=t&&t.closest?t.closest('[data-lbc-open]'):null;
    if(o) openModal(o.getAttribute('data-lbc-open'));
  });
  d.addEventListener('keydown',function(e){
    var open=d.querySelectorAll('.lbc-modal-backdrop.is-open'); if(!open.length) return;
    var top=open[open.length-1];
    if(e.key==='Escape'&&!top.hasAttribute('data-lbc-static')){ closeModal(top); return; }
    if(e.key==='Tab'){   // el foco no se escapa del modal
      var f=focusables(top); if(!f.length) return;
      var first=f[0], last=f[f.length-1];
      if(e.shiftKey&&d.activeElement===first){ e.preventDefault(); last.focus(); }
      else if(!e.shiftKey&&d.activeElement===last){ e.preventDefault(); first.focus(); }
    }
  });

  var toastEl=null, toastT=null;
  function toast(text,ms){
    if(!toastEl){ toastEl=d.createElement('div'); toastEl.className='lbc-toast'; toastEl.setAttribute('role','status'); toastEl.setAttribute('aria-live','polite'); d.body.appendChild(toastEl); }
    toastEl.textContent=text; void toastEl.offsetWidth; toastEl.classList.add('is-visible');
    clearTimeout(toastT); toastT=setTimeout(function(){ toastEl.classList.remove('is-visible'); }, ms||2600);
  }

  function fieldError(input,text){
    input=el(input); if(!input) return;
    var field=input.closest?input.closest('.lbc-field'):null, hint=field?field.querySelector('.lbc-hint'):null;
    if(text){ input.setAttribute('aria-invalid','true'); if(field) field.classList.add('is-error'); if(hint) hint.textContent=text; }
    else{ input.removeAttribute('aria-invalid'); if(field) field.classList.remove('is-error'); }
  }
  // Al corregir un campo marcado con error, el error se retira solo.
  d.addEventListener('input',function(e){ var t=e.target; if(t&&t.getAttribute&&t.getAttribute('aria-invalid')==='true'&&t.classList.contains('lbc-input')) fieldError(t,''); });

  window.LBCUI={ busy:busy, openModal:openModal, closeModal:closeModal, toast:toast, fieldError:fieldError };
})();
