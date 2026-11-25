/* Liberato Community · capa de herramientas de miembros (lbc-tools.js · v101).
   Solo interfaz: en móvil deja a la vista la herramienta activa de la navegación
   y difumina el borde de la fila solo por el lado donde queda algo por ver.
   No hace llamadas de red ni toca datos. */
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
