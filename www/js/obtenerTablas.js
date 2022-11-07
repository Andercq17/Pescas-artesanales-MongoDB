function obtenerTabla(){
  var seleccion = filtro.options[filtro.selectedIndex].value;
  eel.obtenerTabla(seleccion)(tabla)
}
function tabla(valores_tabla){
  const filtro=document.getElementById('filtro');
  var contenedor=document.getElementById('tabla');
  var seleccion = filtro.options[filtro.selectedIndex].value;
  if(seleccion=="Cuenca"){
    contenedor.innerHTML=""
    contenedor.innerHTML+=`
    <thead>
    <tr>
      <th>Id</th>
      <th>Cuenca</th>
    </tr>
    </thead>`;
    for(var i=0;i<valores_tabla.length;i++){
      contenedor.innerHTML+=`<tr>
      <td>${valores_tabla[i][1]}</td>
      <td>${valores_tabla[i][2]}</td>
      </tr>`
    }

  }else if(seleccion=="MetodoPesca"){
    contenedor.innerHTML=""
    contenedor.innerHTML+=`
    <thead>
    <tr>
      <th>Id</th>
      <th>TipoMetodo</th>
    </tr>
    </thead>`;
    for(var i=0;i<valores_tabla.length;i++){
      contenedor.innerHTML+=`<tr>
      <td>${valores_tabla[i][1]}</td>
      <td>${valores_tabla[i][2]}</td>
      </tr>`
    }
    
  }else{
    contenedor.innerHTML=""
    contenedor.innerHTML+=`
    <thead>
    <tr>
      <th>Consecutivo</th>
      <th>Cuenca</th>
      <th>MetodoPesca</th>
      <th>FechaActividad</th>
      <th>PesoPescado</th>
    </tr>
    </thead>`;
    for(var i=0;i<valores_tabla.length;i++){
      contenedor.innerHTML+=`
      <tr>
      <td>${valores_tabla[i][1]}</td>
      <td>${valores_tabla[i][2]}</td>
      <td>${valores_tabla[i][3]}</td>
      <td>${valores_tabla[i][4]}</td>
      <td>${valores_tabla[i][5]}</td>
      </tr>`
    }
  }
}
