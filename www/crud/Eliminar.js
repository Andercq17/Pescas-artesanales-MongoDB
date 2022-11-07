var select;
var tablav;
function seccion(){
    const secciones=document.getElementById('secciones');
    const filtro=document.getElementById('filtro');
    select=secciones.options[secciones.selectedIndex].value
    tablav=filtro.options[filtro.selectedIndex].value;
}

function call(){
    select=""
    obtenerTabla();
    const filtro=document.getElementById('filtro');
    var seleccion = filtro.options[filtro.selectedIndex].value;
    eel.obtenerTabla(seleccion)(insertarCampos)
}
function insertarCampos(valores_tabla){
    const filtro=document.getElementById('filtro');
    const campos=document.getElementById('datos');
    var seleccion = filtro.options[filtro.selectedIndex].value;
    if(seleccion=="Cuenca"){
        campos.innerHTML=""
        campos.innerHTML+=`<p>Elige cual quieres eliminar: </p>
        <select id="secciones" onchange="seccion()">`;
        const secciones=document.getElementById('secciones');
        secciones.innerHTML+=`<option disabled selected value> -- selecciona una opción -- </option>`
        for(var i=0; i<valores_tabla.length;i++){
            var cuenca=valores_tabla[i][1] + " ~ "+ valores_tabla[i][2];
            secciones.innerHTML+=`<option value="${valores_tabla[i][1]}">${cuenca}</option>`;
        }
        campos.innerHTML+=`<br><button onclick="eliminarDatos()" class="ov-btn-grow-skew">Eliminar cuenca</button><br>`;
    }else if(seleccion=="MetodoPesca"){
        campos.innerHTML=""
        campos.innerHTML+=`<p>Elige cual quieres eliminar: </p>
        <select id="secciones" onchange="seccion()">`;
        const secciones=document.getElementById('secciones');
        secciones.innerHTML+=`<option disabled selected value> -- selecciona una opción -- </option>`
        for(var i=0; i<valores_tabla.length;i++){
            var metodop=valores_tabla[i][1] + " ~ "+ valores_tabla[i][2];
            secciones.innerHTML+=`<option value="${valores_tabla[i][1]}">${metodop}</option>`;
        }
        campos.innerHTML+=`<button onclick="eliminarDatos()" class="ov-btn-grow-skew">Eliminar Metodo de pesca</button><br>`;
    }else{
        campos.innerHTML=""
        campos.innerHTML+=`<p>Elige cual quieres eliminar:</p> 
        <select id="secciones" onchange="seccion()">`;
        const secciones=document.getElementById('secciones');
        secciones.innerHTML+=`<option disabled selected value> -- selecciona una opción -- </option>`
        for(var i=0; i<valores_tabla.length;i++){
            secciones.innerHTML+=`<option value="${valores_tabla[i][1]}">${valores_tabla[i][1]}</option>`;
        }
        campos.innerHTML+=`<br><button onclick="eliminarDatos()" class="ov-btn-grow-skew">Eliminar pesca</button><br>`;
    }
}
function eliminarDatos(){
    if(select==null || select == "" ){
        alert("hay valores en blanco")
    }else{
        if(tablav=="Cuenca"){
            eel.eliminarCuenca(select)(validacion)
        }else if(tablav=="MetodoPesca"){
            eel.eliminarMetodo(select)(validacion)
        }else if(tablav=="Pesca"){
            eel.eliminarPesca(select)
        }
    }
    call();
}
function validacion(existe){
    if(existe=="False"){
        alert("Borrado correctamente")
    }else{
        alert("El dato a borrar está siendo usado en la tabla pesca")
    }
}