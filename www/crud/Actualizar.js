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
        campos.innerHTML+=`<p>Elige cual quieres actualizar: </p>
        <select id="secciones" onchange="seccion()">`
        const secciones=document.getElementById('secciones');
        secciones.innerHTML+=`<option disabled selected value> -- selecciona una opción -- </option>`
        for(var i=0; i<valores_tabla.length;i++){
            var cuenca=valores_tabla[i][1] + " ~ "+ valores_tabla[i][2];
            secciones.innerHTML+=`<option value="${valores_tabla[i][1]}">${cuenca}</option>`;
        }
        campos.innerHTML+=`<p>Nombre de cuenca: </p>
        <input type="text" id="cuenca"/>`
        campos.innerHTML+=`<br><button onclick="actualizarDatos()" class="ov-btn-grow-skew">Actualizar cuenca</button><br>`;
    }else if(seleccion=="MetodoPesca"){
        campos.innerHTML=""
        campos.innerHTML+=`<p>Elige cual quieres actualizar: </p>
        <select id="secciones" onchange="seccion()">`;
        const secciones=document.getElementById('secciones');
        secciones.innerHTML+=`<option disabled selected value> -- selecciona una opción -- </option>`
        for(var i=0; i<valores_tabla.length;i++){
            var metodop=valores_tabla[i][1] + " ~ "+ valores_tabla[i][2];
            secciones.innerHTML+=`<option value="${valores_tabla[i][1]}">${metodop}</option>`;
        }
        campos.innerHTML+=`<p>Metodo de pesca:</p> 
        <input type="text" id="metodop"/>`
        campos.innerHTML+=`<button onclick="actualizarDatos()" class="ov-btn-grow-skew">Actualizar Metodo de pesca</button><br>`;
    }else{
        campos.innerHTML=""
        campos.innerHTML+=`<p>Elige cual quieres actualizar: </p> 
        <select id="secciones" onchange="seccion()">`
        const secciones=document.getElementById('secciones');
        secciones.innerHTML+=`<option disabled selected value> -- selecciona una opción -- </option>`
        for(var i=0; i<valores_tabla.length;i++){
            secciones.innerHTML+=`<option value="${valores_tabla[i][1]}">${valores_tabla[i][1]}</option>`;
        }
        eel.obtenerTabla("Cuenca")(obtenerCuencas)
        eel.obtenerTabla("MetodoPesca")(obtenerMetodos)
 }
}
function actualizarDatos(){
    if(select==""){
        alert("Hay valores en blanco")
    }else{
        if(tablav=="Cuenca"){
            var cuenca=document.getElementById('cuenca').value;
            if(cuenca.trim().length > 0){
                let valores=[select,cuenca]
                eel.actualizarCuenca(valores)(validacion)
            }else{
                alert("Hay valores en blanco")
            }
        }else if(tablav=="MetodoPesca"){
            var metodo=document.getElementById('metodop').value;
            if(metodo.trim().length>0){
                let valores=[select,metodo.trim()]
                eel.actualizarMetodo(valores)(validacion)
            }else{
                alert("Hay valores en blanco")
            }
        }else{
            const secciones3=document.getElementById('secciones3');
            const secciones2=document.getElementById('secciones2');
            var cuenca = secciones3.options[secciones3.selectedIndex].value;
            var metodo = secciones2.options[secciones2.selectedIndex].value;
            var fecha=document.getElementById('fecha').value;
            var peso=document.getElementById('peso').value;

            if(peso=="" || fecha=="" || metodo=="" || cuenca==""){
                alert("Hay valores en blanco")
            }else if(Math.sign(peso)==-1){
                alert("No se permiten numeros negativos")
            }else{ 
                let valores=[select,cuenca,metodo,fecha,peso];
                eel.actualizarPesca(valores)
            }  
        }
    }
    call();
}
function obtenerCuencas(valores_tabla){
    const campos=document.getElementById('datos');
    campos.innerHTML+=` <p>Cuenca: </p>
    <select id="secciones3">`
    const secciones3=document.getElementById('secciones3');
    secciones3.innerHTML+=`<option disabled selected value> -- selecciona una opción -- </option>`
    for(var i=0; i<valores_tabla.length;i++){
        var cuenca=valores_tabla[i][1] + " ~ "+ valores_tabla[i][2];
        secciones3.innerHTML+=`<option value="${valores_tabla[i][1]}">${cuenca}</option>`;
    }
}
function obtenerMetodos(valores_tabla){
    const campos=document.getElementById('datos');
    campos.innerHTML+=`<p>Metodos: </p>
    <select id="secciones2">`
    const secciones2=document.getElementById('secciones2');
    secciones2.innerHTML+=`<option disabled selected value> -- selecciona una opción -- </option>`
    for(var i=0; i<valores_tabla.length;i++){
        var metodop=valores_tabla[i][1] + " ~ "+ valores_tabla[i][2];
        secciones2.innerHTML+=`<option value="${valores_tabla[i][1]}">${metodop}</option>`;
    }
    campos.innerHTML+=`
    <p>Fecha de la actividad: </p>
    <input type="date"  id="fecha">
    <p>Peso del pescado: </p>
    <input type="number" id="peso"  min="1" pattern="^[0-9]+"/>`

    campos.innerHTML+=`<br><button onclick="actualizarDatos()" class="ov-btn-grow-skew">Actualizar pesca</button><br>`;


}
function validacion(valores_tabla){
    if(valores_tabla=="False"){
        alert("Actualizado correctamente")
    }else{
        alert("El dato a actualizar está siendo usado en la tabla pesca")
    }
}