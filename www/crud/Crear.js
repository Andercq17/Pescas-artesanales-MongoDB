function call(){
    obtenerTabla();
    insertarCampos();
}
function insertarCampos(){
    const filtro=document.getElementById('filtro');
    const campos=document.getElementById('datos');
    var seleccion = filtro.options[filtro.selectedIndex].value;
    if(seleccion=="Cuenca"){
        campos.innerHTML=""
        campos.innerHTML+=`<p>Nombre de cuenca:</p>
        <input type="text" id="cuenca"/>
        <button onclick="enviarDatos('cuenca')"  class="ov-btn-grow-skew">Enviar nueva cuenca</button><br>`;
    }else if(seleccion=="MetodoPesca"){
        campos.innerHTML=""
        campos.innerHTML+=`<p>Metodo de pesca: </p>
        <input type="text" id="metodop"/>
        <button onclick="enviarDatos('metodo')"  class="ov-btn-grow-skew">Enviar nuevo metodo de pesca</button><br>`;
    }else{
        campos.innerHTML=""
        eel.obtenerTabla("Cuenca")(obtenerCuencas)
        eel.obtenerTabla("MetodoPesca")(obtenerMetodos)
    }
}
function enviarDatos(tabla){
    if (tabla=="pesca"){
        const secciones=document.getElementById('secciones');
        const secciones2=document.getElementById('secciones2');
        var cuenca = secciones.options[secciones.selectedIndex].value;
        var metodo = secciones2.options[secciones2.selectedIndex].value;
        var fecha=document.getElementById('fecha').value;
        var peso=document.getElementById('peso').value;
        let valores=[cuenca,metodo,fecha,peso.trim()];
        if(cuenca=="" || metodo=="" || fecha=="" || peso==""){
            alert("Hay valores en blanco")
        }else if(Math.sign(peso)==-1){
            alert("No se permiten numeros negativos")
        }else{   
            eel.enviarPesca(valores)()
        }
    }else if(tabla=="metodo"){
        var metodo=document.getElementById('metodop').value;
        if(metodo.trim().length > 0 ){
            eel.enviarMetodo(metodo.trim())(validacion)
        }else{
            alert("Hay valores en blanco")
        }
    }else{
        var cuenca=document.getElementById('cuenca').value;
        if(cuenca.trim().length > 0){
            eel.enviarCuenca(cuenca.trim())(validacion)
        }else{
            alert("Hay espacios en blanco")
        }
    }
    obtenerTabla();
}
function obtenerCuencas(valores_tabla){
    const campos=document.getElementById('datos');
    campos.innerHTML+=`<p> Cuenca: </p>
    <select id="secciones">`
    const secciones=document.getElementById('secciones');
    secciones.innerHTML+=`<option disabled selected value> -- selecciona una opción -- </option>`
    for(var i=0; i<valores_tabla.length;i++){
        var cuenca=valores_tabla[i][1] + " ~ "+ valores_tabla[i][2];
        secciones.innerHTML+=`<option value="${valores_tabla[i][1]}">${cuenca}</option>`;
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
    campos.innerHTML+=`<p>
    Fecha de la actividad:</p>
    <input type="date"  id="fecha">
    <p>Peso del pescado:</p>
    <input type="number" id="peso" min="1" pattern="^[0-9]+"/>
    <button onclick="enviarDatos('pesca')"  class="ov-btn-grow-skew"> Enviar nueva pesca</button><br>`;
}
function validacion(valores_tabla){
    if(valores_tabla=="True"){
        alert("Ya existe en la tabla")
    }else{
        alert("Ingresado Correctamente")
    }
}