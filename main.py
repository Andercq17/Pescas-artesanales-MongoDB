import eel
import www.AccesoDatos as ad
eel.init("www")
data=ad.accesoDatos()
#READ
@eel.expose    
def obtenerTabla(tabla):
    if tabla=="Cuenca":
        cuencas=data.obtenerCuenca()
        return cuencas
    elif tabla=="MetodoPesca":
        metodos=data.obtenerMetodo()
        return metodos
    else:
        pesca=data.obtenerPesca()
        return pesca
    
#CREATE
@eel.expose
def enviarPesca(valores):
    data.crearPesca(valores)
@eel.expose
def enviarMetodo(metodo):
    existe=data.crearMetodo(metodo)
    return existe
@eel.expose
def enviarCuenca(cuenca):
    existe=data.crearCuenca(cuenca)
    return existe

# DELETE
@eel.expose
def eliminarCuenca(cuenca):
    existe=data.eliminarCuenca(cuenca)
    return existe
@eel.expose
def eliminarMetodo(metodo):
    existe=data.eliminarMetodo(metodo)
    return existe
@eel.expose
def eliminarPesca(pesca):
    data.eliminarPesca(pesca)
    
#UPDATE  
@eel.expose
def actualizarCuenca(cuenca):
    existe=data.actualizarCuenca(cuenca)
    return existe
@eel.expose
def actualizarMetodo(metodo):
    existe=data.actualizarMetodo(metodo)
    return existe
@eel.expose
def actualizarPesca(pesca):
    data.actualizarPesca(pesca)

eel.start("views/Leer.html", size=(2000,800) , position= (300, 200))

