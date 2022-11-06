import pymongo
import socket
from datetime import datetime
class accesoDatos:
        
    MONGODB_HOST = str(socket.gethostbyname(socket.gethostname() ))
    MONGODB_PORT = '27017'
    URL_CONNECTION = "mongodb://mongoadmin:UnaClav3@" + MONGODB_HOST + ":" + MONGODB_PORT +  "/?authMechanism=DEFAULT"
    MONGODB_DATABASE = 'pescas_artesanales'
    MONGODB_TIMEOUT = 1000
    
    def __realizarConexion(self):  
        client = pymongo.MongoClient(self.URL_CONNECTION, serverSelectionTimeoutMS=self.MONGODB_TIMEOUT)
        client.server_info()
        return client
        
    def obtenerCollection(self,coleccion):
        client=self.__realizarConexion()
        collection = client[self.MONGODB_DATABASE][str(coleccion).capitalize()]
        return collection
    
    def obtenerConteo(self,collection):
        collection_name=self.obtenerCollection('Contador')
        item_details = collection_name.find({"Coleccion" : str(collection).capitalize()})
        lista=[]
        for item in item_details:
            lista.append(item)
        return lista[0]['Valor']
    
    #READ
    def obtenerCuenca(self):
        collection_name=self.obtenerCollection('Cuenca')
        item_details = collection_name.find()
        lista=[]
        for item in item_details:
            lista.append(item)
        return lista
    
    def obtenerMetodo(self):
        collection_name=self.obtenerCollection('Metodos_artesanales')
        item_details = collection_name.find()
        lista=[]
        for item in item_details:
            lista.append(item)
        return lista
    
    def obtenerPesca(self):
        collection_name=self.obtenerCollection('Pesca')
        item_details = collection_name.find()
        lista=[]
        for item in item_details:
            lista.append(item)
        return lista
        
    #CREATE
    def actualizarConteo(self,collection):
        contador=self.obtenerCollection('Contador')
        conteo=self.obtenerConteo(str(collection).capitalize())
        contador.update_one({"Coleccion":str(collection).capitalize()},{'$set':{"Valor":(conteo+1)}})        
        
    def crearPesca(self,valores):
        consecutivo=self.obtenerConteo('Pesca')
        collection=self.obtenerCollection('Pesca')
        collection.insert_one({"Consecutivo":(consecutivo + 1), "Cuenca":int(valores[0]),"Método_pesca":int(valores[1]), "Fecha":str(valores[2]),"Peso_pescado":float(valores[3])})
        self.actualizarConteo('Pesca')
    
    def crearMetodo(self,metodo):
        consecutivo=self.obtenerConteo('Metodos')
        collection=self.obtenerCollection('Metodos_artesanales')
        collection.insert_one({"Valor":(consecutivo + 1), "Tipo_metodo":str(metodo).capitalize()})
        self.actualizarConteo('metodos')
        
    def crearCuenca(self,cuenca):
        consecutivo=self.obtenerConteo('cuenca')
        collection=self.obtenerCollection('cuenca')
        collection.insert_one({"Valor":(consecutivo + 1), "Nombre_cuenca":str(cuenca).capitalize()})
        self.actualizarConteo('cuenca')
        
    #UPDATE
    def actualizarCuenca(self, cuenca):
        collection=self.obtenerCollection('Cuenca')
        collection.update_one({"Valor":int(cuenca[0])},{'$set':{"Nombre_cuenca":str(cuenca[1]).capitalize()}})  
        
    def actualizarMetodo(self, metodo):
        collection=self.obtenerCollection('Metodos_artesanales')
        collection.update_one({"Valor":int(metodo[0])},{'$set':{"Tipo_metodo":str(metodo[1]).capitalize()}})  
        
    def actualizarPesca(self,pesca):
        collection=self.obtenerCollection('Pesca')
        collection.update_one({"Consecutivo":int(pesca[0])},{'$set':{"Cuenca":int(pesca[1]),"Método_pesca":int(pesca[2]),"Fecha":str(pesca[3]),"Peso_pescado":float(pesca[4])}})
         
    #DELETE
    def eliminarCuenca(self,cuenca):
        collection=self.obtenerCollection('Cuenca')
        collection.delete_one({ "Valor": int(cuenca) })
        
    def eliminarMetodo(self, metodo):
        collection=self.obtenerCollection('Metodos_artesanales')
        collection.delete_one({ "Valor": int(metodo) }) 
        
    def eliminarPesca(self, pesca):
        collection=self.obtenerCollection('Pesca')
        collection.delete_one({ "Consecutivo": int(pesca) })
        



a=accesoDatos()
#valores=["2","3","2022/11/06","15.7"]
#a.crearPesca(valores)

#valores=["2","2","2","2022/11/10","17.7"]
#a.actualizarPesca(valores)

#cuenca=[4, "estepede"]
#a.actualizarCuenca(cuenca)

#metodo=["5","anibal"]
#a.actualizarMetodo(metodo)

#a.crearCuenca("perro")
#a.eliminarCuenca(4)

#a.crearMetodo("perrote")
#a.eliminarMetodo(5)

#a.eliminarPesca(3)