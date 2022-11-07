import pymongo
import socket
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

#--------------------------------------------------------------------------------------------
    #READ
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

    def obtenerCuenca(self):
        collection_name=self.obtenerCollection('Cuenca')
        item_details = collection_name.find()
        lista=[]
        for item in item_details:
            lista.append(list(item.values()))
        return lista
    
    def obtenerMetodo(self):
        collection_name=self.obtenerCollection('Metodos_artesanales')
        item_details = collection_name.find()
        lista=[]
        for item in item_details:
            lista.append(list(item.values()))
        return lista
    
    def obtenerPesca(self):
        collection_name=self.obtenerCollection('Pesca')
        item_details = collection_name.find()
        cuencas=self.obtenerCuenca()
        metodos=self.obtenerMetodo()
        lista=[]
        for item in item_details:
            lista.append(list(item.values()))
        for pesca in lista:
            for cuenca in cuencas:
                if cuenca[1]==pesca[2]:
                    pesca[2]=cuenca[2]
            for metodo in metodos:
                if metodo[1]==pesca[3]:
                    pesca[3]=metodo[2]
        return lista
    def obtenerPescaOriginal(self):
        collection_name=self.obtenerCollection('Pesca')
        item_details = collection_name.find()
        lista=[]
        for item in item_details:
            lista.append(list(item.values()))
        return lista
#------------------------------------------------------------------------------------
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
        metodos=self.obtenerMetodo()
        existe=False
        for i in range(len(metodos)):
            if metodos[i][2]==str(metodo).title():
                existe=True
        if existe==False: 
            collection.insert_one({"Valor":(consecutivo + 1), "Tipo_metodo":str(metodo).title()})
            self.actualizarConteo('metodos')
            return "False"
        else:
            return "True"
        
    def crearCuenca(self,cuenca):
        consecutivo=self.obtenerConteo('cuenca')
        collection=self.obtenerCollection('cuenca')
        cuencas=self.obtenerCuenca()
        existe=False
        for i in range(len(cuencas)):
            if cuencas[i][2]==str(cuenca).title():
                existe=True
        if existe==False:
            collection.insert_one({"Valor":(consecutivo + 1), "Nombre_cuenca":str(cuenca).title()})
            self.actualizarConteo('cuenca')
            return "False"
        else:
            return "True"  
#-------------------------------------------------------------------------------------
    #UPDATE
    def actualizarCuenca(self, cuenca):
        collection=self.obtenerCollection('Cuenca')
        pescas=self.obtenerPescaOriginal()
        existe=False
        for i in range(len(pescas)):
            if int(pescas[i][2])==int(cuenca[0]):
               existe=True
        if existe==False:
            collection.update_one({"Valor":int(cuenca[0])},{'$set':{"Nombre_cuenca":str(cuenca[1]).title()}})  
            return "False"
        else:
            return "True"
        
    def actualizarMetodo(self, metodo):
        collection=self.obtenerCollection('Metodos_artesanales')
        pescas=self.obtenerPescaOriginal()
        existe=False
        for i in range(len(pescas)):
            if int(pescas[i][3])==int(metodo[0]):
               existe=True
        if existe==False:
            collection.update_one({"Valor":int(metodo[0])},{'$set':{"Tipo_metodo":str(metodo[1]).title()}})  
            return "False"
        else:
            return "True"
        
    def actualizarPesca(self,pesca):
        collection=self.obtenerCollection('Pesca')
        collection.update_one({"Consecutivo":int(pesca[0])},{'$set':{"Cuenca":int(pesca[1]),"Método_pesca":int(pesca[2]),"Fecha":str(pesca[3]),"Peso_pescado":float(pesca[4])}})

#--------------------------------------------------------------------------------------------------------------------
    #DELETE
    def eliminarCuenca(self,cuenca):
        collection=self.obtenerCollection('Cuenca')
        pescas=self.obtenerPescaOriginal()
        existe=False
        for i in range(len(pescas)):
            if int(pescas[i][2])==int(cuenca):
               existe=True
        if existe==False:
            collection.delete_one({ "Valor": int(cuenca) })
            return "False"
        else:
            return "True"
        
    def eliminarMetodo(self, metodo):
        collection=self.obtenerCollection('Metodos_artesanales')
        pescas=self.obtenerPescaOriginal()
        existe=False
        for i in range(len(pescas)):
            if int(pescas[i][3])==int(metodo):
               existe=True
        if existe==False:
            collection.delete_one({ "Valor": int(metodo) }) 
            return "False"
        else:
            return "True"
        
    def eliminarPesca(self, pesca):
        collection=self.obtenerCollection('Pesca')
        collection.delete_one({"Consecutivo": int(pesca)})