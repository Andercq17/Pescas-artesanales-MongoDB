import sqlite3
import os
class accesoDatos:
    def __obtenerCadenaConexion__(self,id):
        return os.path.abspath(id)
    def __realizarConexion(self):
        cadena_conexion=self.__obtenerCadenaConexion__("pescas_Artesanales.db")
        con=sqlite3.Connection(cadena_conexion) #conexion
        cur= con.cursor() #cursor
        return (cur, con)
    #READ
    def obtenerCuenca(self):
        (cur,con)= self.__realizarConexion() #cursor
        res=cur.execute("select * from cuenca").fetchall()
        lista=[]
        for row in res:
            lista.append(row)
        return lista
    def obtenerMetodo(self):
        (cur,con)= self.__realizarConexion() #cursor
        res=cur.execute("select * from metodoPesca")
        lista=[]
        for row in res:
            lista.append(row)
        return lista
    def obtenerPesca(self):
        (cur,con)= self.__realizarConexion() #cursor
        res=cur.execute("SELECT consecutivo,nombreCuenca,tipoMetodo,fechaActividad,pesoPescado from pesca join cuenca on pesca.cuenca=cuenca.id join metodoPesca on pesca.metodoPesca=metodoPesca.Id ")
        lista=[]
        for row in res:
            lista.append(row)
        return lista
    def obtenerPescaOriginal(self):
        (cur,con)= self.__realizarConexion() #cursor
        res=cur.execute("SELECT * from pesca")
        lista=[]
        for row in res:
            lista.append(row)
        return lista
    #CREATE
    def crearPesca(self,valores):
        (cur,con)= self.__realizarConexion()
        cuencas=self.obtenerCuenca()
        metodos=self.obtenerMetodo()
        metodo=0
        cuenca=0
        for i in range(len(cuencas)):
            if cuencas[i][1]==str(valores[0]).title():
                cuenca=cuencas[i][0]
        for i in range(len(metodos)):
            if metodos[i][1]==str(valores[1]).title():
                metodo=metodos[i][0]
        cur.execute(f"INSERT INTO pesca(cuenca,metodoPesca,fechaActividad,pesoPescado)VALUES({str(cuenca)},{str(metodo)},'{str(valores[2])}',{str(valores[3])})")
        con.commit()
    def crearMetodo(self,metodo):
        (cur,con)= self.__realizarConexion()
        metodos=self.obtenerMetodo()
        existe=False
        for i in range(len(metodos)):
            if metodos[i][1]==str(metodo).title():
                existe=True
        if existe==False: 
            cur.execute(f"INSERT INTO metodoPesca(tipoMetodo)VALUES('{str(metodo).title()}')")
            con.commit() 
            return "False"
        else:
            return "True"
    def crearCuenca(self,cuenca):
        (cur,con)= self.__realizarConexion()
        cuencas=self.obtenerCuenca()
        existe=False
        for i in range(len(cuencas)):
            if cuencas[i][1]==str(cuenca).title():
                existe=True
        if existe==False:
            cur.execute(f"INSERT INTO cuenca(nombreCuenca)VALUES('{str(cuenca).title()}')")
            con.commit()    
            return "False"
        else:
            return "True"  
    #DELETE
    def eliminarCuenca(self,cuenca):
        (cur,con)= self.__realizarConexion()
        pescas=self.obtenerPesca()
        existe=False
        for i in range(len(pescas)):
            if pescas[i][1]==cuenca:
               existe=True
        if existe==False:
            cur.execute(f"DELETE FROM cuenca WHERE nombreCuenca='{str(cuenca)}'")
            con.commit()
            return "False"
        else:
            return "True"
    def eliminarMetodo(self, metodo):
        (cur,con)= self.__realizarConexion()
        pescas=self.obtenerPesca()
        existe=False
        for i in range(len(pescas)):
            if pescas[i][2]==metodo:
               existe=True
        if existe==False:
            cur.execute(f"DELETE FROM metodoPesca WHERE tipoMetodo='{str(metodo)}'")
            con.commit()
            return "False"
        else:
            return "True"
    def eliminarPesca(self, pesca):
        (cur,con)= self.__realizarConexion()
        cur.execute(f"DELETE FROM pesca WHERE consecutivo={pesca}")
        con.commit()
    #UPDATE
    def actualizarCuenca(self, cuenca):
        (cur,con)= self.__realizarConexion()
        pescas=self.obtenerPescaOriginal()
        existe=False
        for i in range(len(pescas)):
            if pescas[i][1]==cuenca[0]:
               existe=True
        if existe==False:
            cur.execute(f"UPDATE cuenca SET nombreCuenca = '{str(cuenca[1]).title()}' WHERE id = {cuenca[0]}")
            con.commit()
            return "False"
        else:
            return "True"
    def actualizarMetodo(self, metodo):
        (cur,con)= self.__realizarConexion()
        pescas=self.obtenerPescaOriginal()
        existe=False
        for i in range(len(pescas)):
            if pescas[i][2]==metodo[0]:
               existe=True
        if existe==False:
            cur.execute(f"UPDATE metodoPesca SET tipoMetodo = '{str(metodo[1]).title()}' WHERE id = {metodo[0]}")
            con.commit()
            return "False"
        else:
            return "True"
    def actualizarPesca(self,pesca):
        (cur,con)= self.__realizarConexion()
        cuencas=self.obtenerCuenca()
        metodos=self.obtenerMetodo()
        cuencaid=0
        metodoid=0
        for i in range(len(cuencas)):
            if cuencas[i][1]==pesca[1]:
               cuencaid=cuencas[i][0]
        for i in range(len(metodos)):
            if metodos[i][1]==pesca[2]:
               metodoid=metodos[i][0]
        cur.execute(f"UPDATE pesca SET cuenca={cuencaid}, metodoPesca={metodoid}, fechaActividad='{pesca[3]}', pesoPescado={pesca[4]} WHERE consecutivo = {pesca[0]}")
        con.commit()