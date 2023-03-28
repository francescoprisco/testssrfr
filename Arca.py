import pyodbc 
from datetime import date
import time
class Arca():
    def __init__(self,arcaConf):
        print("Inizializzo arca")
        self.arcaConf = arcaConf
        self.conn = None
        #self.getOlis()

    def connectDatabase(self):
        try:
            self.conn = pyodbc.connect(driver="{ODBC Driver 17 for SQL Server}", server=self.arcaConf["host"], database=self.arcaConf["db_name"], user=self.arcaConf["db_username"], password=self.arcaConf["db_password"])
        except:
            print("Errore di connessione al database")
    def closeDatabase(self):
        self.conn.close()
    
    def getOlis(self):
        self.connectDatabase()
        print("Sono connesso")
        time.sleep(2)
        cursor = self.conn.cursor()
        query = "SELECT C.Cd_CF, A.Id_DoTes, B.NoteRiga AS note, B.Id_DORig, B.Cd_AR, D.Descrizione, IIF(B.xCd_ARPrdClasse IS NULL,D.Cd_ARPrdClasse,B.xCd_ARPrdClasse) as Cd_ARPrdClasse, CAST( E.UMFatt AS INT) AS qtyBox, " \
                    "A.NumeroDoc, B.DataConsegna, CAST(B.QtaEvadibile AS INT) AS qt " \
                    "FROM [dbo].[DOTes] AS A " \
                    "INNER JOIN [dbo].[DORig] AS B ON A.Id_DoTes=B.Id_DOTes " \
                    "INNER JOIN [dbo].[CF] AS C ON A.Cd_CF=C.Cd_CF " \
                    "INNER JOIN [dbo].[AR] AS D ON B.Cd_AR=D.Cd_AR " \
                    "INNER JOIN [dbo].[ARARMisura] AS E ON B.Cd_AR=E.Cd_AR WHERE E.Cd_ARMisura ='wb' AND " \
                    "B.QtaEvadibile > 0 AND " \
                    "A.Cd_Do LIKE 'OLI' AND A.Esecutivo LIKE 1" \
                    "ORDER BY B.DataConsegna, B.Cd_AR"
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        self.closeDatabase()
        return columns,rows

    def evadeOli(self,oli):
        self.connectDatabase()
        time.sleep(2)
        cursor = self.conn.cursor()
        query = "UPDATE [dbo].[DORig] SET QtaEvadibile='"+str(oli["qt"])+"' WHERE Id_DoRig ={}".format(oli["Id_DORig"])
        cursor.execute(query)
        cursor.commit()
        self.insertPRD(oli)
        self.closeDatabase()
    
    def createPRDHeader(self,olv):
        year = date.today().year
        cursor = self.conn.cursor()
        query = "INSERT INTO [dbo].[DOTes] (Cd_Do, TipoDocumento, Cd_CF, CliFor, Cd_CN, Esecutivo, Prelevabile, Modificabile, ModificabilePdf, Cd_MGEsercizio, EsAnno, NumeroDocRif, Cd_MGCausale, MagPFlag, Cd_LS_1) VALUES ('PRD', 'B', 'F000000', 'F', 'PRD', '1', '1', '1', '1', '"+str(year)+"', '"+str(year)+"', 'OLI "+str(olv["Id_DoTes"])+"', " + "'PRD', '1', 'UACQ')"
        cursor.execute(query)
        cursor.commit()
        query = "SELECT Id_DOTes FROM [dbo].[DOTes] WHERE Cd_Do = 'PRD' ORDER BY Id_DOTes DESC"
        cursor.execute(query)
        IDS = cursor.fetchall()
        PRDId = IDS[0][0]
        print("ID PRD1")
        print(PRDId)
        return PRDId

    def insertOliRow(self,Id_DORig,Id_DOTes,qty):
        self.connectDatabase()
        time.sleep(2)
        cursor = self.conn.cursor()
        query = "SELECT * FROM [dbo].[DORig] WHERE Id_DORig = '"+str(Id_DORig)+"'"
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        rows = list(rows[0])
        indices = [i for i, elem in enumerate(columns) if 'Id_DORig' in elem or 'Ts' in elem or 'DataConsegna_R' in elem or 'ScontoTotale' in elem or 'ExtraInfoPresent' in elem or 'PrezzoResiduoV' in elem or 'PrezzoResiduoE' in elem or 'PrezzoTotaleE' in elem or 'PrezzoUnitarioScontatoV' in elem or 'ValProvvigione_1' in elem or 'ValProvvigione_2' in elem]
        indexQtyEva = columns.index("QtaEvadibile")
        indices.sort(reverse=True)
        rows[indexQtyEva] = qty 
        for index in indices:
            columns.pop(index)
            rows.pop(index)
        columns = ','.join(str(column) for column in columns)
        rowsPoint = ','.join("?" for row in rows)
        query = "INSERT INTO [dbo].[DORig] ("+columns+") VALUES ("+rowsPoint+")"
        cursor.execute(query,tuple(rows))
        cursor.commit()
        query = "SELECT Id_DORig FROM [dbo].[DORig] WHERE Id_DOTes = '"+str(Id_DOTes)+"' ORDER BY Id_DORig DESC"
        cursor.execute(query)
        IDS = cursor.fetchall()
        newId = IDS[0][0]
        self.closeDatabase()
        return newId

    def insertPRD(self,oli):
        rowsLotto = self.checkLotto(oli)
        newPRDID = ""
        if(len(rowsLotto)==0):
            try:
                self.insertLotto(oli)
            except Exception as e:
                print(e)
        try:
            newPRDID = self.createPRDHeader(oli)
        except e:
            print(e)
        year = date.today().year
        ua = self.lastBuy("U",oli)
        cursor = self.conn.cursor()
        query = "INSERT INTO [dbo].[DORig] (Id_DOTes, Cd_MGEsercizio, Cd_DO, TipoDocumento, Riga, Cd_MGCausale, TipoPC, Cd_MG_P,Cd_MG_A, Cd_AR, Cd_ARLotto, Cd_CGConto, Qta, QtaEvadibile, PrezzoUnitarioV, PrezzoTotaleV, Cd_Aliquota, Cd_Aliquota_R) VALUES ('" +str(newPRDID) + "', '" + str(year) + "', 'PRD', 'B', 1, 'PRD', 'P', '00002','00002', '" + oli["Cd_AR"] + "', '" + str(oli["lotto"]) + "', " + "'07010101001', '" + str(oli["qty_produced"]) + "', '" + str(oli["qty_produced"]) + "', " + "'" + str(ua[0][0]) + "', '" + str(oli["qty_produced"]*ua[0][0]) + "', '" + str(ua[0][1]) + "', '" +str(ua[0][1]) + "')"
        cursor.execute(query)
        cursor.commit()

    def insertLotto(self,oli):
        cursor = self.conn.cursor()
        query = "INSERT INTO [dbo].[ARLotto] (CD_ARLotto, Cd_AR, Note_ARLotto) VALUES ('"+str(oli["lotto"])+"', '"+str(oli["Cd_AR"])+"', '' )"
        cursor.execute(query)
        cursor.commit()

    def lastBuy(self,type,oli):
        cursor = self.conn.cursor()
        query = "SELECT TOP (1) A.CostoDb, ISNULL(B.Cd_Aliquota_A,'22') AS cd_aliquota FROM [dbo].[ARCostoDBItem] AS A INNER JOIN [dbo].[AR] AS B ON A.Cd_AR=B.Cd_AR WHERE A.TipoCosto LIKE '" + type + "' AND A.Cd_AR LIKE '" + oli["Cd_AR"] + "' " + "ORDER BY A.Id_ARCostoDBItem DESC"
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        row = cursor.fetchall()
        return row
    
    def checkLotto(self,oli):
        cursor = self.conn.cursor()
        query = "SELECT Cd_ARLotto, Cd_AR FROM [dbo].[ARLotto] WHERE Cd_ARLotto LIKE '"+str(oli["lotto"])+"' AND Cd_AR LIKE '"+oli["Cd_AR"]+"'"
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows

