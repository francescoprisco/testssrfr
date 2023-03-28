from pymongo import MongoClient
from threading import Thread
from ThreadManager import ThreadManager
import time
import bcrypt
from bson.json_util import dumps
from datetime import datetime as dt
import ping3
import re
import traceback

class DatabaseManager:
    def __init__(self,dbConf,tableConf,arca):
        self.conn = None
        self.dbConf = dbConf
        self.tableConf = tableConf
        self.database = self.dbConf["db_name"]
        self.arca = arca
        self.connectDatabase()
        self.initTables()
        self.startDbListener()
    def connectDatabase(self):
        try:
            self.conn = MongoClient(self.dbConf["db_host"],username=self.dbConf["db_username"],password=self.dbConf["db_password"],authSource='admin')
        except Exception:
            print("Errore di connessione al server")

    def initTables(self):
        tableList = self.conn[self.database].list_collection_names()
        if self.tableConf["usersTable"] not in tableList:
            result = self.conn[self.database][str(self.tableConf["usersTable"])].insert_one({})
            self.conn[self.database][str(self.tableConf["usersTable"])].delete_one({"_id":result.inserted_id})
        if self.tableConf["machinesTable"] not in tableList:
            result = self.conn[self.database][str(self.tableConf["machinesTable"])].insert_one({})
            self.conn[self.database][str(self.tableConf["machinesTable"])].delete_one({"_id":result.inserted_id})
        if self.tableConf["olisTable"] not in tableList:
            result = self.conn[self.database][str(self.tableConf["olisTable"])].insert_one({})
            self.conn[self.database][str(self.tableConf["olisTable"])].delete_one({"_id":result.inserted_id})
        if self.tableConf["logsTable"] not in tableList:
            result = self.conn[self.database][str(self.tableConf["logsTable"])].insert_one({})
            self.conn[self.database][str(self.tableConf["logsTable"])].delete_one({"_id":result.inserted_id})
        if self.tableConf["lottosTable"] not in tableList:
            result = self.conn[self.database][str(self.tableConf["lottosTable"])].insert_one({})
            self.conn[self.database][str(self.tableConf["lottosTable"])].delete_one({"_id":result.inserted_id})
    
    #INSERT SECTION#
    def insertUser(self):
        password = "demo"
        password = password.encode('utf-8')
        hashed = bcrypt.hashpw(password, bcrypt.gensalt(10)) 
        userData = {"username":"demo","password":hashed,"firstname":"DemoNome","lastname":"DemoCognome"}
        self.conn[self.database][str(self.tableConf["usersTable"])].insert_one(userData)
        return "Utente {} inserito con successo".format(userData["username"])

    def insertOli(self,olis):
        for oli in olis:
            presence = self.conn[self.database][str(self.tableConf["olisTable"])].find_one({'Id_DORig': oli["Id_DORig"]})
            if(presence == None):
                oli["pausedTimes"] = 0
                self.conn[self.database][str(self.tableConf["olisTable"])].insert_one(oli)

    #GET SECTION#
    def getOnlineOffline(self):
        print("Avvio pinger")
        ping3.EXCEPTIONS = True
        while True:
            machines = self.conn[self.database][str(self.tableConf["machinesTable"])].find()
            for machine in list(machines):
                try:
                    result_of_check = ping3.ping(machine["ip"])
                    changes = {"$set": { "status": "Online"}}
                except Exception as e:
                    changes = {"$set": { "status": "Offline","workingStatus": "Idle","loggedUser":"","logged_at": dt.now().replace(hour=0, minute=0, second=0, microsecond=0)}} 
                machineName = {"name" : machine["name"]}
                self.conn[self.database][str(self.tableConf["machinesTable"])].update_one(machineName, changes)
            time.sleep(60)

    def getOlis(self):
        while True:
            olis = []
            columns, rows = self.arca.getOlis()
            for row in rows:
                olisDict = dict(zip(columns, row))
                olisDict["status"] = "Waiting"
                olisDict["qty_produced"] = 0
                olisDict["lotto"] = ""
                olisDict["started_at"] = dt.now().replace(hour=0, minute=0, second=0, microsecond=0)
                olisDict["ended_at"] = dt.now().replace(hour=0, minute=0, second=0, microsecond=0)
                olis.append(olisDict) 
            self.insertOli(olis)
            time.sleep(120)
        
    #LISTENER SECTION#

    def oliCompletedCancelledStatusListener(self):
        print("Avvio ascolto status oli")
        db = self.conn[self.database][str(self.tableConf["olisTable"])]
        pipeline = [{'$match':{'$or':[{"fullDocument.status": "Cancelled","fullDocument.status": "Completed"},{ "updateDescription.updatedFields.status": { '$exists': True }}]}}]
        stream = db.watch(pipeline,'updateLookup')
        for update_oli_change in stream:
            oli = update_oli_change["fullDocument"]
            try:
                if(oli["status"] == "Completed"):
                    oli["qt"] = 0
                    self.evadeOli(oli)
                elif(oli["status"] == "Cancelled"):
                    if(oli["qt"]>0):
                        remainQty = oli["qt"] - (oli["qty_produced"]/oli["qtyBox"])
                    else:
                        remainQty = 0
                    oli["qt"] = remainQty
                    self.evadeOli(oli)
            except Exception as error:
                with open('error.txt', 'w') as f:
                    f.write(str(error))

    def evadeOli(self,oli):
        newId = None
        self.arca.evadeOli(oli)
        if(oli["status"] == "Cancelled"):
            if(oli["qt"]>0):
                newId =  self.arca.insertOliRow(oli["Id_DORig"],oli["Id_DoTes"],oli["qt"])
                time.sleep(0.5)
        else:
            newId = self.arca.insertOliRow(oli["Id_DORig"],oli["Id_DoTes"],0)
        oli.pop("_id")
        if newId != None:
            oli["Id_DORig"] = newId
        oli["status"] = "Running"
        self.insertOli([oli])

    #listner for logging
    def olisListener(self):
        print("Avvio ascolto oli")
        db = self.conn[self.database][str(self.tableConf["olisTable"])]
        pipeline = []
        stream = db.watch(pipeline,'updateLookup')
        for change in stream:
            datetimeLog = dt.utcnow()
            machineCode = re.sub('\W+','', change["fullDocument"]["Cd_ARPrdClasse"])
            machine = self.conn[self.database][str(self.tableConf["machinesTable"])].find_one({'code': machineCode})
            logValue = {"type":"OLI","action":change["operationType"],"loggedUser":machine["loggedUser"],"machineCode":machineCode,"changes":"lol","fullDocument":change["fullDocument"],"createdAt":datetimeLog}
            self.conn[self.database][str(self.tableConf["logsTable"])].insert_one(logValue)

    def machinesListener(self):
        print("Avvio ascolto machine")
        db = self.conn[self.database][str(self.tableConf["machinesTable"])]
        pipeline = []
        stream = db.watch(pipeline,'updateLookup')
        for change in stream:
            datetimeLog = dt.utcnow()
            logValue = {"type":"MACHINE","action":change["operationType"],"machineCode":change["fullDocument"]["code"],"loggedUser":change["fullDocument"]["loggedUser"],"changes":change["updateDescription"]["updatedFields"],"fullDocument":change["fullDocument"],"createdAt":datetimeLog}
            self.conn[self.database][str(self.tableConf["logsTable"])].insert_one(logValue)

    def usersListener(self):
        print("Avvio ascolto user")
        db = self.conn[self.database][str(self.tableConf["usersTable"])]
        pipeline = []
        stream = db.watch(pipeline,'updateLookup')
        for change in stream:
            datetimeLog = dt.utcnow()
            logValue = {"type":"USER","message":change,"createdAt":datetimeLog}
            self.conn[self.database][str(self.tableConf["logsTable"])].insert_one(logValue)


    def startDbListener(self):
        completedCancelledStatusThread = ThreadManager(target = self.oliCompletedCancelledStatusListener)
        machineThread = ThreadManager(target = self.machinesListener)
        userThread = ThreadManager(target = self.usersListener)
        olisThread = ThreadManager(target = self.olisListener)
        getOlisThread = ThreadManager(target = self.getOlis)
        getOnlineOfflineThread = ThreadManager(target = self.getOnlineOffline)
        completedCancelledStatusThread.start()
        machineThread.start()
        time.sleep(0.25)
        userThread.start()
        time.sleep(0.25)
        olisThread.start()
        time.sleep(0.25)
        getOlisThread.start()
        time.sleep(0.25)
        getOnlineOfflineThread.start()
        time.sleep(0.25)

