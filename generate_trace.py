import sys
import json
import pymongo
import time
import random

def Insert(params,config,conn, idFirstTest):

    obj = config["trace_template"]
    dbname = obj["QueryContent"]["db"]
    colname = obj["QueryContent"]["collection"]
    db = conn[dbname]

    _id = int(params["posIni"])
    amount = int(params["amount"])

    for c in range(amount):
        bytesize = random.randint(params["sizebyte"][0],params["sizebyte"][1])
        data = "a"*(bytesize-config["size_rest"])
        docs = [{"_id":_id,"cmp1":_id,"cmp2":_id,"data":data}]

        errpk=-1 #false
        tini = time.time_ns()
        try:
            db[colname].insert_many(docs)
        except:
            errpk=1 #true
        tend = time.time_ns() - tini

        obj["id"]=c+idFirstTest
        obj["nombrePrueba"]="Insert"
        obj["QueryContent"]["Instruction"]=0
        obj["QueryContent"]["sizebytes"]=bytesize
        obj["QueryContent"]["fields"]=[_id,_id,_id,_id]
        obj["QueryContent"]["errorPk"]=errpk
        obj["time"]=time.time_ns()
        obj["execution"]=tend
        obj["cantElemsColeccion"]=db[colname].count_documents({})

        print(obj)

        _id=_id+1

    return (idFirstTest+amount)


def Select(params,config,conn, idFirstTest):
    obj = config["trace_template"]
    dbname = obj["QueryContent"]["db"]
    colname = obj["QueryContent"]["collection"]
    db = conn[dbname]
    amount = int(params["amount"])
    bytesize = 100

    for c in range(amount):
        idField = random.randint(0,int(obj["QueryContent"]["nbr_cols"])-1)
        op  = random.randint(config["operation_itvl"][0],config["operation_itvl"][1])
        val = random.randint(params["value_itvl"][0],params["value_itvl"][1])

        tini = time.time_ns()
        #update preguntar por multi==true o false
        tend = time.time_ns() - tini

        obj["id"]=c+idFirstTest
        obj["nombrePrueba"]="Select"
        obj["QueryContent"]["Instruction"]=1
        obj["QueryContent"]["sizebytes"]=bytesize
        #obj["QueryContent"]["fields"]=[_id,_id,_id,_id]
        obj["QueryContent"]["filters"]=[{"idField": idField, "operation": op, "value": val }]
        #obj["QueryContent"]["errorPk"]=errpk
        obj["time"]=time.time_ns()
        obj["execution"]=tend
        obj["cantElemsColeccion"]=db[colname].count_documents({})

        print(obj)

    return (idFirstTest+amount)

def Update(params,config,conn, idFirstTest):
    amount = int(params["amount"])
    return (idFirstTest+amount)


fileinpath = sys.argv[1]
with open(fileinpath, 'r') as f:
    jdata = json.load(f)
    conn = pymongo.MongoClient("mongodb://localhost:27017/")
    conn.drop_database(jdata["config"]["trace_template"]["QueryContent"]["db"])

    idFirstTest=0
    for instructions in jdata["trace_test"]:
        idFirstTest = eval(instructions["instruction"])(instructions["params"],jdata["config"],conn,idFirstTest)
