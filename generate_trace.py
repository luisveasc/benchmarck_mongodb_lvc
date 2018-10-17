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

    _id=0
    if params["action"]==1: #NEXIST_doc
        _id=db[colname].count_documents({})

    amount = int(params["amount"])

    for c in range(amount):
        bytesize = random.randint(params["sizebyte"][0],params["sizebyte"][1])
        data = "a"*(bytesize-config["size_rest"])
        docs = [{"_id":_id,"cmp1":_id,"cmp2":_id,"data":data}]

        errpk=-1 #false
        if params["action"]==0 | params["action"]==1:
            tini = time.time_ns()
            db[colname].insert_many(docs)
            tend = time.time_ns() - tini
            if params["action"]==0:
                db[colname].drop()
        else:
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
        obj["QueryContent"]["accion"]=params["action"]
        obj["time"]=time.time_ns()
        obj["execution"]=tend
        obj["cantElemsColeccion"]=db[colname].count_documents({})

        print(obj)
        if params["action"]==1:
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
        idField = random.randint(0,int(obj["QueryContent"]["nbr_cols"])-2)
        op  = random.randint(config["operation_itvl"][0],config["operation_itvl"][1])
        val = random.randint(params["value_itvl"][0],params["value_itvl"][1])

        tini = time.time_ns()
        resp = db[colname].find({config["nameFields"][idField]: {config["operation_mongo"][op]: val}})
        tend = time.time_ns() - tini
        rows = 0
        for x in resp:
            rows=rows+1

        obj["id"]=c+idFirstTest
        obj["nombrePrueba"]="Select"
        obj["QueryContent"]["Instruction"]=1
        obj["QueryContent"]["sizebytes"]=bytesize
        #obj["QueryContent"]["fields"]=[_id,_id,_id,_id]
        obj["QueryContent"]["filters"]=[{"idField": idField, "operation": op, "value": val }]
        obj["time"]=time.time_ns()
        obj["execution"]=tend
        obj["found_rows"] = rows
        obj["cantElemsColeccion"]=db[colname].count_documents({})

        print(obj)

    return (idFirstTest+amount)

def Update(params,config,conn, idFirstTest):
    obj = config["trace_template"]
    dbname = obj["QueryContent"]["db"]
    colname = obj["QueryContent"]["collection"]
    db = conn[dbname]
    amount = int(params["amount"])

    for c in range(amount):

        idField = random.randint(0,int(obj["QueryContent"]["nbr_cols"])-2)
        op=0
        if params["multi"]==False:
            op  = random.randint(config["operation_itvl"][0]+1,config["operation_itvl"][1])
        val = random.randint(params["value_itvl"][0],params["value_itvl"][1])

        bytesize = random.randint(params["sizebyte"][0],params["sizebyte"][1])
        data = "a"*(bytesize-config["size_rest"])

        rows = db[colname].count_documents({config["nameFields"][idField]: {config["operation_mongo"][op]: val}})

        tini = time.time_ns()
        db[colname].update_many({config["nameFields"][idField]: {config["operation_mongo"][op]: val}},{"$set":{"data":data}})#,jtest["UPSERT
        tend = time.time_ns() - tini

        obj["id"]=c+idFirstTest
        obj["nombrePrueba"]="Update"
        obj["QueryContent"]["Instruction"]=2
        obj["QueryContent"]["sizebytes"]=bytesize
        obj["QueryContent"]["filters"]=[{"idField": idField, "operation": op, "value": val }]
        obj["QueryContent"]["fieldsToUpdate"]=[{"idField":2,"value":20}]
        obj["QueryContent"]["multi"]=params["multi"]
        obj["time"]=time.time_ns()
        obj["execution"]=tend
        obj["found_rows"] = rows
        obj["cantElemsColeccion"]=db[colname].count_documents({})

        print(obj)

    return (idFirstTest+amount)


def Delete(params,config,conn, idFirstTest):
    obj = config["trace_template"]
    dbname = obj["QueryContent"]["db"]
    colname = obj["QueryContent"]["collection"]
    db = conn[dbname]
    amount = int(params["amount"])
    bytesize = 100

    for c in range(amount):
        idField = random.randint(0,int(obj["QueryContent"]["nbr_cols"])-2)
        op=0
        if params["multi"]==False:
            op  = random.randint(config["operation_itvl"][0]+1,config["operation_itvl"][1])
        val = random.randint(params["value_itvl"][0],params["value_itvl"][1])

        rows = db[colname].count_documents({config["nameFields"][idField]: {config["operation_mongo"][op]: val}})

        tini = time.time_ns()
        resp = db[colname].delete_many({config["nameFields"][idField]: {config["operation_mongo"][op]: val}})
        tend = time.time_ns() - tini

        obj["id"]=c+idFirstTest
        obj["nombrePrueba"]="Delete"
        obj["QueryContent"]["Instruction"]=3
        obj["QueryContent"]["sizebytes"]=bytesize
        obj["QueryContent"]["filters"]=[{"idField": idField, "operation": op, "value": val }]
        obj["time"]=time.time_ns()
        obj["execution"]=tend
        obj["found_rows"] = rows
        obj["cantElemsColeccion"]=db[colname].count_documents({})

        print(obj)

    return (idFirstTest+amount)

fileinpath = sys.argv[1]
with open(fileinpath, 'r') as f:
    jdata = json.load(f)
    conn = pymongo.MongoClient("mongodb://localhost:27017/")
    conn.drop_database(jdata["config"]["trace_template"]["QueryContent"]["db"])

    idFirstTest=0
    for instructions in jdata["trace_test"]:
        idFirstTest = eval(instructions["instruction"])(instructions["params"],jdata["config"],conn,idFirstTest)
