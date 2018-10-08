import sys
import json
import pymongo
import time
import math

def get_test_json( test ):
    arrtest = test.split(";")
    UPSERT=False
    if arrtest[10]==1: UPSERT=True

    jdata = {
            "ID": int(arrtest[0]),
            "TYPE": arrtest[1],
            "CTYPE": int(arrtest[2]),
            "ACTION": arrtest[3],
            "CAACTION": int(arrtest[4]),
            "CBYTESIZE": int(arrtest[5]),
            "BYTESIZE": int(arrtest[6]),
            "MEMORY": int(arrtest[7]),
            "AMOUNT_TEST": int(arrtest[8]),
            "SIZE_REST": int(arrtest[9]),
            "UPSERT": UPSERT
            }

    return jdata


def insert_base(jtest,conn):
    dbname = "DB1"
    colname = "COLLECTION1"
    db = conn[dbname]
    col = db[colname]
    data = "a"*(jtest["BYTESIZE"]-jtest["SIZE_REST"])
    MB16 = 16000000
    _id = 1

    ACUMULATE = 1
    if (jtest["MEMORY"]/MB16 < 1):
        print (jtest["MEMORY"]/MB16)
        ACUMULATE = 1000

    for i in range(jtest["AMOUNT_TEST"]):
        docs = []
        for j in range(ACUMULATE):
            docs.append( {"_id":_id,"cmp1":_id,"cmp2":_id,"data":data} )
            _id=_id+1
        col.insert_many(docs)
        print("%d de %d => _id:%d" %(i,jtest["AMOUNT_TEST"],_id))

    return _id

def INSERT_NEXIST_COLLECTION(jtest,conn):
    dbname = "DB1"
    colname = "COLLECTION1"

    conn.drop_database(dbname)
    db = conn[dbname]
    data = "a"*(jtest["BYTESIZE"]-jtest["SIZE_REST"])
    docs = [{"_id":0,"cmp1":0,"cmp2":0,"data":data}]

    for i in range(jtest["AMOUNT_TEST"]):
        tini = time.time_ns()
        col = db[colname]
        col.insert_many(docs)
        tend = time.time_ns() - tini
        print("{'db':'%s','col':'%s','idtest':%d,'time_ns':%d}" % (dbname,colname,i,tend))
        col.drop()

    return

def INSERT_NEXIST_DOCUMENT(jtest,conn):
    dbname = "DB1"
    colname = "COLLECTION1"
    conn.drop_database(dbname)
    db = conn[dbname]
    data = "a"*(jtest["BYTESIZE"]-jtest["SIZE_REST"])
    docs = [{"_id":0,"cmp1":0,"cmp2":0,"data":data}]
    col = db[colname]
    col.insert_many(docs)
    MB16 = 16000000
    _id = 1

    ACUMULATE = 1
    if (jtest["MEMORY"]/MB16 < 1):
        ACUMULATE = 1000

    for i in range(jtest["AMOUNT_TEST"]):
        docs = []
        for j in range(ACUMULATE):
            docs.append( {"_id":_id,"cmp1":_id,"cmp2":_id,"data":data} )
            _id=_id+1
        tini = time.time_ns()
        col.insert_many(docs)
        tend = time.time_ns() - tini
        print("{'db':'%s','col':'%s','idtest':%d,'time_ns':%d, 'acum':%d }" % (dbname,colname,i,tend,ACUMULATE))

    return

def INSERT_ERROR_PK(jtest,conn):
    dbname = "DB1"
    colname = "COLLECTION1"
    conn.drop_database(dbname)
    db = conn[dbname]
    data = "a"*(jtest["BYTESIZE"]-jtest["SIZE_REST"])
    docs = [{"_id":0,"cmp1":0,"cmp2":0,"data":data}]
    col = db[colname]
    col.insert_many(docs)

    _id = insert_base(jtest,conn)

    docs = [{"_id":(_id-1),"cmp1":0,"cmp2":0,"data":data}]
    for i in range(jtest["AMOUNT_TEST"]):
        tini = time.time_ns()
        try:
            col = db[colname]
            col.insert_many(docs)
        except:
            X=0#print "error pk"
        tend = time.time_ns() - tini
        print("{'db':'%s','col':'%s','idtest':%d,'time_ns':%d }" % (dbname,colname,i,tend))


def find(jtest,conn,field,operation,valueToSearch):
    dbname = "DB1"
    colname = "COLLECTION1"
    conn.drop_database(dbname)
    db = conn[dbname]
    col = db[colname]

    _id = insert_base(jtest,conn)

    for i in range(jtest["AMOUNT_TEST"]):
        tini = time.time_ns()
        rows = col.find({field: {operation: valueToSearch}}).count()
        tend = time.time_ns() - tini
        print("{'db':'%s','col':'%s','idtest':%d,'time_ns':%d, 'rows':%d}" % (dbname,colname,i,tend,rows))

    return


def SELECT_ONE_NFOUND_SIDX_SSAT(jtest,conn):
    find(jtest,conn,"cmp2","$eq",-1)

def SELECT_ONE_FOUND_SIDX_SSAT(jtest,conn):
    find(jtest,conn,"cmp2","$eq",jtest["AMOUNT_TEST"]-1)

def SELECT_ONE_NFOUND_CIDX_SSAT(jtest,conn):
    find(jtest,conn,"_id","$eq",-1)

def SELECT_ONE_FOUND_CIDX_SSAT(jtest,conn):
    find(jtest,conn,"_id","$eq",jtest["AMOUNT_TEST"]-1)

def SELECT_ALL_NFOUND_SIDX_SSAT(jtest,conn):
    find(jtest,conn,"cmp2","$lt",-1)

def SELECT_ALL_FOUND_SIDX_SSAT(jtest,conn):
    find(jtest,conn,"cmp2","$gt",1)

def SELECT_ALL_NFOUND_CIDX_SSAT(jtest,conn):
    find(jtest,conn,"_id","$lt",-1)

def SELECT_ALL_FOUND_CIDX_SSAT(jtest,conn):
    find(jtest,conn,"_id","$gt",1)


def update(jtest,conn,field,operation,valueToSearch):
    dbname = "DB1"
    colname = "COLLECTION1"
    conn.drop_database(dbname)
    db = conn[dbname]
    col = db[colname]
    data = "a"*(jtest["BYTESIZE"]-jtest["SIZE_REST"])

    _id = insert_base(jtest,conn)

    for i in range(jtest["AMOUNT_TEST"]):
        tini = time.time_ns()
        rows = col.update({field:{operation:valueToSearch}},{"$set":{"data":data}},jtest["UPSERT"])
        tend = time.time_ns() - tini
        print("{'db':'%s','col':'%s','idtest':%d,'time_ns':%d, 'rows':%s}" % (dbname,colname,i,tend,str(rows)) )

    return

def UPDATE_ONE_NFOUND_SIDX_SSAT(jtest,conn):
    update(jtest,conn,"cmp2","$eq",-1)

def UPDATE_ONE_FOUND_SIDX_SSAT(jtest,conn):
    update(jtest,conn,"cmp2","$eq",jtest["AMOUNT_TEST"]-1)

def UPDATE_ONE_NFOUND_CIDX_SSAT(jtest,conn):
    update(jtest,conn,"_id","$eq",-1)

def UPDATE_ONE_FOUND_CIDX_SSAT(jtest,conn):
    update(jtest,conn,"_id","$eq",jtest["AMOUNT_TEST"]-1)

def UPDATE_ALL_NFOUND_SIDX_SSAT(jtest,conn):
    update(jtest,conn,"cmp2","$lt",-1)

def UPDATE_ALL_FOUND_SIDX_SSAT(jtest,conn):
    update(jtest,conn,"cmp2","$gt",1)

def UPDATE_ALL_NFOUND_CIDX_SSAT(jtest,conn):
    update(jtest,conn,"_id","$lt",-1)

def UPDATE_ALL_FOUND_CIDX_SSAT(jtest,conn):
    update(jtest,conn,"_id","$gt",1)



def delete(jtest,conn,field,operation,valueToSearch):
    dbname = "DB1"
    colname = "COLLECTION1"
    conn.drop_database(dbname)
    db = conn[dbname]
    col = db[colname]

    _id = insert_base(jtest,conn)

    for i in range(jtest["AMOUNT_TEST"]):

        docs=[]
        for doc in col.find({field: {operation: valueToSearch}}):
            docs.append(doc)

        tini = time.time_ns()
        rows = col.delete_many({field: {operation: valueToSearch}})
        tend = time.time_ns() - tini
        print("{'db':'%s','col':'%s','idtest':%d,'time_ns':%d, 'rows':%s}" % (dbname,colname,i,tend,rows))

        n=len(docs)
        if n>0:
            col.insert_many(docs)


    return


def DELETE_ONE_NFOUND_SIDX_SSAT(jtest,conn):
    delete(jtest,conn,"cmp2","$eq",-1)

def DELETE_ONE_FOUND_SIDX_SSAT(jtest,conn):
    delete(jtest,conn,"cmp2","$eq",jtest["AMOUNT_TEST"]-1)

def DELETE_ONE_NFOUND_CIDX_SSAT(jtest,conn):
    delete(jtest,conn,"_id","$eq",-1)

def DELETE_ONE_FOUND_CIDX_SSAT(jtest,conn):
    delete(jtest,conn,"_id","$eq",jtest["AMOUNT_TEST"]-1)

def DELETE_ALL_NFOUND_SIDX_SSAT(jtest,conn):
    delete(jtest,conn,"cmp2","$lt",-1)

def DELETE_ALL_FOUND_SIDX_SSAT(jtest,conn):
    delete(jtest,conn,"cmp2","$gt",1)

def DELETE_ALL_NFOUND_CIDX_SSAT(jtest,conn):
    delete(jtest,conn,"_id","$lt",-1)

def DELETE_ALL_FOUND_CIDX_SSAT(jtest,conn):
    delete(jtest,conn,"_id","$gt",1)

print('Argument list: %s' % str(sys.argv))
test = sys.argv[1]
jtest = get_test_json( test )
conn = pymongo.MongoClient("mongodb://localhost:27017/")

eval(jtest["TYPE"]+"_"+jtest["ACTION"])(jtest,conn)
