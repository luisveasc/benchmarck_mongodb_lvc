import sys
import json
import pymongo
import time
import math

def get_test_json( test ):
    arrtest = test.split(";")
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
            #"UPSERT": (arrtest[10]==1),
            "FUNCTION":arrtest[11],
            "IDFUNCTION": arrtest[12]
            }

    return jdata

def get_work_json(jtest,db,col,idtest,time_ns,rows,rows_col):
    jdata=jtest
    jdata["STATUS"]="OK"
    jdata["db"]=db
    jdata["col"]=col
    jdata["idtest"]=idtest
    jdata["time_ns"]=time_ns
    jdata["rows"]=rows
    jdata["rows_col"]=rows_col

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
        ACUMULATE = 1000
        #print (jtest["MEMORY"]/MB16)


    for i in range(jtest["AMOUNT_TEST"]):
        docs = []
        for j in range(ACUMULATE):
            docs.append( {"_id":_id,"cmp1":_id,"cmp2":_id,"data":data} )
            _id=_id+1
        col.insert_many(docs)
        #print("%d de %d => _id:%d" %(i,jtest["AMOUNT_TEST"],_id))

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
        db[colname].insert_many(docs)
        tend = time.time_ns() - tini

        print("%s" % (str( get_work_json(jtest,dbname,colname,i,tend,1,1) )))
        db[colname].drop()

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
    #if (jtest["MEMORY"]/MB16 < 1):
    #    ACUMULATE = 1000

    for i in range(jtest["AMOUNT_TEST"]):
        docs = []
        for j in range(ACUMULATE):
            docs.append( {"_id":_id,"cmp1":_id,"cmp2":_id,"data":data} )
            _id=_id+1
        tini = time.time_ns()
        col.insert_many(docs)
        tend = time.time_ns() - tini

        print("%s" % (str( get_work_json(jtest,dbname,colname,i,tend,ACUMULATE,_id) )))

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
        print("%s" % (str( get_work_json(jtest,dbname,colname,i,tend,1,1) )))

    return


def find(jtest,conn,field,operation,valueToSearch):
    dbname = "DB1"
    colname = "COLLECTION1"
    conn.drop_database(dbname)
    db = conn[dbname]

    _id = insert_base(jtest,conn)

    for i in range(jtest["AMOUNT_TEST"]):
        tini = time.time_ns()
        resp=db[colname].find({field: {operation: valueToSearch}})
        tend = time.time_ns() - tini
        rows = 0
        for x in resp:
            rows=rows+1    
        print("%s" % (str( get_work_json(jtest,dbname,colname,i,tend,rows,_id) )))

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
    data = "b"*(jtest["BYTESIZE"]-jtest["SIZE_REST"])
    MB16 = 16000000
    ACUMULATE = 1
    if (jtest["MEMORY"]/MB16 < 1):
        ACUMULATE = 1000

    _id = insert_base(jtest,conn)
    VALUE=valueToSearch*ACUMULATE

    rows = db[colname].count_documents({field: {operation: VALUE}})

    for i in range(jtest["AMOUNT_TEST"]):
        tini = time.time_ns()
        db[colname].update_many({field:{operation:VALUE}},{"$set":{"data":data}})#,jtest["UPSERT
        tend = time.time_ns() - tini
        #print(">>>>>>>>>>>>>>>>> value:"+field+" - "+operation+" - "+str(VALUE) + " - " +rows.modified_count)
        print("%s" % (str( get_work_json(jtest,dbname,colname,i,tend,rows,_id) )))

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
    update(jtest,conn,"cmp2","$gt",jtest["AMOUNT_TEST"]-4)

def UPDATE_ALL_NFOUND_CIDX_SSAT(jtest,conn):
    update(jtest,conn,"_id","$lt",-1)

def UPDATE_ALL_FOUND_CIDX_SSAT(jtest,conn):
    update(jtest,conn,"_id","$gt",jtest["AMOUNT_TEST"]-4)



def delete(jtest,conn,field,operation,valueToSearch):
    dbname = "DB1"
    colname = "COLLECTION1"
    conn.drop_database(dbname)
    db = conn[dbname]
    MB16 = 16000000
    ACUMULATE = 1
    if (jtest["MEMORY"]/MB16 < 1):
        ACUMULATE = 1000

    _id = insert_base(jtest,conn)

    VALUE=valueToSearch*ACUMULATE
    docs=[]
    for doc in db[colname].find({field: {operation: VALUE}}):
        docs.append(doc)
    rows=len(docs)

    for i in range(jtest["AMOUNT_TEST"]):

        tini = time.time_ns()
        resp = db[colname].delete_many({field: {operation: VALUE}})
        tend = time.time_ns() - tini
        print("%s" % (str( get_work_json(jtest,dbname,colname,i,tend,rows,_id) )))

        if rows>0:
            db[colname].insert_many(docs)

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
    delete(jtest,conn,"cmp2","$gt",jtest["AMOUNT_TEST"]-4)

def DELETE_ALL_NFOUND_CIDX_SSAT(jtest,conn):
    delete(jtest,conn,"_id","$lt",-1)

def DELETE_ALL_FOUND_CIDX_SSAT(jtest,conn):
    delete(jtest,conn,"_id","$gt",jtest["AMOUNT_TEST"]-4)

#print('Argument list: %s' % str(sys.argv))
test = sys.argv[1]
jtest = get_test_json( test )

tini = time.time_ns()
status=""
try:
    status="FINALIZED"
    #print("%s" % (str(jtest)))
    conn = pymongo.MongoClient("mongodb://localhost:27017/")
    eval(jtest["FUNCTION"])(jtest,conn)
except:
    status="ERROR"
    print("%s" % str(jtest) )
tendns = time.time_ns() - tini
tendms = tendns/1000000
tends = tendns/1000000000
print("{'ID' : %d ,'STATUS' : '%s','total_time_ns' : %d , 'total_time_ms' : %d ,'total_time_s' : %d, 'function':'%s', 'idfunction' : '%s', 'params':'%s' }" % (jtest["ID"],status,tendns,tendms,tends,jtest["FUNCTION"],jtest["IDFUNCTION"],test))
