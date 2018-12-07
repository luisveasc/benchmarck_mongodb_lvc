# -*- coding: utf-8 -*-

import sys
import json
import pymongo
import time
import datetime
import math

file = open("teleton2018.json", "r")
userid="0"
tweets = {}
swFirtTime=True
usersIds = []
min=0
max=0

for line in file:
    sizebytes = len(line)*8
    data = json.loads(line)
    userid = data["user"]["id_str"]
    if swFirtTime==True:
        swFirtTime=False
        min=int(data["timestamp_ms"])
        max=int(data["timestamp_ms"])
    else:
        if min > data["timestamp_ms"]: min = int(data["timestamp_ms"])
        if max < data["timestamp_ms"]: max = int(data["timestamp_ms"])

    if userid not in tweets:
        message = {"sizebytes":sizebytes,"hold_ms": 0, "timestamp_ms":data["timestamp_ms"],"id_shard":0}
        tweets[userid] = {"userid":userid,"nbr_msgs":1,"last_timestamp_ms":data["timestamp_ms"],"first_timestamp_ms":data["timestamp_ms"],"messages":[message]}
        usersIds.append(userid)

    else:
        tweets[userid]["nbr_msgs"] = int(tweets[userid]["nbr_msgs"]) + 1
        hold_ms = int(data["timestamp_ms"]) - int(tweets[userid]["last_timestamp_ms"])
        tweets[userid]["messages"].append({"sizebytes":sizebytes,"hold_ms": hold_ms, "timestamp_ms":data["timestamp_ms"],"id_shard":0 })
        tweets[userid]["last_timestamp_ms"]=data["timestamp_ms"]

print(min)
ts = float(min) / 1000.0
print(datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S.%f'))

id_shards={}
for j in range(0, 100):
    id_shards[j]=0

increment = 3600000 #in milisegundos => una hora
windows = ((max-min)/increment)+1
print("windows:"+str(windows))
M=0

for userid in usersIds:
    #tweets[userid]["inicial_hold_ms"] = int(tweets[userid]["first_timestamp_ms"]) - int(min)
    tweets[userid]["messages"][0]["hold_ms"] = int(tweets[userid]["first_timestamp_ms"]) - int(min)

    ini = int(min)
    end = ini + increment
    n = len(tweets[userid]["messages"])

    for i in range (0, n):
        timestamp = int(tweets[userid]["messages"][i]["timestamp_ms"])

        for j in range(0, windows):

            value = 0#int((float(timestamp)-float(min))/(float(max)-float(min))*100.0)
            print(">>>> n:"+str(n)+" - i:"+str(i)+" - j:"+str(j)+" ["+str(ini)+" ; "+str(timestamp)+" ; "+str(end)+"]  => "+str(timestamp >= ini)+" - "+str(timestamp < end) )
            if timestamp >= ini and timestamp < end :
                tweets[userid]["messages"][i]["id_shard"] = j
                id_shards[j]=id_shards[j]+1
                if j==0:
                    M = M + 1
                break
            else:
                ini = end
                end = end + increment

    print(str(tweets[userid])+"\n\n")

print(">>>>>>>>>>>>>>>>>>>>>>>>>> "+str(M))

for j in range(0, windows):
    print(str(j)+";"+str(id_shards[j]))
