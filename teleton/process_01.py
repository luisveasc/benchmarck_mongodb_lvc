# -*- coding: utf-8 -*-

import sys
import json
import pymongo
import time
import datetime
import math

conn = pymongo.MongoClient("mongodb://localhost:27017/")
dbname = "tweets_teleton"
colname = "tweets"
db = conn[dbname]

pipeline =  [ { "$group":
                     {
                       "_id": "1",
                       "min_timestamp_ms": { "$min": "$timestamp_ms" },
                       "max_timestamp_ms": { "$max": "$timestamp_ms" }
                     }
                }
             ]
resp = list(db[colname].aggregate(pipeline))[0]
print(resp)
min=int(resp["min_timestamp_ms"])
max=int(resp["max_timestamp_ms"])

increment = 7200000 #in milisegundos => una hora
windows = round( (max-min)/increment )
print("windows:"+str(windows))
traceByUser={}
usersIds = []
tweets = list(db[colname].find({}).sort([("timestamp_ms", pymongo.ASCENDING)]))
distributionByWindows = [0]*windows

for tweet in tweets:
    user_windows_location = int( windows * (float(int(tweet["timestamp_ms"])-min)/float(max-min+1)))
    distributionByWindows[user_windows_location] =  distributionByWindows[user_windows_location] + 1

    userid = tweet["user"]["id_str"]
    if userid not in traceByUser:
        sizebytes = len(tweet)
        hold_ms = int(tweet["timestamp_ms"]) - min
        message = {"sizebytes":sizebytes,"hold_ms": hold_ms, "timestamp_ms":tweet["timestamp_ms"],"user_windows_location":user_windows_location}
        traceByUser[userid] = {"userid":userid,"nbr_msgs":1,"messages":[message],"last_timestamp_ms":tweet["timestamp_ms"],"first_timestamp_ms":tweet["timestamp_ms"]}
        usersIds.append(userid)
    else:
        traceByUser[userid]["nbr_msgs"] = int(traceByUser[userid]["nbr_msgs"]) + 1
        hold_ms = int(tweet["timestamp_ms"]) - int(traceByUser[userid]["last_timestamp_ms"])
        traceByUser[userid]["messages"].append({"sizebytes":sizebytes,"hold_ms": hold_ms, "timestamp_ms":tweet["timestamp_ms"],"user_windows_location":user_windows_location })
        traceByUser[userid]["last_timestamp_ms"]=tweet["timestamp_ms"]

print("\n")
for amount in distributionByWindows:
    print(amount)
