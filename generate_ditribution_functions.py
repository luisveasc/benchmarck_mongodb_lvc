import sys
import json
import numpy as np

jout =  {"FUNCTIONS_DISTRIBUTION": ["cauchy", "chi2", "expon", "norm", "uniform"],
        "LIMITS_SIZEBYTES":[0,100, 400, 700, 1000, 4000, 7000, 10000, 40000, 70000, 100000, 400000, 700000, 800000, 900000, 1000000, 2000000,3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000, 11000000, 12000000, 13000000, 14000000, 15000000, 16000000],
        "TIMES":[]
        }

pathfile = sys.argv[1]
file = open(pathfile, "r")
ID=-1
arrtime=[]

for row in file:
    #row = "{'ID':0,'STATUS':'FINALIZED','total_time_ns':7080655348,'total_time_ms':7080,'total_time_s':7}"
    if row.find("STATUS':'ERROR")==-1:
        #print(row)
        jrow = json.loads( row.replace("\n","").replace("\'","\"").replace("False","false").replace("True","true") )

        if(jrow["ID"]!=ID):
            IDFUNCTION=jrow["IDFUNCTION"]
            arrtime=[]
        else:
            if(jrow["STATUS"]!="FINALIZED"):
                arrtime.append(jrow["time_ns"])
            else:
                mean=np.mean(arrtime)
                std= np.std(arrtime)

                newarrtime=[]
                for time in arrtime:
                    if mean <= time:
                        time = mean
                    newarrtime.append(time)

                distro = {
                      "funDistr": {
                        "idFunDistr": 3,
                        "nombreFunDistr": "norm",
                        "params": [
                            np.mean(newarrtime),
                            np.std(newarrtime)
                        ]
                      },
                      "funDistr_map": IDFUNCTION
                    }
                print( str(distro) )

        ID=jrow["ID"]
file.close()
