import sys
import json
import numpy as np


def get_json_distro(arrtime,arrrowscol,arrrowsfound,IDFUNCTION,FUNCTION):

    """
    n=len(arrtime)

    if "_SIDX_" in FUNCTION:
        for i in range(n):
            arrtime[i]=arrtime[i]/arrrowscol[i]

    if "_FOUND_CIDX_" in FUNCTION:
        for i in range(n):
            arrtime[i]=arrtime[i]/arrrowsfound[i]

    """
    mean=np.mean(arrtime)
    std=np.std(arrtime)

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
    return distro



jout =  {"FUNCTIONS_DISTRIBUTION": ["cauchy", "chi2", "expon", "norm", "uniform"],
        "LIMITS_SIZEBYTES":[0,100, 400, 700, 1000, 4000, 7000, 10000, 40000, 70000, 100000, 400000, 700000, 800000, 900000, 1000000, 2000000,3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000, 11000000, 12000000, 13000000, 14000000, 15000000, 16000000],
        "TIMES":[]
        }

jcommunication = [ {     "funDistr_map": "4_0_30",     "funDistr": {       "idFunDistr": 3, "params": [0,0],       "nombreFunDistr": "norm"
}   } , { "funDistr_map": "4_0_1",     "funDistr": {       "idFunDistr": 3, "params": [0,0], "nombreFunDistr": "norm"
}   }   , { "funDistr_map": "4_0_2", "funDistr": {       "idFunDistr": 3, "params": [0,0], "nombreFunDistr": "norm" }
} , { "funDistr_map": "4_0_3",     "funDistr": {       "idFunDistr": 3, "params": [0,0],       "nombreFunDistr": "norm"
}   }   , { "funDistr_map": "4_0_4",     "funDistr": {       "idFunDistr": 3, "params": [0,0],       "nombreFunDistr":
"norm"     }   } , { "funDistr_map": "4_0_5", "funDistr": { "idFunDistr": 3, "params": [0,0],       "nombreFunDistr":
"norm" }   }   , { "funDistr_map": "4_0_6", "funDistr": {       "idFunDistr": 3, "params": [0,0],
"nombreFunDistr": "norm"     }   } , { "funDistr_map": "4_0_7",     "funDistr": {       "idFunDistr": 3, "params":
[0,0], "nombreFunDistr": "norm"     }   }   , { "funDistr_map": "4_0_8", "funDistr": {       "idFunDistr": 3, "params":
[0,0],       "nombreFunDistr": "norm"     } } , { "funDistr_map": "4_0_9",     "funDistr": { "idFunDistr": 3, "params":
[0,0],       "nombreFunDistr": "norm" }   }   , { "funDistr_map": "4_0_10",     "funDistr": {       "idFunDistr": 3,
"params": [0,0],       "nombreFunDistr": "norm"     }   } , { "funDistr_map": "4_0_11", "funDistr": {
"idFunDistr": 3, "params": [0,0], "nombreFunDistr": "norm" }   }   , { "funDistr_map": "4_0_12",     "funDistr": {
"idFunDistr": 3, "params": [0,0],       "nombreFunDistr": "norm"     }   } , { "funDistr_map": "4_0_13",     "funDistr":
{       "idFunDistr": 3, "params": [0,0], "nombreFunDistr": "norm"     }   }   , { "funDistr_map": "4_0_14", "funDistr":
{ "idFunDistr": 3, "params": [0,0],       "nombreFunDistr": "norm" }   } , { "funDistr_map": "4_0_15",     "funDistr": {
"idFunDistr": 3, "params": [0,0],       "nombreFunDistr": "norm"     }   }   , { "funDistr_map": "4_0_16", "funDistr": {
"idFunDistr": 3, "params": [0,0],       "nombreFunDistr": "norm"     }   } , { "funDistr_map": "4_0_17", "funDistr": {
"idFunDistr": 3, "params": [0,0],       "nombreFunDistr": "norm"     }   }   , { "funDistr_map": "4_0_18",
"funDistr": { "idFunDistr": 3, "params": [0,0],       "nombreFunDistr": "norm"     } } , { "funDistr_map": "4_0_19",
"funDistr": {       "idFunDistr": 3, "params": [0,0],       "nombreFunDistr": "norm" }   }   , { "funDistr_map":
"4_0_20", "funDistr": {       "idFunDistr": 3, "params": [0,0],       "nombreFunDistr": "norm"     }   } , {
"funDistr_map": "4_0_21",     "funDistr": { "idFunDistr": 3, "params": [0,0], "nombreFunDistr": "norm"     }   }   , {
"funDistr_map": "4_0_22",     "funDistr": {       "idFunDistr": 3, "params": [0,0],       "nombreFunDistr": "norm"     }
} , { "funDistr_map": "4_0_23", "funDistr": {       "idFunDistr": 3, "params": [0,0],       "nombreFunDistr": "norm"
}   }   , { "funDistr_map": "4_0_24",     "funDistr": { "idFunDistr": 3, "params": [0,0],       "nombreFunDistr": "norm"
}   } , { "funDistr_map": "4_0_25",     "funDistr": {       "idFunDistr": 3, "params": [0,0],       "nombreFunDistr":
"norm"     }   }   , { "funDistr_map": "4_0_26", "funDistr": {       "idFunDistr": 3, "params": [0,0], "nombreFunDistr":
"norm"     }   } , { "funDistr_map": "4_0_27", "funDistr": {       "idFunDistr": 3, "params": [0,0],
"nombreFunDistr": "norm"     }   }   , { "funDistr_map": "4_0_28",     "funDistr": { "idFunDistr": 3, "params": [0,0],
"nombreFunDistr": "norm"     } } , { "funDistr_map": "4_0_29",     "funDistr": {       "idFunDistr": 3, "params": [0,0],
"nombreFunDistr": "norm" }   }
]

def correccion_del_ultimo_elemento_del_idfunction(jarr):

    jout=[]
    for jdata in jarr:
        arr = jdata["funDistr_map"].split("_")
        arr[2]=int(arr[2])+1
        jdata["funDistr_map"] = str(arr[0])+"_"+str(arr[1])+"_"+str(arr[2])
        jout.append(jdata)

    return jout


pathinfile = sys.argv[1]
file = open(pathinfile, "r")
IDFUNCTION=""
FUNCTION=""
flgFirstLine=True
arrtime=[]
arrrowscol=[]
arrrowsfound=[]
distro={}

for row in file:

    jrow = json.loads( row.replace("\n","").replace("\'","\"").replace("False","false").replace("True","true") )
    if jrow["STATUS"]!="FINALIZED":

        if(jrow["IDFUNCTION"]!=IDFUNCTION):
            if flgFirstLine==False:
                distro = get_json_distro(arrtime,arrrowscol,arrrowsfound,IDFUNCTION,FUNCTION)
                jout["TIMES"].append(distro)

            flgFirstLine=False
            arrtime=[]
            arrrowscol=[]
            arrrowsfound=[]

        else:
            arrtime.append(jrow["time_ns"])
            arrrowscol.append(jrow["rows_col"])
            arrrowsfound.append(jrow["rows"])


        IDFUNCTION=jrow["IDFUNCTION"]
        FUNCTION=jrow["FUNCTION"]
file.close()

distro = get_json_distro(arrtime,arrrowscol,arrrowsfound,IDFUNCTION,FUNCTION)
jout["TIMES"].append(distro)
jout["TIMES"] = jout["TIMES"] + jcommunication
#jout["TIMES"] = correccion_del_ultimo_elemento_del_idfunction(jout["TIMES"])
print(json.dumps(jout, ensure_ascii=False))
