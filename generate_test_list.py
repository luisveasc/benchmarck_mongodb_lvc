import sys
import json

arrlist=[
    630
    ]

def busca_en_lista(arr, val):
    flg=False
    for x in arr:
        if x==val:
            flg=True
            break
    return flg



filepath = sys.argv[1]
with open(filepath, 'r') as f:
    data = json.load(f)
    LAST_BYTESIZE = data["bytesize"][ len(data["bytesize"]) - 1]
    AMOUNT_TEST = data["ram_to_test"]/LAST_BYTESIZE
    SIZE_REST = data["size_rest"]
    ID=0

    for CTYPE, TYPE in enumerate(data["test_type"]):
        for CAACTION, ACTION in enumerate(data[TYPE]):
            for CBYTESIZE, BYTESIZE in enumerate(data["bytesize"]):
                MEMORY = BYTESIZE * AMOUNT_TEST
                FUNCTION=TYPE+"_"+ACTION
                IDFUNCTION=str(CTYPE)+"_"+str(CAACTION)+"_"+str(CBYTESIZE)

                if busca_en_lista(arrlist, ID) == True:
                    print("%d;%s;%d;%s;%d;%d;%d;%d;%d;%d;%d;%s;%s" % (ID,TYPE,CTYPE,ACTION,CAACTION,CBYTESIZE,BYTESIZE,MEMORY,AMOUNT_TEST,SIZE_REST,data["UPSERT"][0],FUNCTION,IDFUNCTION) )
                ID=ID+1

                #if TYPE=="UPDATE":
                #    print("%d;%s;%d;%s;%d;%d;%d;%d;%d;%d;%d;%s;%s" % (ID,TYPE,CTYPE,ACTION,CAACTION,CBYTESIZE,BYTESIZE,MEMORY,AMOUNT_TEST,SIZE_REST,data["UPSERT"][1],FUNCTION,IDFUNCTION) )
                #    ID=ID+1
