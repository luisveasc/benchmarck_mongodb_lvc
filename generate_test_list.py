import sys
import json

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
                print("%d;%s;%d;%s;%d;%d;%d;%d;%d;%d;%d" % (ID,TYPE,CTYPE,ACTION,CAACTION,CBYTESIZE,BYTESIZE,MEMORY,AMOUNT_TEST,SIZE_REST,data["UPSERT"][0]) )
                ID=ID+1
                if TYPE=="UPDATE":
                    print("%d;%s;%d;%s;%d;%d;%d;%d;%d;%d;%d" % (ID,TYPE,CTYPE,ACTION,CAACTION,CBYTESIZE,BYTESIZE,MEMORY,AMOUNT_TEST,SIZE_REST,data["UPSERT"][1]) )
                    ID=ID+1
