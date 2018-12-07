
#!/bin/bash
archivo_log="/data/db/logdb.log"
data_log="data_log/"
data_python="data_python/data.dat"

python generate_test_list.py config.json > test00.dat
#cat test00.dat | grep  0_0_ > test01.dat
#cat test01.dat | grep DELETE > test00.dat

#rm test00.dat
#rm test01.dat

sudo killall mongod
sleep 3
sudo rm -rf /data/db
sudo mkdir /data/db


> $data_python
while read p; do
  echo "$p"

  echo "INICIA MONGODB-------------------------------------------------------"
  sudo mongod -v --logpath $archivo_log --dbpath /data/db  --fork
  sleep 3

  python3.7 run_test_mongo.py "$p" >> $data_python

  sudo killall mongod
  sleep 3

  #rescato info de log
  cat $archivo_log > $data_log"/"$p".dat"

  sudo rm -rf /data/db
  sudo mkdir /data/db

done < test00.dat
