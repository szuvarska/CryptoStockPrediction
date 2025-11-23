#!/bin/bash
# Czekamy aż NameNode będzie gotowy
until docker exec namenode hdfs dfsadmin -safemode wait; do
  echo "Czekam na NameNode..."
  sleep 5
done

# Tworzymy podstawowe katalogi
docker exec namenode hdfs dfs -mkdir -p /tmp
docker exec namenode hdfs dfs -mkdir -p /user/hive/warehouse
docker exec namenode hdfs dfs -chmod -R 1777 /tmp
docker exec namenode hdfs dfs -chmod -R 1777 /user/hive/warehouse

echo "HDFS skonfigurowany!"
