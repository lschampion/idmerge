historyPath=/user/ymadmin/gahq_ryxx/result/history
incrPath=/user/ymadmin/gahq_ryxx/result/incr
allPath=/user/ymadmin/gahq_ryxx/result/all
bakPath=/user/ymadmin/gahq_ryxx/result/bak/`date '+%Y%m%d'`


hdfs dfs -rm -r ${allPath}
hdfs dfs -rm -r ${bakPath}
hdfs dfs -rm ${historyPath}/*.gz

maxDate=`hdfs dfs -ls /user/ymadmin/gahq_ryxx/result/bak | awk -F / '{print $NF}'|awk 'BEGIN {max = 0} {if ($1+0 > max+0) max=$1} END {print max}'`
hdfs dfs -cp /user/ymadmin/gahq_ryxx/result/bak/${maxDate}/*.gz ${historyPath}

spark-submit \
--master yarn \
--name idunion \
--driver-memory 8G \
--executor-memory 20G \
--num-executors 20 \
--executor-cores 2 \
--jars /data/merge/lib/jars/ \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--class com.mininglamp.increment.IncrMerge /data/merge/lib/IDsMerge-1.0.jar ${historyPath} ${incrPath} ${allPath}

fileNum=`hdfs dfs -ls ${allPath}/*.gz | wc -l`
if [ ${fileNum} -gt 10 ] ;then
    hdfs dfs -mkdir -p ${bakPath}
    hdfs dfs -cp ${allPath}/*.gz ${bakPath}
fi
