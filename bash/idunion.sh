unionPath=/user/ymadmin/gahq_ryxx/preprocess/join/*.gz
mergePath=/user/ymadmin/gahq_ryxx/merge/result/*.gz
outPath=/user/ymadmin/gahq_ryxx/result/incr

hdfs dfs -rm -r ${outPath}

spark-submit \
--master yarn \
--name idunion \
--driver-memory 8G \
--executor-memory 20G \
--num-executors 20 \
--executor-cores 2 \
--jars /data/merge/lib/jars/ \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--class com.mininglamp.union.IdUnion /data/merge/lib/IDsMerge-1.0.jar ${unionPath} ${mergePath} ${outPath}
