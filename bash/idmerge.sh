idcount=1000
preprocePath=/user/ymadmin/gahq_ryxx/preprocess/merge/*.gz
mergePath=/user/ymadmin/gahq_ryxx/merge/
hdfs dfs -rm -r ${mergePath}


spark-submit \
--master yarn \
--name idmerge \
--driver-memory 8G \
--executor-memory 20G \
--num-executors 20 \
--executor-cores 2 \
--jars /data/merge/lib/jars/ \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--class com.mininglamp.merge.IDsMergeMain /data/merge/lib/IDsMerge-1.0.jar ${preprocePath} ${mergePath} ${idcount}
