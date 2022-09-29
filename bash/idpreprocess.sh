sqlPath="/data/merge/conf/sql.properties"
outPath="/user/ymadmin/gahq_ryxx/preprocess"
hdfs dfs -rm -r ${outPath}

spark-submit \
--master yarn \
--name preprocess \
--driver-memory 8G \
--executor-memory 20G \
--num-executors 20 \
--executor-cores 2 \
--jars /data/merge/lib/jars/ \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--class com.mininglamp.preprocess.GahqPreprocess /data/merge/lib/IDsMerge-1.0.jar ${sqlPath} ${outPath}
