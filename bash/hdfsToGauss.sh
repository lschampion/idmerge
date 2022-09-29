hdfsPath=/user/ymadmin/gahq_ryxx/result/all/*.gz
jdbcPath=/data/merge/conf/jdbc.properties

spark-submit \
--master yarn \
--name idunion \
--driver-memory 8G \
--executor-memory 20G \
--num-executors 20 \
--executor-cores 2 \
--jars /data/merge/lib/jars/ \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--class com.mininglamp.union.HdfsToGauss /data/merge/lib/IDsMerge-1.0.jar ${hdfsPath} ${jdbcPath}
