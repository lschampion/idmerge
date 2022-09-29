package com.mininglamp.union

import com.mininglamp.utils.{Constants, JDBCUtil, PropertyUtil, StringUtil}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 把IncrMerge生成的数据写入高斯newimportdata.m_gahq_ryxx_mx表中
  */
object HdfsToGauss {
    def main(args: Array[String]): Unit = {
    println("args===="+args.mkString(","))

    val idMergePath = args(0)
    val properties =  PropertyUtil.getProperties(args(1))
    println("properties===="+properties)

//    val spark = SparkSession.builder().config(new SparkConf().setAppName("HdfsToGauss")).enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder().master("local[*]").config(new SparkConf().setAppName("HdfsToGauss")).getOrCreate()  // 测试
    val sc = spark.sparkContext

    import spark.implicits._
    val res_df: DataFrame =sc.textFile(idMergePath).map(x=>{
      val arr = x.split(":")
      (arr(1),arr(0))
    }).flatMap(x=>{
      x._1.split(Constants.SPACE_DELIMITER).map(
        r=> {
          (x._2, r)
        })
    }).toDF("id","zjxx")

    res_df.show()
//    res_df.write.format("jdbc").mode(SaveMode.Overwrite)
//      .option("driver",properties.getProperty("driver"))
//      .option("url", properties.getProperty("url"))
//      .option("dbtable","newimportdata.m_gahq_ryxx_mx")
//      .option("user", properties.getProperty("user"))
//      .option("password", properties.getProperty("password"))
//      .option("batchsize",4000)
//      .save()

    sc.stop
  }
}
