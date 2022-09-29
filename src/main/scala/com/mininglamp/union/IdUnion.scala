package com.mininglamp.union

import com.mininglamp.utils.{Constants, FileUtils, JDBCUtil, PropertyUtil, StringUtil}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

/**
  * 把IDsMergeMain的结果与GahqPreprocess生成在/join的结果关联，根据证件号码关联，取出最小的人员id
  * IDsMergeMain：
 *    在idMergePath目录下数据格式证件id1+空格+证件id+...
 *  GahqPreprocess
 *    在idUnionPath目录下数据格式证件id1+空格+人员id
 *
 *  生成的数据数据在outPath目录下，数据格式人员id:id1+空格+证件id+...
 */
object IdUnion{
  def main(args: Array[String]): Unit = {
    val idUnionPath = args(0)
    val idMergePath = args(1)
    val outPath = args(2)

//    val spark = SparkSession.builder().appName("IdUnion").config(new SparkConf()).enableHiveSupport().getOrCreate()
//  测试需要在run configuration 添加参数：tmp_data/preprocess/join/*.gz tmp_data/merge/result/*.gz tmp_data/result/incr
    val spark = SparkSession.builder().master("local[*]").appName("IdUnion").config(new SparkConf()).getOrCreate()  // 测试
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    FileUtils.deleteDir(new File(outPath))
//  tmp_data/merge/result/*.gz
    val idMergeRdd=sc.textFile(idMergePath).flatMap(x=>{
      x.split(Constants.SPACE_DELIMITER).map(id=>{
        (id,x)
      })
    })
//    println("idMergeRdd: ")
//    idMergeRdd.sortBy(x=>(x._2.sorted.mkString(" "),x._1),ascending = true,numPartitions = 1)foreach(println)
//    (gazj10001,sbzj10003 gazj10003 gazj10001 xczj10001 sbzj10001 sfzh10001 xczj10003)
//    (gazj10003,sbzj10003 gazj10003 gazj10001 xczj10001 sbzj10001 sfzh10001 xczj10003)
//    (sbzj10001,sbzj10003 gazj10003 gazj10001 xczj10001 sbzj10001 sfzh10001 xczj10003)
//    (sbzj10003,sbzj10003 gazj10003 gazj10001 xczj10001 sbzj10001 sfzh10001 xczj10003)
//    (sfzh10001,sbzj10003 gazj10003 gazj10001 xczj10001 sbzj10001 sfzh10001 xczj10003)
//    (xczj10001,sbzj10003 gazj10003 gazj10001 xczj10001 sbzj10001 sfzh10001 xczj10003)
//    (xczj10003,sbzj10003 gazj10003 gazj10001 xczj10001 sbzj10001 sfzh10001 xczj10003)
//    (gazj10002,sfzh10002 xczj10002 sbzj10002 gazj10002)
//    (sbzj10002,sfzh10002 xczj10002 sbzj10002 gazj10002)
//    (sfzh10002,sfzh10002 xczj10002 sbzj10002 gazj10002)
//    (xczj10002,sfzh10002 xczj10002 sbzj10002 gazj10002)
    val idUnionRdd = sc.textFile(idUnionPath).map(x=>{
      val arr = x.split(Constants.SPACE_DELIMITER)
      (arr(0),arr(1))
    })
//    println("idUnionRdd: ")
//    idUnionRdd.sortBy(x=>x._2,ascending = true,numPartitions = 1).foreach(println)
//    (gazj10001,10001)
//    (sfzh10001,10001)
//    (sbzj10001,10001)
//    (xczj10001,10001)
//    (xczj10002,10002)
//    (gazj10002,10002)
//    (sfzh10002,10002)
//    (sbzj10002,10002)
//    (xczj10003,10003)
//    (gazj10003,10003)
//    (sbzj10003,10003)
   val res=idMergeRdd.join(idUnionRdd).map(x=>{
      (x._2._1,x._2._2)
    }).reduceByKey((r1,r2)=>{
      StringUtil.getMinStr(r1,r2)
    }).map(x=>{
      x._2+":"+x._1
    })
//    println("res: ")
//    res.foreach(println)
//      10002:sfzh10002 xczj10002 sbzj10002 gazj10002
//      10001:sbzj10003 gazj10003 gazj10001 xczj10001 sbzj10001 sfzh10001 xczj10003
    res.saveAsTextFile(outPath, classOf[GzipCodec]);
    sc.stop
  }
}
