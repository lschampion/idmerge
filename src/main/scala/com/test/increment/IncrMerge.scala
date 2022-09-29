package com.test.increment

import com.test.utils.{Constants, FileUtils, PropertyUtil}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File

/**
 * 存量数据和增量数据融合，相同证件id 同时存在存量和增量时，人员id取存量数据中的人员id
 * 存量数据：
 * historyPath
 * 数据格式  id:id1+空格+证件id+...
 * 增量数据：
 * incrPath
 * 数据格式  id:id1+空格+证件id+...
 * 结果数据：
 * allPath
 * 数据格式  id:id1+空格+证件id+...
 *
 */
object IncrMerge {
  def main(args: Array[String]): Unit = {
    println("args====" + args.mkString(","))
    //  tmp_data/result/history
    val historyPath = args(0)
    val incrPath = args(1)
    val allPath = args(2)
    val historyDir = new File(historyPath)
    val allDir = new File(allPath)
    FileUtils.deleteDir(allDir)


    //    val spark = SparkSession.builder().config(new SparkConf().setAppName("IncrMerge")).enableHiveSupport().getOrCreate()
    //  测试需要在run configuration 添加参数：tmp_data/result/history tmp_data/result/incr tmp_data/result/all
    val spark = SparkSession.builder().master("local[*]").config(new SparkConf().setAppName("IncrMerge")).getOrCreate() // 测试代码
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    //    如果不需要测试history和incr聚合的情况，可以仅创建空的history文件夹即可。
    //    if(!historyDir.exists()){
    //      historyDir.mkdir()
    //    }
    //  测试数据（history文件夹自动创建）
    FileUtils.deleteDir(historyDir)
    sc.makeRDD(List("10002:sfzh10002 sbzj10004")).coalesce(1).saveAsTextFile(historyPath, classOf[GzipCodec]);

    val rddAll = sc.textFile(historyPath).map(x => {
      val arr = x.split(":")
      (arr(0), arr(1))
    }).flatMap(x => {
      x._2.split(Constants.SPACE_DELIMITER).map(
        r => {
          (r, x._1)
        })
    }).cache()
    //    println("rddAll: ")
    //    rddAll.foreach(println)
    //    (sfzh10002,10002)
    //    (sbzj10004,10002)
    val rddIncr = sc.textFile(incrPath).map(x => {
      val arr = x.split(":")
      (arr(0), arr(1))
    }).flatMap(x => {
      x._2.split(Constants.SPACE_DELIMITER).map(
        r => {
          (r, x._1)
        })
    })
    //    println("rddIncr: ")
    //    rddIncr.sortBy(x => (x._2, x._1),ascending = true,numPartitions = 1).foreach(println)
    //    (gazj10001,10001)
    //    (gazj10001,10001)
    //    (gazj10003,10001)
    //    (gazj10003,10001)
    //    (sbzj10001,10001)
    //    (sbzj10001,10001)
    //    (sbzj10003,10001)
    //    (sbzj10003,10001)
    //    (sfzh10001,10001)
    //    (sfzh10001,10001)
    //    (xczj10001,10001)
    //    (xczj10001,10001)
    //    (xczj10003,10001)
    //    (xczj10003,10001)
    //    (gazj10002,10002)
    //    (sbzj10002,10002)
    //    (sfzh10002,10002)
    //    (xczj10002,10002)

    //    使用增量left join 全量保证增量不丢失。
    rddIncr.leftOuterJoin(rddAll).map(x => {
      //      （左表的id,(证件号码，右边表的id)）
      (x._2._1, (x._1, x._2._2))
    }).reduceByKey((r1, r2) => {
      //      此处也都是使用的右表的id
      val id = if (r1._2.isEmpty) r2._2 else r1._2
      //      (证件号码1 证件号码2 证件号码3...，右表id)
      (r1._1 + Constants.SPACE_DELIMITER + r2._1, id)
    }).map(x => {
//      (证件号码1 证件号码2 证件号码3..., 右表id如果取不到则用左表id，此处表示incr和history有关联的数据的incr的id被替换成了history的id)
      (x._2._1, x._2._2.getOrElse(x._1))
    }).flatMap(x => {
      x._1.split(Constants.SPACE_DELIMITER).map(r => {
//        (id,set(证件号码))
        (x._2, Set(r))
      })
    }).union(rddAll.map(x => {
//      （id,set(证件号码)）
      (x._2, Set(x._1))
//      此处聚合的条件是incr 中的 id已经被history的id 替换掉了。
    })).reduceByKey((r1, r2) => {
//      set.union(set) set集合取并集
      r1 ++ r2
    }).map(x => {
      x._1 + ":" + x._2.mkString(Constants.SPACE_DELIMITER)
    })
      //    .foreach(println)
      //    10002:sbzj10004 xczj10002 sfzh10002 sbzj10002 gazj10002
      //    10001:sbzj10001 xczj10001 xczj10003 sfzh10001 gazj10003 gazj10001 sbzj10003
      .saveAsTextFile(allPath, classOf[GzipCodec]);

    sc.stop
  }
}
