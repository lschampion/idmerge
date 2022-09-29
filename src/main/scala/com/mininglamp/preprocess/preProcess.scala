package com.mininglamp.preprocess

import com.mininglamp.utils.{Constants, PropertyUtil, StringUtil}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 * 读取高斯的数据，把结果放到hdfs的目录中
 * 生成两个目录/join和/merge
 * /merge 数据格式：证件号码1+空格+证件号码2+。。。
 * 用于后面的证件融合
 * /join  数据格式：证件号码+空格+人员id
 * 用户后面根据证件号码关联人员ID
 */
object preProcess {
  def main(args: Array[String]): Unit = {
    val textFilePath = args(0)
    val hdfsPath = args(1)

    val properties = PropertyUtil.getProperties(textFilePath)
    val jdbcProps = new Properties()
    jdbcProps.put("url", properties.getProperty("url"))
    jdbcProps.put("driver", properties.getProperty("driver"))
    jdbcProps.put("user", properties.getProperty("user"))
    jdbcProps.put("password", properties.getProperty("password"))

    val sparkConf = new SparkConf()
//    val spark = SparkSession.builder().appName("Preprocess").enableHiveSupport().config(sparkConf).getOrCreate()
//    测试需要在run configuration 添加参数：conf/test.properties tmp_data/preprocess
    val spark = SparkSession.builder().appName("Preprocess").master("local[*]").config(sparkConf).getOrCreate()
    val tableName = properties.getProperty("tableName")

    val predicates =
      Array(
        "19900101" -> "20050101",
        "20050101" -> "20060101",
        "20060101" -> "20070101",
        "20070101" -> "20080101",
        "20080101" -> "20090101",
        "20090101" -> "20100101",
        "20100101" -> "20110101",
        "20110101" -> "20120101",
        "20120101" -> "20130101",
        "20130101" -> "20140101",
        "20140101" -> "20150101",
        "20150101" -> "20160101",
        "20160101" -> "20170101",
        "20170101" -> "20180101",
        "20180101" -> "20190101",
        "20190101" -> "20200101",
        "20200101" -> "20210101",
        "20210101" -> "20220101",
        "20220101" -> "20230101",
        "20230101" -> "20240101",
        "20240101" -> "20250101",
        "20250101" -> "20260101",
        "20260101" -> "20270101",
        "20270101" -> "20280101",
        "20280101" -> "20290101",
        "20290101" -> "20300101",
        "20300101" -> "20310101",
        "20310101" -> "20320101",
        "20320101" -> "20330101",
        "20330101" -> "20340101",
        "20340101" -> "20350101",
        "20350101" -> "20360101",
        "20360101" -> "20370101",
        "20370101" -> "20380101",
        "20380101" -> "20390101",
        "20390101" -> "20400101",
        "20400101" -> "20410101",
        "20410101" -> "20420101",
        "20420101" -> "20430101",
        "20430101" -> "20440101",
        "20440101" -> "20450101",
        "20450101" -> "20460101",
        "20460101" -> "20470101",
        "20470101" -> "20480101",
        "20480101" -> "20490101",
        "20490101" -> "20500101"
      ).map {
        case (start, end) =>
          s"slrq >= '$start'::date AND slrq < '$end'::date"
      }
    //  测试代码

    val sc = spark.sparkContext
    val src_list = List(
        ("10001", Set("sfzh10001", "gazj10001", "xczj10001", "sbzj10001"))
      , ("10002", Set("sfzh10002", "gazj10002", "xczj10002", "sbzj10002"))
      , ("10003", Set("sfzh10001", "gazj10003", "xczj10003", "sbzj10003"))
    )
    val rdd = sc.makeRDD(src_list)  // 测试代码
    rdd.foreach(println)
//    val rdd = spark.read.jdbc(jdbcProps.getProperty("url"), tableName, predicates, jdbcProps).rdd.map(r => {
//      val recordId = getStrDefault(r.getAs[String]("mr_id_sq"))
//      val sqlb = getStrDefault(r.getAs[String]("sqlb"))
//      val sfzh = if (sqlb.equals("211")) getStrDefault(r.getAs[String]("sfzjxx")) else "";
//      val gazj = if (sqlb.equals("212")) getStrDefault(r.getAs[String]("gazjxx")) else ""
//      val xczj = getStrDefault(r.getAs[String]("xczjxx"))
//      var sbzj = getStrDefault(r.getAs[String]("sbzjxx"))
//      val spjg = getStrDefault(r.getAs[String]("spjg"))
//      if ("2".equals(spjg)) {
//        sbzj = ""
//      }
//      val set = Set(sfzh, gazj, xczj, sbzj).filter(StringUtil.isNotNull(_))
//      (recordId, set)
//    }).filter(x => x._2 != null && !x._2.isEmpty).repartition(10).cache()

    rdd.map(x => {      x._2.mkString(Constants.SPACE_DELIMITER)    })
//      .saveAsTextFile(hdfsPath + "/merge", classOf[GzipCodec]);
      .saveAsTextFile(hdfsPath+"/merge", classOf[GzipCodec]);  // 测试代码

    rdd.flatMap(x => {
      x._2.map(y => {
        (y, x._1)
      })
    }).reduceByKey((r1, r2) => {
      StringUtil.getMinStr(r1, r2)
    }).map(x => {
      x._1 + Constants.SPACE_DELIMITER + x._2
    })
//      .saveAsTextFile(hdfsPath + "/join", classOf[GzipCodec]);
      .saveAsTextFile( hdfsPath+"/join", classOf[GzipCodec]);  // 测试代码
  }

  def getStrDefault(str: String): String = {
    if (str == null) null else str
  }
}
