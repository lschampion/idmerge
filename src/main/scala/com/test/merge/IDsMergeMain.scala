package com.test.merge

import com.test.utils.{Constants, FileUtils}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.util.StringTokenizer
import org.apache.spark.sql.SparkSession

import java.io.File

/**
 * 把有相同证件id的多条数据融合为1条数据，最终生成的数据在hdfs的{idmergeOutput}/result目录下
 * 生成数据格式 证件id1+空格+证件id2+....
 *
 */
object IDsMergeMain extends App {

  override def main(args: Array[String]): Unit = {
    val idsPath = args(0)
    val idmergeOutput=args(1)
    val threshould = args(2).toInt

//    val sc = new SparkContext( new SparkConf().setAppName("IDsMerge"));
//    //    测试需要在run configuration 添加参数：tmp_data/preprocess/merge/*.gz tmp_data/merge/ 10
    val sc = new SparkContext( new SparkConf().setMaster("local[*]").setAppName("IDsMerge"));  // 测试代码
    sc.setLogLevel("WARN")
    FileUtils.deleteDir(new File(idmergeOutput))

    val iu = new IdsUnion();

    val uidBARdd1: RDD[String] = sc.textFile(idsPath).map { line => {
      val sb=new StringBuilder();
      val st = new StringTokenizer(line);
      while(st.hasMoreTokens()){
        sb.append(st.nextToken()).append(Constants.SPACE_DELIMITER);
      }
      sb.toString().trim();
    }}
//    println("uidBARdd1: ")
//    uidBARdd1.foreach(println)
//      sfzh10001 gazj10003 xczj10003 sbzj10003
//      sfzh10002 gazj10002 xczj10002 sbzj10002
//      sfzh10001 gazj10001 xczj10001 sbzj10001
    iu.encodeIds(uidBARdd1, sc.defaultParallelism , idmergeOutput , threshould);
    val lineCodingRdd = sc.textFile(idmergeOutput + "LineCoding/*.gz");
//    println("LineCoding: ")
//    lineCodingRdd.foreach(println)
//    1 4 6 8
//    0 2 7 10
//    2 3 5 9
    iu.idsMerge(lineCodingRdd, idmergeOutput+"merge/", threshould);
    val uidsMergeRdd = sc.textFile(idmergeOutput + "merge/*/*.gz");
//    println("uidsMerge: ")
//    uidsMergeRdd.foreach(println)
//    1 4 6 8
//    0 2 3 5 7 9 10
    val idCodingRdd=sc.textFile(idmergeOutput + "IDCoding/*.gz")
//    println("idCoding: ")
//    idCodingRdd.foreach(println)
//    sbzj10001 7
//    gazj10003 5
//    sbzj10002 8
//    sfzh10002 6
//    gazj10001 0
//    xczj10002 1
//    sfzh10001 2
//    xczj10003 3
//    gazj10002 4
//    xczj10001 10
//    sbzj10003 9
    iu.decodeIds(uidsMergeRdd, sc.defaultParallelism, idCodingRdd, idmergeOutput + "result/", threshould)
    val resultRdd=sc.textFile(idmergeOutput + "result/*.gz")
//    println("result: ")
//    resultRdd.foreach(println)
//    sbzj10003 gazj10003 gazj10001 xczj10001 sbzj10001 sfzh10001 xczj10003
//    sfzh10002 xczj10002 sbzj10002 gazj10002
    sc.stop
  }

}