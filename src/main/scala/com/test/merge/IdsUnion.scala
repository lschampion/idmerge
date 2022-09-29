package com.test.merge

import java.util.StringTokenizer
import com.test.utils.{Constants, MD5Util, StringsRandom}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import java.io.File
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Set}

class IdsUnion extends App {

  def encodeIds(rdd: RDD[String], defaultParallelism: Int, idmergeOutput: String, threshould: Int): Unit = {
    //    rdd.foreach(println)
    //      sfzh10001 gazj10001 xczj10001 sbzj10001
    //      sfzh10002 gazj10002 xczj10002 sbzj10002
    //      sfzh10001 gazj10003 xczj10003 sbzj10003
    val data: RDD[Array[String]] =
    rdd.map(_.split(Constants.SPACE_DELIMITER).distinct)
      .filter(arr => arr.length < threshould /*&& arr.length >1*/)
      .repartition(defaultParallelism)
      .persist(StorageLevel.MEMORY_AND_DISK_SER);
    //    data.foreach(arr=>println(arr.mkString(" ")))
    //      sfzh10001 gazj10001 xczj10001 sbzj10001
    //      sfzh10001 gazj10003 xczj10003 sbzj10003
    //      sfzh10002 gazj10002 xczj10002 sbzj10002


    //    println("data-flatmap: ")
    //    data.flatMap(ele=>ele).zipWithIndex().sortBy(x=>x._2,ascending = true,numPartitions = 1).foreach(println)
    //    (sfzh10001,0)
    //    (gazj10003,1)
    //    (xczj10003,2)
    //    (sbzj10003,3)
    //    (sfzh10002,4)
    //    (gazj10002,5)
    //    (xczj10002,6)
    //    (sbzj10002,7)
    //    (sfzh10001,8)
    //    (gazj10001,9)
    //    (xczj10001,10)
    //    (sbzj10001,11)

    val encoder: RDD[(String, Int)] =
      data.flatMap(arr => arr)
        .distinct()
        .zipWithIndex()
        .map {
          case (id, code) =>
            (id, code.toInt)
        }
        .partitionBy(new HashPartitioner(defaultParallelism))
        .persist(StorageLevel.MEMORY_AND_DISK_SER);

    //    encoder.sortBy(x=>(x._2,x._1),ascending = true,numPartitions = 1).foreach( x =>println(x._1,x._2));
    //    (gazj10001,0)
    //    (xczj10002,1)
    //    (sfzh10001,2)
    //    (xczj10003,3)
    //    (gazj10002,4)
    //    (gazj10003,5)
    //    (sfzh10002,6)
    //    (sbzj10001,7)
    //    (sbzj10002,8)
    //    (sbzj10003,9)
    //    (xczj10001,10)
    encoder.map {
      case (id, code) =>
        s"$id $code";
    }.saveAsTextFile(idmergeOutput + "IDCoding", classOf[GzipCodec]);

    data.zipWithIndex()
      .flatMap {
        case (arr, index) =>
          arr.map((_, index))
      }
      //        .sortBy(x=>(x._2,x._1),ascending = true,numPartitions = 1).foreach(println)
      //    (gazj10003,0)
      //    (sbzj10003,0)
      //    (sfzh10001,0)
      //    (xczj10003,0)
      //    (gazj10002,1)
      //    (sbzj10002,1)
      //    (sfzh10002,1)
      //    (xczj10002,1)
      //    (gazj10001,2)
      //    (sbzj10001,2)
      //    (sfzh10001,2)
      //    (xczj10001,2)
      .partitionBy(new HashPartitioner(defaultParallelism))
      .join(encoder)
      .map {
        case (id, (index, code)) =>
          (index, code)
      }.groupByKey(defaultParallelism)
      .map {
        case (index, codes) =>
          codes.toArray.sorted.mkString(Constants.SPACE_DELIMITER)
      }
      //      .foreach(println)
      //    1 4 6 8
      //    2 3 5 9
      //    0 2 7 10
      .saveAsTextFile(idmergeOutput + "LineCoding", classOf[GzipCodec]);
  }

  def decodeIds(idsRdd: RDD[String], defaultParallelism: Int, idCoding: RDD[String], idmergeOutput: String, threshould: Int): Unit = {
    //    println("idsRdd: ")
    //    idsRdd.foreach(println)
    //    1 4 6 8
    //    0 2 3 5 7 9 10
    val data: RDD[(Int, String)] =
    idsRdd
      .flatMap {
        l =>
          val vs: Array[Int] = l.split(Constants.SPACE_DELIMITER).map(_.toInt)
          //            for (elem <- vs) {print(elem+"-")};println("")
          //            1-4-6-8-
          //            0-2-3-5-7-9-10-
          val codeGroupList = ArrayBuffer[(Int, String)]();
          for (code <- vs) {
            codeGroupList.+=((code, vs(0).toString));
          }
          codeGroupList
      }
      .partitionBy(new HashPartitioner(defaultParallelism))
      .persist(StorageLevel.MEMORY_AND_DISK_SER);
    //        println("data: ")
    //        data.sortBy(x=>(x._2,x._1),ascending = true,numPartitions = 1).foreach(println)
    //    (0,0)
    //    (2,0)
    //    (3,0)
    //    (5,0)
    //    (7,0)
    //    (9,0)
    //    (10,0)
    //    (1,1)
    //    (4,1)
    //    (6,1)
    //    (8,1)
    val decoder: RDD[(Int, String)] =
    idCoding.map {
      l =>
        val vs = l.split(Constants.SPACE_DELIMITER)
        //  这里仅仅使用vs数组的第一个作为reduceby时候的key,其实也可以使用整个vs.mkString(" ")作为key,但是增加了key值的长度ð没有必要。
        (vs(1).toInt, vs(0))
    }
      .partitionBy(new HashPartitioner(defaultParallelism))
      .persist(StorageLevel.MEMORY_AND_DISK_SER);
    //    println("decoder: ")
    //    decoder.foreach(println)
    //    (4,gazj10002)
    //    (3,xczj10003)
    //    (6,sfzh10002)
    //    (7,sbzj10001)
    //    (5,gazj10003)
    //    (2,sfzh10001)
    //    (10,xczj10001)
    //    (1,xczj10002)
    //    (9,sbzj10003)
    //    (0,gazj10001)
    //    (8,sbzj10002)
    val rdd = data.join(decoder)
      .map {
        case (code, (group, id)) =>
          (group, Set(id))
      }
    //    println("rdd: ")
    //    rdd.foreach(println)
    //    (0,Set(gazj10001))
    //    (0,Set(xczj10001))
    //    (1,Set(sbzj10002))
    //    (0,Set(xczj10003))
    //    (0,Set(sfzh10001))
    //    (1,Set(gazj10002))
    //    (1,Set(xczj10002))
    //    (0,Set(sbzj10003))
    //    (0,Set(sbzj10001))
    //    (1,Set(sfzh10002))
    //    (0,Set(gazj10003))
    //    ++ 是集合运算一般适合不可变集合取并集，可变集合可以使用 _.union(_)
    rdd.reduceByKey(_ ++ _)
      .filter(_._2.size < threshould)
      .map(code_set => {
        val idsSB = new StringBuilder();
        for (id <- code_set._2) {
          idsSB.append(id).append(Constants.SPACE_DELIMITER);
        }
        idsSB.toString().trim()
      })
      .map(ids => ids)
      //      .foreach(println)
      //    sbzj10003 gazj10003 gazj10001 xczj10001 sbzj10001 sfzh10001 xczj10003
      //    sfzh10002 xczj10002 sbzj10002 gazj10002
      .saveAsTextFile(idmergeOutput, classOf[GzipCodec]);

  }

  def idsMerge(lineCoding: RDD[String], idmergeOutput: String, threshould: Int) {
    val idsFlatTmp: RDD[(Int, (mutable.Set[Int], Int))] =
      lineCoding.filter { idLine => {
        val st = new StringTokenizer(idLine);
        st.countTokens() < threshould;
      }
      }.map { idLine => {
        val set = Set[Int]()
        val st = new StringTokenizer(idLine);
        while (st.hasMoreTokens()) {
          set += (st.nextToken().toInt);
        }
        set
      }
      }.flatMap(idset => {
        val arr = new ArrayBuffer[(Int, (mutable.Set[Int], Int))]()
        idset.foreach(id => {
          arr += ((id, (idset, 0)))
        })
        arr
      })
    //    idsFlatTmp.sortBy(x=>x._1,ascending = true,numPartitions = 1).foreach(println)
    //    (0,(Set(0, 2, 10, 7),0))
    //    (1,(Set(1, 6, 4, 8),0))
    //    (2,(Set(9, 5, 2, 3),0))
    //    (2,(Set(0, 2, 10, 7),0))
    //    (3,(Set(9, 5, 2, 3),0))
    //    (4,(Set(1, 6, 4, 8),0))
    //    (5,(Set(9, 5, 2, 3),0))
    //    (6,(Set(1, 6, 4, 8),0))
    //    (7,(Set(0, 2, 10, 7),0))
    //    (8,(Set(1, 6, 4, 8),0))
    //    (9,(Set(9, 5, 2, 3),0))
    //    (10,(Set(0, 2, 10, 7),0))
    shuffleLoops(idsFlatTmp, idmergeOutput + "rst", threshould);
  }

  private def shuffleLoops(idsFlatInit: RDD[(Int, (Set[Int], Int))],
                           idmergeOutput: String, threshould: Int): Unit = {

    //    println("idsFlatInit: ")
    //    idsFlatInit.sortBy(x=>x._2._1.toList.sorted.mkString("-"),ascending = true,numPartitions = 1).foreach(println)
    //    (0,(Set(0, 2, 10, 7),0))
    //    (2,(Set(0, 2, 10, 7),0))
    //    (10,(Set(0, 2, 10, 7),0))
    //    (7,(Set(0, 2, 10, 7),0))
    //    (1,(Set(1, 6, 4, 8),0))
    //    (6,(Set(1, 6, 4, 8),0))
    //    (4,(Set(1, 6, 4, 8),0))
    //    (8,(Set(1, 6, 4, 8),0))
    //    (9,(Set(9, 5, 2, 3),0))
    //    (5,(Set(9, 5, 2, 3),0))
    //    (2,(Set(9, 5, 2, 3),0))
    //    (3,(Set(9, 5, 2, 3),0))
    val maxhash = threshould * 5;
    var idsFlat = idsFlatInit;
    var loopCount = 0;
    while (true) {
      idsFlat = idsFlat
        .reduceByKey((x, y) => {
          //          两个set 相互聚合
          val xyset = x._1.union(y._1);
          val xySize = xyset.size;
          val xSize = x._1.size;
          val ySize = y._1.size;
          if (xySize.equals(xSize) || xySize.equals(ySize)) {
            (xyset, Math.max(x._2, y._2))
          } else {
            (xyset, Math.min(x._2, y._2) + 1)
          }
        })
        .cache();
      //      idsFlat.sortBy(x=>x._1,ascending = true,numPartitions = 1).foreach(println)
      //      (0,(Set(0, 2, 10, 7),0))
      //      (1,(Set(1, 6, 4, 8),0))
      //      (2,(Set(0, 9, 5, 2, 3, 10, 7),1))
      //      (3,(Set(9, 5, 2, 3),0))
      //      (4,(Set(1, 6, 4, 8),0))
      //      (5,(Set(9, 5, 2, 3),0))
      //      (6,(Set(1, 6, 4, 8),0))
      //      (7,(Set(0, 2, 10, 7),0))
      //      (8,(Set(1, 6, 4, 8),0))
      //      (9,(Set(9, 5, 2, 3),0))
      //      (10,(Set(0, 2, 10, 7),0))
      idsFlat
        //        .sortBy(x=>x._1,ascending = true,numPartitions = 1)
        .filter(f => {
          (f._2._2 + 1).equals(loopCount) && f._2._1.size <= threshould;
        })
        .map(xo => {
          val sb = new StringBuilder();
          val idList = xo._2._1.toList.sorted;
          for (id <- idList) {
            sb.append(id).append(Constants.SPACE_DELIMITER);
          }
          val outids = sb.toString().trim();
          val randnum = StringsRandom.generateRangeNumber(1, maxhash);
          (randnum + Constants.UNDERLINE_DELIMITER + MD5Util.getMD5(outids), outids)
        })
        //        .sortBy(x=>x._1,ascending = true,numPartitions = 1).foreach(x=>println(loopCount+" -> "+x))
        //      首次循环
        //      没有要输出的结果
        //      第二次循环
        //      1 -> (29_10A0DFC09EE0EA5F87BCF1EF761DA7DA,1 4 6 8)
        //      1 -> (32_10A0DFC09EE0EA5F87BCF1EF761DA7DA,1 4 6 8)
        //      1 -> (40_10A0DFC09EE0EA5F87BCF1EF761DA7DA,1 4 6 8)
        //      1 -> (8_10A0DFC09EE0EA5F87BCF1EF761DA7DA,1 4 6 8)
        //      第三次循环
        //      2 -> (17_44338040CF307E7F5C0E151960AFDA39,0 2 3 5 7 9 10)
        //      2 -> (18_44338040CF307E7F5C0E151960AFDA39,0 2 3 5 7 9 10)
        //      2 -> (1_44338040CF307E7F5C0E151960AFDA39,0 2 3 5 7 9 10)
        //      2 -> (27_44338040CF307E7F5C0E151960AFDA39,0 2 3 5 7 9 10)
        //      2 -> (37_44338040CF307E7F5C0E151960AFDA39,0 2 3 5 7 9 10)
        //      2 -> (46_44338040CF307E7F5C0E151960AFDA39,0 2 3 5 7 9 10)
        //      2 -> (47_44338040CF307E7F5C0E151960AFDA39,0 2 3 5 7 9 10)
        .reduceByKey((outids1, outids2) => {
          outids1;
        })
        .map(outids => {
          (outids._1.split(Constants.UNDERLINE_DELIMITER)(1), outids._2)
        })
        .reduceByKey((outids1, outids2) => {
          outids1;
        })
        .map((outids: (String, String)) => {
          outids._2
        })
        //                .foreach(x=>println(loopCount+" -> "+x))
        //      1 -> 1 4 6 8
        //      2 -> 0 2 3 5 7 9 10
        .saveAsTextFile(idmergeOutput + loopCount, classOf[GzipCodec]);

      idsFlat = idsFlat
        .filter(idSet => {
          (idSet._2._2 + 1 >= loopCount) && idSet._2._1.size < threshould;
        }).flatMap(idset => {
        val arr = new ArrayBuffer[(Int, (mutable.Set[Int], Int))]()
        idset._2._1.foreach(id => {
          arr += ((id, (idset._2._1, idset._2._2)))
        })
        arr
      })
      //      println("final idsFlat.length: "+idsFlat.collect().length)
      //      final idsFlat.length: 47
      //      final idsFlat.length: 65
      //      final idsFlat.length: 49
      //      idsFlat.sortBy(x=>(x._2._1.toString(),x._1),ascending = true,numPartitions = 1).foreach(x=>println(loopCount+" -> "+x))
      //      0 -> (0,(Set(0, 2, 10, 7),0))
      //      0 -> (0,(Set(0, 2, 10, 7),0))
      //      0 -> (0,(Set(0, 2, 10, 7),0))
      //      0 -> (2,(Set(0, 2, 10, 7),0))
      //      0 -> (2,(Set(0, 2, 10, 7),0))
      //      0 -> (2,(Set(0, 2, 10, 7),0))
      //      0 -> (7,(Set(0, 2, 10, 7),0))
      //      0 -> (7,(Set(0, 2, 10, 7),0))
      //      0 -> (7,(Set(0, 2, 10, 7),0))
      //      0 -> (10,(Set(0, 2, 10, 7),0))
      //      0 -> (10,(Set(0, 2, 10, 7),0))
      //      0 -> (10,(Set(0, 2, 10, 7),0))
      //      0 -> (0,(Set(0, 9, 5, 2, 3, 10, 7),1))
      //      0 -> (2,(Set(0, 9, 5, 2, 3, 10, 7),1))
      //      0 -> (3,(Set(0, 9, 5, 2, 3, 10, 7),1))
      //      0 -> (5,(Set(0, 9, 5, 2, 3, 10, 7),1))
      //      0 -> (7,(Set(0, 9, 5, 2, 3, 10, 7),1))
      //      0 -> (9,(Set(0, 9, 5, 2, 3, 10, 7),1))
      //      0 -> (10,(Set(0, 9, 5, 2, 3, 10, 7),1))
      //      0 -> (1,(Set(1, 6, 4, 8),0))
      //      0 -> (1,(Set(1, 6, 4, 8),0))
      //      0 -> (1,(Set(1, 6, 4, 8),0))
      //      0 -> (1,(Set(1, 6, 4, 8),0))
      //      0 -> (4,(Set(1, 6, 4, 8),0))
      //      0 -> (4,(Set(1, 6, 4, 8),0))
      //      0 -> (4,(Set(1, 6, 4, 8),0))
      //      0 -> (4,(Set(1, 6, 4, 8),0))
      //      0 -> (6,(Set(1, 6, 4, 8),0))
      //      0 -> (6,(Set(1, 6, 4, 8),0))
      //      0 -> (6,(Set(1, 6, 4, 8),0))
      //      0 -> (6,(Set(1, 6, 4, 8),0))
      //      0 -> (8,(Set(1, 6, 4, 8),0))
      //      0 -> (8,(Set(1, 6, 4, 8),0))
      //      0 -> (8,(Set(1, 6, 4, 8),0))
      //      0 -> (8,(Set(1, 6, 4, 8),0))
      //      0 -> (2,(Set(9, 5, 2, 3),0))
      //      0 -> (2,(Set(9, 5, 2, 3),0))
      //      0 -> (2,(Set(9, 5, 2, 3),0))
      //      0 -> (3,(Set(9, 5, 2, 3),0))
      //      0 -> (3,(Set(9, 5, 2, 3),0))
      //      0 -> (3,(Set(9, 5, 2, 3),0))
      //      0 -> (5,(Set(9, 5, 2, 3),0))
      //      0 -> (5,(Set(9, 5, 2, 3),0))
      //      0 -> (5,(Set(9, 5, 2, 3),0))
      //      0 -> (9,(Set(9, 5, 2, 3),0))
      //      0 -> (9,(Set(9, 5, 2, 3),0))
      //      0 -> (9,(Set(9, 5, 2, 3),0))
      loopCount = loopCount + 1;
      if (idsFlat.isEmpty()) {
        return
      }
    }
  }

}