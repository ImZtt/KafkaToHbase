package com.dinglicom.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable

/**
 * 多维度输出
 * @author Administrator
 */
object HttpMultiDimension {
  
  //spark-submit  /root/wubo/spark-lte-multi-counter.jar /topics/log_2016110314-172.16.30.106.1478152800237.DAT.txt
  //spark-submit /root/wubo/spark-lte-multi-counter.jar /csvxdr/lteu1_http/startdate=2016112209/*.DAT
  def main(args: Array[String]) {
    val conf = new SparkConf()
    //conf.setAppName("test_multi").setMaster("local");
    conf.setAppName("test_multi")
    
    if (args.length != 1) {
      println("http file path not exist!")
      return
    }
    val sc = new SparkContext(conf)

    val file = sc.textFile(args(0))

    //val file = sc.textFile("E:\\Scala_tt\\data\\log_2016110314-172.16.30.106.1478152800237.DAT.txt")

    //val filterRDD = file.filter {!_.split("\\|").apply(91).equals("0")}

    val filterRDD = file.filter { x =>
      {
        var rs = true
        var tmpValues = x.split("\\|");
        var citycode = tmpValues(91)
        var lac = tmpValues(20).toLong
        var cell = tmpValues(21).toLong

        //过滤citycode等于0的记录
        if (citycode.equals("0")) {
          rs = false
        } else {
          rs = true
        }
        rs
      }
    }

    var cc = filterRDD.flatMap(x => {
      var tmpValues = x.split("\\|");
      var u16citycode = tmpValues(91)
      var lac = tmpValues(20)
      var cell = tmpValues(21)

      var u32ultraffic = tmpValues(38).toLong
      var u32dltraffic = tmpValues(39).toLong
      var u32ulippacketnum = tmpValues(40).toLong
      var u32dlippacketnum = tmpValues(41).toLong

      val valArray = Array[Long](u32ultraffic, u32dltraffic, u32ulippacketnum, u32dlippacketnum)

      val result = Array(new Tuple2("1@" + u16citycode, valArray), new Tuple2("2@" + u16citycode + "," + lac + "," + cell, valArray))
      //val result = Array(new Tuple2(u16citycode, valArray) )
      result
    }).reduceByKey((a, b) => {

      var tmpArr: Array[Long] = new Array[Long](a.length)

      for (i <- 0 until a.length) //
      {
        tmpArr(i) = a(i) + b(i)
      }

      tmpArr

    }).map(e => {

      var mapKey = e._1
      var keyArray = mapKey.toString().split("\\@")

      var mapVal = new StringBuilder()
      mapVal.append(keyArray(1)).append(",")
      for (i <- 0 until e._2.length) {
        mapVal.append(e._2(i)).append(",");
      }
      (mapKey, mapVal.substring(0, mapVal.length - 1))
    })
    cc.partitionBy(new HashPartitioner(1)).saveAsHadoopFile("/user/cloudil/sparktest", classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])
    //cc.repartition(1).saveAsHadoopFile("/user/cloudil/sparktest", classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])

    sc.stop()
  }
}

/**
 * 多文件输出格式定义类
 */
class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {

  /**
   * key值不输出
   */
  override def generateActualKey(key: Any, value: Any): Any =
    {
      NullWritable.get().asInstanceOf[Any]
    }

  /**
   * 根据key值，写入不同文件
   */
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    {
      var keyArray = key.toString().split("\\@")
      if (keyArray(0).equals("1")) {
        "city"
      } else {
        "cell"
      }
    }
}
