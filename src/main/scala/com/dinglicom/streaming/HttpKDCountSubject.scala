package com.dinglicom.streaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext

object HttpKDCountSubject {

  def main(args: Array[String]) {

    val jobName = "http_kd_count"

    val conf = new SparkConf().setAppName(jobName)
    val context = new StreamingContext(conf, Seconds(300))

    val kafkaParams = Map[String, String]("group.id" -> "lte_http_user", "metadata.broker.list" -> "172.16.30.101:9092", "serializer.class" -> "kafka.serializer.StringEncoder", "auto.offset.reset" -> "smallst")

    val topics = Set("lteu1_http")

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](context, kafkaParams, topics);

    var cc = kafkaStream.filter ( y => {
      y._2.split("\\|")(70).indexOf("宽带") >= 0
    } ).map( x => {
      var tmpValues = x._2.split("\\|");
      var url = tmpValues(70)
      var imsi = tmpValues(88)
      var kdCnt = 1
      new Tuple2(imsi,Array[Long](kdCnt))
     
    }).reduceByKey((a, b) => {

      var tmpArr: Array[Long] = new Array[Long](a.length)

      for (i <- 0 until a.length) //
      {
        tmpArr(i) = a(i) + b(i)
      }

      tmpArr

    }).map(e => {

      var builder = new StringBuilder()
      builder.append(e._1).append(",")
      for (i <- 0 until e._2.length) {
        builder.append(e._2(i)).append(",");
      }

      builder.substring(0, builder.length - 1)

    }).repartition(1).saveAsTextFiles("/user/cloudil/subject/http_kd_count");

    context.start();
    context.awaitTermination();

  }

}