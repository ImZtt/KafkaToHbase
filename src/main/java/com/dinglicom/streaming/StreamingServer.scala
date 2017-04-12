package com.dinglicom.streaming;

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import scala.tools.nsc.doc.model.Object

object StreamingServer {
  
  def main(args: Array[String]): Unit = {
    val jobName = "Kafka2Hfile"
    System.setProperty("hadoop.home.dir", "E:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
	  val conf: SparkConf = new SparkConf().setAppName("xtq").setMaster("local[2]");
    val context = new StreamingContext(conf, Seconds(1))

    val kafkaParams = Map[String, String]("group.id" -> "lte_user_xdr", "metadata.broker.list" -> "localhost:9092", "serializer.class" -> "kafka.serializer.StringEncoder", "auto.offset.reset" -> "largest")

    val topics = Set("http")

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](context, kafkaParams, topics);
   
      var cc = kafkaStream.map(x => {
      val tmpValues = x._2.split("\\|");
      val x1 = x._1
      (x._1,x._2)
      
    })
  }
}