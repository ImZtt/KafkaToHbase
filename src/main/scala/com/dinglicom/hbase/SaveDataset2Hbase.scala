package com.dinglicom.hbase

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.DLKafkaUtils
import org.apache.spark.streaming.kafka.KafkaCluster
import scala.reflect.ClassTag

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, DefaultDecoder, StringDecoder}
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}

object SaveDataset2Hbase {
  
  def main(args: Array[String]): Unit = {
   val jobName = "Kafka2Hbase"

    val conf = new SparkConf().setAppName(jobName)
    conf.set("spark.port.maxRetries","1000")
    val context = new StreamingContext(conf, Seconds(60))

    val kafkaParams = Map[String, String]("group.id" -> "lte_user_xdr", "metadata.broker.list" -> "172.16.40.57:9092", "serializer.class" -> "kafka.serializer.StringEncoder", "auto.offset.reset" -> "largest")

    val topics = Set("http")

    // Create a direct stream
    val kafkaStream = DLKafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](context, kafkaParams, topics)

     var cc = kafkaStream.map(x => {
      var tmpValues = x._2.split("\\|");
       //rowkey:begintime+endtime+msisdn
      val rowKey = tmpValues(0)
      val value = x._2
      (rowKey, value)
      
    })
    cc.saveAsTextFiles("/user/cloudil/kafkatest/", "data")
    context.start()
    context.awaitTermination()

  }

}