package com.dinglicom.streaming;

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import scala.tools.nsc.doc.model.Object
import kafka.producer.ProducerConfig
import java.util.Properties
import kafka.producer.Producer
import scala.util.Random
import kafka.producer.Producer
import kafka.producer.Producer
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import java.util.Date
object kafkaP {
  
  def main(args: Array[String]): Unit = {
  val topic = "http"
  val props = new Properties()
  props.put("metadata.broker.list", "localhost:9092")
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  //props.put("partitioner.class", "com.colobu.kafka.SimplePartitioner")
//  props.put("producer.type", "async")
  //props.put("request.required.acks", "1")

  val config = new ProducerConfig(props)
  val producer = new Producer[String, String](config)
  val t = System.currentTimeMillis()
  while (true) {
    val runtime = new Date().getTime();
    val data = new KeyedMessage[String, String](topic, "", runtime+"");
    producer.send(data);
  }

  producer.close();
  }
}