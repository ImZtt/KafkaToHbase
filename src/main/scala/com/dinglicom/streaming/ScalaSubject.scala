package com.dinglicom.streaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext

object ScalaSubject {

  def main(args: Array[String]) {

    val jobName = "xdr_http_counter"

    def createSteamingContext(): StreamingContext = {
      val sparkconf = new SparkConf().setAppName(jobName);
      val sc = new StreamingContext(sparkconf, Seconds(300));

      //sc.checkpoint(checkpointPath)
      sc
    }

    //val context = StreamingContext.getOrCreate(checkpointPath, createSteamingContext);
    val context = createSteamingContext()
    
    //val kafkaParams = Map[String, String]("group.id"-> "http_xdr", "metadata.broker.list" -> "172.16.30.101:9092", "serializer.class" -> "kafka.serializer.StringEncoder")
    val kafkaParams = Map("group.id"-> "http_xdr", "metadata.broker.list" -> "172.16.30.101:9092", "serializer.class" -> "kafka.serializer.StringEncoder")
    
    val topics = Set("lteu1_http")
    
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](context, kafkaParams, topics);
    
    //kafkaStream.checkpoint(Seconds(10));
    
    var cc = kafkaStream.map (x => {
        var tmpValues = x._2.split("\\|");
        var u16citycode = tmpValues(91)
        
        var u32ultraffic = tmpValues(38).toLong
        var u32dltraffic = tmpValues(39).toLong
        var u32ulippacketnum = tmpValues(40).toLong
        var u32dlippacketnum = tmpValues(41).toLong
        
        new Tuple2(u16citycode, Array[Long](u32ultraffic,u32dltraffic,u32ulippacketnum,u32dlippacketnum))
      } 
    ).reduceByKey((a, b) => {
      
      var tmpArr:Array[Long] = new Array[Long](a.length)
      
      for(i <- 0 until a.length) //
      {
        tmpArr(i) = a(i) + b(i)
      }
      
      tmpArr
      
    }).map(e => {
      
      var builder = new StringBuilder()
      builder.append(e._1).append(",")
      for (i <- 0 until e._2.length)
      {
        builder.append(e._2(i)).append(",");
      }
      
      builder.substring(0, builder.length-1)
      
    }).repartition(1).saveAsTextFiles("/user/cloudil/sparkstreaming/output");
    
    context.start();
    context.awaitTermination();
    
  }

}