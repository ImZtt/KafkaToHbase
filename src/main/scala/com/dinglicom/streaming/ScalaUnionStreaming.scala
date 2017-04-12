package com.dinglicom.streaming

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream

object ScalaUnionStreaming {
  def main(args: Array[String]) {
    var conf = new SparkConf
    conf.setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    
    val ssc = new StreamingContext(sc, Seconds(10))
    
    val streams = (1 to 5) map { _ => {
      ssc.socketTextStream("localhost", 9099 , StorageLevel.MEMORY_ONLY_SER)
    }
    }
    
    val unionStreams = ssc.union(streams);
    
    
  }
}