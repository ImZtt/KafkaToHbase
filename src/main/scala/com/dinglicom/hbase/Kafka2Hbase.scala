package com.dinglicom.hbase

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.{HTable, _}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
object Kafka2Hbase {
  def main(args: Array[String]): Unit = {
   val jobName = "Kafka2Hbase"

    val conf = new SparkConf().setAppName(jobName)
    val context = new StreamingContext(conf, Seconds(60))

    val kafkaParams = Map[String, String]("group.id" -> "lte_user_xdr", "metadata.broker.list" -> "172.16.40.57:9092,172.16.40.98:9092,172.16.40.141:9092,172.16.40.142:9092", "serializer.class" -> "kafka.serializer.StringEncoder", "auto.offset.reset" -> "largest")

    val topics = Set("http")

    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](context, kafkaParams, topics)

     var cc = kafkaStream.map(x => {
      var tmpValues = x._2.split("\\|");
       //rowkey:begintime+endtime+msisdn
      val rowKey = tmpValues(1)+"-"+tmpValues(20)+"-"+tmpValues(21)
      val value = x._2
      (rowKey, value)
      
    })
    cc.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
    	  //Hbase配置
    	  val tableName = "LTE_XDR"
			  val hbaseConf = HBaseConfiguration.create()
			  //设置不写入wal日志
//			  val m_wal = Durability.SKIP_WAL
			  hbaseConf.set("hbase.defaults.for.version.skip", "true")
			  val StatTable = new HTable(hbaseConf, TableName.valueOf(tableName))
        partitionOfRecords.foreach(pair => {
          //用户ID
          val rowKey = pair._1
          //点击次数
          val value = pair._2
          //组装数据
          val put = new Put(Bytes.toBytes(rowKey))
          put.setWriteToWAL(false)
//          put.setDurability(m_wal)
          put.addColumn("cf".getBytes,"1q".getBytes,Bytes.toBytes(value))
          StatTable.setAutoFlush(false, false)
          //写入数据缓存20M
          StatTable.setWriteBufferSize(50*1024*1024)
          StatTable.put(put)
          //提交
        })
        StatTable.flushCommits()
      })
    })
    context.start()
    context.awaitTermination()

  }

}