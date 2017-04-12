package com.dinglicom.streaming

import java.util.Date
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.client.{ HTable, Table, _ }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{ HFileOutputFormat2, LoadIncrementalHFiles }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ HBaseConfiguration, KeyValue, TableName }
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.OutputFormat
import scala.collection.mutable.HashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * @author wubo
 */
object KafkaStreamingToHbase {

  //spark-submit --num-executors 42 --executor-memory 1G kafkaStreamingToHbase.jar kafkatohbase s1mme,s6a,s10s11,sgs,s1u,dns,mms,http,ftp,email,voip,rtsp,im,p2p,video 172.16.40.57:9092,172.16.40.98:9092,172.16.40.141:9092,172.16.40.142:9092
  //spark-submit --num-executors 41 --executor-memory 1G kafkaStreamingToHbase.jar testId http,dns main 172.16.40.57:9092
  //s1mme,s6a,s10s11,sgs,s1u,dns,mms,http,ftp,email,voip,rtsp,im,p2p,video
  //spark-submit --num-executors 40 --executor-memory 2G kafkaStreamingToHbase2.jar kafkatohbase s1mme,s6a,s10s11,sgs,s1u,dns,mms,http,ftp,email,voip,rtsp,im,p2p,video 172.16.40.57:9092,172.16.40.98:9092,172.16.40.141:9092,172.16.40.142:9092
  //172.16.40.57:9092,172.16.43.33:9092,172.16.40.141:9092,172.16.40.142:9092
  def confTable(zkQuorum: String, tableName: String): HTableInterface = {
    val conf = HBaseConfiguration.create()
    conf.set("HBase.zookeeper.quorum", zkQuorum)
    val connection = HConnectionManager.createConnection(conf)
    val table = connection.getTable(tableName)
    return table
  }
  

  //p2p,rtsp,voip,video
  //kafka-console-consumer.sh --zookeeper dn179:2181 dn178:2181 --topic p2p
  def main(args: Array[String]): Unit = {
    if (args.length < 0) {
      System.err.println("Usage: kafkaStreamingToHbase <group> <topics> <brokers>")
      System.exit(1)
    }
    //提取参数//yarn-cluster 
    val Array(group, topics, brokers) = args
    val jobName = "sparkstreamingToHbase3"
    val outTable = args(0)
    val conf = new SparkConf().setAppName(jobName)
    conf.set("spark.port.maxRetries","100")
    //第一参数为sparkContext对象，第二个参数为批次时间；
    val context = new StreamingContext(conf, Seconds(60))
    //do checkpoint metadata to hdfs
    //context.checkpoint("/user/root/spark/checkpoint")
    //172.16.40.141:9092
    //172.16.40.57:9092,172.16.40.98:9092,172.16.40.141:9092,172.16.40.142:9092
    val kafkaParams = Map[String, String]("group.id" -> group, "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder") //, "auto.offset.reset" -> "smallest"
    val topicsSet = topics.split(",").toSet
    // Create a direct stream 
    //kafkautil里面提供了两个创建dstream的方法，一个是老版本中有的createStream方法，还有一个是后面新加的createDirectStream方法，新加的效率会高一些
    //StringDecoder：为缓冲区buffer定义了编码规范
    //StringDecoder:The string_decoder module provides an API for decoding Buffer objects into strings in a manner that preserves encoded multi-byte UTF-8 and UTF-16 characters. It can be accessed using:
     //Direct的方式是会直接操作kafka底层的元数据信息
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](context, kafkaParams, topicsSet)
    
    var filterStream = kafkaStream.filter { x =>
      {
        var rs = true
       //_2把两个数据放在一起较方便
        var tmpValues = x._2.split("\\|");
        var xdrInterface = tmpValues(1).toLong
        var processtype = tmpValues(23).toLong
        //过滤数据长度或数值不合法的记录
        if ((xdrInterface == 5 || xdrInterface == 6 || xdrInterface == 7 || xdrInterface == 8 || xdrInterface == 9) || (xdrInterface == 11)) {
          rs = true
        } else {
          rs = false //过滤
        }
        rs
      }
    }
   /**
     * _2即tuple2就是用来把几个数据放在一起的比较方便的方式，上面的例子定义了一个tuple2和一个tuple3，区别就是tuple2放两个数据、tuple3放3个数据。好像最大是tuple21
    */
      var cc = filterStream.map(x => {
      var tmpValues = x._2.split("\\|");
      val xdrid = tmpValues(2)
      var xdrInterface = tmpValues(1).toLong
      var processtype = tmpValues(23).toLong

      var datatype = ""
      var msisdn = 0L
      var citycode = 0L
      var begintime = 0L
      var endtime = 0L
      
      //取时间:控制面-用户面
      if (xdrInterface == 5 || xdrInterface == 6 || xdrInterface == 7 || xdrInterface == 8 || xdrInterface == 9) 
      {
        begintime = tmpValues(8).toLong //u64ProcessStartTime 
        endtime = tmpValues(9).toLong //u64ProcessEndTime        
      }
      else if(xdrInterface == 11)
      {
        begintime = tmpValues(24).toLong
        endtime = tmpValues(25).toLong         
      }
      
      if (xdrInterface == 5) // lteu1_s1mme_zc
      {
        datatype = "i"
        msisdn = tmpValues(175).toLong
        citycode = tmpValues(179).toLong
      } 
      else if (xdrInterface == 6) // ltec1_s6a_zc
      {
        datatype = "k"
        msisdn = tmpValues(34).toLong
        citycode = tmpValues(38).toLong
      } 
      else if (xdrInterface == 7 || xdrInterface == 8) // ltec1_s10s11_zc
      {
        datatype = "n"
        msisdn = tmpValues(128).toLong
        citycode = tmpValues(132).toLong
      }
      else if (xdrInterface == 9) // ltec1_sgs_zc
      {
        datatype = "p"
        msisdn = tmpValues(40).toLong
        citycode = tmpValues(44).toLong
      }      
      else if (xdrInterface == 11 && processtype == 100) // lteu1_s1u_zc
      {
        datatype = "j"
        msisdn = tmpValues(63).toLong
        citycode = tmpValues(67).toLong
      }  
      else if (xdrInterface == 11 && processtype == 101) // lteu1_dns_zc
      {
        datatype = "a"
        msisdn = tmpValues(70).toLong
        citycode = tmpValues(74).toLong
      }
      else if (xdrInterface == 11 && processtype == 102) // lteu1_mms_zc
      {
        datatype = "f"
        msisdn = tmpValues(81).toLong
        citycode = tmpValues(85).toLong
      }       
      else if (xdrInterface == 11 && processtype == 103) // lteu1_http_zc
      {
        datatype = "m"
        msisdn = tmpValues(87).toLong
        citycode = tmpValues(91).toLong
      } 
      else if (xdrInterface == 11 && processtype == 104) // lteu1_ftp_zc
      {
        datatype = "c"
        msisdn = tmpValues(74).toLong
        citycode = tmpValues(78).toLong
      } 
      else if (xdrInterface == 11 && processtype == 105) // lteu1_email_zc
      {
        datatype = "b"
        msisdn = tmpValues(72).toLong
        citycode = tmpValues(76).toLong
      } 
      else if (xdrInterface == 11 && processtype == 106) // lteu1_voip_zc
      {
        datatype = "l"
        msisdn = tmpValues(70).toLong
        citycode = tmpValues(74).toLong
      }
      else if (xdrInterface == 11 && processtype == 107) // lteu1_rtsp_zc
      {
        datatype = "h"
        msisdn = tmpValues(73).toLong
        citycode = tmpValues(77).toLong
      }       
      else if (xdrInterface == 11 && processtype == 108) // lteu1_p2p_zc
      {
        datatype = "g"
        msisdn = tmpValues(66).toLong
        citycode = tmpValues(70).toLong
      } 
      else if(xdrInterface == 11 && processtype == 109) // lteu1_onlinevideo_zc
      {
        datatype = "o"
        msisdn = tmpValues(74).toLong
        citycode = tmpValues(78).toLong       
      }
      else if (xdrInterface == 11 && processtype == 110) // lteu1_im_zc
      {
        datatype = "e"
        msisdn = tmpValues(67).toLong
        citycode = tmpValues(71).toLong
      }       
      //一级索引rowkey:(endtime的倒序） +　数据类型编码（每个数据类型一个字符）　＋ 城市编码(每个城市一个字符) +　xdrindex
      //var oneIdxKey = exchangeStrReverse(endtime) + "-" + datatype + "-" + citycode + "-" + xdrid
      var oneIdxKey = new StringBuffer(exchangeStrReverse(endtime))
      oneIdxKey.append("-").append(datatype).append("-").append(citycode).append("-").append(xdrid)    
      
      //二级索引rowkey:前缀（1-表示msisdn、2-表示imei、3-表示imsi） +　msisdn获取imei或者imsi的倒序 + startime +  -xdrindex- + 数据类型编码（每个数据类型一个字符）
      //var twoIdxKey = "1-" + msisdn + "-" + begintime + "-" + xdrid + "-" + datatype
      var twoIdxKey = new StringBuffer("1-")
      twoIdxKey.append(exchangeStrReverse(msisdn)).append("-").append(begintime).append("-").append(xdrid).append("-").append(datatype) 
      
      var rowkey = oneIdxKey.toString() + "@" + twoIdxKey.toString()
      val value = x._2

      (rowkey, value)
    })
    
    cc.foreachRDD(rdd => {    
       //创建线程池
      val threadPool: ExecutorService = Executors.newFixedThreadPool(2)
      try 
      {
        //提交2个线程
        threadPool.execute(new HbaseUtil("LTE_XDR_IDX", rdd))
        threadPool.execute(new HbaseUtil("LTE_XDR", rdd))
      } 
      finally 
      {
        threadPool.shutdown()
      }
      /*
      //插入索引表       
      val idxJobConf = hadoopJobConf("LTE_XDR_IDX")
      rdd.map(pair => {
        var mutilKey = pair._1.split("\\@");
        //create hbase put
        val put = new Put(Bytes.toBytes(mutilKey(1)))
        //add column
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cf"), Bytes.toBytes(mutilKey(0)))
        //retuen type
        (new ImmutableBytesWritable, put)
      }).saveAsNewAPIHadoopDataset(idxJobConf) //save as HTable   

      //插入XDR明细表 
      val detailJobConf = hadoopJobConf("LTE_XDR")
      rdd.map(pair => {
        var mutilKey = pair._1.split("\\@");
        val value = pair._2
        //create hbase put
        val put = new Put(Bytes.toBytes(mutilKey(0)))
        //add column
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cf"), Bytes.toBytes(value))
        //retuen type
        (new ImmutableBytesWritable, put)
      }).saveAsNewAPIHadoopDataset(detailJobConf) //save as HTable   
      *    
      */
    })
    
    /*
    cc.foreachRDD(rdd => {
      //使用 rdd.foreachPartition 创建一个HTable 对象， 一个RDD分区中的所有数据，都使用这一个HTable
      rdd.foreachPartition(partitionOfRecords => {
        //Hbase配置
        val xdrIdxTable = confTable("LTE_XDR_IDX")
        //插入索引表     
        partitionOfRecords.foreach(pair => {
          var mutilKey = pair._1.split("\\@");
          //create hbase put
          val put = new Put(Bytes.toBytes(mutilKey(1)))
          put.add(Bytes.toBytes("cf"), Bytes.toBytes("cf"), Bytes.toBytes(mutilKey(0)))
          xdrIdxTable.setAutoFlush(false, false)
          //写入数据缓存128M
          xdrIdxTable.setWriteBufferSize(128 * 1024 * 1024)
          xdrIdxTable.put(put)
          //提交
          xdrIdxTable.flushCommits()
        })
        //插入XDR主表 
        val xdrTable = confTable("LTE_XDR")
        partitionOfRecords.foreach(pair => {
          var mutilKey = pair._1.split("\\@");
          val value = pair._2
          //create hbase put
          val put = new Put(Bytes.toBytes(mutilKey(0)))
          put.add(Bytes.toBytes("cf"), Bytes.toBytes("cf"), Bytes.toBytes(value))
          xdrIdxTable.setAutoFlush(false, false)
          //写入数据缓存128M
          xdrIdxTable.setWriteBufferSize(512 * 1024 * 1024)
          xdrIdxTable.put(put)
          //提交
          xdrIdxTable.flushCommits()
        })        
      })
    })
    */
    context.start()
    context.awaitTermination()
  }
  
  def hadoopJobConf(tableName: String): JobConf = {
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "172.16.42.105,172.16.42.106,172.16.43.111,172.16.43.93,172.16.43.94")
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    hconf.set("hbase.defaults.for.version.skip", "true")
    hconf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    hconf.set("hbase.rootdir", "/hbase")
    hconf.setClass("mapreduce.job.outputformat.class", classOf[TableOutputFormat[String]], classOf[OutputFormat[String, Mutation]])
    val jobConf = new JobConf(hconf)
    return jobConf
  }
  
  def confTable(tableName: String): HTableInterface = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "172.16.42.105,172.16.42.106,172.16.43.111,172.16.43.93,172.16.43.94")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.defaults.for.version.skip", "true")
    val connection = HConnectionManager.createConnection(hbaseConf)
    val table = connection.getTable(tableName)
    return table
  }
    
  def exchangeStrReverse(time: Long): String ={
    var rs = new StringBuffer(String.valueOf(time))
    rs.reverse().toString()
  }
}