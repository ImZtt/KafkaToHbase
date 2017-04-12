package com.dinglicom.streaming

import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.client.{ HTable, Table, _ }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{ HFileOutputFormat2, LoadIncrementalHFiles }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ HBaseConfiguration, KeyValue, TableName }
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.OutputFormat
import scala.collection.mutable.HashMap

/**
 * @author wubo
 */
class HbaseUtil(val tableName: String, val rdd: RDD[(String, String)]) extends Runnable {

  override def run() = {
      if ("LTE_XDR_IDX".equals(tableName)) {
        //插入索引表   
        val idxJobConf = hadoopJobConf("LTE_XDR_IDX")
        //一个普通的RDD通过map()转为pair RDD,传递的函数需要返回键值对。
        rdd.map(pair => {
          var mutilKey = pair._1.split("\\@");
          //create hbase put
          val put = new Put(Bytes.toBytes(mutilKey(1)))
          //add column
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cf"), Bytes.toBytes(mutilKey(0)))
          //retuen type
          (new ImmutableBytesWritable, put)
        }).saveAsNewAPIHadoopDataset(idxJobConf) //save as HTable  
      }
      else{
        
        //插入XDR主表 
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
      }
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
}