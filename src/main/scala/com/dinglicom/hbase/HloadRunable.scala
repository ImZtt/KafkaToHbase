package com.dinglicom.hbase

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
import org.apache.hadoop.conf.Configuration
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.DLPairDStreamFunctions
/**
 * @author wx
 */
class HloadRunable(data: DStream[(String, String, String, Long)],//从kafka接入的数据流
                 conf: Configuration,                           //hload进程的conf
                 tableName: String,                             //要插入的表名
                 isMaintable: Boolean,                          //是否是明细表
                 family: Array[Byte],                           //列族
                 colum: Array[Byte]                             //列名
                    ) extends Runnable {

  override def run() = {
     val dd = data.map(x=>{
      if(isMaintable){        
    	  val rowKey = Bytes.toBytes(x._1)
			  val value = Bytes.toBytes(x._3)
	      (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, x._4, value))
      }else{
    	  val rowKey = Bytes.toBytes(x._2)
			  val value = Bytes.toBytes(x._1)
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, x._4, value))
      }
     })
     val s2rdd = dd.repartition(10).transform(rdd=>rdd.sortBy(x=>new String(x._2.getRow) ,true))
     val tran = new DLPairDStreamFunctions(s2rdd)
     tran.saveAsNewAPIHadoopFilesdl("/user/cloudil/hbase/"+tableName,"",
       classOf[ImmutableBytesWritable],
       classOf[KeyValue],
       classOf[HFileOutputFormat2],
       tableName,
       conf)
  }
  
}