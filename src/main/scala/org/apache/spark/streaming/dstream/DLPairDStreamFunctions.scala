/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.dstream

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{OutputFormat => NewOutputFormat}
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, Time}
import org.apache.spark.streaming.StreamingContext.rddToFileName
import org.apache.spark.util.{SerializableConfiguration, SerializableJobConf}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{HTable, Table, _}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
/**
 * Extra functions available on DStream of (key, value) pairs through an implicit conversion.
 */
class DLPairDStreamFunctions[K, V](self: DStream[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K])
  extends Serializable
{
  def ssc = self.ssc

  private[streaming] def sparkContext = self.context.sparkContext

  private[streaming] def defaultPartitioner(numPartitions: Int = self.ssc.sc.defaultParallelism) = {
    new HashPartitioner(numPartitions)
  }

/**
 * 加载hfile的方法
 */
def hLoad(tableName: String,
          time: Time,
          hconf: Configuration = ssc.sparkContext.hadoopConfiguration
          ): Unit = {
//       val tableName = "HFILE_XDR"
    		//创建hbase的链接,利用默认的配置文件,实际上读取的hbase的master地址
    		val conn = ConnectionFactory.createConnection(hconf)
    		//根据表名获取表
    		val table: Table = conn.getTable(TableName.valueOf(tableName))
    		val timestamp = time.milliseconds
    		
    		//获取hbase表的region分布
    		val regionLocator = conn.getRegionLocator(TableName.valueOf(tableName))
    		//创建一个hadoop的mapreduce的job
    		val job = Job.getInstance(hconf)
    		//设置job名称
    		job.setJobName("DumpHFile")
        /**
         * 这两个方法是map端输出的数据类型，默认的是LongWritable.class Text.class
         * 输出文件的内容KeyValue
         * 此处最重要,需要设置文件输出的key,因为我们要生成HFil,所以outkey要用ImmutableBytesWritable
         * hadoop的map和reduce输出默认是一致的，当map和reduce输出不一致时需要设置map的setMapOutputKeyClass和setMapOutputValueClass的属性
         */
        job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    		job.setMapOutputValueClass(classOf[KeyValue])
        /**
         * hadoop mr 输出需要导入hbase的话最好先输出成HFile格式， 再导入到HBase,因为HFile是HBase的内部存储格式
         * 配置HFileOutputFormat2的信息
         */
    		HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)
      		//第二个参数为生成文件后缀，暂时不设置
        val load = new LoadIncrementalHFiles(hconf)
        //开始导入
        load.doBulkLoad(new Path("/user/cloudil/hbase/"+tableName+"-"+timestamp+"/"), table.asInstanceOf[HTable])
        table.close()
        conn.close()
  }
  /**
   * Save each RDD in `this` DStream as a Hadoop file. The file name at each batch interval is
   * generated based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix".
   */
  def saveAsNewAPIHadoopFilesdl(
      prefix: String,
      suffix: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
      tableName: String,
      conf: Configuration = ssc.sparkContext.hadoopConfiguration
    ): Unit = ssc.withScope {
    // Wrap conf in SerializableWritable so that ForeachDStream can be serialized for checkpoints
    val serializableConf = new SerializableConfiguration(conf)
    val saveFunc = (rdd: RDD[(K, V)], time: Time) => {
      val file = rddToFileName(prefix, suffix, time)
      //saveAsNewAPIHadoopFile用于将RDD数据保存到HDFS上，使用新版本Hadoop API。
      rdd.saveAsNewAPIHadoopFile(
        file, keyClass, valueClass, outputFormatClass, serializableConf.value)
      hLoad(tableName,time,conf)
    }
    self.foreachRDD(saveFunc)
  }

  private def keyClass: Class[_] = kt.runtimeClass

  private def valueClass: Class[_] = vt.runtimeClass
}
