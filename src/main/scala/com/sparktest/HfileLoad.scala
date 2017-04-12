package com.sparktest
import java.util.Date

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{HTable, Table, _}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
object HfileLoad {
  def main(args: Array[String]) {
    //创建sparkcontext,用默认的配置
//    val sc = new SparkContext(new SparkConf())
//    //hbase的列族
//    val columnFamily1 = "cf"
    //hbase的默认配置文件
    val conf = HBaseConfiguration.create()
    //当前时间
    val date = new Date().getTime
    //初始化RDD,用 sc.parallelize 生成一个RDD
   
    //开始即那个HFile导入到Hbase,此处都是hbase的api操作
    val load = new LoadIncrementalHFiles(conf)
    //hbase的表名
    val tableName = "LTE_XDR"
    //创建hbase的链接,利用默认的配置文件,实际上读取的hbase的master地址
    val conn = ConnectionFactory.createConnection(conf)
    //根据表名获取表
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    try {
      //获取hbase表的region分布
      //Region是Hbase中分布式存储和负载均衡的最小单元，不同Region分布到不同RegionServer上。
      val regionLocator = conn.getRegionLocator(TableName.valueOf(tableName))
      //创建一个hadoop的mapreduce的job
      val job = Job.getInstance(conf)
      //设置job名称
      job.setJobName("DumpFile")
      //此处最重要,需要设置文件输出的key,因为我们要生成HFil,所以outkey要用ImmutableBytesWritable
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      //输出文件的内容KeyValue
      job.setMapOutputValueClass(classOf[KeyValue])
      //配置HFileOutputFormat2的信息
      //hadoop mr 输出需要导入hbase的话最好先输出成HFile格式， 再导入到HBase,因为HFile是HBase的内部存储格式
      HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)
      
      //开始导入
      load.doBulkLoad(new Path("/user/cloudil/hbase/"+args(0)+"/"), table.asInstanceOf[HTable])
    } finally {
      table.close()
      conn.close()
    }
  }
}