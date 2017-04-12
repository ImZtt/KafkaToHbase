package com.sparktest
import java.util.Date

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{ HTable, Table, _ }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{ HFileOutputFormat2, LoadIncrementalHFiles }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ HBaseConfiguration, KeyValue, TableName }
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{ SparkConf, SparkContext }

object Spark2hfile {
  val conf: SparkConf = new SparkConf().setAppName("xtq").setMaster("local[2]");
  val sc: SparkContext = new SparkContext(conf)
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    //创建sparkcontext,用默认的配置  
    //hbase的列族  
    val columnFamily1 = "f1"
    //hbase的默认配置文件  
    val conf = HBaseConfiguration.create()
    //当前时间  
    val date = new Date().getTime
    //初始化RDD,用 sc.parallelize 生成一个RDD  
    /**
     * textFile的参数是一个path,这个path可以是：
     *
     * 1. 一个文件路径，这时候只装载指定的文件
     *
     * 2. 一个目录路径，这时候只装载指定目录下面的所有文件（不包括子目录下面的文件）
     *
     * 3. 通过通配符的形式加载多个文件或者加载多个目录下面的所有文件
     */
    val sr = sc.textFile("/dinglicom/xdrfile/H150035201612131056010457100.AVL", 1)
    //    val sourceRDD = sc.parallelize(Array(  
    //      (Bytes.toBytes("41"), //41是rowkey  
    //        (Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo1"))), //分别设置family  colum  和 value  
    //      (Bytes.toBytes("41"),  
    //        (Bytes.toBytes(columnFamily1), Bytes.toBytes("b"), Bytes.toBytes("foo2.b"))),  
    //      (Bytes.toBytes("42"),  
    //        (Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo2.a"))),  
    //      (Bytes.toBytes("42"),  
    //        (Bytes.toBytes(columnFamily1), Bytes.toBytes("c"), Bytes.toBytes("foo2.c"))),  
    //      (Bytes.toBytes("43"),  
    //        (Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo3"))),  
    //      (Bytes.toBytes("44"),  
    //        (Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo.1"))),  
    //      (Bytes.toBytes("44"),  
    //        (Bytes.toBytes(columnFamily1), Bytes.toBytes("b"), Bytes.toBytes("foo.2"))),  
    //      (Bytes.toBytes("45"),  
    //        (Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("bar.1"))),  
    //      (Bytes.toBytes("45"),  
    //        (Bytes.toBytes(columnFamily1), Bytes.toBytes("d"), Bytes.toBytes("bar.2")))))  
    //  
    val rdd = sr.map(x => {
      //将rdd转换成HFile需要的格式,我们上面定义了Hfile的key是ImmutableBytesWritable,那么我们定义的RDD也是要以ImmutableBytesWritable的实例为key  
      //KeyValue的实例为value  
      //rowkey  

      var tmpValues = x.split("\\|");
      //将rdd转换成HFile需要的格式,我们上面定义了Hfile的key是ImmutableBytesWritable,那么我们定义的RDD也是要以ImmutableBytesWritable的实例为key
      //KeyValue的实例为value
      //rowkey:begintime+endtime+msisdn
      val rowKey = Bytes.toBytes(tmpValues(0) + "-" + tmpValues(20) + "-" + tmpValues(21))
      val date = tmpValues(21).toLong
      val value = Bytes.toBytes(x)
      (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, "cf".getBytes, "v".getBytes, date, value))
    })
    //将正常的rdd排序
    /**
     * _2即tuple2就是用来把几个数据放在一起的比较方便的方式，上面的例子定义了一个tuple2和一个tuple3，区别就是tuple2放两个数据、tuple3放3个数据。好像最大是tuple21
     */
    val cc = rdd.sortBy(x => x._2.getKeyString, true, 1)
    //生成的HFile的临时保存路径  
    val stagingFolder = "/dinglicom/hbase/"
    //将日志保存到指定目录  saveAsNewAPIHadoopFile指数据保存到hdfs中
    cc.saveAsNewAPIHadoopFile(stagingFolder,
   /**
    * ClassOf相当于getClass 
    * getClass 方法得到的是 Class[A]的某个子类，而 classOf[A] 得到是正确的 Class[A]
    * 但是去比较的话，这两个类型是equals为true的
    * getClass()方法的用途：可以获取一个类的定义信息，然后使用反射去访问其全部信息(包括函数和字段)。还可以查找该类的ClassLoader，以便检查类文件所在位置等。
    */
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      conf)
  }
}