package com.dinglicom.hbase

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
object Kafaka2Hfile {

  def main(args: Array[String]) {

    val jobName = "Kafaka2Hfile"

    val conf = new SparkConf().setAppName(jobName)
    val context = new StreamingContext(conf, Seconds(60))

    val kafkaParams = Map[String, String]("group.id" -> "lte_user_xdr", "metadata.broker.list" -> "172.16.40.57:9092,172.16.40.98:9092,172.16.40.141:9092,172.16.40.142:9092", "serializer.class" -> "kafka.serializer.StringEncoder", "auto.offset.reset" -> "largest")

    val topics = Set("http")

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](context, kafkaParams, topics);
    val hconf = HBaseConfiguration.create()
    val family = Bytes.toBytes("cf")
    val colum = Bytes.toBytes("1q")
//    val time = new SimpleDateFormat("yyyyMMddHHmm").format(new Date())
//    var cc = kafkaStream.map(x => {
//      val tmpValues = x._2.split("\\|");
//      //将rdd转换成HFile需要的格式,我们上面定义了Hfile的key是ImmutableBytesWritable,那么我们定义的RDD也是要以ImmutableBytesWritable的实例为key
//      //KeyValue的实例为value
//      //rowkey:begintime+endtime+msisdn
//      val rowKey = tmpValues(2).substring(16)+"-"+tmpValues(24)+"-"+tmpValues(25)
//      val value = x._2
//      (rowKey,value)
//      
//    })
//    val s2rdd = cc.transform(rdd=>rdd.map(y=>{(y._1,y._2)}).sortBy(x=>x._1 ,true))
//    s2rdd.saveAsTextFiles("/user/cloudil/csv/"+time+"/","")
      var cc = kafkaStream.map(x => {
      val tmpValues = x._2.split("\\|");
      //将rdd转换成HFile需要的格式,我们上面定义了Hfile的key是ImmutableBytesWritable,那么我们定义的RDD也是要以ImmutableBytesWritable的实例为key
      //KeyValue的实例为value
      //rowkey:begintime+endtime+msisdn
      val rowKey = Bytes.toBytes(tmpValues(2).substring(16)+"-"+tmpValues(24)+"-"+tmpValues(25))
      val date = tmpValues(25).toLong
      val value = Bytes.toBytes(x._2)
      (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      
    })
//    val s2rdd = cc.transform(rdd=>rdd.map(y=>{(y._1,y._2)}).sortBy(x=>x._2.getKeyString ,true))
    var s2rdd = cc.repartition(2).transform(rdd=>rdd.map(y=>{(y._1,y._2)}).sortBy(x=>x._2.getKeyString ,true))
    s2rdd.saveAsNewAPIHadoopFiles("/user/cloudil/hbase/hfile","",
       classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hconf);
//    cc.foreachRDD(rdd=>{
//      rdd.sortBy(x=>x._2.getKeyString ,true).saveAsNewAPIHadoopFile("/user/cloudil/hbase/"+time+"/",  
//      classOf[ImmutableBytesWritable],  
//      classOf[KeyValue],  
//      classOf[HFileOutputFormat2],  
//      hconf) 
//    })
    
//    //hbase的表名
//    val tableName = "LTE_XDR"
//    		//创建hbase的链接,利用默认的配置文件,实际上读取的hbase的master地址
//    		val conn = ConnectionFactory.createConnection(hconf)
//    		//根据表名获取表
//    		val table: Table = conn.getTable(TableName.valueOf(tableName))
//    		
//    		//获取hbase表的region分布
//    		val regionLocator = conn.getRegionLocator(TableName.valueOf(tableName))
//    		//创建一个hadoop的mapreduce的job
//    		val job = Job.getInstance(hconf)
//    		//设置job名称
//    		job.setJobName("DumpHFile")
//    		//此处最重要,需要设置文件输出的key,因为我们要生成HFil,所以outkey要用ImmutableBytesWritable
//    		job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
//    		//输出文件的内容KeyValue
//    		job.setMapOutputValueClass(classOf[KeyValue])
//    		//配置HFileOutputFormat2的信息
//    		HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)
//    		//第二个参数为生成文件后缀，暂时不设置
//    		
//    //开始即那个HFile导入到Hbase,此处都是hbase的api操作
//
//      val load = new LoadIncrementalHFiles(hconf)
//      //开始导入
//      load.doBulkLoad(new Path("/user/cloudil/hbase/"+time+"/"), table.asInstanceOf[HTable])
//    
//      table.close()
//      conn.close()
    context.start();
    context.awaitTermination();

  }
//  def saveAndLoad(
//      ds: DStream[(ImmutableBytesWritable, KeyValue)],
//    ): Unit = ssc.withScope {
//    // Wrap conf in SerializableWritable so that ForeachDStream can be serialized for checkpoints
//    val serializableConf = new SerializableConfiguration(conf)
//    val saveFunc = (rdd: RDD[(K, V)], time: Time) => {
//      val file = rddToFileName(prefix, suffix, time)
//      rdd.saveAsNewAPIHadoopFile(
//        file, keyClass, valueClass, outputFormatClass, serializableConf.value)
//    }
//    
//  }
}