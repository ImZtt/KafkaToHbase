package com.dinglicom.streaming
import java.util.Date
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka.DLKafkaUtils
import org.apache.spark.streaming.dstream.InputDStream
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{HTable, Table, _}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import java.text.SimpleDateFormat
import org.apache.spark.streaming.dstream.DLPairDStreamFunctions
import scala.collection.mutable.StringBuilder

object HbaseLoad {
  /**
   * 索引表hfile结构创建
   */
  def idxProcess(kafkastream: InputDStream[(String,String)],
                 conf: Configuration,
                 tableName: String,
                 family: Array[Byte],
                 colum: Array[Byte]
                 ):Unit={
    val dd = kafkastream.map(x=>{
      val tmpValues = x._2.split("\\|");
      val topic = tmpValues(0)
      if("http".equals(topic)){
    	  val r_endtime = new StringBuilder(tmpValues(25)).reverse
    	  val v = new StringBuilder().append(r_endtime).append("-").
    	      append("m").append("-").append(tmpValues(91)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val value = Bytes.toBytes(v)
			  val date = tmpValues(25).toLong
			  val r_msisdn = new StringBuilder(tmpValues(87)).reverse
			  val rowkey = new StringBuilder().append("1-").append(r_msisdn).
			      append("-").append(tmpValues(24)).append("-").
			      append(tmpValues(2).substring(16)).append("-m").append("-").append(tmpValues(25)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("s1u".equals(topic)){
        val r_endtime = new StringBuilder(tmpValues(25)).reverse
    	  val v = new StringBuilder().append(r_endtime).append("-").
    	      append("j").append("-").append(tmpValues(67)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val value = Bytes.toBytes(v)
			  val date = tmpValues(25).toLong
			  val r_msisdn = new StringBuilder(tmpValues(63)).reverse
			  val rowkey = new StringBuilder().append("1-").append(r_msisdn).
			      append("-").append(tmpValues(67)).append("-").
			      append(tmpValues(2).substring(16)).append("-j").append("-").append(tmpValues(25)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("dns".equals(topic)){
        val r_endtime = new StringBuilder(tmpValues(25)).reverse
    	  val v = new StringBuilder().append(r_endtime).append("-").
    	      append("a").append("-").append(tmpValues(74)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val value = Bytes.toBytes(v)
			  val date = tmpValues(25).toLong
			  val r_msisdn = new StringBuilder(tmpValues(70)).reverse
			  val rowkey = new StringBuilder().append("1-").append(r_msisdn).
			      append("-").append(tmpValues(24)).append("-").
			      append(tmpValues(2).substring(16)).append("-a").append("-").append(tmpValues(25)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("mms".equals(topic)){
        val r_endtime = new StringBuilder(tmpValues(25)).reverse
    	  val v = new StringBuilder().append(r_endtime).append("-").
    	      append("f").append("-").append(tmpValues(85)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val value = Bytes.toBytes(v)
			  val date = tmpValues(25).toLong
			  val r_msisdn = new StringBuilder(tmpValues(81)).reverse
			  val rowkey = new StringBuilder().append("1-").append(r_msisdn).
			      append("-").append(tmpValues(24)).append("-").
			      append(tmpValues(2).substring(16)).append("-f").append("-").append(tmpValues(25)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("email".equals(topic)){
        val r_endtime = new StringBuilder(tmpValues(25)).reverse
    	  val v = new StringBuilder().append(r_endtime).append("-").
    	      append("b").append("-").append(tmpValues(76)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val value = Bytes.toBytes(v)
			  val date = tmpValues(25).toLong
			  val r_msisdn = new StringBuilder(tmpValues(72)).reverse
			  val rowkey = new StringBuilder().append("1-").append(r_msisdn).
			      append("-").append(tmpValues(24)).append("-").
			      append(tmpValues(2).substring(16)).append("-b").append("-").append(tmpValues(25)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("ftp".equals(topic)){
        val r_endtime = new StringBuilder(tmpValues(25)).reverse
    	  val v = new StringBuilder().append(r_endtime).append("-").
    	      append("c").append("-").append(tmpValues(78)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val value = Bytes.toBytes(v)
			  val date = tmpValues(25).toLong
			  val r_msisdn = new StringBuilder(tmpValues(74)).reverse
			  val rowkey = new StringBuilder().append("1-").append(r_msisdn).
			      append("-").append(tmpValues(24)).append("-").
			      append(tmpValues(2).substring(16)).append("-c").append("-").append(tmpValues(25)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("voip".equals(topic)){
        val r_endtime = new StringBuilder(tmpValues(25)).reverse
    	  val v = new StringBuilder().append(r_endtime).append("-").
    	      append("l").append("-").append(tmpValues(74)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val value = Bytes.toBytes(v)
			  val date = tmpValues(25).toLong
			  val r_msisdn = new StringBuilder(tmpValues(70)).reverse
			  val rowkey = new StringBuilder().append("1-").append(r_msisdn).
			      append("-").append(tmpValues(24)).append("-").
			      append(tmpValues(2).substring(16)).append("-l").append("-").append(tmpValues(25)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("rtsp".equals(topic)){
        val r_endtime = new StringBuilder(tmpValues(25)).reverse
    	  val v = new StringBuilder().append(r_endtime).append("-").
    	      append("h").append("-").append(tmpValues(77)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val value = Bytes.toBytes(v)
			  val date = tmpValues(25).toLong
			  val r_msisdn = new StringBuilder(tmpValues(73)).reverse
			  val rowkey = new StringBuilder().append("1-").append(r_msisdn).
			      append("-").append(tmpValues(24)).append("-").
			      append(tmpValues(2).substring(16)).append("-h").append("-").append(tmpValues(25)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("im".equals(topic)){
        val r_endtime = new StringBuilder(tmpValues(25)).reverse
    	  val v = new StringBuilder().append(r_endtime).append("-").
    	      append("e").append("-").append(tmpValues(71)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val value = Bytes.toBytes(v)
			  val date = tmpValues(25).toLong
			  val r_msisdn = new StringBuilder(tmpValues(67)).reverse
			  val rowkey = new StringBuilder().append("1-").append(r_msisdn).
			      append("-").append(tmpValues(24)).append("-").
			      append(tmpValues(2).substring(16)).append("-e").append("-").append(tmpValues(25)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("p2p".equals(topic)){
        val r_endtime = new StringBuilder(tmpValues(25)).reverse
    	  val v = new StringBuilder().append(r_endtime).append("-").
    	      append("g").append("-").append(tmpValues(70)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val value = Bytes.toBytes(v)
			  val date = tmpValues(25).toLong
			  val r_msisdn = new StringBuilder(tmpValues(66)).reverse
			  val rowkey = new StringBuilder().append("1-").append(r_msisdn).
			      append("-").append(tmpValues(24)).append("-").
			      append(tmpValues(2).substring(16)).append("-g").append("-").append(tmpValues(25)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("s1mme".equals(topic)){
        val r_endtime = new StringBuilder(tmpValues(9)).reverse
    	  val v = new StringBuilder().append(r_endtime).append("-").
    	      append("i").append("-").append(tmpValues(179)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val value = Bytes.toBytes(v)
			  val date = tmpValues(9).toLong
			  val r_msisdn = new StringBuilder(tmpValues(175)).reverse
			  val rowkey = new StringBuilder().append("1-").append(r_msisdn).
			      append("-").append(tmpValues(8)).append("-").
			      append(tmpValues(2).substring(16)).append("-i").append("-").append(tmpValues(9)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("sgs".equals(topic)){
        val r_endtime = new StringBuilder(tmpValues(9)).reverse
    	  val v = new StringBuilder().append(r_endtime).append("-").
    	      append("s").append("-").append(tmpValues(44)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val value = Bytes.toBytes(v)
			  val date = tmpValues(9).toLong
			  val r_msisdn = new StringBuilder(tmpValues(40)).reverse
			  val rowkey = new StringBuilder().append("1-").append(r_msisdn).
			      append("-").append(tmpValues(8)).append("-").
			      append(tmpValues(2).substring(16)).append("-s").append("-").append(tmpValues(9)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("s10s11".equals(topic)){
        val r_endtime = new StringBuilder(tmpValues(9)).reverse
    	  val v = new StringBuilder().append(r_endtime).append("-").
    	      append("n").append("-").append(tmpValues(132)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val value = Bytes.toBytes(v)
			  val date = tmpValues(9).toLong
			  val r_msisdn = new StringBuilder(tmpValues(128)).reverse
			  val rowkey = new StringBuilder().append("1-").append(r_msisdn).
			      append("-").append(tmpValues(8)).append("-").
			      append(tmpValues(2).substring(16)).append("-n").append("-").append(tmpValues(9)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("s6a".equals(topic)){
        val r_endtime = new StringBuilder(tmpValues(9)).reverse
    	  val v = new StringBuilder().append(r_endtime).append("-").
    	      append("k").append("-").append(tmpValues(38)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val value = Bytes.toBytes(v)
			  val date = tmpValues(9).toLong
			  val r_msisdn = new StringBuilder(tmpValues(34)).reverse
			  val rowkey = new StringBuilder().append("1-").append(r_msisdn).
			      append("-").append(tmpValues(8)).append("-").
			      append(tmpValues(2).substring(16)).append("-k").append("-").append(tmpValues(9)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else{
        //其余暂归为video
        val r_endtime = new StringBuilder(tmpValues(25)).reverse
    	  val v = new StringBuilder().append(r_endtime).append("-").
    	      append("q").append("-").append(tmpValues(78)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val value = Bytes.toBytes(v)
			  val date = tmpValues(25).toLong
			  val r_msisdn = new StringBuilder(tmpValues(74)).reverse
			  val rowkey = new StringBuilder().append("1-").append(r_msisdn).
			      append("-").append(tmpValues(24)).append("-").
			      append(tmpValues(2).substring(16)).append("-q").append("-").append(tmpValues(25)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }
    })
    val s2rdd = dd.repartition(4).transform(rdd=>rdd.sortBy(x=>new String(x._2.getRow) ,true))
    val tran = new DLPairDStreamFunctions(s2rdd)
//    val hconf = HBaseConfiguration.create()
    tran.saveAsNewAPIHadoopFilesdl("/user/cloudil/hbase/"+tableName,"",
       classOf[ImmutableBytesWritable],
       classOf[KeyValue],
       classOf[HFileOutputFormat2],
       tableName,
       conf)
  }
  /**
   * 明细表的hfile结构创建
   */
   def mainProcess(kafkastream: InputDStream[(String,String)],
                 conf: Configuration,
                 tableName: String,
                 family: Array[Byte],
                 colum: Array[Byte]
                 ):Unit={
      val cc = kafkastream.map(x => {
      val tmpValues = x._2.split("\\|");
      //将rdd转换成HFile需要的格式,我们上面定义了Hfile的key是ImmutableBytesWritable,那么我们定义的RDD也是要以ImmutableBytesWritable的实例为key
      //KeyValue的实例为value
      //rowkey:begintime+endtime+msisdn
      val topic = tmpValues(0)
      if("http".equals(topic)){
    	  val r_endt = new StringBuilder(tmpValues(25)).reverse
        val strb = new StringBuilder
        val rowkey = strb.append(r_endt).append("-").
    	      append("m").append("-").append(tmpValues(91)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  val date = tmpValues(25).toLong
			  val value = Bytes.toBytes(x._2.substring(x._2.indexOf("|")))
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("s1u".equals(topic)){
        val r_endt = new StringBuilder(tmpValues(25)).reverse
        val strb = new StringBuilder
        val rowkey = strb.append(r_endt).append("-").
    	      append("j").append("-").append(tmpValues(67)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  val date = tmpValues(25).toLong
			  val value = Bytes.toBytes(x._2.substring(x._2.indexOf("|")))
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("dns".equals(topic)){
        val r_endt = new StringBuilder(tmpValues(25)).reverse
        val strb = new StringBuilder
        val rowkey = strb.append(r_endt).append("-").
    	      append("a").append("-").append(tmpValues(74)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  val date = tmpValues(25).toLong
			  val value = Bytes.toBytes(x._2.substring(x._2.indexOf("|")))
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("mms".equals(topic)){
        val r_endt = new StringBuilder(tmpValues(25)).reverse
        val strb = new StringBuilder
        val rowkey = strb.append(r_endt).append("-").
    	      append("f").append("-").append(tmpValues(85)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  val date = tmpValues(25).toLong
			  val value = Bytes.toBytes(x._2.substring(x._2.indexOf("|")))
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("email".equals(topic)){
        val r_endt = new StringBuilder(tmpValues(25)).reverse
        val strb = new StringBuilder
        val rowkey = strb.append(r_endt).append("-").
    	      append("b").append("-").append(tmpValues(76)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  val date = tmpValues(25).toLong
			  val value = Bytes.toBytes(x._2.substring(x._2.indexOf("|")))
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("ftp".equals(topic)){
        val r_endt = new StringBuilder(tmpValues(25)).reverse
        val strb = new StringBuilder
        val rowkey = strb.append(r_endt).append("-").
    			  append("c").append("-").append(tmpValues(78)).append("-").
    			  append(tmpValues(2).substring(16)).toString()
    			  val rowKey = Bytes.toBytes(rowkey)
    			  val date = tmpValues(25).toLong
    			  val value = Bytes.toBytes(x._2.substring(x._2.indexOf("|")))
    			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("voip".equals(topic)){
        val r_endt = new StringBuilder(tmpValues(25)).reverse
        val strb = new StringBuilder
        val rowkey = strb.append(r_endt).append("-").
    	      append("l").append("-").append(tmpValues(74)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  val date = tmpValues(25).toLong
			  val value = Bytes.toBytes(x._2.substring(x._2.indexOf("|")))
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("rtsp".equals(topic)){
        val r_endt = new StringBuilder(tmpValues(25)).reverse
        val strb = new StringBuilder
        val rowkey = strb.append(r_endt).append("-").
    	      append("h").append("-").append(tmpValues(77)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  val date = tmpValues(25).toLong
			  val value = Bytes.toBytes(x._2.substring(x._2.indexOf("|")))
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("im".equals(topic)){
        val r_endt = new StringBuilder(tmpValues(25)).reverse
        val strb = new StringBuilder
        val rowkey = strb.append(r_endt).append("-").
    	      append("e").append("-").append(tmpValues(71)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  val date = tmpValues(25).toLong
			  val value = Bytes.toBytes(x._2.substring(x._2.indexOf("|")))
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("p2p".equals(topic)){
        val r_endt = new StringBuilder(tmpValues(25)).reverse
        val strb = new StringBuilder
        val rowkey = strb.append(r_endt).append("-").
    	      append("g").append("-").append(tmpValues(70)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  val date = tmpValues(25).toLong
			  val value = Bytes.toBytes(x._2.substring(x._2.indexOf("|")))
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("s1mme".equals(topic)){
        val r_endt = new StringBuilder(tmpValues(9)).reverse
        val strb = new StringBuilder
        val rowkey = strb.append(r_endt).append("-").
    	      append("i").append("-").append(tmpValues(179)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  val date = tmpValues(9).toLong
			  val value = Bytes.toBytes(x._2.substring(x._2.indexOf("|")))
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("sgs".equals(topic)){
        val r_endt = new StringBuilder(tmpValues(9)).reverse
        val strb = new StringBuilder
        val rowkey = strb.append(r_endt).append("-").
    	      append("g").append("-").append(tmpValues(44)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  val date = tmpValues(9).toLong
			  val value = Bytes.toBytes(x._2.substring(x._2.indexOf("|")))
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))  
      }else if("s10s11".equals(topic)){
        val r_endt = new StringBuilder(tmpValues(9)).reverse
        val strb = new StringBuilder
        val rowkey = strb.append(r_endt).append("-").
    	      append("n").append("-").append(tmpValues(132)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  val date = tmpValues(9).toLong
			  val value = Bytes.toBytes(x._2.substring(x._2.indexOf("|")))
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else if("s6a".equals(topic)){
        val r_endt = new StringBuilder(tmpValues(9)).reverse
        val strb = new StringBuilder
        val rowkey = strb.append(r_endt).append("-").
    	      append("k").append("-").append(tmpValues(38)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  val date = tmpValues(9).toLong
			  val value = Bytes.toBytes(x._2.substring(x._2.indexOf("|")))
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }else{
        //其余暂归为video
        val r_endt = new StringBuilder(tmpValues(25)).reverse
        val strb = new StringBuilder
        val rowkey = strb.append(r_endt).append("-").
    	      append("q").append("-").append(tmpValues(78)).append("-").
    	      append(tmpValues(2).substring(16)).toString()
			  val rowKey = Bytes.toBytes(rowkey)
			  val date = tmpValues(25).toLong
			  val value = Bytes.toBytes(x._2.substring(x._2.indexOf("|")))
			  (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      }
      
    })
    val s2rdd = cc.repartition(4).transform(rdd=>rdd.sortBy(x=>new String(x._2.getRow) ,true))
    val tran = new DLPairDStreamFunctions(s2rdd)
//    val hconf = HBaseConfiguration.create()
    tran.saveAsNewAPIHadoopFilesdl("/user/cloudil/hbase/"+tableName,"",
       classOf[ImmutableBytesWritable],
       classOf[KeyValue],
       classOf[HFileOutputFormat2],
       tableName,
       conf)
   }
   
  def main(args: Array[String]) {
    val jobName = "Kafka2Hfile"
    val conf = new SparkConf().setAppName(jobName)
    //以防spark.ui端口被占用
    conf.set("spark.port.maxRetries","1000")
    val context = new StreamingContext(conf, Seconds(60))

    val kafkaParams = Map[String, String](
                            "group.id" -> "lte_user_xdr",
                            "metadata.broker.list" -> "172.16.40.141:9092,172.16.40.142:9092",
                            "serializer.class" -> "kafka.serializer.StringEncoder",
                            "auto.offset.reset" -> "largest")

    val topics = Set("http","s1u","dns","mms","email",
        "ftp","voip","video","rtsp","im","p2p",
        "s1mme","s10s11","sgs","s6a")

    val kafkaStream = DLKafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        context, kafkaParams, topics);
    val mainTable = "HFILE_XDR"
		val idxTable = "HFILE_XDR_IDX"
    val family = Bytes.toBytes("cf")    
    val colum = Bytes.toBytes("1q")
    val hconf = HBaseConfiguration.create()
    
    idxProcess(kafkaStream, hconf, idxTable, family, colum)
    mainProcess(kafkaStream, hconf, mainTable, family, colum)
    context.start();
    context.awaitTermination();

  }
}
 
