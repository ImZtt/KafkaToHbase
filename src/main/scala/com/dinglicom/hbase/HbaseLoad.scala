package com.dinglicom.hbase
import java.util.Date
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka.KafkaUtils
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
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import org.apache.spark.streaming.dstream.DLPairDStreamFunctions
/**
 * @author wx
 */
object HbaseLoad {
  def main(args: Array[String]) {

    val jobName = "Kafka2Hfile"

    val conf = new SparkConf().setAppName(jobName)
    conf.set("spark.port.maxRetries","1000")
    val context = new StreamingContext(conf, Seconds(60))
    val kafkaParams = Map[String, String]("group.id" -> "lte_user_xdr",
        "metadata.broker.list" -> "172.16.40.141:9092,172.16.40.142:9092", 
        "serializer.class" -> "kafka.serializer.StringEncoder",
        "auto.offset.reset" -> "largest")

    val topics = Set("http","s1u","dns","mms","email",
        "ftp","voip","video","rtsp","im","p2p",
        "s1mme","s10s11","sgs","s6a")

    val kafkaStream = KafkaUtils.createDirectStream[String, String,
      StringDecoder, StringDecoder](context, kafkaParams, topics);
   
    val family = Bytes.toBytes("cf")
    val colum = Bytes.toBytes("1q")
    val hconf = HBaseConfiguration.create()
    consumerProcess(kafkaStream,hconf,family,  colum)
//    hloadProcess(cc, hconf, "HFILE_XDR", true,  family,  colum)
//    hloadProcess(cc, hconf, "HFILE_XDR_IDX", false,  family,  colum)
    context.start();
    context.awaitTermination();
  }
  
  /**
   * 从kafka接入的数据消费进程
   */
  def consumerProcess(kafkaStream: InputDStream[(String,String)],
                     conf: Configuration,                           //hload进程的conf
                     family: Array[Byte],                           //列族
                     colum: Array[Byte]                             //列名
                 ):Unit={
     val filterStream = kafkaStream.filter { x =>
      {
        var rs = true
        val tmpValues = x._2.split("\\|");
        val xdrInterface = tmpValues(1).toLong

        //过滤数据长度或数值不合法的记录
        if ((xdrInterface == 5 || xdrInterface == 6 
            || xdrInterface == 7 || xdrInterface == 8 
            || xdrInterface == 9) || (xdrInterface == 11)) {
          rs = true
        } else {
          rs = false //过滤
        }
        rs
      }
    }
    val cc = filterStream.map(x => {
      val tmpValues = x._2.split("\\|");
      val xdrid = tmpValues(2)
      val xdrInterface = tmpValues(1).toLong
      val processtype = tmpValues(23).toLong

      var datatype = ""
      var msisdn = ""
      var citycode = ""
      var begintime = ""
      var endtime = ""
      
      //取时间:控制面-用户面
      if (xdrInterface == 5 || xdrInterface == 6 
          || xdrInterface == 7 || xdrInterface == 8 || xdrInterface == 9) 
      {
        begintime = tmpValues(8) //u64ProcessStartTime 
        endtime = tmpValues(9) //u64ProcessEndTime        
      }
      else// if(xdrInterface == 11)
      {
        begintime = tmpValues(24)
        endtime = tmpValues(25)         
      }
      
      if (xdrInterface == 5) // lteu1_s1mme_zc
      {
        datatype = "i"
        msisdn = tmpValues(175)
        citycode = tmpValues(179)
      } 
      else if (xdrInterface == 6) // ltec1_s6a_zc
      {
        datatype = "k"
        msisdn = tmpValues(34)
        citycode = tmpValues(38)
      } 
      else if (xdrInterface == 7 || xdrInterface == 8) // ltec1_s10s11_zc
      {
        datatype = "n"
        msisdn = tmpValues(128)
        citycode = tmpValues(132)
      }
      else if (xdrInterface == 9) // ltec1_sgs_zc
      {
        datatype = "p"
        msisdn = tmpValues(40)
        citycode = tmpValues(44)
      }      
      else if (xdrInterface == 11 && processtype == 100) // lteu1_s1u_zc
      {
        datatype = "j"
        msisdn = tmpValues(63)
        citycode = tmpValues(67)
      }  
      else if (xdrInterface == 11 && processtype == 101) // lteu1_dns_zc
      {
        datatype = "a"
        msisdn = tmpValues(70)
        citycode = tmpValues(74)
      }
      else if (xdrInterface == 11 && processtype == 102) // lteu1_mms_zc
      {
        datatype = "f"
        msisdn = tmpValues(81)
        citycode = tmpValues(85)
      }       
      else if (xdrInterface == 11 && processtype == 103) // lteu1_http_zc
      {
        datatype = "m"
        msisdn = tmpValues(87)
        citycode = tmpValues(91)
      } 
      else if (xdrInterface == 11 && processtype == 104) // lteu1_ftp_zc
      {
        datatype = "c"
        msisdn = tmpValues(74)
        citycode = tmpValues(78)
      } 
      else if (xdrInterface == 11 && processtype == 105) // lteu1_email_zc
      {
        datatype = "b"
        msisdn = tmpValues(72)
        citycode = tmpValues(76)
      } 
      else if (xdrInterface == 11 && processtype == 106) // lteu1_voip_zc
      {
        datatype = "l"
        msisdn = tmpValues(70)
        citycode = tmpValues(74)
      }
      else if (xdrInterface == 11 && processtype == 107) // lteu1_rtsp_zc
      {
        datatype = "h"
        msisdn = tmpValues(73)
        citycode = tmpValues(77)
      }       
      else if (xdrInterface == 11 && processtype == 108) // lteu1_p2p_zc
      {
        datatype = "g"
        msisdn = tmpValues(66)
        citycode = tmpValues(70)
      } 
      else if(xdrInterface == 11 && processtype == 109) // lteu1_onlinevideo_zc
      {
        datatype = "o"
        msisdn = tmpValues(74)
        citycode = tmpValues(78)       
      }
      else if (xdrInterface == 11 && processtype == 110) // lteu1_im_zc
      {
        datatype = "e"
        msisdn = tmpValues(67)
        citycode = tmpValues(71)
      }      
      //一级索引rowkey:(endtime的倒序） +　数据类型编码（每个数据类型一个字符）　＋ 城市编码(每个城市一个字符) +　xdrindex
      val mainRowkey = new StringBuilder(endtime).reverse
      mainRowkey.append("-").append(datatype).append("-").
      append(citycode).append("-").append(xdrid)    
      
      //二级索引rowkey:前缀（1-表示msisdn、2-表示imei、3-表示imsi） +　msisdn获取imei或者imsi的倒序
      //+ startime +  -xdrindex- + 数据类型编码（每个数据类型一个字符）
      val idxRowkey = new StringBuilder("1-")
      idxRowkey.append(new StringBuilder(msisdn).reverse).append("-").append(begintime)
        .append("-").append(xdrid).append("-").append(datatype).append("-").append(endtime) 
      val timestamp = endtime.toLong
      val value = x._2
      (mainRowkey.toString(),idxRowkey.toString(),value,timestamp)
    })
//    val threadPool: ExecutorService = Executors.newFixedThreadPool(2)
//    try 
//    {
//      //提交2个线程
//      threadPool.execute(new HloadRunable(cc, conf, "HFILE_XDR", true,  family,  colum))
//      threadPool.execute(new HloadRunable(cc, conf, "HFILE_XDR_IDX", false,  family,  colum))
//    }catch {
//      case t: Throwable => t.printStackTrace() // TODO: handle error
//    } 
//    finally 
//    {
//      threadPool.shutdown()
//    }
    hloadProcess(cc, conf, "HFILE_XDR", true,  family,  colum)
    hloadProcess(cc, conf, "HFILE_XDR_IDX", false,  family,  colum)
   }
  /**
   * 数据处理进程
   */
  def hloadProcess(data: DStream[(String, String, String, Long)],//从kafka接入的数据流
                 conf: Configuration,                           //hload进程的conf
                 tableName: String,                             //要插入的表名
                 isMaintable: Boolean,                          //是否是明细表
                 family: Array[Byte],                           //列族
                 colum: Array[Byte]                             //列名
                 ):Unit={
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
     val s2rdd = dd.repartition(20).transform(rdd=>rdd.sortBy(x=>new String(x._2.getRow) ,true))
     val tran = new DLPairDStreamFunctions(s2rdd)
     tran.saveAsNewAPIHadoopFilesdl("/user/cloudil/hbase/"+tableName,"",
       classOf[ImmutableBytesWritable],
       classOf[KeyValue],
       classOf[HFileOutputFormat2],
       tableName,
       conf)
   }
}
 
