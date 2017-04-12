package com.dinglicom.subject

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.HashPartitioner

object TestSubject {
  
  def main(args:Array[String])
  {
    var sparkConf = new SparkConf
    var context = new SparkContext(sparkConf)
    
    // 使用自定义inputformat
//    var a = context.newAPIHadoopFile("", classOf[TextInputFormat], classOf[LongWritable], classOf[Text]);
    
    var a = context.textFile("");
    
    
    var b = a.map( x => {
      
      (x, Array(1, 2))
      
    });
    
    /**
     * saveAsHadoopFile是将RDD存储在HDFS上的文件中，支持老版本Hadoop API。
     * 可以指定outputKeyClass、outputValueClass以及压缩格式。
     * 每个分区输出一个文件。
     */
    b.saveAsHadoopFile("/iteblog", classOf[String], classOf[String],
        classOf[RDDMultipleTextOutputFormat])
    
    //使用自定义outputformat
//    b.saveAsHadoopFile("/iteblog", classOf[String], classOf[String],
//        classOf[RDDMultipleTextOutputFormat])
    
  
    
  }
  
  class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
      key.asInstanceOf[String]
  }
 
}