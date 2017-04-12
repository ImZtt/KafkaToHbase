package com.dinglicom.subject

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author Administrator
 */
object WordCount {
  
  def main(args: Array[String]){
    
    /**
     * 第1步：创建spark的配置对象SparkConf，设置程序运行时的配置信息
     * 例如说通过setMaster来设置程序要链接的Spark集群的Master的URL
     * 如果设置为local，则本地运行
     */
    val conf = new SparkConf();
    conf.setAppName("My First Spark App!")
    conf.setMaster("local")
    
    /**
     * 第2步，创建SparkContext对象
     * 这是Spark程序所有功能的唯一入口，无论采用Scala、Java都必须有
     * SparkContext核心作用Spark应用程序运行所需要的核心组件
     * 同时还会负责Spark程序在Master注册程序等
     * SparkContext是整个Spark最重要的对象
     */
    val sc = new SparkContext(conf)
    
    /**
     * 第3步：更加具体数据来源（HDFS、HBase等）通过SparkContext来创建RDD
     * RDD 创建有三种方式：根据外部来源（HDFS）、根据Scala集合、由其它RDD操作
     * 数据会被RDD划分成一系列的partitions，分配到每个partitions的数据属于一个Task处理范畴
     */
    val lines = sc.textFile("H:\\Scala_tt\\text.txt", 1)//读取本地文件并设置
    
    /**
     * 第4步：对初始的RDD进行Trasformation级别的处理等高阶函数等的编程，来惊喜具体数据的计算
     * 4.1：将每行字符拆分为单个单词
     */
    val words = lines.flatMap { line => line.split(" ") }
    
    /**
     * 4.2：在单词拆分的基础上对每个单词实例计算
     */
    val pairs = lines.map { word => (word,1) }
    
    /**
     * 4.3：对么个单词实例为1的基础上统计每个单词出现的总次数
     */
    val wordcount = pairs.reduceByKey(_+_)
    
    println(lines)
    
    wordcount.foreach(wordNumberPair => println(wordNumberPair._1 + ":" +wordNumberPair._2))
    
    sc.stop()
  }
}
















