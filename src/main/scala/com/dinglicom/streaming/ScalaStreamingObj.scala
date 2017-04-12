package com.dinglicom.streaming

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.Accumulator
import java.util.Date
import scala.collection.mutable.ArrayBuffer

object ScalaStreamingObj {
  
  val minMs5 = 0
//  val minMs1
//  val CITY
//  val INTERFACE
//  val XDRID
//  val RAT
//  val IMSI
//  val IMEI
//  val MSISDN
//  val TYPE
//  val STARTDATE
//  val ENDDATE
//  val STATUS
//  val REQUEST_CAUSE
//  val FAILURE_CAUSE
//  val KEYWORD1
//  val KEYWORD2
//  val KEYWORD3
//  val KEYWORD4
//  val MME_UE_S1APID
//  val OLD_MME_GROUPID
//  val OLD_MMECODE
//  val OLD_MTMSI
//  val MME_GROUPID
//  val MMECODE
//  val MTMSI
//  val TMSI
//  val USERIPV4
//  val USERIPV6
//  val MMEIP
//  val ENBIP
//  val MMEPORT
//  val ENBPORT
//  val TAC
//  val CELLID
//  val OTHERTAC
//  val OTHERECI
//  val APN
//  val EPSBEARER_NUMBER
//  val bearer_json
  
  
  def main(args: Array[String]) {
    
    val sparkConf = new SparkConf;
    //val sparkContext = new SparkContext(sparkConf);
    
    val ssc = new StreamingContext(sparkConf, Seconds(1));

    val dStream = ssc.textFileStream("");
    
    dStream.foreach { rdd => {
        var now = System.currentTimeMillis();
        val inCounter = ssc.sparkContext.accumulator(0L, "in counter");
        val outCounter = ssc.sparkContext.accumulator(0L, "out counter");
        val error1 = ssc.sparkContext.accumulator(0L, "error column size counter");
        val error2 = ssc.sparkContext.accumulator(0L, "null column counter");
        val error3 = ssc.sparkContext.accumulator(0L, "data column counter");
        val error4 = ssc.sparkContext.accumulator(0L, "out time counter");
        val validCounter = ssc.sparkContext.accumulator(0L, "valid counter");
        val beforeCounter = ssc.sparkContext.accumulator(0L, "bef counter");
        val afterCounter = ssc.sparkContext.accumulator(0L, "after counter");
        
        val notnullFilter = ("1,10".split(",")).map { x => x.toInt };
        
        val numberFilter = ("1,10".split(",")).map { x => x.toInt };
        
        val csize = ssc.sparkContext.broadcast(0)
        val deout = ssc.sparkContext.broadcast(1)
        val dein = ssc.sparkContext.broadcast(",")
        
        println("a rdd start at:" + new Date(now))
        
        
        val x = rdd.map { x => {
            inCounter.add(1);
            x.split(dein.value, -1)
          } 
        }.filter { rs => {
            var i = 0;
            var valid = true;
            while (valid && (i < notnullFilter.length))
            {
              
              i = i+1
            }
            
            rs.length < csize.value
          } 
        }.filter { data => {
            //
            var valid = true
            //逻辑判断
            valid
          }
        }.map { ds => {
          
            var kb = new ArrayBuffer[String]
            var vb = new ArrayBuffer[Object]
            
            kb.+=("A")
            
            new Tuple2(kb, vb)
          }
        }
        
    
      }
    }
    
  }
  
}