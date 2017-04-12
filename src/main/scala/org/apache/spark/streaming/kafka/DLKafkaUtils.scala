package org.apache.spark.streaming.kafka
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, DefaultDecoder, StringDecoder}
import scala.reflect.ClassTag
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaInputDStream, JavaPairInputDStream, JavaPairReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.util.WriteAheadLogUtils
import org.apache.spark.{SparkContext, SparkException}
import scala.collection.mutable.StringBuilder
/**
 * 改造createDirectStream方法，把topic加入到message中
 */
object DLKafkaUtils {
  def createDirectStream[
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag] (
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      topics: Set[String]
  ): InputDStream[(K, V)] = {
    val messageHandler = (mmd: MessageAndMetadata[K, V]) => {
      val mess = mmd.valueDecoder.fromBytes((new StringBuilder().append(mmd.topic).append(mmd.message().toString()).toString()).getBytes)
      (mmd.key, mess)
      }
    val kc = new KafkaCluster(kafkaParams)
    val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)

    val result = for {
      topicPartitions <- kc.getPartitions(topics).right
      leaderOffsets <- (if (reset == Some("smallest")) {
        kc.getEarliestLeaderOffsets(topicPartitions)
      } else {
        kc.getLatestLeaderOffsets(topicPartitions)
      }).right
    } yield {
      val fromOffsets = leaderOffsets.map { case (tp, lo) =>
          (tp, lo.offset)
      }
      new DirectKafkaInputDStream[K, V, KD, VD, (K, V)](
        ssc, kafkaParams, fromOffsets, messageHandler)
    }
    KafkaCluster.checkErrors(result)
  }
}