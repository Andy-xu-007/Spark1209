package org.example.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * 无状态操作
 */
object TransForm {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("TransForm").setMaster("local[*]")

    // 创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("ubuntu-pro", 9999)

    // 转换为RDD操作，但是返回值还是DStream，下面的flatMap之类的算子是RDD的，之前的flatMap算子是DStream的
    val wordAndCountDStream: DStream[(String, Int)] = lineDStream.transform(rdd => {
      val value = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      value
    })

    wordAndCountDStream.print

    ssc.start()
    ssc.awaitTermination()
  }
}
