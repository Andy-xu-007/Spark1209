package org.example.SparkStreaming

import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object Windows {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("UPdateStateByKey").setMaster("local[*]")

    // 创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("ubuntu-pro", 9999)

    ssc.sparkContext.setCheckpointDir("HDFS path")

    // 压平
    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    // 单词 =》 元组
    val wordAndOne: DStream[(String, Int)] = wordDStream.map((_,1))

    // 窗口操作（窗口大小6，滑动步长3）
    val wordAndCount = wordAndOne.reduceByKeyAndWindow(
      (x: Int, y: Int) => x + y,   // reduceFunc
      // (a: Int, b: Int) => a - b,   // invReduceFunc，上次的状态加上本次多出来的，再减去上次多出来的
      Seconds(6),
      Seconds(3)
    )

    // 打印
    wordAndCount.print

    ssc.start()
    ssc.awaitTermination()
  }
}
