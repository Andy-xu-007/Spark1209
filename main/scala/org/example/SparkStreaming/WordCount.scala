package org.example.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * ubuntu-pro上输入 nc -lk 9999
 */
object WordCount {
  def main(args: Array[String]): Unit = {

    // 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("ubuntu-pro", 9999)

    // 压平
    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    // 单词 =》 元组
    val wordAndOne: DStream[(String, Int)] = wordDStream.map((_,1))

    // 按K聚合，统计单词
    val wordAndCount: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)

    // 触发计算
    wordAndCount.print()

    //开启流处理
    ssc.start()
    // 等待所有线程全部结束之后，再退出
    ssc.awaitTermination()

  }
}
