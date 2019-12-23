package org.example.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * 有状态转换
 */
object UPdateStateByKey {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("UPdateStateByKey").setMaster("local[*]")

    // 创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("ubuntu-pro", 9999)

    ssc.sparkContext.setCheckpointDir("./Athena")

    // 压平
    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    // 单词 =》 元组
    val wordAndOne: DStream[(String, Int)] = wordDStream.map((_,1))

    val update: (Seq[Int], Option[Int]) => Some[Int] = (values:Seq[Int], status:Option[Int]) => {
      // 当前批次内容的计算
      val sum = values.sum
      // 取出状态信息中上一次状态
      val lastStatus = status.getOrElse(0)
      Some(sum + lastStatus)
    }

    // 有状态转换
    val wordAndCount: DStream[(String, Int)] = wordAndOne.updateStateByKey(update)

    wordAndCount.print

    ssc.start()
    ssc.awaitTermination()

  }
}
