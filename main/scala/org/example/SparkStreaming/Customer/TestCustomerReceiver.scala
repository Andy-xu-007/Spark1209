package org.example.SparkStreaming.Customer

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestCustomerReceiver {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("TestCustomerReceiver")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 通过自动接收器创建DStream
    val customerReceiverDStream = ssc.receiverStream(new CustomerReceiver("node3", 9999))

    customerReceiverDStream.print

    ssc.start()
    ssc.awaitTermination()
  }
}
