package org.example.Kafka

import java.lang

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.util.BatchedWriteAheadLog.Record

object KafkaStreaming {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    // Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)
    // 创建SparkConf
    val sparkConf = new SparkConf().setAppName("KafkaStreaming").setMaster("local[*]")

    // 创建streamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // kafka参数声明
    val brokers = "namenode:9092, node1:9092, node2:9092"
    val topics = Array("first")
    val groupID = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"

    // 一下是基于spark-streaming-kafka-0-8的视频代码
/*    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupID,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )*/
val kafkaParams = Map(
  "bootstrap.servers" -> brokers,
  "group.id" -> groupID,
  "auto.offset.reset" -> "earliest",
  //上游数据是avro格式的，配置avro schema registry解析arvo数据
  "schema.registry.url" -> "namenode:8081",
  "key.deserializer" -> classOf[StringDeserializer],
  //值是avro数据，需要通过KafkaAvroDeserializer反序列化
  "value.deserializer" -> classOf[StringDeserializer],
  //自己来决定什么时候提交offset
  "enable.auto.commit" -> (false: lang.Boolean)
)

    // 读取Kafka数据创建DStreaming，kafka010版本的api
    val stream = KafkaUtils.createDirectStream[String,Object](
      ssc,
      //持久化策略
      PreferConsistent,
      //订阅信息
      Subscribe[String,Object](topics, kafkaParams)
    )

    // 处理每个微批的rdd
    stream.foreachRDD(rdd => {
      if (rdd != null && !rdd.isEmpty()){
        // 获取OffSet
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 对每个分区分别处理
        rdd.foreachPartition(iterator =>{
          if(iterator != null && !iterator.isEmpty){
            // 作相应的处理
            while(iterator.hasNext){
              // 处理每一条记录
              val next = iterator.next
              // 这个就是接受到的数据值对象
              next.value().asInstanceOf[Record]
              // 可以插入数据库，或者输出到别的地方
              // . . .
            }
          }
        })
        // 将偏移量提交（偏移量是提交到kafka管理的）
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRange)
        println("submit offset!")
      }

    })

    ssc.start()
    ssc.awaitTermination()

  }
}
