package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 创建sparkContext对象
    val sc = new SparkContext(sparkConf)

    // 读取一个文件
    val line: RDD[String] = sc.textFile("C:\\Users\\nxf32672\\Desktop\\word.txt")

    // 压平
    val words: RDD[String] = line.flatMap(_.split(" "))

    // 单词 =》 元组
    val wordAndOne: RDD[(String, Int)] = words.map((_,1))

    // 统计单词
    val wordAndCount: RDD[(String,Int)] = wordAndOne.reduceByKey(_+_)

    // 保存到文件
    wordAndCount.saveAsTextFile("C:\\Users\\nxf32672\\Desktop\\output")

    // 退出
    sc.stop()


  }
}
