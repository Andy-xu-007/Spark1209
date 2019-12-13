package org.example.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestTrans {
  def main(args: Array[String]): Unit = {

    // 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("TestTrans").setMaster("local[*]")

    // 创建sparkContext对象
    val sc: SparkContext = new SparkContext(sparkConf)

    // 创建 search 对象
    val search = new Search("a")

    // 创建RDD
    val words: RDD[String] = sc.parallelize(Array("aa", "ab", "bb"))

    // 过滤出包含"a"的string
    val filtered: RDD[String] = search.getMatch3(words)

    filtered.foreach(println)
    sc.stop()
  }
}
