package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Test")
    val sc = new SparkContext(sparkConf)

    // 读取数据，创建RDD
    val lines: RDD[String] = sc.textFile("")

    // 切分取出省份加广告 ((province, ad), 1)
    val provinceAndADToOne: RDD[((String, String), Int)] = lines.map(x => {
      val fields: Array[String] = x.split(" ")
      ((fields(1), fields(4)), 1)
    })

    // 统计省份广告被点击的总次数 ((province, ad), 16)
    val provinceAndADToCount: RDD[((String, String), Int)] = provinceAndADToOne.reduceByKey(_ + _)

    // 维度转换(province, (ad, 16))
    val provinceToADAndCount = provinceAndADToCount.map(x => (x._1._1, (x._1._2, x._2)))

    // 将同一个省份不同的广告做聚合(province, (ad1, 16), (ad2, 33), . . . )
    val provinceToADGroup: RDD[(String, Iterable[(String, Int)])] = provinceToADAndCount.groupByKey()

    // 排序同时取Top3
    val top3 = provinceToADGroup.mapValues(x => {
      x.toList.sortWith((a, b) => a._2 > b._2).take(3)
    })

    // 打印
    top3.collect().foreach(println)

    sc.stop()


  }
}