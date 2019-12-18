package org.example.accu

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object TestAccu {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val sparkConf = new SparkConf().setAppName("TestAccu").setMaster("local[*]")

    // 创建sparkContext对象
    val sc = new SparkContext(sparkConf)

    // 创建累加器
    val sum: LongAccumulator = sc.longAccumulator("sum")

    // 创建RDD
    val value: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))

    value.map(x => {
      sum.add(x)
      x
    })

    println(sum.value)
    sc.stop()


  }
}
