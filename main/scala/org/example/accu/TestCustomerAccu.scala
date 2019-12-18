package org.example.accu

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestCustomerAccu {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val sparkConf = new SparkConf().setAppName("TestCustomerAccu").setMaster("local[*]")

    // 创建sparkContext对象
    val sc = new SparkContext(sparkConf)

    // 创建累加器
    val accu = new CustomerAccu
    // 注册累加器
    val sum = sc.register(accu, "sum")

    // 创建RDD
    val value: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))

    val maped = value.map(x => {
      accu.add(x)
      x
    })

    // 打印RDD数据
    maped.collect().foreach(println)

    println(" ******* ")

    // 打印累加器的值
    println(accu.value)

    sc.stop()

  }
}
