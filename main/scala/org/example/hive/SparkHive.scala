package org.example.hive

import org.apache.spark.sql.SparkSession

object SparkHive {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession对象
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkHive")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("show tables").show()

    spark.stop()
  }
}
