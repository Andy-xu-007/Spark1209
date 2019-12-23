package org.example.mysql

import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession对象
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Test")
      .getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    // 创建一个DataFrame
    val dataFrame = spark.read.json("E:\\workspace\\spark1208N\\src\\main\\data\\people.json")

    // DSL，默认show 20行
    dataFrame.filter($"age" > 20).show()

    // SQL 风格
    // 创建临时表
    dataFrame.createTempView("people")
    // 执行查询
    spark.sql("select * from people")

    // 关闭连接，内部封装了sc
    spark.stop()
  }
}
