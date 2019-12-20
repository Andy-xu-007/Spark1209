package org.example.UDAF

import org.apache.spark.sql.SparkSession

object TestUDAF {
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
    val dataFrame = spark.read.json("C:\\Users\\nxf32672\\Desktop\\andy\\spark1209\\src\\main\\data\\people.json")

    // 创建临时表
    dataFrame.createTempView("people")

    // 注册UDAF
    spark.udf.register("myAvg", CustomerUDAF)

    // 应用
    spark.sql("select myAvg(age) from people").show()

    spark.stop()
  }
}
