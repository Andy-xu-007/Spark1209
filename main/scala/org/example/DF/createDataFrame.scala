package org.example.DF

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object createDataFrame {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession对象
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("createDF")
      .getOrCreate()
    // 获取SparkContext对象
    val sc: SparkContext = spark.sparkContext

    // 导入隐式转换
    import spark.implicits._

    // 1，创建RDD
    val rdd: RDD[Int] = sc.parallelize(Array(1,2,3,4))
    // 2，转换为RDD[Row]
    val rddRow = rdd.map(x => Row(x))
    //3，创建structType
    val structType: StructType = StructType(StructField("id", IntegerType) :: Nil)

    // 创建DataFrame
    val df: DataFrame = spark.createDataFrame(rddRow, structType)

    // 展示数据
    df.show()

    spark.stop()
  }
}
