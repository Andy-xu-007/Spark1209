package org.example.mysql

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object MYSQLRDD {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val sparkConf = new SparkConf().setAppName("MYSQLRDD").setMaster("local[*]")

    // 创建sparkContext对象
    val sc = new SparkContext(sparkConf)

    // 定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://namenode:3306/rdd"
    val userName = "root"
    val passWD = "000000"

    // 创建JDBC_RDD
    val jdbcRDD = new JdbcRDD[Int](sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, userName, passWD)
      },
      "select id from books where ? <= id and id <= ?",
      1,
      10,
      1,
      rs => rs.getInt(1)
    )

    jdbcRDD.foreach(println)
    sc.stop()

  }
}
