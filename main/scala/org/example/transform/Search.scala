package org.example.transform

import org.apache.spark.rdd.RDD

/**
 * 1，只有是用sc生成的code才会发送到executor执行，普通的代码是不会发送到executor
 * 2，正常情况下从driver传递给executor需要序列化，即使该成员在class中已经通过this来引用
 * 3，序列化class xxx extends Serializable
 * 4，在方法getMatch3中，可以通过str来接受该方法成为string类型，string可以序列化
 * 5，task中只包含所有关于RDD的操作
 */

class Search(query: String) {

  // 过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  // 过滤出包含字符串的RDD
  def getMatch1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的RDD
  def getMatch2(rdd: RDD[String]): RDD[String] = {
    rdd.filter((x => x.contains(query)))
  }

    //过滤出包含字符串的RDD
    def getMatch3(rdd: RDD[String]): RDD[String] = {
      val str = this.query  // 这一步是在Driver完成的
      rdd.filter((x => x.contains(str)))
  }

}
