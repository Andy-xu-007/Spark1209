package org.example.hbase

import Util.PropertiesUtil
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 所有 Spark 和 HBase 集成的基础是 HBaseContext。 HBaseContext 接受 HBase 配置并将其推送到 Spark Executor。
 *    我们可以在静态位置为每个 Spark Executor 建立一个 HBase 连接。
 *
 * Spark Executor 可以位于与 Region Server 相同的节点上, 也可以位于不依赖于共存位置的不同节点上。
 *    可以将每个 Spark Executor 看作一个多线程 client 应用程序。 这允许在 Executor 上运行的任何 Spark Task 访问共享连接对象。
 *
 * http://makaidong.com/cssdongl/228_1393736.html
 */

object TestHbase_new {
  def main(args: Array[String]): Unit = {
    val tableName = "Sensor"

    val sparkConf = new SparkConf().setAppName("HBaseDistributedScanExample" + tableName).setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    conf.set(HConstants.ZOOKEEPER_QUORUM, "namenode,node1,node2")

    val scan = new Scan()
    scan.setCaching(100)
    scan.withStartRow(Bytes.toBytes("THERMALITO 16/12/19 16:22"))
    scan.withStopRow(Bytes.toBytes("THERMALITO 17/12/19 16:22"))
  }
}
