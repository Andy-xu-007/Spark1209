package org.example.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes

object TestHbase_old {
  def main(args: Array[String]): Unit = {

    // 屏蔽不必要的日志显示在终端上

    // 创建SparkConf
    val sparkConf = new SparkConf().setAppName("TestHbase_old").setMaster("local[*]")

    // 创建sparkContext对象
    val sc = new SparkContext(sparkConf)

    // 创建configure
    val tableName = "test"
    val conf: Configuration = HBaseConfiguration.create()
    // 设置zookeeper集群，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set(HConstants.ZOOKEEPER_QUORUM, "namenode,node1,node2")
    //设置zookeeper连接端口，默认2181
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    // 读取HBase数据创建RDD
    val hbasaRDD = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )

    hbasaRDD.foreach(x => {
      val value = x._2
      val cells = value.rawCells()
      cells.foreach(y => {
        println(Bytes.toString(CellUtil.cloneRow))
      })
    })

    // 退出
    sc.stop()
  }
}
