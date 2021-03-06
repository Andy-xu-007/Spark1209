package org.example.SparkStreaming.Customer

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class CustomerReceiver(hostName: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK){


  // 开启方法
  override def onStart(): Unit = {
    new Thread("CustomerReceiver"){

      override def run(): Unit = {
        // 接受数据方法
        receive()
      }
    }.start()
  }

  // 接受数据，将数据传递给Spark
  def receive(): Unit = {
    // 数据定义的声明
    var socket: Socket = null
    var input: String = null

    var bufferedReader: BufferedReader = null

    try{
      // 创建Socket
      socket = new Socket(hostName, port)
      // 创建BufferReader用于读取端口传来的数据
      bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
      // 获取数据
      input = bufferedReader.readLine()

      while (input != null){
        store(input)
        input = bufferedReader.readLine()
      }

    }catch {
          // 出现异常，关闭socket并重启，避免重复创建
      case e: Exception => {
        bufferedReader.close()
        socket.close()
        restart("重启！！！")
      }
    }

  }

  override def onStop(): Unit = {}
}
