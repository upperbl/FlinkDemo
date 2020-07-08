package com.ds.kafka.producerer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object KafkaLogProducer {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers","gx1.leaphd.com:6667")
  /*  properties.put("zookeeper.connect.server","gx1.leaphd.com:2181")*/
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String,String](properties)
    var iterator = Source.fromFile("E:\\soft\\idea\\ideasrc\\FlinkStudy\\FlinkDemo1\\src\\main\\resources\\UserBehavior.csv").getLines()
    while (iterator.hasNext){
    println(iterator.next())
    producer.send(new ProducerRecord[String,String]("DSTOPIC2",iterator.next()))
      if(iterator.hasNext==false){
        Thread.sleep(100)
        iterator = Source.fromFile("E:\\soft\\idea\\ideasrc\\FlinkStudy\\FlinkDemo1\\src\\main\\resources\\UserBehavior.csv").getLines()
      }
    }

  }
}
