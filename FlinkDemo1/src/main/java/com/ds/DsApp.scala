package com.ds
import java.util.Properties
import com.ds.protocol.UserBehavior
import com.ds.windowFun.{CountAgg, ProcessWindowFun, TopNHotItems, WindowRestultFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
object DsApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val properties = new Properties()
    properties.put("bootstrap.servers","gx1.leaphd.com:6667")
    properties.put("zookeeper.connect.server","gx1.leaphd.com:2181")
    properties.put("group.id","DSTOPIC5")
    val kafakStream = env.addSource(new FlinkKafkaConsumer[String]("DSTOPIC2", new SimpleStringSchema(), properties))
    val filterStream = kafakStream.map(data => {
      val dataArray = data.split(",")
      UserBehavior(dataArray(0).toLong,
        dataArray(1).toLong,
        dataArray(2).toInt,
        dataArray(3),
        dataArray(4).toLong)
    }).filter(_.behavior == "pv")
    val windowStream = filterStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1)) {
      override def extractTimestamp(t: UserBehavior): Long = t.timestamp
    }).keyBy(_.itemId).timeWindow(Time.seconds(10), Time.seconds(2))
    val dataStream = windowStream.aggregate(new CountAgg(), new ProcessWindowFun()).keyBy(_.windowEnd).process(new TopNHotItems(3))
    dataStream.print()
    env.execute("DS")
  }
}
