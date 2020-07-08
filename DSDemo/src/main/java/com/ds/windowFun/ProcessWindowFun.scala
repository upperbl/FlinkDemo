package com.ds.windowFun
import com.ds.protocol.ItemViewCount
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
class ProcessWindowFun extends ProcessWindowFunction[Long,ItemViewCount,Long,TimeWindow]{
  override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key,context.window.getEnd,elements.iterator.next()))
  }
}
