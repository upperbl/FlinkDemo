package com.ds.windowFun
import com.ds.protocol.ItemViewCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.util.Collector
import scala.collection.mutable.ListBuffer
class TopNHotItems(N:Int) extends ProcessFunction[ItemViewCount,String]{
  lazy val itemList: ListState[ItemViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemList", classOf[ItemViewCount]))
  override def processElement(i: ItemViewCount, context: ProcessFunction[ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    itemList.add(i)
    context.timerService().registerEventTimeTimer(i.windowEnd+100)
  }
  override def onTimer(timestamp: Long, ctx: ProcessFunction[ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val buffer = ListBuffer[ItemViewCount]()
    import scala.collection.JavaConversions._
    for(list <-itemList.get()){
      buffer.append(list)
    }
    itemList.clear()
    val topItems = buffer.sortBy(_.count)(Ordering.Long.reverse).take(N)
    val str = new StringBuilder()
    str.append("=============time: ").append(timestamp).append("========================").append("\n")
    for(i<-topItems){
      str.append("item: ").append(i.itemId).append("    count: ").append(i.count).append("\n")
    }
    out.collect(str.toString())
  }
}
