package com.he.datastream

import com.he.datastream.KafkaHelper.{Test1, Test2}
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer


object SideOut {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10*1000L)
    env.setStateBackend(new FsStateBackend("file:///D://tmp/"))
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    自带的反序列化：new JSONKeyValueDeserializationSchema(false)
    val test1 = new FlinkKafkaConsumer[Test1]("test1", KafkaHelper.schemaTest1, KafkaHelper.prop)
    test1.setStartFromGroupOffsets()
    test1.assignTimestampsAndWatermarks(KafkaHelper.strategy1)

    val test2 = new FlinkKafkaConsumer[Test2]("test2", KafkaHelper.schemaTest2, KafkaHelper.prop)
    test2.setStartFromGroupOffsets()
    test2.assignTimestampsAndWatermarks(KafkaHelper.strategy2)

    val source1: DataStream[Test1] = env.addSource(test1)
    val source2: DataStream[Test2] = env.addSource(test2)


    windowSideOut(source2)

    env.execute(this.getClass.getSimpleName)
  }

  def windowSideOut(source2: DataStream[Test2]): Unit ={
//    滚动窗口时间
    val windows = TumblingEventTimeWindows.of(Time.seconds(10))
//    迟到时间
    val latenessTime = Time.seconds(10)
//    sideOut tag
    val lateTag = new OutputTag[Test2]("late")

    val res = source2.keyBy(_.orderType)
      .window(windows)
      .allowedLateness(latenessTime)
      .sideOutputLateData(lateTag)
      .aggregate(new MyAggregateFun())

    res.print("count")
    res.getSideOutput(lateTag).printToErr("late")
  }

  def joinSideOut(): Unit ={

  }

}

