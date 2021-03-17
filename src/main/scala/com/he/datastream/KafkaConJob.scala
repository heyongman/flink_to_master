package com.he.datastream

import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime, ZoneOffset}
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.api.scala._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.sink.AlertSink

object KafkaConJob {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10*1000L)
    env.setStateBackend(new FsStateBackend("file:///D://tmp/"))
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val prop = new Properties()
    prop.setProperty("bootstrap.servers","172.29.196.160:9093")
    prop.setProperty("group.id","flink-test")
//     java.util.regex.Pattern.compile("test-topic-[0-9]")
//    new SimpleStringSchema()
//    水印策略
    val strategy = WatermarkStrategy
      .forBoundedOutOfOrderness[ObjectNode](Duration.ofMinutes(1))
      .withTimestampAssigner(new SerializableTimestampAssigner[ObjectNode] {
        override def extractTimestamp(t: ObjectNode, l: Long): Long =
          LocalDateTime.parse(t.get("value").get("createTime").asText(),DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
          .toInstant(ZoneOffset.of("+8")).toEpochMilli
      })
      .withIdleness(Duration.ofMinutes(2)) //这个设置了为什么没效果呢
//    窗口
    val windows = TumblingEventTimeWindows.of(Time.seconds(5))
    val queryWindows = Time.seconds(10)

    val test1 = new FlinkKafkaConsumer[ObjectNode]("test1", new JSONKeyValueDeserializationSchema(false), prop)
    test1.setStartFromEarliest()
    test1.assignTimestampsAndWatermarks(strategy)
//    指定offset方式
//    val partitionToV = new util.HashMap[KafkaTopicPartition,java.lang.Long]()
//    partitionToV.put(new KafkaTopicPartition("test1",0),0L)
//    test1.setStartFromSpecificOffsets(partitionToV)
    val test2 = new FlinkKafkaConsumer[ObjectNode]("test2", new JSONKeyValueDeserializationSchema(false), prop)
    test2.setStartFromEarliest()
    test2.assignTimestampsAndWatermarks(strategy)

    val source1 = env.addSource(test1)
      .map(x => x.get("value"))
    val source2 = env.addSource(test2)
      .map(x => x.get("value"))

    val outputTag3 = new OutputTag[(JsonNode,JsonNode,JsonNode,JsonNode,JsonNode,Int)]("join late")

    val join = source1.join(source2)
      .where(_.get("id")).equalTo(_.get("id"))
      .window(windows)
      .apply((s1,s2) => (s1.get("id"),s1.get("createTime"),s1.get("orderId"),s2.get("orderType"),s2.get("orderDetail"),1))

      .keyBy(_._4)
      .timeWindow(queryWindows)
      .allowedLateness(queryWindows)
      .sideOutputLateData(outputTag3)
      .sum(5)


      .name("join")

    join.print().setParallelism(1)

//    处理超出迟到时间的迟到数据
//    val outputTag1 = new OutputTag[JsonNode]("source1 late")
//    val outputTag2 = new OutputTag[JsonNode]("source2 late")
//    source1
//      .windowAll(windows)
//      .sideOutputLateData(outputTag1)
//      .apply((a:TimeWindow,b:Iterable[JsonNode],c:Collector[JsonNode])=>{})
//      .printToErr()
//    source2
//      .windowAll(windows)
//      .sideOutputLateData(outputTag2)
//      .apply((a:TimeWindow,b:Iterable[JsonNode],c:Collector[JsonNode])=>{})
//      .printToErr()

    join
      .getSideOutput(outputTag3)
      .printToErr()


    env.execute("KafkaConJob")
  }

}
