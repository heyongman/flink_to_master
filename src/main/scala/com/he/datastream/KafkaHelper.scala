package com.he.datastream

import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime, ZoneOffset}
import java.util.Properties

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

object KafkaHelper {
  val prop = new Properties()
  prop.setProperty("bootstrap.servers","192.168.40.102:9093")
  prop.setProperty("group.id","flink-test")

  //  bean1
  case class Test1(id: String,createTime: String,orderId: String)
  //  bean2
  case class Test2(id: String,createTime: String,orderType: String,orderDetail: String)
  //  bean1 schema
  val schemaTest1: KafkaDeserializationSchema[Test1] = new KafkaDeserializationSchema[Test1] {
    private var mapper:ObjectMapper = _

    override def isEndOfStream(t: Test1): Boolean = false

    override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): Test1 = {
      if (this.mapper == null) this.mapper = new ObjectMapper()
      val test1 = this.mapper.readValue(consumerRecord.value(), classOf[Test1])
      test1
    }

    override def getProducedType: TypeInformation[Test1] = TypeExtractor.getForClass(classOf[Test1])
  }

  //  bean2 schema
  val schemaTest2: KafkaDeserializationSchema[Test2] = new KafkaDeserializationSchema[Test2] {
    private var mapper:ObjectMapper = _

    override def isEndOfStream(t: Test2): Boolean = false

    override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): Test2 = {
      if (this.mapper == null) this.mapper = new ObjectMapper()
      val test2 = this.mapper.readValue(consumerRecord.value(), classOf[Test2])
      test2
    }

    override def getProducedType: TypeInformation[Test2] = TypeExtractor.getForClass(classOf[Test2])
  }

  //    test1水印策略
  val strategy1: WatermarkStrategy[Test1] = WatermarkStrategy
    .forBoundedOutOfOrderness[Test1](Duration.ofSeconds(10))
    .withTimestampAssigner(new SerializableTimestampAssigner[Test1] {
      override def extractTimestamp(t: Test1, l: Long): Long =
        LocalDateTime.parse(t.createTime,DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
          .toInstant(ZoneOffset.of("+8")).toEpochMilli
    })
  //      .withIdleness(Duration.ofSeconds(10)) //这个设置了为什么没效果呢

  //    test2水印策略
  val strategy2: WatermarkStrategy[Test2] = WatermarkStrategy
    .forBoundedOutOfOrderness[Test2](Duration.ofSeconds(10))
    .withTimestampAssigner(new SerializableTimestampAssigner[Test2] {
      override def extractTimestamp(t: Test2, l: Long): Long =
        LocalDateTime.parse(t.createTime,DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
          .toInstant(ZoneOffset.of("+8")).toEpochMilli
    })
  //      .withIdleness(Duration.ofSeconds(10)) //这个设置了为什么没效果呢


}
