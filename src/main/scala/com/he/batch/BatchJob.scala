package com.he.batch

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.createTypeInformation

object BatchJob {

  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)

    val value = env.fromElements("Hello", "world","1","I think I hear them. Stand, ho! Who's there?")
    val counts = value.flatMap(_.toLowerCase.split("\\W+").filter(_.nonEmpty))
      .map((_, 1, 2))
      .groupBy(0)
//    全局排序
      .sum(1)
      .sortPartition(1,Order.DESCENDING)
      .setParallelism(1)
//    分组取前几
//      .sortGroup(1,Order.ASCENDING)
//      .first(2)
//    JDBCInputFormat.buildJDBCInputFormat().finish()

    counts.print()


  }
}
