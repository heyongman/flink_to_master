package com.he.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.apache.flink.api.scala._

object CommonApi {
  def main(args: Array[String]): Unit = {
//    FLINK BATCH QUERY
    val env = ExecutionEnvironment.getExecutionEnvironment
    val fTableEnv = BatchTableEnvironment.create(env)

//    BLINK BATCH QUERY
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val bTableEnv = TableEnvironment.create(settings)

    val value: DataSet[String] = env.readCsvFile[String]("")
    val table: Table = fTableEnv.fromDataSet(value)



  }

}
