package com.he

import scala.io.Source

object Test1 {

  def main(args: Array[String]): Unit = {
//    val source = Source.fromFile("/Users/he/proj/bigdata_proj/flink_to_master/src/main/resources/log4j2.properties","utf-8")
//    source.getLines()
//      .flatMap(_.toLowerCase.split(" |\\.").filter(_.nonEmpty))
//      .foreach(println)

    val source = Source.fromURL("http://www.baidu.com")

    source.getLines()
      .foreach(println)
  }
}
