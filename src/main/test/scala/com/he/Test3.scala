package com.he

import java.time.{LocalDateTime, ZoneOffset,LocalDate}
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalField

object Test3 {
  def init(): Int ={
    println("access")
    1
  }

  private lazy val source = init()

  def main(args: Array[String]): Unit = {
//    println(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
//    val a:String = "2020-07-13 14:16:17"
//    val time = LocalDateTime.parse(a, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
//    println(time.toEpochSecond(ZoneOffset.of("+8")))
//    println(LocalDate.now().plusMonths(-1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))

//    println(source)
//    println(source)
  }

}
