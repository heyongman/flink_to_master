/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.he.datastream

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction

/**
  * Skeleton code for implementing a fraud detector.
  */
object FraudDetector {
  val SMALL_AMOUNT: Double = 1.00
  val LARGE_AMOUNT: Double = 500.00
  val ONE_MINUTE: Long     = 60 * 1000L
}

@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {
  @transient private var flagState: ValueState[java.lang.Boolean] = _
  @transient private var timerState: ValueState[java.lang.Long] = _

  /**
   * 初始化state
   * ValueState 的作用域始终限于当前的 key，即信用卡帐户。
   * 如果标记状态不为空，则该帐户的上一笔交易是小额的，因此，如果当前这笔交易的金额很大，那么检测程序将输出报警信息。
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    val flagDesc = new ValueStateDescriptor("flag",Types.BOOLEAN)
    flagState = getRuntimeContext.getState(flagDesc)
    val timerDesc = new ValueStateDescriptor("timer",Types.LONG)
    timerState = getRuntimeContext.getState(timerDesc)
  }

  /**
   * 注意，ValueState<Boolean> 实际上有 3 种状态：
   * unset (null)，true，和 false，ValueState 是允许空值的。
   *
   * 我们的程序只使用了 unset (null) 和 true 两种来判断标记状态被设置了与否。
   * @param transaction
   * @param context
   * @param collector
   * @throws
   */
  @throws[Exception]
  def processElement(
      transaction: Transaction,
      context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
      collector: Collector[Alert]): Unit = {

//    大订单
    if (flagState.value() != null){
      if (transaction.getAmount > FraudDetector.LARGE_AMOUNT){
        val alert = new Alert
        alert.setId(transaction.getAccountId)
        collector.collect(alert)
      }

//      在检查之后，不论是什么状态，都需要被清空，定时器也需要删除。
//      要删除一个定时器，你需要记录这个定时器的触发时间，这同样需要状态来实现，所以你需要在标记状态后也创建一个记录定时器时间的状态。
      context.timerService().deleteProcessingTimeTimer(timerState.value())
      flagState.clear()
      timerState.clear()
    }

    //    小订单
    if (transaction.getAmount < FraudDetector.SMALL_AMOUNT){
      flagState.update(true)
//      注册处理时间定时器
      val timer = context.timerService().currentProcessingTime() + FraudDetector.ONE_MINUTE
      context.timerService().registerProcessingTimeTimer(timer)
      timerState.update(timer)
    }

  }

  /**
   * 一分钟后就移除状态
   * @param timestamp
   * @param ctx
   * @param out
   */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, Transaction, Alert]#OnTimerContext, out: Collector[Alert]): Unit = {
    flagState.clear()
    timerState.clear()
  }


}
