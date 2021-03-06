/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.util

import org.apache.spark.internal.Logging
import org.apache.spark.util.{Clock, SystemClock}

private[streaming]
class RecurringTimer(clock: Clock, period: Long, callback: (Long) => Unit, name: String)
  extends Logging {

  private val thread = new Thread("RecurringTimer - " + name) {
    setDaemon(true)
    override def run() { loop }
  }

  @volatile private var prevTime = -1L
  @volatile private var nextTime = -1L
  @volatile private var stopped = false

  /**
   * Get the time when this timer will fire if it is started right now.
   * The time will be a multiple of this timer's period and more than
   * current system time.
   */
  def getStartTime(): Long = {
    (math.floor(clock.getTimeMillis().toDouble / period) + 1).toLong * period
  }

  /**
   * Get the time when the timer will fire if it is restarted right now.
   * This time depends on when the timer was started the first time, and was stopped
   * for whatever reason. The time must be a multiple of this timer's period and
   * more than current time.
   */
  def getRestartTime(originalStartTime: Long): Long = {
    val gap = clock.getTimeMillis() - originalStartTime
    (math.floor(gap.toDouble / period).toLong + 1) * period + originalStartTime
  }

  /**
   * Start at the given start time.
   */
  def start(startTime: Long): Long = synchronized {
    nextTime = startTime
    thread.start()
    logInfo("Started timer for " + name + " at time " + nextTime)
    nextTime
  }

  /**
   * Start at the earliest time it can start based on the period. 尽早开始，可以根据时间段开始。
   */
  def start(): Long = {
    start(getStartTime())
  }

  /**
   * Stop the timer, and return the last time the callback was made.
   *
   * @param interruptTimer True will interrupt the callback if it is in progress (not guaranteed to
   *                       give correct time in this case). False guarantees that there will be at
   *                       least one callback after `stop` has been called.
   */
  def stop(interruptTimer: Boolean): Long = synchronized {
    if (!stopped) {
      stopped = true
      if (interruptTimer) {
        thread.interrupt()
      }
      thread.join()
      logInfo("Stopped timer for " + name + " after time " + prevTime)
    }
    prevTime
  }

  private def triggerActionForNextInterval(): Unit = {
    clock.waitTillTime(nextTime)
    // 定时执行回调函数，改函数为updateCurrentBuffer
    callback(nextTime)
    prevTime = nextTime
    nextTime += period
    logDebug("Callback for " + name + " called at time " + prevTime)
  }

  /**
   * Repeatedly call the callback every interval. 每隔一段时间重复调用回调
    * 数据块生成定时器真正处理的工作，该工作定时的执行回调函数，其为初始化数据块生成定时器传入的updateCurrentBuffer参数
    * 在该函数中先把内存中数据currentbuffer赋值给newblockbuffer，然后把newblockbuffer封装成一个数据块，最后把数据块放到blockforpushing队列中。
   */
  private def loop() {
    try {
      while (!stopped) {
        triggerActionForNextInterval()  // 触发下一个间隔的动作
      }
      triggerActionForNextInterval()
    } catch {
      case e: InterruptedException =>
    }
  }
}

private[streaming]
object RecurringTimer extends Logging {

  def main(args: Array[String]) {
    var lastRecurTime = 0L
    val period = 1000

    def onRecur(time: Long) {
      val currentTime = System.currentTimeMillis()
      logInfo("" + currentTime + ": " + (currentTime - lastRecurTime))
      lastRecurTime = currentTime
    }
    val timer = new  RecurringTimer(new SystemClock(), period, onRecur, "Test")
    timer.start()
    Thread.sleep(30 * 1000)
    timer.stop(true)
  }
}

