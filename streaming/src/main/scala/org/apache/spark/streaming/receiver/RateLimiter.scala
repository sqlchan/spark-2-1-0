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

package org.apache.spark.streaming.receiver

import com.google.common.util.concurrent.{RateLimiter => GuavaRateLimiter}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
 * Provides waitToPush() method to limit the rate at which receivers consume data.
  * 提供waitToPush()方法来限制接收器使用数据的速度。
 *
 * waitToPush method will block the thread if too many messages have been pushed too quickly,
 * and only return when a new message has been pushed. It assumes that only one message is
 * pushed at a time.
  * waitToPush方法将在推送太多消息太快时阻塞线程，并且只在推送新消息时返回。它假设一次只推送一条消息。
 *
 * The spark configuration spark.streaming.receiver.maxRate gives the maximum number of messages
 * per second that each receiver will accept.
  * 星火组态火花。流。接收器。maxRate给出每个接收方每秒将接受的最大消息数。
 *
 * @param conf spark configuration
 */
private[receiver] abstract class RateLimiter(conf: SparkConf) extends Logging {

  // treated as an upper limit  作为上限处理
  private val maxRateLimit = conf.getLong("spark.streaming.receiver.maxRate", Long.MaxValue)
  private lazy val rateLimiter = GuavaRateLimiter.create(getInitialRateLimit().toDouble)

  def waitToPush() {
    rateLimiter.acquire()
  }

  /**
   * Return the current rate limit. If no limit has been set so far, it returns {{{Long.MaxValue}}}.
    * 返回当前的速率限制。如果到目前为止没有设置任何限制，则返回{{{Long.MaxValue}}}。
   */
  def getCurrentLimit: Long = rateLimiter.getRate.toLong

  /**
   * Set the rate limit to `newRate`. The new rate will not exceed the maximum rate configured by
   * {{{spark.streaming.receiver.maxRate}}}, even if `newRate` is higher than that.
   *
   * @param newRate A new rate in records per second. It has no effect if it's 0 or negative.
   */
  private[receiver] def updateRate(newRate: Long): Unit =
    if (newRate > 0) {
      if (maxRateLimit > 0) {
        rateLimiter.setRate(newRate.min(maxRateLimit))
      } else {
        rateLimiter.setRate(newRate)
      }
    }

  /**
   * Get the initial rateLimit to initial rateLimiter
   */
  private def getInitialRateLimit(): Long = {
    math.min(conf.getLong("spark.streaming.backpressure.initialRate", maxRateLimit), maxRateLimit)
  }
}
