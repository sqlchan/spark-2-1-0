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

package org.apache.spark.streaming.scheduler.rate

import org.apache.spark.internal.Logging

/**
 * Implements a proportional-integral-derivative (PID) controller which acts on
 * the speed of ingestion of elements into Spark Streaming. A PID controller works
 * by calculating an '''error''' between a measured output and a desired value. In the
 * case of Spark Streaming the error is the difference between the measured processing
 * rate (number of elements/processing delay) and the previous rate.
  * 实现了一个比例积分导数(PID)控制器，该控制器作用于在火花流中吸收元素的速度。
  * PID控制器的工作原理是在测量输出和期望值之间计算“误差”。
  * 在火花流的情况下，误差是测量的处理速率(元件数/处理延迟)与之前的速率之间的差。
 *
 * @see https://en.wikipedia.org/wiki/PID_controller
 *
 * @param batchIntervalMillis the batch duration, in milliseconds  批处理持续时间，单位为毫秒
 * @param proportional how much the correction should depend on the current
 *        error. This term usually provides the bulk of correction and should be positive or zero.
 *        A value too large would make the controller overshoot the setpoint, while a small value
 *        would make the controller too insensitive. The default value is 1.
 * @param integral how much the correction should depend on the accumulation
 *        of past errors. This value should be positive or 0. This term accelerates the movement
 *        towards the desired value, but a large value may lead to overshooting. The default value
 *        is 0.2.
 * @param derivative how much the correction should depend on a prediction
 *        of future errors, based on current rate of change. This value should be positive or 0.
 *        This term is not used very often, as it impacts stability of the system. The default
 *        value is 0.
 * @param minRate what is the minimum rate that can be estimated.
 *        This must be greater than zero, so that the system always receives some data for rate
 *        estimation to work.
  *        可以估计的最小速率是多少? 这必须大于零，以便系统总是接收到一些数据，以便进行速率估计。
 */
private[streaming] class PIDRateEstimator(
    batchIntervalMillis: Long,
    proportional: Double,
    integral: Double,
    derivative: Double,
    minRate: Double
  ) extends RateEstimator with Logging {

  private var firstRun: Boolean = true
  private var latestTime: Long = -1L
  private var latestRate: Double = -1D
  private var latestError: Double = -1L

  require(
    batchIntervalMillis > 0,
    s"Specified batch interval $batchIntervalMillis in PIDRateEstimator is invalid.")
  require(
    proportional >= 0,
    s"Proportional term $proportional in PIDRateEstimator should be >= 0.")
  require(
    integral >= 0,
    s"Integral term $integral in PIDRateEstimator should be >= 0.")
  require(
    derivative >= 0,
    s"Derivative term $derivative in PIDRateEstimator should be >= 0.")
  require(
    minRate > 0,
    s"Minimum rate in PIDRateEstimator should be > 0")

  logInfo(s"Created PIDRateEstimator with proportional = $proportional, integral = $integral, " +
    s"derivative = $derivative, min rate = $minRate")

  def compute(
      time: Long, // in milliseconds
      numElements: Long,
      processingDelay: Long, // in milliseconds
      schedulingDelay: Long // in milliseconds
    ): Option[Double] = {
    logTrace(s"\ntime = $time, # records = $numElements, " +
      s"processing time = $processingDelay, scheduling delay = $schedulingDelay")
    this.synchronized {
      if (time > latestTime && numElements > 0 && processingDelay > 0) {

        // in seconds, should be close to batchDuration  以秒为单位，应接近批持续时间
        val delaySinceUpdate = (time - latestTime).toDouble / 1000

        // in elements/second  在元素中/秒
        val processingRate = numElements.toDouble / processingDelay * 1000

        // In our system `error` is the difference between the desired rate and the measured rate
        // based on the latest batch information. We consider the desired rate to be latest rate,
        // which is what this estimator calculated for the previous batch.
        // in elements/second
        // 在我们的系统中，“错误”是期望的速率和基于最新批处理信息的测量速率之间的差。
        // 我们认为期望的速率是最新的速率，这是这个估计值为前一批计算出来的。在元素中/秒
        val error = latestRate - processingRate

        // The error integral, based on schedulingDelay as an indicator for accumulated errors.
        // A scheduling delay s corresponds to s * processingRate overflowing elements. Those
        // are elements that couldn't be processed in previous batches, leading to this delay.
        // In the following, we assume the processingRate didn't change too much.
        // From the number of overflowing elements we can calculate the rate at which they would be
        // processed by dividing it by the batch interval. This rate is our "historical" error,
        // or integral part, since if we subtracted this rate from the previous "calculated rate",
        // there wouldn't have been any overflowing elements, and the scheduling delay would have
        // been zero.

        // (in elements/second)
        val historicalError = schedulingDelay.toDouble * processingRate / batchIntervalMillis

        // in elements/(second ^ 2)
        val dError = (error - latestError) / delaySinceUpdate

        val newRate = (latestRate - proportional * error -
                                    integral * historicalError -
                                    derivative * dError).max(minRate)
        logTrace(s"""
            | latestRate = $latestRate, error = $error
            | latestError = $latestError, historicalError = $historicalError
            | delaySinceUpdate = $delaySinceUpdate, dError = $dError
            """.stripMargin)

        latestTime = time
        if (firstRun) {
          latestRate = processingRate
          latestError = 0D
          firstRun = false
          logTrace("First run, rate estimation skipped")
          None
        } else {
          latestRate = newRate
          latestError = error
          logTrace(s"New rate = $newRate")
          Some(newRate)
        }
      } else {
        logTrace("Rate estimation skipped")
        None
      }
    }
  }
}
