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

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Duration

/**
 * A component that estimates the rate at which an `InputDStream` should ingest
 * records, based on updates at every batch completion.
 *一个组件，它根据每次批处理完成时的更新来估计“InputDStream”应该以什么速度摄取记录。
 * @see [[org.apache.spark.streaming.scheduler.RateController]]
 */
private[streaming] trait RateEstimator extends Serializable {

  /**
   * Computes the number of records the stream attached to this `RateEstimator`
   * should ingest per second, given an update on the size and completion
   * times of the latest batch.
   * 计算附加到此“RateEstimator”的流每秒应摄取的记录数量，并给出最新批处理的大小和完成时间的更新。
   * @param time The timestamp of the current batch interval that just finished
    *             刚刚完成的当前批处理间隔的时间戳
   * @param elements The number of records that were processed in this batch  此批处理的记录的数量
   * @param processingDelay The time in ms that took for the job to complete  在ms中完成工作所需的时间
   * @param schedulingDelay The time in ms that the job spent in the scheduling queue 作业在调度队列中花费的时间
   */
  def compute(
      time: Long,
      elements: Long,
      processingDelay: Long,
      schedulingDelay: Long): Option[Double]
}

object RateEstimator {

  /**
   * Return a new `RateEstimator` based on the value of
   * `spark.streaming.backpressure.rateEstimator`.
   *
   * The only known and acceptable estimator right now is `pid`. 目前唯一已知和可接受的估计量是“pid”
   *
   * @return An instance of RateEstimator
   * @throws IllegalArgumentException if the configured RateEstimator is not `pid`.
   */
  def create(conf: SparkConf, batchInterval: Duration): RateEstimator =
    conf.get("spark.streaming.backpressure.rateEstimator", "pid") match {
      case "pid" =>
        val proportional = conf.getDouble("spark.streaming.backpressure.pid.proportional", 1.0)
        val integral = conf.getDouble("spark.streaming.backpressure.pid.integral", 0.2)
        val derived = conf.getDouble("spark.streaming.backpressure.pid.derived", 0.0)
        val minRate = conf.getDouble("spark.streaming.backpressure.pid.minRate", 100)
        new PIDRateEstimator(batchInterval.milliseconds, proportional, integral, derived, minRate)

      case estimator =>
        throw new IllegalArgumentException(s"Unknown rate estimator: $estimator")
    }
}
