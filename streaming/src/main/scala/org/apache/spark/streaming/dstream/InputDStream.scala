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

package org.apache.spark.streaming.dstream

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDDOperationScope
import org.apache.spark.streaming.{Duration, StreamingContext, Time}
import org.apache.spark.streaming.scheduler.RateController
import org.apache.spark.util.Utils

/**
 * This is the abstract base class for all input streams. This class provides methods
 * start() and stop() which are called by Spark Streaming system to start and stop
 * receiving data, respectively.
  * 这是所有输入流的抽象基类。该类提供了Spark流系统分别调用start()和stop()方法来启动和停止接收数据。
  *
 * Input streams that can generate RDDs from new data by running a service/thread only on
 * the driver node (that is, without running a receiver on worker nodes), can be
 * implemented by directly inheriting this InputDStream. For example,
 * FileInputDStream, a subclass of InputDStream, monitors a HDFS directory from the driver for
 * new files and generates RDDs with the new files. For implementing input streams
 * that requires running a receiver on the worker nodes, use
 * [[org.apache.spark.streaming.dstream.ReceiverInputDStream]] as the parent class.
  * 仅在驱动程序节点上运行服务/线程(即不在工作节点上运行接收器)就可以从新数据生成RDDs的输入流，可以通过直接继承这个InputDStream来实现。
  * 例如，InputDStream的子类FileInputDStream从驱动程序中监视HDFS目录以查找新文件，并使用新文件生成RDDs。
  * 要实现需要在工作节点上运行接收器的输入流，请使用[org.apache.spark.stream.dstream]。ReceiverInputDStream作为父类。
 *
 * @param _ssc Streaming context that will execute this input stream  将执行此输入流的流上下文
 */
abstract class InputDStream[T: ClassTag](_ssc: StreamingContext)
  extends DStream[T](_ssc) {

  private[streaming] var lastValidTime: Time = null

  ssc.graph.addInputStream(this)

  /** This is an unique identifier for the input stream. 这是输入流的唯一标识符*/
  val id = ssc.getNewInputStreamId()

  // Keep track of the freshest rate for this stream using the rateEstimator
  // 使用rateEstimator跟踪此流的最新速率
  protected[streaming] val rateController: Option[RateController] = None

  /** A human-readable name of this InputDStream */
  private[streaming] def name: String = {
    // e.g. FlumePollingDStream -> "Flume polling stream"
    val newName = Utils.getFormattedClassName(this)
      .replaceAll("InputDStream", "Stream")
      .split("(?=[A-Z])")
      .filter(_.nonEmpty)
      .mkString(" ")
      .toLowerCase
      .capitalize
    s"$newName [$id]"
  }

  /**
   * The base scope associated with the operation that created this DStream.
    * 与创建此DStream的操作相关的基本范围。
   *
   * For InputDStreams, we use the name of this DStream as the scope name.
   * If an outer scope is given, we assume that it includes an alternative name for this stream.
   */
  protected[streaming] override val baseScope: Option[String] = {
    val scopeName = Option(ssc.sc.getLocalProperty(SparkContext.RDD_SCOPE_KEY))
      .map { json => RDDOperationScope.fromJson(json).name + s" [$id]" }
      .getOrElse(name.toLowerCase)
    Some(new RDDOperationScope(scopeName).toJson)
  }

  /**
   * Checks whether the 'time' is valid wrt slideDuration for generating RDD.
   * Additionally it also ensures valid times are in strictly increasing order.
   * This ensures that InputDStream.compute() is called strictly on increasing
   * times.
    * 检查“时间”是否为生成RDD的有效wrt滑动持续时间。
    * 此外，它还确保了有效的时间是严格增加的顺序。这确保在增加时间时严格调用InputDStream.compute()。
   */
  override private[streaming] def isTimeValid(time: Time): Boolean = {
    if (!super.isTimeValid(time)) {
      false // Time not valid
    } else {
      // Time is valid, but check it it is more than lastValidTime
      if (lastValidTime != null && time < lastValidTime) {
        logWarning(s"isTimeValid called with $time whereas the last valid time " +
          s"is $lastValidTime")
      }
      lastValidTime = time
      true
    }
  }

  override def dependencies: List[DStream[_]] = List()

  override def slideDuration: Duration = {
    if (ssc == null) throw new Exception("ssc is null")
    if (ssc.graph.batchDuration == null) throw new Exception("batchDuration is null")
    ssc.graph.batchDuration
  }

  /** Method called to start receiving data. Subclasses must implement this method. */
  def start(): Unit

  /** Method called to stop receiving data. Subclasses must implement this method. */
  def stop(): Unit
}
