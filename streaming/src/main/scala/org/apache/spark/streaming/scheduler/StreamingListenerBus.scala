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

package org.apache.spark.streaming.scheduler

import org.apache.spark.scheduler.{LiveListenerBus, SparkListener, SparkListenerEvent}
import org.apache.spark.util.ListenerBus

/**
 * A Streaming listener bus to forward events to StreamingListeners. This one will wrap received
 * Streaming events as WrappedStreamingListenerEvent and send them to Spark listener bus. It also
 * registers itself with Spark listener bus, so that it can receive WrappedStreamingListenerEvents,
 * unwrap them as StreamingListenerEvent and dispatch them to StreamingListeners.
  * 流侦听器总线，用于将事件转发到流侦听器。这个将把接收到的流事件包装为WrappedStreamingListenerEvent，并将它们发送到Spark侦听器总线。
  * 它还向Spark侦听器总线注册自己，以便接收包装的streaminglistenerevents，将它们展开为StreamingListenerEvent并将它们分发给streaminglistener侦听器。
 */
private[streaming] class StreamingListenerBus(sparkListenerBus: LiveListenerBus)
  extends SparkListener with ListenerBus[StreamingListener, StreamingListenerEvent] {

  /**
   * Post a StreamingListenerEvent to the Spark listener bus asynchronously. This event will be
   * dispatched to all StreamingListeners in the thread of the Spark listener bus.
    * 将StreamingListenerEvent异步发布到Spark侦听器总线。此事件将被分派给Spark侦听器总线线程中的所有streaminglistener。
   */
  def post(event: StreamingListenerEvent) {
    sparkListenerBus.post(new WrappedStreamingListenerEvent(event))
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case WrappedStreamingListenerEvent(e) =>
        postToAll(e)
      case _ =>
    }
  }

  protected override def doPostEvent(
      listener: StreamingListener,
      event: StreamingListenerEvent): Unit = {
    event match {
      case receiverStarted: StreamingListenerReceiverStarted =>
        listener.onReceiverStarted(receiverStarted)
      case receiverError: StreamingListenerReceiverError =>
        listener.onReceiverError(receiverError)
      case receiverStopped: StreamingListenerReceiverStopped =>
        listener.onReceiverStopped(receiverStopped)
      case batchSubmitted: StreamingListenerBatchSubmitted =>
        listener.onBatchSubmitted(batchSubmitted)
      case batchStarted: StreamingListenerBatchStarted =>
        listener.onBatchStarted(batchStarted)
      case batchCompleted: StreamingListenerBatchCompleted =>
        listener.onBatchCompleted(batchCompleted)
      case outputOperationStarted: StreamingListenerOutputOperationStarted =>
        listener.onOutputOperationStarted(outputOperationStarted)
      case outputOperationCompleted: StreamingListenerOutputOperationCompleted =>
        listener.onOutputOperationCompleted(outputOperationCompleted)
      case _ =>
    }
  }

  /**
   * Register this one with the Spark listener bus so that it can receive Streaming events and
   * forward them to StreamingListeners.
    * 向Spark侦听器总线注册这个事件，以便它能够接收流事件并将它们转发给streaminglistener。
   */
  def start(): Unit = {
    sparkListenerBus.addListener(this) // for getting callbacks on spark events  用于获取spark事件的回调
  }

  /**
   * Unregister this one with the Spark listener bus and all StreamingListeners won't receive any
   * events after that.
    * 取消在Spark侦听器总线上的注册，所有streaminglistener在此之后将不会接收任何事件。
   */
  def stop(): Unit = {
    sparkListenerBus.removeListener(this)
  }

  /**
   * Wrapper for StreamingListenerEvent as SparkListenerEvent so that it can be posted to Spark
   * listener bus.
    * 将StreamingListenerEvent包装为SparkListenerEvent，以便将其发布到Spark侦听器总线
   */
  private case class WrappedStreamingListenerEvent(streamingListenerEvent: StreamingListenerEvent)
    extends SparkListenerEvent {

    // Do not log streaming events in event log as history server does not support streaming
    // events (SPARK-12140). TODO Once SPARK-12140 is resolved we should set it to true.
    // 在事件日志中不记录流事件，因为历史服务器不支持流事件
    protected[spark] override def logEvent: Boolean = false
  }
}
