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

import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch

import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Abstract class that is responsible for supervising a Receiver in the worker.  抽象类，该类负责监视worker中的接收者。
 * It provides all the necessary interfaces for handling the data received by the receiver.
  * 它提供了处理接收方接收的数据所需的所有接口。
 */
private[streaming] abstract class ReceiverSupervisor(
    receiver: Receiver[_],
    conf: SparkConf
  ) extends Logging {

  /** Enumeration to identify current state of the Receiver  枚举以标识接收器的当前状态*/
  object ReceiverState extends Enumeration {
    type CheckpointState = Value
    val Initialized, Started, Stopped = Value
  }
  import ReceiverState._

  // Attach the supervisor to the receiver  把主管和接收者连在一起
  receiver.attachSupervisor(this)

  private val futureExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("receiver-supervisor-future", 128))

  /** Receiver id  接收机id*/
  protected val streamId = receiver.streamId

  /** Has the receiver been marked for stop. 收报机已作停止标志了吗?*/
  private val stopLatch = new CountDownLatch(1)

  /** Time between a receiver is stopped and started again  接收器之间的时间停止并重新启动*/
  private val defaultRestartDelay = conf.getInt("spark.streaming.receiverRestartDelay", 2000)

  /** The current maximum rate limit for this receiver. 此接收器的当前最大速率限制。*/
  private[streaming] def getCurrentRateLimit: Long = Long.MaxValue

  /** Exception associated with the stopping of the receiver  与接收器停止相关的异常*/
  @volatile protected var stoppingError: Throwable = null

  /** State of the receiver  接收机状态*/
  @volatile private[streaming] var receiverState = Initialized

  /** Push a single data item to backend data store.
    * 将单个数据项推送到后端数据存储。*/
  def pushSingle(data: Any): Unit

  /** Store the bytes of received data as a data block into Spark's memory. */
  def pushBytes(
      bytes: ByteBuffer,
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    ): Unit

  /** Store an iterator of received data as a data block into Spark's memory. */
  def pushIterator(
      iterator: Iterator[_],
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    ): Unit

  /** Store an ArrayBuffer of received data as a data block into Spark's memory.
    * 将接收数据的ArrayBuffer作为数据块存储到Spark的内存中。 */
  def pushArrayBuffer(
      arrayBuffer: ArrayBuffer[_],
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    ): Unit

  /**
   * Create a custom [[BlockGenerator]] that the receiver implementation can directly control
   * using their provided [[BlockGeneratorListener]].
   *
   * Note: Do not explicitly start or stop the `BlockGenerator`, the `ReceiverSupervisorImpl`
   * will take care of it.
   */
  def createBlockGenerator(blockGeneratorListener: BlockGeneratorListener): BlockGenerator

  /** Report errors. */
  def reportError(message: String, throwable: Throwable): Unit

  /**
   * Called when supervisor is started.
   * Note that this must be called before the receiver.onStart() is called to ensure
   * things like [[BlockGenerator]]s are started before the receiver starts sending data.
   */
  protected def onStart() { }

  /**
   * Called when supervisor is stopped.
   * Note that this must be called after the receiver.onStop() is called to ensure
   * things like [[BlockGenerator]]s are cleaned up after the receiver stops sending data.
   */
  protected def onStop(message: String, error: Option[Throwable]) { }

  /** Called when receiver is started. Return true if the driver accepts us */
  protected def onReceiverStart(): Boolean

  /** Called when receiver is stopped  当接收器停止时调用*/
  protected def onReceiverStop(message: String, error: Option[Throwable]) { }

  /** Start the supervisor  开始主管*/
  def start() {
    onStart()
    startReceiver()
  }

  /** Mark the supervisor and the receiver for stopping  在主管和接收人停车时作记号*/
  def stop(message: String, error: Option[Throwable]) {
    stoppingError = error.orNull
    stopReceiver(message, error)
    onStop(message, error)
    futureExecutionContext.shutdownNow()
    stopLatch.countDown()
  }

  /** Start receiver  开始接收*/
  def startReceiver(): Unit = synchronized {
    try {
      // 先调用子类的onreceiverstart方法进行注册，如果注册成功，则继续进行流数据接收器receiver启动
      if (onReceiverStart()) {
        logInfo(s"Starting receiver $streamId")
        receiverState = Started
        receiver.onStart()
        logInfo(s"Called receiver $streamId onStart")
      } else {
        // The driver refused us
        stop("Registered unsuccessfully because Driver refused to start receiver " + streamId, None)
      }
    } catch {
      case NonFatal(t) =>
        stop("Error starting receiver " + streamId, Some(t))
    }
  }

  /** Stop receiver 停止接受*/
  def stopReceiver(message: String, error: Option[Throwable]): Unit = synchronized {
    try {
      logInfo("Stopping receiver with message: " + message + ": " + error.getOrElse(""))
      receiverState match {
        case Initialized =>
          logWarning("Skip stopping receiver because it has not yet stared")
        case Started =>
          receiverState = Stopped
          receiver.onStop()
          logInfo("Called receiver onStop")
          onReceiverStop(message, error)
        case Stopped =>
          logWarning("Receiver has been stopped")
      }
    } catch {
      case NonFatal(t) =>
        logError(s"Error stopping receiver $streamId ${Utils.exceptionString(t)}")
    }
  }

  /** Restart receiver with delay  延时重启接收机*/
  def restartReceiver(message: String, error: Option[Throwable] = None) {
    restartReceiver(message, error, defaultRestartDelay)
  }

  /** Restart receiver with delay */
  def restartReceiver(message: String, error: Option[Throwable], delay: Int) {
    Future {
      // This is a blocking action so we should use "futureExecutionContext" which is a cached
      // thread pool.
      logWarning("Restarting receiver with delay " + delay + " ms: " + message,
        error.getOrElse(null))
      stopReceiver("Restarting receiver with delay " + delay + "ms: " + message, error)
      logDebug("Sleeping for " + delay)
      Thread.sleep(delay)
      logInfo("Starting receiver again")
      startReceiver()
      logInfo("Receiver started again")
    }(futureExecutionContext)
  }

  /** Check if receiver has been marked for starting  检查接收器是否已标记启动*/
  def isReceiverStarted(): Boolean = {
    logDebug("state = " + receiverState)
    receiverState == Started
  }

  /** Check if receiver has been marked for stopping */
  def isReceiverStopped(): Boolean = {
    logDebug("state = " + receiverState)
    receiverState == Stopped
  }


  /** Wait the thread until the supervisor is stopped  等待线程，直到监控器停止*/
  def awaitTermination() {
    logInfo("Waiting for receiver to be stopped")
    stopLatch.await()
    if (stoppingError != null) {
      logError("Stopped receiver with error: " + stoppingError)
      throw stoppingError
    } else {
      logInfo("Stopped receiver without error")
    }
  }
}
