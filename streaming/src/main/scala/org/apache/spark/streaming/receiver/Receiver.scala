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

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.storage.StorageLevel

/**
 * :: DeveloperApi ::
 * Abstract class of a receiver that can be run on worker nodes to receive external data. A
 * custom receiver can be defined by defining the functions `onStart()` and `onStop()`. `onStart()`
 * should define the setup steps necessary to start receiving data,
 * and `onStop()` should define the cleanup steps necessary to stop receiving data.
 * Exceptions while receiving can be handled either by restarting the receiver with `restart(...)`
 * or stopped completely by `stop(...)`.
  * 可在工作节点上运行以接收外部数据的接收器的抽象类。
  * 自定义接收器可以通过定义函数' onStart() '和' onStop() '来定义。
  * ' onStart() '应该定义开始接收数据所需的设置步骤，' onStop() '应该定义停止接收数据所需的清理步骤。
  * 接收时的异常可以通过“restart(…)”重新启动接收方来处理，也可以通过“stop(…)”完全停止接收。
 *
 * A custom receiver in Scala would look like this.
 *
 * {{{
 *  class MyReceiver(storageLevel: StorageLevel) extends NetworkReceiver[String](storageLevel) {
 *      def onStart() {
 *          // Setup stuff (start threads, open sockets, etc.) to start receiving data.
  *          设置东西(启动线程，打开套接字，等等)开始接收数据
 *          // Must start new thread to receive data, as onStart() must be non-blocking.
  *          必须启动新的线程来接收数据，因为onStart()必须是非阻塞的。
 *
 *          // Call store(...) in those threads to store received data into Spark's memory.
  *          调用这些线程中的store(…)来将接收到的数据存储到Spark的内存中。
 *
 *          // Call stop(...), restart(...) or reportError(...) on any thread based on how
 *          // different errors need to be handled.
  *          根据需要如何处理不同的错误，在任何线程上调用stop(…)、restart(…)或reportError(…)。
 *
 *          // See corresponding method documentation for more details
 *      }
 *
 *      def onStop() {
 *          // Cleanup stuff (stop threads, close sockets, etc.) to stop receiving data.
 *      }
 *  }
 * }}}
 *
 * A custom receiver in Java would look like this.
 *
 * {{{
 * class MyReceiver extends Receiver<String> {
 *     public MyReceiver(StorageLevel storageLevel) {
 *         super(storageLevel);
 *     }
 *
 *     public void onStart() {
 *          // Setup stuff (start threads, open sockets, etc.) to start receiving data.
 *          // Must start new thread to receive data, as onStart() must be non-blocking.
 *
 *          // Call store(...) in those threads to store received data into Spark's memory.
 *
 *          // Call stop(...), restart(...) or reportError(...) on any thread based on how
 *          // different errors need to be handled.
 *
 *          // See corresponding method documentation for more details
 *     }
 *
 *     public void onStop() {
 *          // Cleanup stuff (stop threads, close sockets, etc.) to stop receiving data.
 *     }
 * }
 * }}}
 */
@DeveloperApi
abstract class Receiver[T](val storageLevel: StorageLevel) extends Serializable {

  /**
   * This method is called by the system when the receiver is started. This function
   * must initialize all resources (threads, buffers, etc.) necessary for receiving data.
   * This function must be non-blocking, so receiving the data must occur on a different
   * thread. Received data can be stored with Spark by calling `store(data)`.
    * 此方法在接收端启动时由系统调用。此函数必须初始化接收数据所需的所有资源(线程、缓冲区等)。
    * 这个函数必须是非阻塞的，因此接收数据必须发生在不同的线程上。
    * 接收到的数据可以通过调用“store(data)”用Spark存储。
   *
   * If there are errors in threads started here, then following options can be done
   * (i) `reportError(...)` can be called to report the error to the driver.
   * The receiving of data will continue uninterrupted.
   * (ii) `stop(...)` can be called to stop receiving data. This will call `onStop()` to
   * clear up all resources allocated (threads, buffers, etc.) during `onStart()`.
   * (iii) `restart(...)` can be called to restart the receiver. This will call `onStop()`
   * immediately, and then `onStart()` after a delay.
    * 如果在这里启动的线程中存在错误，那么可以执行以下选项
    * (i)可以调用reportError(…)向驱动程序报告错误。数据的接收将继续不间断。
    * (2)可以调用“stop(…)”停止接收数据。这将调用“onStop()”来清除“onStart()”期间分配的所有资源(线程、缓冲区等)。
    * (3)可以调用“restart(…)”来重新启动接收器。这将立即调用onStop()，然后在延迟之后调用onStart()
   */
  def onStart(): Unit

  /**
   * This method is called by the system when the receiver is stopped. All resources
   * (threads, buffers, etc.) set up in `onStart()` must be cleaned up in this method.
   */
  def onStop(): Unit

  /** Override this to specify a preferred location (hostname). 覆盖它以指定首选位置(主机名) */
  def preferredLocation: Option[String] = None

  /**
   * Store a single item of received data to Spark's memory.
    * 将接收到的单个数据项存储到Spark的内存中。
   * These single items will be aggregated together into data blocks before
   * being pushed into Spark's memory.
    * 这些单独的项在放入Spark的内存之前将聚合到一起，形成数据块。
   */
  def store(dataItem: T) {
    supervisor.pushSingle(dataItem)
  }

  /** Store an ArrayBuffer of received data as a data block into Spark's memory.
    * 将接收数据的ArrayBuffer作为数据块存储到Spark的内存中 */
  def store(dataBuffer: ArrayBuffer[T]) {
    supervisor.pushArrayBuffer(dataBuffer, None, None)
  }

  /**
   * Store an ArrayBuffer of received data as a data block into Spark's memory.
    * 将接收数据的ArrayBuffer作为数据块存储到Spark的内存中。
   * The metadata will be associated with this block of data for being used in the corresponding InputDStream.
    * 元数据将与此数据块相关联，以便在相应的InputDStream中使用。
   */
  def store(dataBuffer: ArrayBuffer[T], metadata: Any) {
    supervisor.pushArrayBuffer(dataBuffer, Some(metadata), None)
  }

  /** Store an iterator of received data as a data block into Spark's memory.
    * 将接收数据的迭代器作为数据块存储到Spark的内存中*/
  def store(dataIterator: Iterator[T]) {
    supervisor.pushIterator(dataIterator, None, None)
  }

  /**
   * Store an iterator of received data as a data block into Spark's memory.
   * The metadata will be associated with this block of data
   * for being used in the corresponding InputDStream.
   */
  def store(dataIterator: java.util.Iterator[T], metadata: Any) {
    supervisor.pushIterator(dataIterator.asScala, Some(metadata), None)
  }

  /** Store an iterator of received data as a data block into Spark's memory. */
  def store(dataIterator: java.util.Iterator[T]) {
    supervisor.pushIterator(dataIterator.asScala, None, None)
  }

  /**
   * Store an iterator of received data as a data block into Spark's memory.
   * The metadata will be associated with this block of data
   * for being used in the corresponding InputDStream.
   */
  def store(dataIterator: Iterator[T], metadata: Any) {
    supervisor.pushIterator(dataIterator, Some(metadata), None)
  }

  /**
   * Store the bytes of received data as a data block into Spark's memory. Note
   * that the data in the ByteBuffer must be serialized using the same serializer
   * that Spark is configured to use.
    * 将接收到的数据字节作为数据块存储到Spark的内存中。
    * 注意，ByteBuffer中的数据必须使用Spark配置使用的序列化器进行序列化
   */
  def store(bytes: ByteBuffer) {
    supervisor.pushBytes(bytes, None, None)
  }

  /**
   * Store the bytes of received data as a data block into Spark's memory.
   * The metadata will be associated with this block of data
   * for being used in the corresponding InputDStream.
   */
  def store(bytes: ByteBuffer, metadata: Any) {
    supervisor.pushBytes(bytes, Some(metadata), None)
  }

  /** Report exceptions in receiving data.  报告接收数据中的异常。 */
  def reportError(message: String, throwable: Throwable) {
    supervisor.reportError(message, throwable)
  }

  /**
   * Restart the receiver. This method schedules the restart and returns
   * immediately. The stopping and subsequent starting of the receiver
   * (by calling `onStop()` and `onStart()`) is performed asynchronously
   * in a background thread. The delay between the stopping and the starting
   * is defined by the Spark configuration `spark.streaming.receiverRestartDelay`.
   * The `message` will be reported to the driver.
    * 重新启动接收机。此方法安排重新启动并立即返回。
    * 接收方的停止和后续启动(通过调用“onStop()”和“onStart()”)在后台线程中异步执行。
    * 停止和启动之间的延迟由Spark配置“Spark .stream .receiverrestartdelay”定义。“信息”将被报告给司机。
   */
  def restart(message: String) {
    supervisor.restartReceiver(message)
  }

  /**
   * Restart the receiver. This method schedules the restart and returns
   * immediately. The stopping and subsequent starting of the receiver
   * (by calling `onStop()` and `onStart()`) is performed asynchronously
   * in a background thread. The delay between the stopping and the starting
   * is defined by the Spark configuration `spark.streaming.receiverRestartDelay`.
   * The `message` and `exception` will be reported to the driver.
   */
  def restart(message: String, error: Throwable) {
    supervisor.restartReceiver(message, Some(error))
  }

  /**
   * Restart the receiver. This method schedules the restart and returns
   * immediately. The stopping and subsequent starting of the receiver
   * (by calling `onStop()` and `onStart()`) is performed asynchronously
   * in a background thread.
   */
  def restart(message: String, error: Throwable, millisecond: Int) {
    supervisor.restartReceiver(message, Some(error), millisecond)
  }

  /** Stop the receiver completely.  完全停止接收。*/
  def stop(message: String) {
    supervisor.stop(message, None)
  }

  /** Stop the receiver completely due to an exception */
  def stop(message: String, error: Throwable) {
    supervisor.stop(message, Some(error))
  }

  /** Check if the receiver has started or not.  检查接收器是否已启动。*/
  def isStarted(): Boolean = {
    supervisor.isReceiverStarted()
  }

  /**
   * Check if receiver has been marked for stopping. Use this to identify when
   * the receiving of data should be stopped. 检查接收器是否已标记停止。使用它来确定何时应该停止接收数据。
   */
  def isStopped(): Boolean = {
    supervisor.isReceiverStopped()
  }

  /**
   * Get the unique identifier the receiver input stream that this
   * receiver is associated with. 获取与此接收器关联的接收器输入流的唯一标识符。
   */
  def streamId: Int = id

  /*
   * =================
   * Private methods
   * =================
   */

  /** Identifier of the stream this receiver is associated with.
    * 此接收器关联的流的标识符。*/
  private var id: Int = -1

  /** Handler object that runs the receiver. This is instantiated lazily in the worker.
    * 运行接收器的处理程序对象。这是在worker中惰性实例化的。
    * */
  @transient private var _supervisor: ReceiverSupervisor = null

  /** Set the ID of the DStream that this receiver is associated with.
    * 设置此接收器关联的DStream的ID
    * */
  private[streaming] def setReceiverId(_id: Int) {
    id = _id
  }

  /** Attach Network Receiver executor to this receiver.
    * 将网络接收器执行器连接到此接收器。 */
  private[streaming] def attachSupervisor(exec: ReceiverSupervisor) {
    assert(_supervisor == null)
    _supervisor = exec
  }

  /** Get the attached supervisor. */
  private[streaming] def supervisor: ReceiverSupervisor = {
    assert(_supervisor != null,
      "A ReceiverSupervisor has not been attached to the receiver yet. Maybe you are starting " +
        "some computation in the receiver before the Receiver.onStart() has been called.")
    _supervisor
  }
}

