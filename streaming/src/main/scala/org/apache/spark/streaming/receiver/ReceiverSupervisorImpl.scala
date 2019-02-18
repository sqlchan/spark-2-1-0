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
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.google.common.base.Throwables
import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.util.WriteAheadLogUtils
import org.apache.spark.util.RpcUtils

/**
 * Concrete implementation of [[org.apache.spark.streaming.receiver.ReceiverSupervisor]]
 * which provides all the necessary functionality for handling the data received by
 * the receiver. Specifically, it creates a [[org.apache.spark.streaming.receiver.BlockGenerator]]
 * object that is used to divide the received data stream into blocks of data.
  * org.apache.spark. stream.receiver 的具体实现。它为处理接收方接收的数据提供了所有必要的功能。
  * 具体地说，它创建了一个[org.apache.spark.stream.receiver]。块生成器对象，用于将接收到的数据流划分为数据块。
 */
private[streaming] class ReceiverSupervisorImpl(
    receiver: Receiver[_],
    env: SparkEnv,
    hadoopConf: Configuration,
    checkpointDirOption: Option[String]
  ) extends ReceiverSupervisor(receiver, env.conf) with Logging {

  private val host = SparkEnv.get.blockManager.blockManagerId.host
  private val executorId = SparkEnv.get.blockManager.blockManagerId.executorId

  private val receivedBlockHandler: ReceivedBlockHandler = {
    if (WriteAheadLogUtils.enableReceiverLog(env.conf)) {
      if (checkpointDirOption.isEmpty) {
        throw new SparkException(
          "Cannot enable receiver write-ahead log without checkpoint directory set. " +
            "Please use streamingContext.checkpoint() to set the checkpoint directory. " +
            "See documentation for more details.")
      }
      new WriteAheadLogBasedBlockHandler(env.blockManager, env.serializerManager, receiver.streamId,
        receiver.storageLevel, env.conf, hadoopConf, checkpointDirOption.get)
    } else {
      new BlockManagerBasedBlockHandler(env.blockManager, receiver.storageLevel)
    }
  }


  /** Remote RpcEndpointRef for the ReceiverTracker   接收跟踪器的远程RpcEndpointRef*/
  private val trackerEndpoint = RpcUtils.makeDriverRef("ReceiverTracker", env.conf, env.rpcEnv)

  /** RpcEndpointRef for receiving messages from the ReceiverTracker in the driver  用于从驱动程序中的ReceiverTracker接收消息*/
  private val endpoint = env.rpcEnv.setupEndpoint(
    "Receiver-" + streamId + "-" + System.currentTimeMillis(), new ThreadSafeRpcEndpoint {
      override val rpcEnv: RpcEnv = env.rpcEnv

      override def receive: PartialFunction[Any, Unit] = {
        case StopReceiver =>
          logInfo("Received stop signal")
          ReceiverSupervisorImpl.this.stop("Stopped by driver", None)
        case CleanupOldBlocks(threshTime) =>
          logDebug("Received delete old batch signal")
          cleanupOldBlocks(threshTime)
        case UpdateRateLimit(eps) =>
          logInfo(s"Received a new rate limit: $eps.")
          registeredBlockGenerators.asScala.foreach { bg =>
            bg.updateRate(eps)
          }
      }
    })

  /** Unique block ids if one wants to add blocks directly  如果要直接添加块，则使用唯一块id*/
  private val newBlockId = new AtomicLong(System.currentTimeMillis())

  private val registeredBlockGenerators = new ConcurrentLinkedQueue[BlockGenerator]()

  /** Divides received data records into data blocks for pushing in BlockManager.
    * 将接收到的数据记录划分为数据块，以推入BlockManager。*/
  private val defaultBlockGeneratorListener = new BlockGeneratorListener {
    def onAddData(data: Any, metadata: Any): Unit = { }

    def onGenerateBlock(blockId: StreamBlockId): Unit = { }

    def onError(message: String, throwable: Throwable) {
      reportError(message, throwable)
    }

    def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]) {
      pushArrayBuffer(arrayBuffer, None, Some(blockId))
    }
  }
  private val defaultBlockGenerator = createBlockGenerator(defaultBlockGeneratorListener)

  /** Get the current rate limit of the default block generator  获取默认块生成器的当前速率限制*/
  override private[streaming] def getCurrentRateLimit: Long = defaultBlockGenerator.getCurrentLimit

  /** Push a single record of received data into block generator.  将接收到的数据的单个记录推入块生成器。*/
  def pushSingle(data: Any) {
    defaultBlockGenerator.addData(data)
  }

  /** Store an ArrayBuffer of received data as a data block into Spark's memory.
    * 将接收数据的ArrayBuffer作为数据块存储到Spark的内存中。*/
  def pushArrayBuffer(
      arrayBuffer: ArrayBuffer[_],
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ) {
    pushAndReportBlock(ArrayBufferBlock(arrayBuffer), metadataOption, blockIdOption)
  }

  /** Store an iterator of received data as a data block into Spark's memory.
    * 将接收数据的迭代器作为数据块存储到Spark的内存中*/
  def pushIterator(
      iterator: Iterator[_],
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ) {
    pushAndReportBlock(IteratorBlock(iterator), metadataOption, blockIdOption)
  }

  /** Store the bytes of received data as a data block into Spark's memory.
    * 将接收到的数据字节作为数据块存储到Spark的内存中。*/
  def pushBytes(
      bytes: ByteBuffer,
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ) {
    pushAndReportBlock(ByteBufferBlock(bytes), metadataOption, blockIdOption)
  }

  /** Store block and report it to driver  存储块并报告给驱动程序*/
  def pushAndReportBlock(
      receivedBlock: ReceivedBlock,
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ) {
    val blockId = blockIdOption.getOrElse(nextBlockId)
    val time = System.currentTimeMillis
    val blockStoreResult = receivedBlockHandler.storeBlock(blockId, receivedBlock)
    logDebug(s"Pushed block $blockId in ${(System.currentTimeMillis - time)} ms")
    val numRecords = blockStoreResult.numRecords
    val blockInfo = ReceivedBlockInfo(streamId, numRecords, metadataOption, blockStoreResult)
    trackerEndpoint.askWithRetry[Boolean](AddBlock(blockInfo))
    logDebug(s"Reported block $blockId")
  }

  /** Report error to the receiver tracker */
  def reportError(message: String, error: Throwable) {
    val errorString = Option(error).map(Throwables.getStackTraceAsString).getOrElse("")
    trackerEndpoint.send(ReportError(streamId, message, errorString))
    logWarning("Reported error " + message + " - " + error)
  }

  override protected def onStart() {
    registeredBlockGenerators.asScala.foreach { _.start() }
  }

  override protected def onStop(message: String, error: Option[Throwable]) {
    registeredBlockGenerators.asScala.foreach { _.stop() }
    env.rpcEnv.stop(endpoint)
  }

  override protected def onReceiverStart(): Boolean = {
    val msg = RegisterReceiver(
      streamId, receiver.getClass.getSimpleName, host, executorId, endpoint)
    trackerEndpoint.askWithRetry[Boolean](msg)
  }

  override protected def onReceiverStop(message: String, error: Option[Throwable]) {
    logInfo("Deregistering receiver " + streamId)
    val errorString = error.map(Throwables.getStackTraceAsString).getOrElse("")
    trackerEndpoint.askWithRetry[Boolean](DeregisterReceiver(streamId, message, errorString))
    logInfo("Stopped receiver " + streamId)
  }

  override def createBlockGenerator(
      blockGeneratorListener: BlockGeneratorListener): BlockGenerator = {
    // Cleanup BlockGenerators that have already been stopped  清除已经停止的块生成器
    val stoppedGenerators = registeredBlockGenerators.asScala.filter{ _.isStopped() }
    stoppedGenerators.foreach(registeredBlockGenerators.remove(_))

    val newBlockGenerator = new BlockGenerator(blockGeneratorListener, streamId, env.conf)
    registeredBlockGenerators.add(newBlockGenerator)
    newBlockGenerator
  }

  /** Generate new block ID */
  private def nextBlockId = StreamBlockId(streamId, newBlockId.getAndIncrement)

  private def cleanupOldBlocks(cleanupThreshTime: Time): Unit = {
    logDebug(s"Cleaning up blocks older then $cleanupThreshTime")
    receivedBlockHandler.cleanupOldBlocks(cleanupThreshTime.milliseconds)
  }
}
