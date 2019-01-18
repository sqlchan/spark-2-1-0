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

import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.util.{WriteAheadLog, WriteAheadLogUtils}
import org.apache.spark.util.{Clock, Utils}

/** Trait representing any event in the ReceivedBlockTracker that updates its state.
  * 特征表示ReceivedBlockTracker中更新其状态的任何事件 */
private[streaming] sealed trait ReceivedBlockTrackerLogEvent

private[streaming] case class BlockAdditionEvent(receivedBlockInfo: ReceivedBlockInfo)
  extends ReceivedBlockTrackerLogEvent
private[streaming] case class BatchAllocationEvent(time: Time, allocatedBlocks: AllocatedBlocks)
  extends ReceivedBlockTrackerLogEvent
private[streaming] case class BatchCleanupEvent(times: Seq[Time])
  extends ReceivedBlockTrackerLogEvent

/** Class representing the blocks of all the streams allocated to a batch
  * 类表示分配给批处理的所有流的块 */
private[streaming]
case class AllocatedBlocks(streamIdToAllocatedBlocks: Map[Int, Seq[ReceivedBlockInfo]]) {
  def getBlocksOfStream(streamId: Int): Seq[ReceivedBlockInfo] = {
    streamIdToAllocatedBlocks.getOrElse(streamId, Seq.empty)
  }
}

/**
 * Class that keep track of all the received blocks, and allocate them to batches
 * when required. All actions taken by this class can be saved to a write ahead log
 * (if a checkpoint directory has been provided), so that the state of the tracker
 * (received blocks and block-to-batch allocations) can be recovered after driver failure.
  * 类来跟踪所有接收到的块，并在需要时将它们分配给批处理。该类所采取的所有操作都可以保存到写前日志中(如果提供了检查点目录)，
  * 以便在驱动程序失败后恢复跟踪器的状态(接收到的块和块到批处理的分配)。
 *
 * Note that when any instance of this class is created with a checkpoint directory,
 * it will try reading events from logs in the directory.
  * 注意，当使用检查点目录创建该类的任何实例时，它将尝试从目录中的日志中读取事件。
 */
private[streaming] class ReceivedBlockTracker(
    conf: SparkConf,
    hadoopConf: Configuration,
    streamIds: Seq[Int],
    clock: Clock,
    recoverFromWriteAheadLog: Boolean,
    checkpointDirOption: Option[String])
  extends Logging {

  private type ReceivedBlockQueue = mutable.Queue[ReceivedBlockInfo]

  private val streamIdToUnallocatedBlockQueues = new mutable.HashMap[Int, ReceivedBlockQueue]
  private val timeToAllocatedBlocks = new mutable.HashMap[Time, AllocatedBlocks]
  private val writeAheadLogOption = createWriteAheadLog()

  private var lastAllocatedBatchTime: Time = null

  // Recover block information from write ahead logs  从写前日志恢复块信息
  if (recoverFromWriteAheadLog) {
    recoverPastEvents()
  }

  /** Add received block. This event will get written to the write ahead log (if enabled).
    * 添加块。此事件将写入提前写入日志(如果启用)*/
  def addBlock(receivedBlockInfo: ReceivedBlockInfo): Boolean = {
    try {
      val writeResult = writeToLog(BlockAdditionEvent(receivedBlockInfo))
      if (writeResult) {
        synchronized {
          getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo
        }
        logDebug(s"Stream ${receivedBlockInfo.streamId} received " +
          s"block ${receivedBlockInfo.blockStoreResult.blockId}")
      } else {
        logDebug(s"Failed to acknowledge stream ${receivedBlockInfo.streamId} receiving " +
          s"block ${receivedBlockInfo.blockStoreResult.blockId} in the Write Ahead Log.")
      }
      writeResult
    } catch {
      case NonFatal(e) =>
        logError(s"Error adding block $receivedBlockInfo", e)
        false
    }
  }

  /**
   * Allocate all unallocated blocks to the given batch.
   * This event will get written to the write ahead log (if enabled).
    * 将所有未分配的块分配给给定批处理。此事件将写入提前写入日志(如果启用)。
   */
  def allocateBlocksToBatch(batchTime: Time): Unit = synchronized {
    if (lastAllocatedBatchTime == null || batchTime > lastAllocatedBatchTime) {
      val streamIdToBlocks = streamIds.map { streamId =>
          (streamId, getReceivedBlockQueue(streamId).dequeueAll(x => true))
      }.toMap
      val allocatedBlocks = AllocatedBlocks(streamIdToBlocks)
      if (writeToLog(BatchAllocationEvent(batchTime, allocatedBlocks))) {
        timeToAllocatedBlocks.put(batchTime, allocatedBlocks)
        lastAllocatedBatchTime = batchTime
      } else {
        logInfo(s"Possibly processed batch $batchTime needs to be processed again in WAL recovery")
      }
    } else {
      // This situation occurs when:  这种情况发生在:
      // 1. WAL is ended with BatchAllocationEvent, but without BatchCleanupEvent,
      // WAL以BatchAllocationEvent结束，但是没有BatchCleanupEvent，
      // possibly processed batch job or half-processed batch job need to be processed again,
      // 可能需要重新处理已处理批作业或半处理批作业
      // so the batchTime will be equal to lastAllocatedBatchTime.
      // 2. Slow checkpointing makes recovered batch time older than WAL recovered
      // lastAllocatedBatchTime.
      // This situation will only occurs in recovery time.
      logInfo(s"Possibly processed batch $batchTime needs to be processed again in WAL recovery")
    }
  }

  /** Get the blocks allocated to the given batch. 获取分配给给定批处理的块 */
  def getBlocksOfBatch(batchTime: Time): Map[Int, Seq[ReceivedBlockInfo]] = synchronized {
    timeToAllocatedBlocks.get(batchTime).map { _.streamIdToAllocatedBlocks }.getOrElse(Map.empty)
  }

  /** Get the blocks allocated to the given batch and stream. */
  def getBlocksOfBatchAndStream(batchTime: Time, streamId: Int): Seq[ReceivedBlockInfo] = {
    synchronized {
      timeToAllocatedBlocks.get(batchTime).map {
        _.getBlocksOfStream(streamId)
      }.getOrElse(Seq.empty)
    }
  }

  /** Check if any blocks are left to be allocated to batches.  如果还有块要分配给批处理，那就麻烦了*/
  def hasUnallocatedReceivedBlocks: Boolean = synchronized {
    !streamIdToUnallocatedBlockQueues.values.forall(_.isEmpty)
  }

  /**
   * Get blocks that have been added but not yet allocated to any batch. This method
   * is primarily used for testing. 获取已添加但尚未分配给任何批处理的块。此方法主要用于测试
   */
  def getUnallocatedBlocks(streamId: Int): Seq[ReceivedBlockInfo] = synchronized {
    getReceivedBlockQueue(streamId).toSeq
  }

  /**
   * Clean up block information of old batches. If waitForCompletion is true, this method
   * returns only after the files are cleaned up.
    * 清理旧批次的块信息。如果waitForCompletion为true，则此方法仅在清理文件后返回。
   */
  def cleanupOldBatches(cleanupThreshTime: Time, waitForCompletion: Boolean): Unit = synchronized {
    require(cleanupThreshTime.milliseconds < clock.getTimeMillis())
    val timesToCleanup = timeToAllocatedBlocks.keys.filter { _ < cleanupThreshTime }.toSeq
    logInfo(s"Deleting batches: ${timesToCleanup.mkString(" ")}")
    if (writeToLog(BatchCleanupEvent(timesToCleanup))) {
      timeToAllocatedBlocks --= timesToCleanup
      writeAheadLogOption.foreach(_.clean(cleanupThreshTime.milliseconds, waitForCompletion))
    } else {
      logWarning("Failed to acknowledge batch clean up in the Write Ahead Log.")
    }
  }

  /** Stop the block tracker. */
  def stop() {
    writeAheadLogOption.foreach { _.close() }
  }

  /**
   * Recover all the tracker actions from the write ahead logs to recover the state (unallocated
   * and allocated block info) prior to failure.
    * 从写前日志中恢复所有跟踪器操作，以便在失败之前恢复状态(未分配和已分配块信息)。
   */
  private def recoverPastEvents(): Unit = synchronized {
    // Insert the recovered block information  插入恢复的块信息
    def insertAddedBlock(receivedBlockInfo: ReceivedBlockInfo) {
      logTrace(s"Recovery: Inserting added block $receivedBlockInfo")
      receivedBlockInfo.setBlockIdInvalid()
      getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo
    }

    // Insert the recovered block-to-batch allocations and clear the queue of received blocks
    // (when the blocks were originally allocated to the batch, the queue must have been cleared).
    // 插入恢复的块到批处理分配并清除接收块的队列(当块最初分配给批处理时，队列必须已被清除)。
    def insertAllocatedBatch(batchTime: Time, allocatedBlocks: AllocatedBlocks) {
      logTrace(s"Recovery: Inserting allocated batch for time $batchTime to " +
        s"${allocatedBlocks.streamIdToAllocatedBlocks}")
      streamIdToUnallocatedBlockQueues.values.foreach { _.clear() }
      timeToAllocatedBlocks.put(batchTime, allocatedBlocks)
      lastAllocatedBatchTime = batchTime
    }

    // Cleanup the batch allocations  清理批处理分配
    def cleanupBatches(batchTimes: Seq[Time]) {
      logTrace(s"Recovery: Cleaning up batches $batchTimes")
      timeToAllocatedBlocks --= batchTimes
    }

    writeAheadLogOption.foreach { writeAheadLog =>
      logInfo(s"Recovering from write ahead logs in ${checkpointDirOption.get}")
      writeAheadLog.readAll().asScala.foreach { byteBuffer =>
        logInfo("Recovering record " + byteBuffer)
        Utils.deserialize[ReceivedBlockTrackerLogEvent](
          JavaUtils.bufferToArray(byteBuffer), Thread.currentThread().getContextClassLoader) match {
          case BlockAdditionEvent(receivedBlockInfo) =>
            insertAddedBlock(receivedBlockInfo)
          case BatchAllocationEvent(time, allocatedBlocks) =>
            insertAllocatedBatch(time, allocatedBlocks)
          case BatchCleanupEvent(batchTimes) =>
            cleanupBatches(batchTimes)
        }
      }
    }
  }

  /** Write an update to the tracker to the write ahead log   将跟踪器的更新写入到写入前日志*/
  private def writeToLog(record: ReceivedBlockTrackerLogEvent): Boolean = {
    if (isWriteAheadLogEnabled) {
      logTrace(s"Writing record: $record")
      try {
        writeAheadLogOption.get.write(ByteBuffer.wrap(Utils.serialize(record)),
          clock.getTimeMillis())
        true
      } catch {
        case NonFatal(e) =>
          logWarning(s"Exception thrown while writing record: $record to the WriteAheadLog.", e)
          false
      }
    } else {
      true
    }
  }

  /** Get the queue of received blocks belonging to a particular stream   获取属于特定流的接收块的队列*/
  private def getReceivedBlockQueue(streamId: Int): ReceivedBlockQueue = {
    streamIdToUnallocatedBlockQueues.getOrElseUpdate(streamId, new ReceivedBlockQueue)
  }

  /** Optionally create the write ahead log manager only if the feature is enabled
    * 只有在启用写前日志管理器时，才可以选择创建写前日志管理器*/
  private def createWriteAheadLog(): Option[WriteAheadLog] = {
    checkpointDirOption.map { checkpointDir =>
      val logDir = ReceivedBlockTracker.checkpointDirToLogDir(checkpointDirOption.get)
      WriteAheadLogUtils.createLogForDriver(conf, logDir, hadoopConf)
    }
  }

  /** Check if the write ahead log is enabled. This is only used for testing purposes.
    * 检查是否启用了写前日志。这仅用于测试目的。*/
  private[streaming] def isWriteAheadLogEnabled: Boolean = writeAheadLogOption.nonEmpty
}

private[streaming] object ReceivedBlockTracker {
  def checkpointDirToLogDir(checkpointDir: String): String = {
    new Path(checkpointDir, "receivedBlockMetadata").toString
  }
}
