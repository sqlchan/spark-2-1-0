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

/** 特征表示ReceivedBlockTracker中更新其状态的任何事件。 */
private[streaming] sealed trait ReceivedBlockTrackerLogEvent

private[streaming] case class BlockAdditionEvent(receivedBlockInfo: ReceivedBlockInfo)
  extends ReceivedBlockTrackerLogEvent
private[streaming] case class BatchAllocationEvent(time: Time, allocatedBlocks: AllocatedBlocks)
  extends ReceivedBlockTrackerLogEvent
private[streaming] case class BatchCleanupEvent(times: Seq[Time])
  extends ReceivedBlockTrackerLogEvent

/** 类表示分配给批处理的所有流的块 */
private[streaming]
case class AllocatedBlocks(streamIdToAllocatedBlocks: Map[Int, Seq[ReceivedBlockInfo]]) {
  def getBlocksOfStream(streamId: Int): Seq[ReceivedBlockInfo] = {
    streamIdToAllocatedBlocks.getOrElse(streamId, Seq.empty)
  }
}

/** 该类跟踪所有接收到的块，并在需要时将它们分配到批。*/
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


  /** 添加块。此事件将被写入写前日志(如果启用). */
  def addBlock(receivedBlockInfo: ReceivedBlockInfo): Boolean = {
    val writeResult = writeToLog(BlockAdditionEvent(receivedBlockInfo))
    getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo
    writeResult
  }

  /** 将所有未分配的块分配给给定的批。 */
  def allocateBlocksToBatch(batchTime: Time): Unit = synchronized {
    if (lastAllocatedBatchTime == null || batchTime > lastAllocatedBatchTime) {
      val streamIdToBlocks = streamIds.map { streamId =>
        (streamId, getReceivedBlockQueue(streamId).dequeueAll(x => true))
      }.toMap
      val allocatedBlocks = AllocatedBlocks(streamIdToBlocks)
      if (writeToLog(BatchAllocationEvent(batchTime, allocatedBlocks))) {
        timeToAllocatedBlocks.put(batchTime, allocatedBlocks)
        lastAllocatedBatchTime = batchTime
      }
    }
  }

  /** 获取分配给给定批处理的块。 */
  def getBlocksOfBatch(batchTime: Time): Map[Int, Seq[ReceivedBlockInfo]] = synchronized {
    timeToAllocatedBlocks.get(batchTime).map {
      _.streamIdToAllocatedBlocks
    }.getOrElse(Map.empty)
  }

  /** 清理旧批次的批次信息。如果waitForCompletion为真，则此方法仅在清理完文件后返回。 */
  def cleanupOldBatches(cleanupThreshTime: Time, waitForCompletion: Boolean): Unit = synchronized {
    require(cleanupThreshTime.milliseconds < clock.getTimeMillis())
    val timesToCleanup = timeToAllocatedBlocks.keys.filter {
      _ < cleanupThreshTime
    }.toSeq
    if (writeToLog(BatchCleanupEvent(timesToCleanup))) {
      timeToAllocatedBlocks --= timesToCleanup
      writeAheadLogOption.foreach(_.clean(cleanupThreshTime.milliseconds, waitForCompletion))
    }
  }


}
