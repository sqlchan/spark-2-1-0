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

import org.apache.spark.rdd.{BlockRDD, RDD}
import org.apache.spark.storage.BlockId
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.rdd.WriteAheadLogBackedBlockRDD
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.{RateController, ReceivedBlockInfo, StreamInputInfo}
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.streaming.util.WriteAheadLogUtils

/**
 * Abstract class for defining any [[org.apache.spark.streaming.dstream.InputDStream]]
 * that has to start a receiver on worker nodes to receive external data.
 * Specific implementations of ReceiverInputDStream must
 * define [[getReceiver]] function that gets the receiver object of type
 * [[org.apache.spark.streaming.receiver.Receiver]] that will be sent
 * to the workers to receive data.
  * 抽象类，用于定义任何[org.apache.spark.stream.dstream。它必须启动工作节点上的接收器来接收外部数据。
  * ReceiverInputDStream的特定实现必须定义[[getReceiver]]函数，
  * 该函数获取类型为[org.apache.spark.stream.receiver]的receiver对象。将被发送给工人以接收数据。
  *
  * @param _ssc Streaming context that will execute this input stream
 * @tparam T Class type of the object of this stream 此流的对象的类类型
 */
abstract class ReceiverInputDStream[T: ClassTag](_ssc: StreamingContext)
  extends InputDStream[T](_ssc) {

  /**
   * Asynchronously maintains & sends new rate limits to the receiver through the receiver tracker.
    * 通过接收器跟踪器异步维护和发送新的速率限制。
   */
  override protected[streaming] val rateController: Option[RateController] = {
    if (RateController.isBackPressureEnabled(ssc.conf)) {
      Some(new ReceiverRateController(id, RateEstimator.create(ssc.conf, ssc.graph.batchDuration)))
    } else {
      None
    }
  }

  /**
   * Gets the receiver object that will be sent to the worker nodes
   * to receive data. This method needs to defined by any specific implementation
   * of a ReceiverInputDStream.
    * 获取将发送到工作节点以接收数据的接收器对象。此方法需要由ReceiverInputDStream的任何特定实现来定义。
   */
  def getReceiver(): Receiver[T]

  // Nothing to start or stop as both taken care of by the ReceiverTracker.
  // 没有启动或停止，因为都由ReceiverTracker负责。
  def start() {}

  def stop() {}

  /**
   * Generates RDDs with blocks received by the receiver of this stream. 使用此流的接收方接收的块生成RDDs。*/
  override def compute(validTime: Time): Option[RDD[T]] = {
    val blockRDD = {

      if (validTime < graph.startTime) {
        // If this is called for any time before the start time of the context,
        // then this returns an empty RDD. This may happen when recovering from a
        // driver failure without any write ahead log to recover pre-failure data.
        // 如果在上下文的启动时间之前的任何时间调用这个函数，那么它将返回一个空RDD。
        // 这可能发生在从驱动程序故障中恢复时，没有任何写前日志来恢复故障前数据。
        new BlockRDD[T](ssc.sc, Array.empty)
      } else {
        // Otherwise, ask the tracker for all the blocks that have been allocated to this stream
        // for this batch  否则，向跟踪器请求为该批处理分配给此流的所有块
        val receiverTracker = ssc.scheduler.receiverTracker
        val blockInfos = receiverTracker.getBlocksOfBatch(validTime).getOrElse(id, Seq.empty)

        // Register the input blocks information into InputInfoTracker 将输入块信息注册到InputInfoTracker中
        val inputInfo = StreamInputInfo(id, blockInfos.flatMap(_.numRecords).sum)
        ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

        // Create the BlockRDD
        createBlockRDD(validTime, blockInfos)
      }
    }
    Some(blockRDD)
  }

  private[streaming] def createBlockRDD(time: Time, blockInfos: Seq[ReceivedBlockInfo]): RDD[T] = {

    if (blockInfos.nonEmpty) {
      val blockIds = blockInfos.map { _.blockId.asInstanceOf[BlockId] }.toArray

      // Are WAL record handles present with all the blocks  是否所有块都有WAL记录句柄
      val areWALRecordHandlesPresent = blockInfos.forall { _.walRecordHandleOption.nonEmpty }

      if (areWALRecordHandlesPresent) {
        // If all the blocks have WAL record handle, then create a WALBackedBlockRDD
        // 如果所有块都有WAL记录句柄，那么创建一个WALBackedBlockRDD
        val isBlockIdValid = blockInfos.map { _.isBlockIdValid() }.toArray
        val walRecordHandles = blockInfos.map { _.walRecordHandleOption.get }.toArray
        new WriteAheadLogBackedBlockRDD[T](
          ssc.sparkContext, blockIds, walRecordHandles, isBlockIdValid)
      } else {
        // Else, create a BlockRDD. However, if there are some blocks with WAL info but not
        // others then that is unexpected and log a warning accordingly.
        // 否则，创建一个BlockRDD。然而，如果有一些带有WAL信息的块，但没有其他块，那么这是意料之外的，并相应地记录一个警告。
        if (blockInfos.exists(_.walRecordHandleOption.nonEmpty)) {
          if (WriteAheadLogUtils.enableReceiverLog(ssc.conf)) {
            logError("Some blocks do not have Write Ahead Log information; " +
              "this is unexpected and data may not be recoverable after driver failures")
          } else {
            logWarning("Some blocks have Write Ahead Log information; this is unexpected")
          }
        }
        val validBlockIds = blockIds.filter { id =>
          ssc.sparkContext.env.blockManager.master.contains(id)
        }
        if (validBlockIds.length != blockIds.length) {
          logWarning("Some blocks could not be recovered as they were not found in memory. " +
            "To prevent such data loss, enable Write Ahead Log (see programming guide " +
            "for more details.")
        }
        new BlockRDD[T](ssc.sc, validBlockIds)
      }
    } else {
      // If no block is ready now, creating WriteAheadLogBackedBlockRDD or BlockRDD
      // according to the configuration 如果现在没有块准备好，则根据配置创建WriteAheadLogBackedBlockRDD或BlockRDD
      if (WriteAheadLogUtils.enableReceiverLog(ssc.conf)) {
        new WriteAheadLogBackedBlockRDD[T](
          ssc.sparkContext, Array.empty, Array.empty, Array.empty)
      } else {
        new BlockRDD[T](ssc.sc, Array.empty)
      }
    }
  }

  /**
   * A RateController that sends the new rate to receivers, via the receiver tracker.
    * 通过接收器跟踪器将新速率发送给接收器的速率控制程序
   */
  private[streaming] class ReceiverRateController(id: Int, estimator: RateEstimator)
      extends RateController(id, estimator) {
    override def publish(rate: Long): Unit =
      ssc.scheduler.receiverTracker.sendRateUpdate(id, rate)
  }
}

