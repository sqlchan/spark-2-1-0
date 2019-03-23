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

import scala.util.{Failure, Success, Try}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Checkpoint, CheckpointWriter, Time}
import org.apache.spark.streaming.api.python.PythonDStream
import org.apache.spark.streaming.util.RecurringTimer
import org.apache.spark.util.{Clock, EventLoop, ManualClock, Utils}

/** JobGenerator的事件类 */
private[scheduler] sealed trait JobGeneratorEvent
private[scheduler] case class GenerateJobs(time: Time) extends JobGeneratorEvent
private[scheduler] case class ClearMetadata(time: Time) extends JobGeneratorEvent
private[scheduler] case class DoCheckpoint(
                                            time: Time, clearCheckpointDataLater: Boolean) extends JobGeneratorEvent
private[scheduler] case class ClearCheckpointData(time: Time) extends JobGeneratorEvent

/** 该类从DStreams生成作业，并驱动检查点和清理DStream元数据。 */
private[streaming]
class JobGenerator(jobScheduler: JobScheduler) extends Logging {

  private val ssc = jobScheduler.ssc
  private val graph = ssc.graph

  val clock = Utils.classForName(clockClass).newInstance().asInstanceOf[Clock]

  //定时器
  private val timer = new RecurringTimer(clock, ssc.graph.batchDuration.milliseconds,
    longTime => eventLoop.post(GenerateJobs(new Time(longTime))), "JobGenerator")

  private  val shouldCheckpoint = null

  private var eventLoop: EventLoop[JobGeneratorEvent] = null

  // 最后一批执行完  完成、检查点和元数据清理的
  private var lastProcessedBatch: Time = null

  /** 创造job */
  def start(): Unit = synchronized {

    eventLoop = new EventLoop[JobGeneratorEvent]("JobGenerator") {
      override protected def onReceive(event: JobGeneratorEvent): Unit = processEvent(event)
    }
    eventLoop.start()
    startFirstTime()
  }

  /**一个批处理完成后调用的回调函数。*/
  def onBatchCompletion(time: Time) {eventLoop.post(ClearMetadata(time))}

  /**一个批处理的检查点已经被写入后调用的回调函数。*/
  def onCheckpointCompletion(time: Time, clearCheckpointDataLater: Boolean) {eventLoop.post(ClearCheckpointData(time))}

  /** 处理所有事件*/
  private def processEvent(event: JobGeneratorEvent) {
    event match {
      case GenerateJobs(time) => generateJobs(time)
      case ClearMetadata(time) => clearMetadata(time)
      case DoCheckpoint(time, clearCheckpointDataLater) =>
        doCheckpoint(time, clearCheckpointDataLater)
      case ClearCheckpointData(time) => clearCheckpointData(time)
    }
  }

  private def startFirstTime() {
    val startTime = new Time(timer.getStartTime())
    graph.start(startTime - graph.batchDuration)
    timer.start(startTime.milliseconds)
  }

  /**  为给定的“时间”生成作业并执行检查点 */
  private def generateJobs(time: Time) {
    Try {
      jobScheduler.receiverTracker.allocateBlocksToBatch(time) //  将接收到的块分配为批处理
      graph.generateJobs(time) // generate jobs using allocated block
    } match {
      case Success(jobs) =>
        val streamIdToInputInfos = jobScheduler.inputInfoTracker.getInfo(time)
        jobScheduler.submitJobSet(JobSet(time, jobs, streamIdToInputInfos))
    }
    eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = false))
  }

  /**  清除给定“时间”的DStream元数据 */
  private def clearMetadata(time: Time) {
    ssc.graph.clearMetadata(time)
    eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = true))
  }

  /** 清除给定“时间”的DStream检查点数据*/
  private def clearCheckpointData(time: Time) {
    ssc.graph.clearCheckpointData(time)
    // 所有已处理的批次等检查点信息都已保存到检查点，因此删除块元数据和数据WAL文件是安全的
    val maxRememberDuration = graph.getMaxInputStreamRememberDuration()
    jobScheduler.receiverTracker.cleanupOldBlocksAndBatches(time - maxRememberDuration)
    jobScheduler.inputInfoTracker.cleanup(time - maxRememberDuration)
    markBatchFullyProcessed(time)
  }

  /**  为给定的“时间”执行检查点*/
  private def doCheckpoint(time: Time, clearCheckpointDataLater: Boolean) {
    if (shouldCheckpoint && (time - graph.zeroTime).isMultipleOf(ssc.checkpointDuration)) {
      ssc.graph.updateCheckpointData(time)
      checkpointWriter.write(new Checkpoint(ssc, time), clearCheckpointDataLater)
    } else if (clearCheckpointDataLater) {
      markBatchFullyProcessed(time)
    }
  }

  private def markBatchFullyProcessed(time: Time) {
    lastProcessedBatch = time
  }
}
