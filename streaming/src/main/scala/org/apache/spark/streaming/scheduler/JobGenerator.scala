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

/** Event classes for JobGenerator  JobGenerator的事件类*/
private[scheduler] sealed trait JobGeneratorEvent
private[scheduler] case class GenerateJobs(time: Time) extends JobGeneratorEvent
private[scheduler] case class ClearMetadata(time: Time) extends JobGeneratorEvent
private[scheduler] case class DoCheckpoint(
    time: Time, clearCheckpointDataLater: Boolean) extends JobGeneratorEvent
private[scheduler] case class ClearCheckpointData(time: Time) extends JobGeneratorEvent

/**
 * This class generates jobs from DStreams as well as drives checkpointing and cleaning
 * up DStream metadata.
  * 这个类从DStreams生成作业，并驱动检查点和清理DStream元数据
 */
private[streaming]
class JobGenerator(jobScheduler: JobScheduler) extends Logging {

  private val ssc = jobScheduler.ssc
  private val conf = ssc.conf
  private val graph = ssc.graph

  val clock = {
    val clockClass = ssc.sc.conf.get(
      "spark.streaming.clock", "org.apache.spark.util.SystemClock")
    try {
      Utils.classForName(clockClass).newInstance().asInstanceOf[Clock]
    } catch {
      case e: ClassNotFoundException if clockClass.startsWith("org.apache.spark.streaming") =>
        val newClockClass = clockClass.replace("org.apache.spark.streaming", "org.apache.spark")
        Utils.classForName(newClockClass).newInstance().asInstanceOf[Clock]
    }
  }

  private val timer = new RecurringTimer(clock, ssc.graph.batchDuration.milliseconds,
    longTime => eventLoop.post(GenerateJobs(new Time(longTime))), "JobGenerator")

  // This is marked lazy so that this is initialized after checkpoint duration has been set
  // in the context and the generator has been started.
  // 这被标记为lazy，以便在上下文中设置检查点持续时间并启动生成器之后对其进行初始化。
  private lazy val shouldCheckpoint = ssc.checkpointDuration != null && ssc.checkpointDir != null

  private lazy val checkpointWriter = if (shouldCheckpoint) {
    new CheckpointWriter(this, ssc.conf, ssc.checkpointDir, ssc.sparkContext.hadoopConfiguration)
  } else {
    null
  }

  // eventLoop is created when generator starts.   eventLoop是在生成器启动时创建的。
  // This not being null means the scheduler has been started and not stopped
  // 这不是null意味着调度程序已经启动而没有停止
  private var eventLoop: EventLoop[JobGeneratorEvent] = null

  // last batch whose completion,checkpointing and metadata cleanup has been completed
  // 完成、检查点和元数据清理的最后一批
  private var lastProcessedBatch: Time = null

  /** Start generation of jobs  开始新一代工作*/
  def start(): Unit = synchronized {
    if (eventLoop != null) return // generator has already been started

    // Call checkpointWriter here to initialize it before eventLoop uses it to avoid a deadlock.
    // 在eventLoop使用checkpointWriter来避免死锁之前，在这里调用checkpointWriter来初始化它
    // See SPARK-10125
    checkpointWriter

    eventLoop = new EventLoop[JobGeneratorEvent]("JobGenerator") {
      override protected def onReceive(event: JobGeneratorEvent): Unit = processEvent(event)

      override protected def onError(e: Throwable): Unit = {
        jobScheduler.reportError("Error in job generator", e)
      }
    }
    eventLoop.start()

    if (ssc.isCheckpointPresent) {
      restart()
    } else {
      startFirstTime()
    }
  }

  /**
   * Stop generation of jobs. processReceivedData = true makes this wait until jobs
   * of current ongoing time interval has been generated, processed and corresponding
   * checkpoints written.
    * 停止创造就业机会。processReceivedData = true使此操作等待当前正在进行的时间间隔的作业生成、处理并编写相应的检查点。
   */
  def stop(processReceivedData: Boolean): Unit = synchronized {
    if (eventLoop == null) return // generator has already been stopped

    if (processReceivedData) {
      logInfo("Stopping JobGenerator gracefully")
      val timeWhenStopStarted = System.currentTimeMillis()
      val stopTimeoutMs = conf.getTimeAsMs(
        "spark.streaming.gracefulStopTimeout", s"${10 * ssc.graph.batchDuration.milliseconds}ms")
      val pollTime = 100

      // To prevent graceful stop to get stuck permanently  防止优雅的停顿永久卡住
      def hasTimedOut: Boolean = {
        val timedOut = (System.currentTimeMillis() - timeWhenStopStarted) > stopTimeoutMs
        if (timedOut) {
          logWarning("Timed out while stopping the job generator (timeout = " + stopTimeoutMs + ")")
        }
        timedOut
      }

      // Wait until all the received blocks in the network input tracker has
      // been consumed by network input DStreams, and jobs have been generated with them
      // 等待直到网络输入DStreams使用网络输入跟踪器中的所有接收块，并使用它们生成作业
      logInfo("Waiting for all received blocks to be consumed for job generation")
      while(!hasTimedOut && jobScheduler.receiverTracker.hasUnallocatedBlocks) {
        Thread.sleep(pollTime)
      }
      logInfo("Waited for all received blocks to be consumed for job generation")

      // Stop generating jobs
      val stopTime = timer.stop(interruptTimer = false)
      graph.stop()
      logInfo("Stopped generation timer")

      // Wait for the jobs to complete and checkpoints to be written 等待作业完成并编写检查点
      def haveAllBatchesBeenProcessed: Boolean = {
        lastProcessedBatch != null && lastProcessedBatch.milliseconds == stopTime
      }
      logInfo("Waiting for jobs to be processed and checkpoints to be written")
      while (!hasTimedOut && !haveAllBatchesBeenProcessed) {
        Thread.sleep(pollTime)
      }
      logInfo("Waited for jobs to be processed and checkpoints to be written")
    } else {
      logInfo("Stopping JobGenerator immediately")
      // Stop timer and graph immediately, ignore unprocessed data and pending jobs
      timer.stop(true)
      graph.stop()
    }

    // First stop the event loop, then stop the checkpoint writer; see SPARK-14701
    // 首先停止事件循环，然后停止检查点写入器;看到火花- 14701
    eventLoop.stop()
    if (shouldCheckpoint) checkpointWriter.stop()
    logInfo("Stopped JobGenerator")
  }

  /**
   * Callback called when a batch has been completely processed.
    * 当批处理完成时调用的回调。
   */
  def onBatchCompletion(time: Time) {
    eventLoop.post(ClearMetadata(time))
  }

  /**
   * Callback called when the checkpoint of a batch has been written.
    * 写入批处理的检查点时调用的回调。
   */
  def onCheckpointCompletion(time: Time, clearCheckpointDataLater: Boolean) {
    if (clearCheckpointDataLater) {
      eventLoop.post(ClearCheckpointData(time))
    }
  }

  /** Processes all events  处理所有事件*/
  private def processEvent(event: JobGeneratorEvent) {
    logDebug("Got event " + event)
    event match {
      case GenerateJobs(time) => generateJobs(time)
      case ClearMetadata(time) => clearMetadata(time)
      case DoCheckpoint(time, clearCheckpointDataLater) =>
        doCheckpoint(time, clearCheckpointDataLater)
      case ClearCheckpointData(time) => clearCheckpointData(time)
    }
  }

  /** Starts the generator for the first time  第一次启动生成器*/
  private def startFirstTime() {
    val startTime = new Time(timer.getStartTime())
    graph.start(startTime - graph.batchDuration)
    timer.start(startTime.milliseconds)
    logInfo("Started JobGenerator at " + startTime)
  }

  /** Restarts the generator based on the information in checkpoint  根据检查点中的信息重新启动生成器*/
  private def restart() {
    // If manual clock is being used for testing, then
    // either set the manual clock to the last checkpointed time,
    // or if the property is defined set it to that time
    // 如果手动时钟用于测试，那么要么将手动时钟设置为最后一个检查点时间，要么将属性定义为该时间
    if (clock.isInstanceOf[ManualClock]) {
      val lastTime = ssc.initialCheckpoint.checkpointTime.milliseconds
      val jumpTime = ssc.sc.conf.getLong("spark.streaming.manualClock.jump", 0)
      clock.asInstanceOf[ManualClock].setTime(lastTime + jumpTime)
    }

    val batchDuration = ssc.graph.batchDuration

    // Batches when the master was down, that is,
    // between the checkpoint and current restart time
    // 当主服务器宕机时(即检查点和当前重启时间之间)进行批处理
    val checkpointTime = ssc.initialCheckpoint.checkpointTime
    val restartTime = new Time(timer.getRestartTime(graph.zeroTime.milliseconds))
    val downTimes = checkpointTime.until(restartTime, batchDuration)
    logInfo("Batches during down time (" + downTimes.size + " batches): "
      + downTimes.mkString(", "))

    // Batches that were unprocessed before failure  失败前未处理的批次
    val pendingTimes = ssc.initialCheckpoint.pendingTimes.sorted(Time.ordering)
    logInfo("Batches pending processing (" + pendingTimes.length + " batches): " +
      pendingTimes.mkString(", "))
    // Reschedule jobs for these times  为这些时间重新安排工作
    val timesToReschedule = (pendingTimes ++ downTimes).filter { _ < restartTime }
      .distinct.sorted(Time.ordering)
    logInfo("Batches to reschedule (" + timesToReschedule.length + " batches): " +
      timesToReschedule.mkString(", "))
    timesToReschedule.foreach { time =>
      // Allocate the related blocks when recovering from failure, because some blocks that were
      // added but not allocated, are dangling in the queue after recovering, we have to allocate
      // those blocks to the next batch, which is the batch they were supposed to go.
      // 当从失败中恢复时分配相关的块，因为一些添加但未分配的块在恢复后挂在队列中，我们必须将这些块分配给下一批，这是它们应该去的批。
      jobScheduler.receiverTracker.allocateBlocksToBatch(time) // allocate received blocks to batch   将接收到的块分配到批处理
      jobScheduler.submitJobSet(JobSet(time, graph.generateJobs(time)))
    }

    // Restart the timer
    timer.start(restartTime.milliseconds)
    logInfo("Restarted JobGenerator at " + restartTime)
  }

  /** Generate jobs and perform checkpointing for the given `time`.   生成作业并为给定的“时间”执行检查点*/
  private def generateJobs(time: Time) {
    // Checkpoint all RDDs marked for checkpointing to ensure their lineages are
    // truncated periodically. Otherwise, we may run into stack overflows (SPARK-6847).
    // 检查点所有标记为检查点的rdd，以确保它们的血统定期被截断。否则，我们可能会遇到堆栈溢出
    ssc.sparkContext.setLocalProperty(RDD.CHECKPOINT_ALL_MARKED_ANCESTORS, "true")
    Try {
      jobScheduler.receiverTracker.allocateBlocksToBatch(time) // allocate received blocks to batch  将接收到的块分配到批处理
      graph.generateJobs(time) // generate jobs using allocated block  使用分配的块生成作业
    } match {
      case Success(jobs) =>
        val streamIdToInputInfos = jobScheduler.inputInfoTracker.getInfo(time)
        jobScheduler.submitJobSet(JobSet(time, jobs, streamIdToInputInfos))
      case Failure(e) =>
        jobScheduler.reportError("Error generating jobs for time " + time, e)
        PythonDStream.stopStreamingContextIfPythonProcessIsDead(e)
    }
    eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = false))
  }

  /** Clear DStream metadata for the given `time`. */
  private def clearMetadata(time: Time) {
    ssc.graph.clearMetadata(time)

    // If checkpointing is enabled, then checkpoint,
    // else mark batch to be fully processed  如果启用了检查点，则检查点，否则将批处理标记为完全处理
    if (shouldCheckpoint) {
      eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = true))
    } else {
      // If checkpointing is not enabled, then delete metadata information about
      // received blocks (block data not saved in any case). Otherwise, wait for
      // checkpointing of this batch to complete.
      // 如果未启用检查点，则删除关于接收块的元数据信息(在任何情况下都不保存块数据)。否则，等待该批的检查点完成。
      val maxRememberDuration = graph.getMaxInputStreamRememberDuration()
      jobScheduler.receiverTracker.cleanupOldBlocksAndBatches(time - maxRememberDuration)
      jobScheduler.inputInfoTracker.cleanup(time - maxRememberDuration)
      markBatchFullyProcessed(time)
    }
  }

  /** Clear DStream checkpoint data for the given `time`. 清除给定“时间”的DStream检查点数据 */
  private def clearCheckpointData(time: Time) {
    ssc.graph.clearCheckpointData(time)

    // All the checkpoint information about which batches have been processed, etc have
    // been saved to checkpoints, so its safe to delete block metadata and data WAL files
    // 已处理批次等的所有检查点信息都已保存到检查点，因此删除块元数据和数据WAL文件是安全的
    val maxRememberDuration = graph.getMaxInputStreamRememberDuration()
    jobScheduler.receiverTracker.cleanupOldBlocksAndBatches(time - maxRememberDuration)
    jobScheduler.inputInfoTracker.cleanup(time - maxRememberDuration)
    markBatchFullyProcessed(time)
  }

  /** Perform checkpoint for the given `time`. */
  private def doCheckpoint(time: Time, clearCheckpointDataLater: Boolean) {
    if (shouldCheckpoint && (time - graph.zeroTime).isMultipleOf(ssc.checkpointDuration)) {
      logInfo("Checkpointing graph for time " + time)
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
