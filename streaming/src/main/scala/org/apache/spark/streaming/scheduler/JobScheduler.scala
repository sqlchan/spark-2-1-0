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

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.Failure

import org.apache.commons.lang3.SerializationUtils

import org.apache.spark.ExecutorAllocationClient
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.streaming._
import org.apache.spark.streaming.api.python.PythonDStream
import org.apache.spark.streaming.ui.UIUtils
import org.apache.spark.util.{EventLoop, ThreadUtils}


private[scheduler] sealed trait JobSchedulerEvent
private[scheduler] case class JobStarted(job: Job, startTime: Long) extends JobSchedulerEvent
private[scheduler] case class JobCompleted(job: Job, completedTime: Long) extends JobSchedulerEvent
private[scheduler] case class ErrorReported(msg: String, e: Throwable) extends JobSchedulerEvent

/**
 * This class schedules jobs to be run on Spark. It uses the JobGenerator to generate
 * the jobs and runs them using a thread pool.
  * 该类将作业安排在Spark上运行。它使用JobGenerator生成作业，并使用线程池运行它们
 */
private[streaming]
class JobScheduler(val ssc: StreamingContext) extends Logging {

  // Use of ConcurrentHashMap.keySet later causes an odd runtime problem due to Java 7/8 diff
  // ConcurrentHashMap的使用。由于Java 7/8差异，keySet稍后会导致奇怪的运行时问题
  // https://gist.github.com/AlainODea/1375759b8720a3f9f094
  private val jobSets: java.util.Map[Time, JobSet] = new ConcurrentHashMap[Time, JobSet]
  private val numConcurrentJobs = ssc.conf.getInt("spark.streaming.concurrentJobs", 1)
  private val jobExecutor =
    ThreadUtils.newDaemonFixedThreadPool(numConcurrentJobs, "streaming-job-executor")
  private val jobGenerator = new JobGenerator(this)
  val clock = jobGenerator.clock
  val listenerBus = new StreamingListenerBus(ssc.sparkContext.listenerBus)

  // These two are created only when scheduler starts.
  // eventLoop not being null means the scheduler has been started and not stopped
  // 只有在调度程序启动时才创建这两个。eventLoop不为空意味着调度程序已经启动而没有停止
  var receiverTracker: ReceiverTracker = null
  // A tracker to track all the input stream information as well as processed record number
  // 一种跟踪器，用于跟踪所有输入流信息以及处理后的记录号
  var inputInfoTracker: InputInfoTracker = null

  private var executorAllocationManager: Option[ExecutorAllocationManager] = None

  private var eventLoop: EventLoop[JobSchedulerEvent] = null

  def start(): Unit = synchronized {
    if (eventLoop != null) return // scheduler has already been started

    logDebug("Starting JobScheduler")
    eventLoop = new EventLoop[JobSchedulerEvent]("JobScheduler") {
      override protected def onReceive(event: JobSchedulerEvent): Unit = processEvent(event)

      override protected def onError(e: Throwable): Unit = reportError("Error in job scheduler", e)
    }
    eventLoop.start()

    // attach rate controllers of input streams to receive batch completion updates
    // 附加输入流的速率控制器以接收批处理完成更新
    // 添加 速度控制器 rateController
    for {
      inputDStream <- ssc.graph.getInputStreams
      rateController <- inputDStream.rateController
    } ssc.addStreamingListener(rateController)

    //启动spark streaming消息总线
    listenerBus.start()
    receiverTracker = new ReceiverTracker(ssc)  // 这个类管理ReceiverInputDStreams的接收者的执行。
    inputInfoTracker = new InputInfoTracker(ssc)  // 该类管理所有输入流及其输入数据统计信息。这些信息将通过StreamingListener公开，以便进行监视。

    val executorAllocClient: ExecutorAllocationClient = ssc.sparkContext.schedulerBackend match {
      case b: ExecutorAllocationClient => b.asInstanceOf[ExecutorAllocationClient]
      case _ => null
    }

    executorAllocationManager = ExecutorAllocationManager.createIfEnabled(
      executorAllocClient,
      receiverTracker,
      ssc.conf,
      ssc.graph.batchDuration.milliseconds,
      clock)
    executorAllocationManager.foreach(ssc.addStreamingListener)
    // 启动 receiverTracker 和 jobGenerator
    receiverTracker.start()   // 启动端点和接收器执行线程
    jobGenerator.start()      // 初始化定时器的开启时间、启动DstreamGraph、启动定时器
    executorAllocationManager.foreach(_.start())
    logInfo("Started JobScheduler")
  }

  def stop(processAllReceivedData: Boolean): Unit = synchronized {
    if (eventLoop == null) return // scheduler has already been stopped
    logDebug("Stopping JobScheduler")

    if (receiverTracker != null) {
      // First, stop receiving
      receiverTracker.stop(processAllReceivedData)
    }

    if (executorAllocationManager != null) {
      executorAllocationManager.foreach(_.stop())
    }

    // Second, stop generating jobs. If it has to process all received data,
    // then this will wait for all the processing through JobScheduler to be over.
    // 第二，停止创造就业机会。如果它必须处理所有接收到的数据，那么它将等待JobScheduler完成所有处理
    jobGenerator.stop(processAllReceivedData)

    // Stop the executor for receiving new jobs
    logDebug("Stopping job executor")
    jobExecutor.shutdown()

    // Wait for the queued jobs to complete if indicated  如果指定，等待排队作业完成
    val terminated = if (processAllReceivedData) {
      jobExecutor.awaitTermination(1, TimeUnit.HOURS)  // just a very large period of time
    } else {
      jobExecutor.awaitTermination(2, TimeUnit.SECONDS)
    }
    if (!terminated) {
      jobExecutor.shutdownNow()
    }
    logDebug("Stopped job executor")

    // Stop everything else
    listenerBus.stop()
    eventLoop.stop()
    eventLoop = null
    logInfo("Stopped JobScheduler")
  }

  // 提交作业
  def submitJobSet(jobSet: JobSet) {
    if (jobSet.jobs.isEmpty) {
      logInfo("No jobs added for time " + jobSet.time)
    } else {
      listenerBus.post(StreamingListenerBatchSubmitted(jobSet.toBatchInfo))
      jobSets.put(jobSet.time, jobSet)
      // 遍历jobset里的所有作业，然后通过jobexecutor这个线程池把所有的作业进行提交
      // jobhandle做两类事：作业运行前后分别发送jobsend消息和jobcompleted消息给jobschedule；进行作业的运行
      jobSet.jobs.foreach(job => jobExecutor.execute(new JobHandler(job)))
      logInfo("Added jobs for time " + jobSet.time)
    }
  }

  def getPendingTimes(): Seq[Time] = {
    jobSets.asScala.keys.toSeq
  }

  def reportError(msg: String, e: Throwable) {
    eventLoop.post(ErrorReported(msg, e))
  }

  def isStarted(): Boolean = synchronized {
    eventLoop != null
  }

  private def processEvent(event: JobSchedulerEvent) {
    try {
      event match {
        case JobStarted(job, startTime) => handleJobStart(job, startTime)
        case JobCompleted(job, completedTime) => handleJobCompletion(job, completedTime)
        case ErrorReported(m, e) => handleError(m, e)
      }
    } catch {
      case e: Throwable =>
        reportError("Error in job scheduler", e)
    }
  }

  private def handleJobStart(job: Job, startTime: Long) {
    val jobSet = jobSets.get(job.time)
    val isFirstJobOfJobSet = !jobSet.hasStarted
    jobSet.handleJobStart(job)
    if (isFirstJobOfJobSet) {
      // "StreamingListenerBatchStarted" should be posted after calling "handleJobStart" to get the
      // correct "jobSet.processingStartTime".
      // “StreamingListenerBatchStarted”应该在调用“handleJobStart”之后发布，以获得正确的“jobSet.processingStartTime”。
      listenerBus.post(StreamingListenerBatchStarted(jobSet.toBatchInfo))
    }
    job.setStartTime(startTime)
    listenerBus.post(StreamingListenerOutputOperationStarted(job.toOutputOperationInfo))
    logInfo("Starting job " + job.id + " from job set of time " + jobSet.time)
  }

  private def handleJobCompletion(job: Job, completedTime: Long) {
    val jobSet = jobSets.get(job.time)
    jobSet.handleJobCompletion(job)
    job.setEndTime(completedTime)
    listenerBus.post(StreamingListenerOutputOperationCompleted(job.toOutputOperationInfo))
    logInfo("Finished job " + job.id + " from job set of time " + jobSet.time)
    if (jobSet.hasCompleted) {
      jobSets.remove(jobSet.time)
      jobGenerator.onBatchCompletion(jobSet.time)
      logInfo("Total delay: %.3f s for time %s (execution: %.3f s)".format(
        jobSet.totalDelay / 1000.0, jobSet.time.toString,
        jobSet.processingDelay / 1000.0
      ))
      listenerBus.post(StreamingListenerBatchCompleted(jobSet.toBatchInfo))
    }
    job.result match {
      case Failure(e) =>
        reportError("Error running job " + job, e)
      case _ =>
    }
  }

  private def handleError(msg: String, e: Throwable) {
    logError(msg, e)
    ssc.waiter.notifyError(e)
    PythonDStream.stopStreamingContextIfPythonProcessIsDead(e)
  }

  private class JobHandler(job: Job) extends Runnable with Logging {
    import JobScheduler._

    def run() {
      val oldProps = ssc.sparkContext.getLocalProperties
      try {
        ssc.sparkContext.setLocalProperties(SerializationUtils.clone(ssc.savedProperties.get()))
        val formattedTime = UIUtils.formatBatchTime(
          job.time.milliseconds, ssc.graph.batchDuration.milliseconds, showYYYYMMSS = false)
        val batchUrl = s"/streaming/batch/?id=${job.time.milliseconds}"
        val batchLinkText = s"[output operation ${job.outputOpId}, batch time ${formattedTime}]"

        ssc.sc.setJobDescription(
          s"""Streaming job from <a href="$batchUrl">$batchLinkText</a>""")
        ssc.sc.setLocalProperty(BATCH_TIME_PROPERTY_KEY, job.time.milliseconds.toString)
        ssc.sc.setLocalProperty(OUTPUT_OP_ID_PROPERTY_KEY, job.outputOpId.toString)
        // Checkpoint all RDDs marked for checkpointing to ensure their lineages are
        // truncated periodically. Otherwise, we may run into stack overflows (SPARK-6847).
        ssc.sparkContext.setLocalProperty(RDD.CHECKPOINT_ALL_MARKED_ANCESTORS, "true")

        // We need to assign `eventLoop` to a temp variable. Otherwise, because
        // `JobScheduler.stop(false)` may set `eventLoop` to null when this method is running, then
        // it's possible that when `post` is called, `eventLoop` happens to null.
        var _eventLoop = eventLoop
        if (_eventLoop != null) {
          _eventLoop.post(JobStarted(job, clock.getTimeMillis()))
          // Disable checks for existing output directories in jobs launched by the streaming
          // scheduler, since we may need to write output to an existing directory during checkpoint
          // recovery; see SPARK-4835 for more details.
          // 禁用流调度器启动的作业中现有输出目录的检查，因为在检查点恢复期间可能需要将输出写入现有目录;有关详细信息，请参见SPARK-4835。
          PairRDDFunctions.disableOutputSpecValidation.withValue(true) {
            job.run()
          }
          _eventLoop = eventLoop
          if (_eventLoop != null) {
            _eventLoop.post(JobCompleted(job, clock.getTimeMillis()))
          }
        } else {
          // JobScheduler has been stopped.
        }
      } finally {
        ssc.sparkContext.setLocalProperties(oldProps)
      }
    }
  }
}

private[streaming] object JobScheduler {
  val BATCH_TIME_PROPERTY_KEY = "spark.streaming.internal.batchTime"
  val OUTPUT_OP_ID_PROPERTY_KEY = "spark.streaming.internal.outputOpId"
}
