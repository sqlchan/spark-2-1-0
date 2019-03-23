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

/** 该类将作业安排在Spark上运行。它使用JobGenerator生成作业，并使用线程池运行它们 */
private[streaming]
class JobScheduler(val ssc: StreamingContext) extends Logging {

  private val jobSets: java.util.Map[Time, JobSet] = new ConcurrentHashMap[Time, JobSet]
  private val numConcurrentJobs = ssc.conf.getInt("spark.streaming.concurrentJobs", 1)
  private val jobExecutor =
    ThreadUtils.newDaemonFixedThreadPool(numConcurrentJobs, "streaming-job-executor")
  private val jobGenerator = new JobGenerator(this)
  val clock = jobGenerator.clock

  var receiverTracker: ReceiverTracker = null

  private var eventLoop: EventLoop[JobSchedulerEvent] = null

  def start(): Unit = synchronized {

    eventLoop = new EventLoop[JobSchedulerEvent]("JobScheduler") {
      override protected def onReceive(event: JobSchedulerEvent): Unit = processEvent(event)
    }
    eventLoop.start()

    // 添加 速度控制器 rateController
    ssc.addStreamingListener(rateController)

    receiverTracker = new ReceiverTracker(ssc)  // 这个类管理ReceiverInputDStreams的接收者的执行。
    inputInfoTracker = new InputInfoTracker(ssc)  // 该类管理所有输入流及其输入数据统计信息。这些信息将通过StreamingListener公开，以便进行监视。

    // 启动 receiverTracker 和 jobGenerator
    receiverTracker.start()   // 启动端点和接收器执行线程
    jobGenerator.start()      // 初始化定时器的开启时间、启动DstreamGraph、启动定时器
    executorAllocationManager.foreach(_.start())
  }

  // 提交作业
  def submitJobSet(jobSet: JobSet) {
      jobSets.put(jobSet.time, jobSet)
      // 遍历jobset里的所有作业，然后通过jobexecutor这个线程池把所有的作业进行提交
      // jobhandle做两类事：作业运行前后分别发送jobsend消息和jobcompleted消息给jobschedule；进行作业的运行
      jobSet.jobs.foreach(job => jobExecutor.execute(new JobHandler(job)))
  }

  def getPendingTimes(): Seq[Time] = {
    jobSets.asScala.keys.toSeq
  }

  private def processEvent(event: JobSchedulerEvent) {

      event match {
        case JobStarted(job, startTime) => handleJobStart(job, startTime)
        case JobCompleted(job, completedTime) => handleJobCompletion(job, completedTime)
        case ErrorReported(m, e) => handleError(m, e)
      }
  }

  private def handleJobStart(job: Job, startTime: Long) {
    val jobSet = jobSets.get(job.time)
    jobSet.handleJobStart(job)
    job.setStartTime(startTime)
  }

  private def handleJobCompletion(job: Job, completedTime: Long) {
    val jobSet = jobSets.get(job.time)
    jobSet.handleJobCompletion(job)
    job.setEndTime(completedTime)
    if (jobSet.hasCompleted) {
      jobSets.remove(jobSet.time)
      jobGenerator.onBatchCompletion(jobSet.time)
    }
  }

  private class JobHandler(job: Job) extends Runnable with Logging {
    def run() {
      try {
        var _eventLoop = eventLoop
        if (_eventLoop != null) {
          _eventLoop.post(JobStarted(job, clock.getTimeMillis()))
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
      }
    }
  }
}

