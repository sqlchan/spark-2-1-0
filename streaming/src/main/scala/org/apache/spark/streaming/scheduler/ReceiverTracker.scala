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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.mutable.HashMap
import scala.concurrent.ExecutionContext
import scala.language.existentials
import scala.util.{Failure, Success}

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc._
import org.apache.spark.scheduler.{ExecutorCacheTaskLocation, TaskLocation}
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.receiver._
import org.apache.spark.streaming.util.WriteAheadLogUtils
import org.apache.spark.util.{SerializableConfiguration, ThreadUtils, Utils}


/** Enumeration to identify current state of a Receiver
  * 枚举以标识接收器的当前状态*/
private[streaming] object ReceiverState extends Enumeration {
  type ReceiverState = Value
  val INACTIVE, SCHEDULED, ACTIVE = Value
}

/**
 * Messages used by the NetworkReceiver and the ReceiverTracker to communicate
 * with each other.
  * 由网络接收方和接收方跟踪方用来相互通信的消息。
 */
private[streaming] sealed trait ReceiverTrackerMessage
private[streaming] case class RegisterReceiver(
    streamId: Int,
    typ: String,
    host: String,
    executorId: String,
    receiverEndpoint: RpcEndpointRef
  ) extends ReceiverTrackerMessage
private[streaming] case class AddBlock(receivedBlockInfo: ReceivedBlockInfo)
  extends ReceiverTrackerMessage
private[streaming] case class ReportError(streamId: Int, message: String, error: String)
private[streaming] case class DeregisterReceiver(streamId: Int, msg: String, error: String)
  extends ReceiverTrackerMessage

/**
 * Messages used by the driver and ReceiverTrackerEndpoint to communicate locally.
  * 驱动程序和ReceiverTrackerEndpoint用于本地通信的消息。
 */
private[streaming] sealed trait ReceiverTrackerLocalMessage

/**
 * This message will trigger ReceiverTrackerEndpoint to restart a Spark job for the receiver.
  * 此消息将触发ReceiverTrackerEndpoint重新启动接收方的Spark作业。
 */
private[streaming] case class RestartReceiver(receiver: Receiver[_])
  extends ReceiverTrackerLocalMessage

/**
 * This message is sent to ReceiverTrackerEndpoint when we start to launch Spark jobs for receivers
 * at the first time.
  * 当我们第一次开始为接收者启动Spark作业时，此消息被发送到ReceiverTrackerEndpoint。
 */
private[streaming] case class StartAllReceivers(receiver: Seq[Receiver[_]])
  extends ReceiverTrackerLocalMessage

/**
 * This message will trigger ReceiverTrackerEndpoint to send stop signals to all registered
 * receivers.
  * 此消息将触发ReceiverTrackerEndpoint向所有注册的接收方发送停止信号。
 */
private[streaming] case object StopAllReceivers extends ReceiverTrackerLocalMessage

/**
 * A message used by ReceiverTracker to ask all receiver's ids still stored in
 * ReceiverTrackerEndpoint.
  * ReceiverTracker使用的一条消息，询问所有仍然存储在ReceiverTrackerEndpoint中的接收者的id。
 */
private[streaming] case object AllReceiverIds extends ReceiverTrackerLocalMessage

private[streaming] case class UpdateReceiverRateLimit(streamUID: Int, newRate: Long)
  extends ReceiverTrackerLocalMessage

private[streaming] case object GetAllReceiverInfo extends ReceiverTrackerLocalMessage

/**
 * This class manages the execution of the receivers of ReceiverInputDStreams. Instance of
 * this class must be created after all input streams have been added and StreamingContext.start()
 * has been called because it needs the final set of input streams at the time of instantiation.
  * 这个类管理ReceiverInputDStreams的接收者的执行。
  * 必须在添加了所有输入流并调用StreamingContext.start()之后创建该类的实例，
  * 因为它在实例化时需要最后一组输入流。
 *
 * @param skipReceiverLaunch Do not launch the receiver. This is useful for testing.
 */
private[streaming]
class ReceiverTracker(ssc: StreamingContext, skipReceiverLaunch: Boolean = false) extends Logging {

  private val receiverInputStreams = ssc.graph.getReceiverInputStreams()
  private val receiverInputStreamIds = receiverInputStreams.map { _.id }
  private val receivedBlockTracker = new ReceivedBlockTracker(
    ssc.sparkContext.conf,
    ssc.sparkContext.hadoopConfiguration,
    receiverInputStreamIds,
    ssc.scheduler.clock,
    ssc.isCheckpointPresent,
    Option(ssc.checkpointDir)
  )
  private val listenerBus = ssc.scheduler.listenerBus

  /** Enumeration to identify current state of the ReceiverTracker
    * 枚举以标识ReceiverTracker的当前状态*/
  object TrackerState extends Enumeration {
    type TrackerState = Value
    val Initialized, Started, Stopping, Stopped = Value
  }
  import TrackerState._

  /** State of the tracker. Protected by "trackerStateLock"  跟踪器的状态。保护“trackerStateLock”*/
  @volatile private var trackerState = Initialized

  // endpoint is created when generator starts. 端点是在生成器启动时创建的。
  // This not being null means the tracker has been started and not stopped  这不是null意味着跟踪器已经启动，而不是停止
  private var endpoint: RpcEndpointRef = null

  private val schedulingPolicy = new ReceiverSchedulingPolicy()   // 调度策略

  // Track the active receiver job number. When a receiver job exits ultimately, countDown will
  // be called.  跟踪活动接收器作业编号。当接收方作业最终退出时，将调用倒计时。
  private val receiverJobExitLatch = new CountDownLatch(receiverInputStreams.length)

  /**
   * Track all receivers' information. The key is the receiver id, the value is the receiver info.
    * 跟踪所有接收方的信息。键是接收者id，值是接收者信息。
   * It's only accessed in ReceiverTrackerEndpoint. 它只能在ReceiverTrackerEndpoint中访问。
   */
  private val receiverTrackingInfos = new HashMap[Int, ReceiverTrackingInfo]

  /**
   * Store all preferred locations for all receivers. We need this information to schedule
   * receivers. It's only accessed in ReceiverTrackerEndpoint.
    * 为所有接收器存储所有首选位置。我们需要这些信息来安排接收方。它只能在ReceiverTrackerEndpoint中访问。
   */
  private val receiverPreferredLocations = new HashMap[Int, Option[String]]

  /** Start the endpoint and receiver execution thread.  启动端点和接收器执行线程。*/
  def start(): Unit = synchronized {
    if (isTrackerStarted) {
      throw new SparkException("ReceiverTracker already started")
    }

    if (!receiverInputStreams.isEmpty) {
      endpoint = ssc.env.rpcEnv.setupEndpoint(
        "ReceiverTracker", new ReceiverTrackerEndpoint(ssc.env.rpcEnv))
      if (!skipReceiverLaunch) launchReceivers()
      logInfo("ReceiverTracker started")
      trackerState = Started
    }
  }

  /** Stop the receiver execution thread. 停止接收方执行线程。*/
  def stop(graceful: Boolean): Unit = synchronized {
    if (isTrackerStarted) {
      // First, stop the receivers  首先，停止接收器
      trackerState = Stopping
      if (!skipReceiverLaunch) {
        // Send the stop signal to all the receivers  把停止信号发送给所有的接收器
        endpoint.askWithRetry[Boolean](StopAllReceivers)

        // Wait for the Spark job that runs the receivers to be over  等待运行接收器的Spark作业结束
        // That is, for the receivers to quit gracefully. 也就是说，让接收者优雅地退出。
        receiverJobExitLatch.await(10, TimeUnit.SECONDS)

        if (graceful) {
          logInfo("Waiting for receiver job to terminate gracefully")
          receiverJobExitLatch.await()
          logInfo("Waited for receiver job to terminate gracefully")
        }

        // Check if all the receivers have been deregistered or not  检查所有接收方是否已注销注册
        val receivers = endpoint.askWithRetry[Seq[Int]](AllReceiverIds)
        if (receivers.nonEmpty) {
          logWarning("Not all of the receivers have deregistered, " + receivers)
        } else {
          logInfo("All of the receivers have deregistered successfully")
        }
      }

      // Finally, stop the endpoint 最后，停止端点
      ssc.env.rpcEnv.stop(endpoint)
      endpoint = null
      receivedBlockTracker.stop()
      logInfo("ReceiverTracker stopped")
      trackerState = Stopped
    } else if (isTrackerInitialized) {
      trackerState = Stopping
      // `ReceivedBlockTracker` is open when this instance is created. We should
      // close this even if this `ReceiverTracker` is not started.
      // “ReceivedBlockTracker”在创建这个实例时是打开的。即使这个“ReceiverTracker”没有启动，我们也应该关闭它。
      receivedBlockTracker.stop()
      logInfo("ReceiverTracker stopped")
      trackerState = Stopped
    }
  }

  /** Allocate all unallocated blocks to the given batch. 将所有未分配的块分配给给定批处理。*/
  def allocateBlocksToBatch(batchTime: Time): Unit = {
    if (receiverInputStreams.nonEmpty) {
      receivedBlockTracker.allocateBlocksToBatch(batchTime)
    }
  }

  /** Get the blocks for the given batch and all input streams. 获取给定批处理和所有输入流的块。*/
  def getBlocksOfBatch(batchTime: Time): Map[Int, Seq[ReceivedBlockInfo]] = {
    receivedBlockTracker.getBlocksOfBatch(batchTime)
  }

  /** Get the blocks allocated to the given batch and stream. 获取分配给给定批处理和流的块。*/
  def getBlocksOfBatchAndStream(batchTime: Time, streamId: Int): Seq[ReceivedBlockInfo] = {
    receivedBlockTracker.getBlocksOfBatchAndStream(batchTime, streamId)
  }

  /**
   * Clean up the data and metadata of blocks and batches that are strictly
   * older than the threshold time. Note that this does not
    * 清除严格超过阈值时间的块和批的数据和元数据。请注意，这不是
   */
  def cleanupOldBlocksAndBatches(cleanupThreshTime: Time) {
    // Clean up old block and batch metadata
    receivedBlockTracker.cleanupOldBatches(cleanupThreshTime, waitForCompletion = false)

    // Signal the receivers to delete old block data  向接收端发出删除旧块数据的信号
    if (WriteAheadLogUtils.enableReceiverLog(ssc.conf)) {
      logInfo(s"Cleanup old received batch data: $cleanupThreshTime")
      synchronized {
        if (isTrackerStarted) {
          endpoint.send(CleanupOldBlocks(cleanupThreshTime))
        }
      }
    }
  }

  /**
   * Get the executors allocated to each receiver.  获取分配给每个接收方的执行器。
   * @return a map containing receiver ids to optional executor ids. 包含接收者id到可选执行器id的映射。
   */
  def allocatedExecutors(): Map[Int, Option[String]] = synchronized {
    if (isTrackerStarted) {
      endpoint.askWithRetry[Map[Int, ReceiverTrackingInfo]](GetAllReceiverInfo).mapValues {
        _.runningExecutor.map {
          _.executorId
        }
      }
    } else {
      Map.empty
    }
  }

  def numReceivers(): Int = {
    receiverInputStreams.size
  }

  /** Register a receiver  注册一个接收器*/
  private def registerReceiver(
      streamId: Int,
      typ: String,
      host: String,
      executorId: String,
      receiverEndpoint: RpcEndpointRef,
      senderAddress: RpcAddress
    ): Boolean = {
    if (!receiverInputStreamIds.contains(streamId)) {
      throw new SparkException("Register received for unexpected id " + streamId)
    }

    if (isTrackerStopping || isTrackerStopped) {
      return false
    }

    val scheduledLocations = receiverTrackingInfos(streamId).scheduledLocations
    val acceptableExecutors = if (scheduledLocations.nonEmpty) {
        // This receiver is registering and it's scheduled by
        // ReceiverSchedulingPolicy.scheduleReceivers. So use "scheduledLocations" to check it.
      // 这个接收器正在注册，它由receiverscheduling policy . scheereceivers调度。所以使用“scheduledLocations”来检查它。
        scheduledLocations.get
      } else {
        // This receiver is scheduled by "ReceiverSchedulingPolicy.rescheduleReceiver", so calling
        // "ReceiverSchedulingPolicy.rescheduleReceiver" again to check it.
        scheduleReceiver(streamId)
      }

    def isAcceptable: Boolean = acceptableExecutors.exists {
      case loc: ExecutorCacheTaskLocation => loc.executorId == executorId
      case loc: TaskLocation => loc.host == host
    }

    if (!isAcceptable) {
      // Refuse it since it's scheduled to a wrong executor  拒绝它，因为它被安排给了一个错误的执行人
      false
    } else {
      val name = s"${typ}-${streamId}"
      val receiverTrackingInfo = ReceiverTrackingInfo(
        streamId,
        ReceiverState.ACTIVE,
        scheduledLocations = None,
        runningExecutor = Some(ExecutorCacheTaskLocation(host, executorId)),
        name = Some(name),
        endpoint = Some(receiverEndpoint))
      receiverTrackingInfos.put(streamId, receiverTrackingInfo)
      listenerBus.post(StreamingListenerReceiverStarted(receiverTrackingInfo.toReceiverInfo))
      logInfo("Registered receiver for stream " + streamId + " from " + senderAddress)
      true
    }
  }

  /** Deregister a receiver  取消注册接收机*/
  private def deregisterReceiver(streamId: Int, message: String, error: String) {
    val lastErrorTime =
      if (error == null || error == "") -1 else ssc.scheduler.clock.getTimeMillis()
    val errorInfo = ReceiverErrorInfo(
      lastErrorMessage = message, lastError = error, lastErrorTime = lastErrorTime)
    val newReceiverTrackingInfo = receiverTrackingInfos.get(streamId) match {
      case Some(oldInfo) =>
        oldInfo.copy(state = ReceiverState.INACTIVE, errorInfo = Some(errorInfo))
      case None =>
        logWarning("No prior receiver info")
        ReceiverTrackingInfo(
          streamId, ReceiverState.INACTIVE, None, None, None, None, Some(errorInfo))
    }
    receiverTrackingInfos(streamId) = newReceiverTrackingInfo
    listenerBus.post(StreamingListenerReceiverStopped(newReceiverTrackingInfo.toReceiverInfo))
    val messageWithError = if (error != null && !error.isEmpty) {
      s"$message - $error"
    } else {
      s"$message"
    }
    logError(s"Deregistered receiver for stream $streamId: $messageWithError")
  }

  /** Update a receiver's maximum ingestion rate  更新接收器的最大摄入速率*/
  def sendRateUpdate(streamUID: Int, newRate: Long): Unit = synchronized {
    if (isTrackerStarted) {
      endpoint.send(UpdateReceiverRateLimit(streamUID, newRate))
    }
  }

  /** Add new blocks for the given stream  为给定的流添加新的块*/
  private def addBlock(receivedBlockInfo: ReceivedBlockInfo): Boolean = {
    receivedBlockTracker.addBlock(receivedBlockInfo)
  }

  /** Report error sent by a receiver 报告接收方发送的错误*/
  private def reportError(streamId: Int, message: String, error: String) {
    val newReceiverTrackingInfo = receiverTrackingInfos.get(streamId) match {
      case Some(oldInfo) =>
        val errorInfo = ReceiverErrorInfo(lastErrorMessage = message, lastError = error,
          lastErrorTime = oldInfo.errorInfo.map(_.lastErrorTime).getOrElse(-1L))
        oldInfo.copy(errorInfo = Some(errorInfo))
      case None =>
        logWarning("No prior receiver info")
        val errorInfo = ReceiverErrorInfo(lastErrorMessage = message, lastError = error,
          lastErrorTime = ssc.scheduler.clock.getTimeMillis())
        ReceiverTrackingInfo(
          streamId, ReceiverState.INACTIVE, None, None, None, None, Some(errorInfo))
    }

    receiverTrackingInfos(streamId) = newReceiverTrackingInfo
    listenerBus.post(StreamingListenerReceiverError(newReceiverTrackingInfo.toReceiverInfo))
    val messageWithError = if (error != null && !error.isEmpty) {
      s"$message - $error"
    } else {
      s"$message"
    }
    logWarning(s"Error reported by receiver for stream $streamId: $messageWithError")
  }

  private def scheduleReceiver(receiverId: Int): Seq[TaskLocation] = {
    val preferredLocation = receiverPreferredLocations.getOrElse(receiverId, None)
    val scheduledLocations = schedulingPolicy.rescheduleReceiver(
      receiverId, preferredLocation, receiverTrackingInfos, getExecutors)
    updateReceiverScheduledExecutors(receiverId, scheduledLocations)
    scheduledLocations
  }

  private def updateReceiverScheduledExecutors(
      receiverId: Int, scheduledLocations: Seq[TaskLocation]): Unit = {
    val newReceiverTrackingInfo = receiverTrackingInfos.get(receiverId) match {
      case Some(oldInfo) =>
        oldInfo.copy(state = ReceiverState.SCHEDULED,
          scheduledLocations = Some(scheduledLocations))
      case None =>
        ReceiverTrackingInfo(
          receiverId,
          ReceiverState.SCHEDULED,
          Some(scheduledLocations),
          runningExecutor = None)
    }
    receiverTrackingInfos.put(receiverId, newReceiverTrackingInfo)
  }

  /** Check if any blocks are left to be processed 检查是否还有要处理的块*/
  def hasUnallocatedBlocks: Boolean = {
    receivedBlockTracker.hasUnallocatedReceivedBlocks
  }

  /**
   * Get the list of executors excluding driver  获取不包括驱动程序的执行器列表
   */
  private def getExecutors: Seq[ExecutorCacheTaskLocation] = {
    if (ssc.sc.isLocal) {
      val blockManagerId = ssc.sparkContext.env.blockManager.blockManagerId
      Seq(ExecutorCacheTaskLocation(blockManagerId.host, blockManagerId.executorId))
    } else {
      ssc.sparkContext.env.blockManager.master.getMemoryStatus.filter { case (blockManagerId, _) =>
        blockManagerId.executorId != SparkContext.DRIVER_IDENTIFIER // Ignore the driver location
      }.map { case (blockManagerId, _) =>
        ExecutorCacheTaskLocation(blockManagerId.host, blockManagerId.executorId)
      }.toSeq
    }
  }

  /**
   * Run the dummy Spark job to ensure that all slaves have registered. This avoids all the
   * receivers to be scheduled on the same node.
    * 运行模拟的Spark作业，以确保所有从服务器都已注册。这避免了将所有接收器安排在同一个节点上。
   *
   * TODO Should poll the executor number and wait for executors according to
   * "spark.scheduler.minRegisteredResourcesRatio" and
   * "spark.scheduler.maxRegisteredResourcesWaitingTime" rather than running a dummy job.
    * TODO应该轮询执行程序号，并根据“spark.scheduler”等待执行程序。minRegisteredResourcesRatio”
    * 和“spark.scheduler。maxRegisteredResourcesWaitingTime“而不是运行一个虚拟作业”。
   */
  private def runDummySparkJob(): Unit = {
    if (!ssc.sparkContext.isLocal) {
      ssc.sparkContext.makeRDD(1 to 50, 50).map(x => (x, 1)).reduceByKey(_ + _, 20).collect()
    }
    assert(getExecutors.nonEmpty)
  }

  /**
   * Get the receivers from the ReceiverInputDStreams, distributes them to the
   * worker nodes as a parallel collection, and runs them.
    * 从ReceiverInputDStreams获取接收方，将它们作为并行集合分发到工作节点，并运行它们。
   */
  private def launchReceivers(): Unit = {
    // 获取所有inputDStream中定义的流数据接收器
    val receivers = receiverInputStreams.map { nis =>
      val rcvr = nis.getReceiver()
      rcvr.setReceiverId(nis.id)
      rcvr
    }

    runDummySparkJob()

    logInfo("Starting " + receivers.length + " receivers")
    // 向终端点发送分发并启动所有流数据接收器的消息
    endpoint.send(StartAllReceivers(receivers))
  }

  /** Check if tracker has been marked for initiated 检查跟踪器是否已标记为已启动*/
  private def isTrackerInitialized: Boolean = trackerState == Initialized

  /** Check if tracker has been marked for starting */
  private def isTrackerStarted: Boolean = trackerState == Started

  /** Check if tracker has been marked for stopping */
  private def isTrackerStopping: Boolean = trackerState == Stopping

  /** Check if tracker has been marked for stopped */
  private def isTrackerStopped: Boolean = trackerState == Stopped

  /** RpcEndpoint to receive messages from the receivers.  RpcEndpoint接收来自接收者的消息*/
  private class ReceiverTrackerEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint {

    private val walBatchingThreadPool = ExecutionContext.fromExecutorService(
      ThreadUtils.newDaemonCachedThreadPool("wal-batching-thread-pool"))

    @volatile private var active: Boolean = true

    override def receive: PartialFunction[Any, Unit] = {
      // Local messages
      case StartAllReceivers(receivers) =>
        //根据流数据接收器分发策略，匹配流数据接收器Receiver和executor
        val scheduledLocations = schedulingPolicy.scheduleReceivers(receivers, getExecutors)
        for (receiver <- receivers) {
          val executors = scheduledLocations(receiver.streamId)
          updateReceiverScheduledExecutors(receiver.streamId, executors)
          // 在hashmap中保存流数据接收器receiver的首选位置
          receiverPreferredLocations(receiver.streamId) = receiver.preferredLocation
          // 在指定的executor中启动流数据接收器receiver
          startReceiver(receiver, executors)
        }
      case RestartReceiver(receiver) =>
        // Old scheduled executors minus the ones that are not active any more  旧的计划执行器减去那些不再活动的执行器
        val oldScheduledExecutors = getStoredScheduledExecutors(receiver.streamId)
        val scheduledLocations = if (oldScheduledExecutors.nonEmpty) {
            // Try global scheduling again
            oldScheduledExecutors
          } else {
            val oldReceiverInfo = receiverTrackingInfos(receiver.streamId)
            // Clear "scheduledLocations" to indicate we are going to do local scheduling  清除“scheduledLocations”以表明我们将进行本地调度
            val newReceiverInfo = oldReceiverInfo.copy(
              state = ReceiverState.INACTIVE, scheduledLocations = None)
            receiverTrackingInfos(receiver.streamId) = newReceiverInfo
            schedulingPolicy.rescheduleReceiver(
              receiver.streamId,
              receiver.preferredLocation,
              receiverTrackingInfos,
              getExecutors)
          }
        // Assume there is one receiver restarting at one time, so we don't need to update
        // receiverTrackingInfos  假设有一个接收者在同一时间重新启动，因此我们不需要更新receiverTrackingInfos
        startReceiver(receiver, scheduledLocations)
      case c: CleanupOldBlocks =>
        receiverTrackingInfos.values.flatMap(_.endpoint).foreach(_.send(c))
      case UpdateReceiverRateLimit(streamUID, newRate) =>
        for (info <- receiverTrackingInfos.get(streamUID); eP <- info.endpoint) {
          eP.send(UpdateRateLimit(newRate))
        }
      // Remote messages  远程消息
      case ReportError(streamId, message, error) =>
        reportError(streamId, message, error)
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      // Remote messages
      case RegisterReceiver(streamId, typ, host, executorId, receiverEndpoint) =>
        val successful =
          registerReceiver(streamId, typ, host, executorId, receiverEndpoint, context.senderAddress)
        context.reply(successful)
      case AddBlock(receivedBlockInfo) =>
        if (WriteAheadLogUtils.isBatchingEnabled(ssc.conf, isDriver = true)) {
          walBatchingThreadPool.execute(new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              if (active) {
                context.reply(addBlock(receivedBlockInfo))
              } else {
                throw new IllegalStateException("ReceiverTracker RpcEndpoint shut down.")
              }
            }
          })
        } else {
          context.reply(addBlock(receivedBlockInfo))
        }
      case DeregisterReceiver(streamId, message, error) =>
        deregisterReceiver(streamId, message, error)
        context.reply(true)

      // Local messages
      case AllReceiverIds =>
        context.reply(receiverTrackingInfos.filter(_._2.state != ReceiverState.INACTIVE).keys.toSeq)
      case GetAllReceiverInfo =>
        context.reply(receiverTrackingInfos.toMap)
      case StopAllReceivers =>
        assert(isTrackerStopping || isTrackerStopped)
        stopReceivers()
        context.reply(true)
    }

    /**
     * Return the stored scheduled executors that are still alive.  返回仍然存在的已存储的计划执行程序。
     */
    private def getStoredScheduledExecutors(receiverId: Int): Seq[TaskLocation] = {
      if (receiverTrackingInfos.contains(receiverId)) {
        val scheduledLocations = receiverTrackingInfos(receiverId).scheduledLocations
        if (scheduledLocations.nonEmpty) {
          val executors = getExecutors.toSet
          // Only return the alive executors
          scheduledLocations.get.filter {
            case loc: ExecutorCacheTaskLocation => executors(loc)
            case loc: TaskLocation => true
          }
        } else {
          Nil
        }
      } else {
        Nil
      }
    }

    /**
     * Start a receiver along with its scheduled executors  启动接收器及其计划的执行器
     */
    private def startReceiver(
        receiver: Receiver[_],
        scheduledLocations: Seq[TaskLocation]): Unit = {
      def shouldStartReceiver: Boolean = {
        // It's okay to start when trackerState is Initialized or Started  trackerState初始化或启动时可以启动
        !(isTrackerStopping || isTrackerStopped)
      }

      val receiverId = receiver.streamId
      if (!shouldStartReceiver) {
        onReceiverJobFinish(receiverId)
        return
      }

      val checkpointDirOption = Option(ssc.checkpointDir)
      val serializableHadoopConf =
        new SerializableConfiguration(ssc.sparkContext.hadoopConfiguration)

      // Function to start the receiver on the worker node 函数启动工作节点上的接收器
      val startReceiverFunc: Iterator[Receiver[_]] => Unit =
        (iterator: Iterator[Receiver[_]]) => {
          if (!iterator.hasNext) {
            throw new SparkException(
              "Could not start receiver as object not found.")
          }
          if (TaskContext.get().attemptNumber() == 0) {
            val receiver = iterator.next()
            assert(iterator.hasNext == false)
            //创建流数据接收管理器，用于监督该数据接收器
            val supervisor = new ReceiverSupervisorImpl(
              receiver, SparkEnv.get, serializableHadoopConf.value, checkpointDirOption)
            supervisor.start()  //方法中调用自身的onstart和startreceiver方法；onstart中启动blockgenerator；在startreceive中完成流数据接收器的注册启动
            supervisor.awaitTermination()
          } else {
            // It's restarted by TaskScheduler, but we want to reschedule it again. So exit it.
            // 它由TaskScheduler重新启动，但我们希望重新调度它。所以退出。
          }
        }

      // Create the RDD using the scheduledLocations to run the receiver in a Spark job
      // 使用scheduledLocations创建RDD，以在Spark作业中运行接收器
      val receiverRDD: RDD[Receiver[_]] =
        if (scheduledLocations.isEmpty) {
          ssc.sc.makeRDD(Seq(receiver), 1)
        } else {
          val preferredLocations = scheduledLocations.map(_.toString).distinct
          ssc.sc.makeRDD(Seq(receiver -> preferredLocations))
        }
      receiverRDD.setName(s"Receiver $receiverId")
      ssc.sparkContext.setJobDescription(s"Streaming job running receiver $receiverId")
      ssc.sparkContext.setCallSite(Option(ssc.getStartSite()).getOrElse(Utils.getCallSite()))

      val future = ssc.sparkContext.submitJob[Receiver[_], Unit, Unit](
        receiverRDD, startReceiverFunc, Seq(0), (_, _) => Unit, ())
      // We will keep restarting the receiver job until ReceiverTracker is stopped
      // 我们将继续重新启动接收方作业，直到ReceiverTracker停止
      future.onComplete {
        case Success(_) =>
          if (!shouldStartReceiver) {
            onReceiverJobFinish(receiverId)
          } else {
            logInfo(s"Restarting Receiver $receiverId")
            self.send(RestartReceiver(receiver))
          }
        case Failure(e) =>
          if (!shouldStartReceiver) {
            onReceiverJobFinish(receiverId)
          } else {
            logError("Receiver has been stopped. Try to restart it.", e)
            logInfo(s"Restarting Receiver $receiverId")
            self.send(RestartReceiver(receiver))
          }
      }(ThreadUtils.sameThread)
      logInfo(s"Receiver ${receiver.streamId} started")
    }

    override def onStop(): Unit = {
      active = false
      walBatchingThreadPool.shutdown()
    }

    /**
     * Call when a receiver is terminated. It means we won't restart its Spark job.
      * 接收方终止时调用。这意味着我们不会重启它的Spark任务
     */
    private def onReceiverJobFinish(receiverId: Int): Unit = {
      receiverJobExitLatch.countDown()
      receiverTrackingInfos.remove(receiverId).foreach { receiverTrackingInfo =>
        if (receiverTrackingInfo.state == ReceiverState.ACTIVE) {
          logWarning(s"Receiver $receiverId exited but didn't deregister")
        }
      }
    }

    /** Send stop signal to the receivers.  向接收端发送停止信号。*/
    private def stopReceivers() {
      receiverTrackingInfos.values.flatMap(_.endpoint).foreach { _.send(StopReceiver) }
      logInfo("Sent stop signal to all " + receiverTrackingInfos.size + " receivers")
    }
  }

}
