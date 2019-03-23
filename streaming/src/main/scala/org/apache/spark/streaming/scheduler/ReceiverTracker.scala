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

/** 由网络接收方和接收方跟踪方用来相互通信的消息。 */
private[streaming] sealed trait ReceiverTrackerMessage
private[streaming] case class RegisterReceiver() extends ReceiverTrackerMessage
private[streaming] case class AddBlock(receivedBlockInfo: ReceivedBlockInfo) extends ReceiverTrackerMessage
private[streaming] case class DeregisterReceiver(streamId: Int, msg: String, error: String) extends ReceiverTrackerMessage

/*** 驱动程序和ReceiverTrackerEndpoint用于本地通信的消息。*/
private[streaming] sealed trait ReceiverTrackerLocalMessage
private[streaming] case class RestartReceiver(receiver: Receiver[_]) extends ReceiverTrackerLocalMessage
private[streaming] case class StartAllReceivers(receiver: Seq[Receiver[_]]) extends ReceiverTrackerLocalMessage
private[streaming] case object StopAllReceivers extends ReceiverTrackerLocalMessage
private[streaming] case object AllReceiverIds extends ReceiverTrackerLocalMessage
private[streaming] case class UpdateReceiverRateLimit(streamUID: Int, newRate: Long) extends ReceiverTrackerLocalMessage
private[streaming] case object GetAllReceiverInfo extends ReceiverTrackerLocalMessage

/** 这个类管理ReceiverInputDStreams的接收者的执行。
  * 必须在添加了所有输入流并调用StreamingContext.start()之后创建该类的实例，
  * 因为它在实例化时需要最后一组输入流。 */
private[streaming]
class ReceiverTracker(ssc: StreamingContext, skipReceiverLaunch: Boolean = false) extends Logging {

  private val receiverInputStreams = ssc.graph.getReceiverInputStreams()
  private val receiverInputStreamIds = receiverInputStreams.map { _.id }
  private val receivedBlockTracker = new ReceivedBlockTracker()

  private var endpoint: RpcEndpointRef = null

  /** Start the endpoint and receiver execution thread.  启动端点和接收器执行线程。*/
  def start(): Unit = synchronized {
      launchReceivers()
  }

  /**  将所有未分配的块分配给给定批处理。*/
  def allocateBlocksToBatch(batchTime: Time): Unit = {
      receivedBlockTracker.allocateBlocksToBatch(batchTime)
  }

  /**  获取给定批处理和所有输入流的块。*/
  def getBlocksOfBatch(batchTime: Time): Map[Int, Seq[ReceivedBlockInfo]] = {receivedBlockTracker.getBlocksOfBatch(batchTime)}

  /**  获取分配给给定批处理和流的块。*/
  def getBlocksOfBatchAndStream(batchTime: Time, streamId: Int): Seq[ReceivedBlockInfo] = {receivedBlockTracker.getBlocksOfBatchAndStream(batchTime, streamId)}

  /**
    * 清除严格超过阈值时间的块和批的数据和元数据。请注意，这不是
   */
  def cleanupOldBlocksAndBatches(cleanupThreshTime: Time) {
    receivedBlockTracker.cleanupOldBatches(cleanupThreshTime, waitForCompletion = false)
    //  向接收端发出删除旧块数据的信号
    endpoint.send(CleanupOldBlocks(cleanupThreshTime))
  }


  private def scheduleReceiver(receiverId: Int): Seq[TaskLocation] = {
    val preferredLocation = receiverPreferredLocations.getOrElse(receiverId, None)
    val scheduledLocations = schedulingPolicy.rescheduleReceiver(
      receiverId, preferredLocation, receiverTrackingInfos, getExecutors)
    updateReceiverScheduledExecutors(receiverId, scheduledLocations)
    scheduledLocations
  }

  /**运行模拟的Spark作业，以确保所有从服务器都已注册。这避免了将所有接收器安排在同一个节点上。 */
  private def runDummySparkJob(): Unit = {
    if (!ssc.sparkContext.isLocal) {
      ssc.sparkContext.makeRDD(1 to 50, 50).map(x => (x, 1)).reduceByKey(_ + _, 20).collect()
    }
    assert(getExecutors.nonEmpty)
  }

  /** 从ReceiverInputDStreams获取接收方，将它们作为并行集合分发到工作节点，并运行它们。 */
  private def launchReceivers(): Unit = {
    // 获取所有inputDStream中定义的流数据接收器
    val receivers = receiverInputStreams.map { nis =>
    // 向终端点发送分发并启动所有流数据接收器的消息
    endpoint.send(StartAllReceivers(receivers))
  }

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
