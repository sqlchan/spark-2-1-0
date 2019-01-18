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

import scala.collection.Map
import scala.collection.mutable

import org.apache.spark.scheduler.{ExecutorCacheTaskLocation, TaskLocation}
import org.apache.spark.streaming.receiver.Receiver

/**
 * A class that tries to schedule receivers with evenly distributed. There are two phases for
 * scheduling receivers.
  * 一个试图用均匀分布来调度接收者的类。调度接收器有两个阶段
 *
 *  - The first phase is global scheduling when ReceiverTracker is starting and we need to schedule
 *    all receivers at the same time. ReceiverTracker will call `scheduleReceivers` at this phase.
 *    It will try to schedule receivers such that they are evenly distributed. ReceiverTracker
 *    should update its `receiverTrackingInfoMap` according to the results of `scheduleReceivers`.
  * 第一阶段是全局调度，当ReceiverTracker启动时，我们需要同时调度所有的receiver。ReceiverTracker将在这个阶段调用“scheduler - eceivers”。
  * 它将设法安排接收器，使其均匀分布。ReceiverTracker应该根据“scheduler - eceivers”的结果更新它的“receiverTrackingInfoMap”。
 *    `ReceiverTrackingInfo.scheduledLocations` for each receiver should be set to a location list
 *    that contains the scheduled locations. Then when a receiver is starting, it will send a
 *    register request and `ReceiverTracker.registerReceiver` will be called. In
 *    `ReceiverTracker.registerReceiver`, if a receiver's scheduled locations is set, it should
 *    check if the location of this receiver is one of the scheduled locations, if not, the register
 *    will be rejected.
  *    “ReceiverTrackingInfo。应该将每个接收方的scheduledLocations设置为包含计划位置的位置列表。
  *    然后，当接收器启动时，它将发送一个注册请求和“ReceiverTracker”。将调用registerReceiver '。
  *    在“ReceiverTracker。，如果设置了接收方的计划位置，则应检查该接收方的位置是否是计划位置之一，如果不是，则拒绝注册。
 *  - The second phase is local scheduling when a receiver is restarting. There are two cases of
 *    receiver restarting:
  *    第二阶段是当接收端重新启动时的本地调度。接收端重启有两种情况:
 *    - If a receiver is restarting because it's rejected due to the real location and the scheduled
 *      locations mismatching, in other words, it fails to start in one of the locations that
 *      `scheduleReceivers` suggested, `ReceiverTracker` should firstly choose the executors that
 *      are still alive in the list of scheduled locations, then use them to launch the receiver
 *      job.
  *      如果接收机重新启动,因为它是拒绝是因为真正的位置和预定位置失配,换句话说,它未能开始的位置,“scheduleReceivers”建议,
  *      “ReceiverTracker”首先应该选择列表中仍然活着的执行人预定位置,然后使用它们来发射接收器的工作。
 *    - If a receiver is restarting without a scheduled locations list, or the executors in the list
 *      are dead, `ReceiverTracker` should call `rescheduleReceiver`. If so, `ReceiverTracker`
 *      should not set `ReceiverTrackingInfo.scheduledLocations` for this receiver, instead, it
 *      should clear it. Then when this receiver is registering, we can know this is a local
 *      scheduling, and `ReceiverTrackingInfo` should call `rescheduleReceiver` again to check if
 *      the launching location is matching.
  *      如果接收方在没有预定位置列表的情况下重新启动，或者列表中的执行器已经死亡，
  *      ' ReceiverTracker '应该调用' rescheduler ereceiver '。如果是，' ReceiverTracker '不应该设置' ReceiverTrackingInfo '。
  *      相反，应该清除这个接收器的scheduledLocations。当这个接收器注册时，我们可以知道这是一个本地调度，
  *      ' ReceiverTrackingInfo '应该再次调用' rescheduling ereceiver '来检查启动位置是否匹配。
 *
 * In conclusion, we should make a global schedule, try to achieve that exactly as long as possible,
 * otherwise do local scheduling.
  * 综上所述，我们应该制定一个全局调度，尽量准确地实现它，否则做局部调度。
 */
private[streaming] class ReceiverSchedulingPolicy {

  /**
   * Try our best to schedule receivers with evenly distributed. However, if the
   * `preferredLocation`s of receivers are not even, we may not be able to schedule them evenly
   * because we have to respect them.
    * 尽我们最大的努力使接收器分布均匀。然而，如果接收方的优先位置不是均匀的，
    * 我们可能无法均匀地安排它们，因为我们必须尊重它们。
   *
   * Here is the approach to schedule executors: 下面是调度执行器的方法:
   * <ol>
   *   <li>First, schedule all the receivers with preferred locations (hosts), evenly among the
   *       executors running on those host.</li>  首先，调度所有具有首选位置(主机)的接收方，在这些主机上运行的执行器之间平均分配。
   *   <li>Then, schedule all other receivers evenly among all the executors such that overall
   *       distribution over all the receivers is even.</li> 然后，在所有执行器中均匀地调度所有其他接收器，使所有接收器的总体分布均匀。
   * </ol>
   *
   * This method is called when we start to launch receivers at the first time.
   * 这个方法在我们第一次发射接收器的时候调用。
   * @return a map for receivers and their scheduled locations  接收器及其预定位置的地图
   */
  def scheduleReceivers(
      receivers: Seq[Receiver[_]],
      executors: Seq[ExecutorCacheTaskLocation]): Map[Int, Seq[TaskLocation]] = {
    if (receivers.isEmpty) {
      return Map.empty
    }

    if (executors.isEmpty) {
      return receivers.map(_.streamId -> Seq.empty).toMap
    }

    val hostToExecutors = executors.groupBy(_.host)
    val scheduledLocations = Array.fill(receivers.length)(new mutable.ArrayBuffer[TaskLocation])
    val numReceiversOnExecutor = mutable.HashMap[ExecutorCacheTaskLocation, Int]()
    // Set the initial value to 0
    executors.foreach(e => numReceiversOnExecutor(e) = 0)

    // Firstly, we need to respect "preferredLocation". So if a receiver has "preferredLocation",
    // we need to make sure the "preferredLocation" is in the candidate scheduled executor list.
    // 首先，我们需要尊重“优先位置”。因此，如果接收方有“preferredLocation”，
    // 我们需要确保“preferredLocation”在候选计划执行器列表中。
    for (i <- 0 until receivers.length) {
      // Note: preferredLocation is host but executors are host_executorId  注意:preferredLocation是host，但执行器是host_executorId
      receivers(i).preferredLocation.foreach { host =>
        hostToExecutors.get(host) match {
          case Some(executorsOnHost) =>
            // preferredLocation is a known host. Select an executor that has the least receivers in
            // this host  preferredLocation是一个已知的主机。选择此主机中接收器最少的执行程序
            val leastScheduledExecutor =
              executorsOnHost.minBy(executor => numReceiversOnExecutor(executor))
            scheduledLocations(i) += leastScheduledExecutor
            numReceiversOnExecutor(leastScheduledExecutor) =
              numReceiversOnExecutor(leastScheduledExecutor) + 1
          case None =>
            // preferredLocation is an unknown host.
            // Note: There are two cases:
            // 1. This executor is not up. But it may be up later.
            // 2. This executor is dead, or it's not a host in the cluster.
            // Currently, simply add host to the scheduled executors.
            // preferredLocation是一个未知主机。
            //注:有两种情况:
            //1. 这位遗嘱执行人还没起床。但它可能晚些时候才会出现。
            //2. 这个执行程序已经死亡，或者它不是集群中的主机。目前，只需将主机添加到计划的执行器。

            // Note: host could be `HDFSCacheTaskLocation`, so use `TaskLocation.apply` to handle
            // this case
            scheduledLocations(i) += TaskLocation(host)
        }
      }
    }

    // For those receivers that don't have preferredLocation, make sure we assign at least one
    // executor to them.  对于那些没有优先位置的接收者，确保我们为他们分配了至少一个执行器。
    for (scheduledLocationsForOneReceiver <- scheduledLocations.filter(_.isEmpty)) {
      // Select the executor that has the least receivers  选择接收器最少的执行器
      val (leastScheduledExecutor, numReceivers) = numReceiversOnExecutor.minBy(_._2)
      scheduledLocationsForOneReceiver += leastScheduledExecutor
      numReceiversOnExecutor(leastScheduledExecutor) = numReceivers + 1
    }

    // Assign idle executors to receivers that have less executors  将空闲执行器分配给执行器较少的接收器
    val idleExecutors = numReceiversOnExecutor.filter(_._2 == 0).map(_._1)
    for (executor <- idleExecutors) {
      // Assign an idle executor to the receiver that has least candidate executors.
      // 将空闲执行程序分配给具有最少候选执行程序的接收方
      val leastScheduledExecutors = scheduledLocations.minBy(_.size)
      leastScheduledExecutors += executor
    }

    receivers.map(_.streamId).zip(scheduledLocations).toMap
  }

  /**
   * Return a list of candidate locations to run the receiver. If the list is empty, the caller can
   * run this receiver in arbitrary executor.
   *
   * This method tries to balance executors' load. Here is the approach to schedule executors
   * for a receiver.
    * 返回要运行接收器的候选位置列表。如果列表为空，调用者可以在任意执行器中运行此接收器。
    * 此方法试图平衡执行程序的负载。下面是为接收方安排执行器的方法。
   * <ol>
   *   <li>
   *     If preferredLocation is set, preferredLocation should be one of the candidate locations.
    *     如果设置了preferredLocation, preferredLocation应该是候选位置之一。
   *   </li>
   *   <li>
   *     Every executor will be assigned to a weight according to the receivers running or
   *     scheduling on it. 每个执行器将根据其上运行或调度的接收器被分配一个权重
   *     <ul>
   *       <li>
   *         If a receiver is running on an executor, it contributes 1.0 to the executor's weight.
    *         如果接收器在执行器上运行，它将为执行器的权重贡献1.0。
   *       </li>
   *       <li>
   *         If a receiver is scheduled to an executor but has not yet run, it contributes
   *         `1.0 / #candidate_executors_of_this_receiver` to the executor's weight.</li>
    *         如果一个接收者被调度到一个执行器，但是还没有运行，那么它将为执行器的权重贡献“1.0 / #candidate_executors_of_this_receiver”。
   *     </ul>
   *     At last, if there are any idle executors (weight = 0), returns all idle executors.
   *     Otherwise, returns the executors that have the minimum weight.
    *     最后，如果有空闲执行器(权值= 0)，则返回所有空闲执行器。否则，返回权重最小的执行器。
   *   </li>
   * </ol>
   *
   * This method is called when a receiver is registering with ReceiverTracker or is restarting.
    * 当接收方在ReceiverTracker上注册或重新启动时调用此方法
   */
  def rescheduleReceiver(
      receiverId: Int,
      preferredLocation: Option[String],
      receiverTrackingInfoMap: Map[Int, ReceiverTrackingInfo],
      executors: Seq[ExecutorCacheTaskLocation]): Seq[TaskLocation] = {
    if (executors.isEmpty) {
      return Seq.empty
    }

    // Always try to schedule to the preferred locations 尽量安排到你喜欢的地点
    val scheduledLocations = mutable.Set[TaskLocation]()
    // Note: preferredLocation could be `HDFSCacheTaskLocation`, so use `TaskLocation.apply` to
    // handle this case
    scheduledLocations ++= preferredLocation.map(TaskLocation(_))

    val executorWeights: Map[ExecutorCacheTaskLocation, Double] = {
      receiverTrackingInfoMap.values.flatMap(convertReceiverTrackingInfoToExecutorWeights)
        .groupBy(_._1).mapValues(_.map(_._2).sum) // Sum weights for each executor
    }

    val idleExecutors = executors.toSet -- executorWeights.keys
    if (idleExecutors.nonEmpty) {
      scheduledLocations ++= idleExecutors
    } else {
      // There is no idle executor. So select all executors that have the minimum weight.
      val sortedExecutors = executorWeights.toSeq.sortBy(_._2)
      if (sortedExecutors.nonEmpty) {
        val minWeight = sortedExecutors(0)._2
        scheduledLocations ++= sortedExecutors.takeWhile(_._2 == minWeight).map(_._1)
      } else {
        // This should not happen since "executors" is not empty 这不应该发生，因为“executor”不是空的
      }
    }
    scheduledLocations.toSeq
  }

  /**
   * This method tries to convert a receiver tracking info to executor weights. Every executor will
   * be assigned to a weight according to the receivers running or scheduling on it:
    * 此方法试图将接收器跟踪信息转换为执行器权重。每个执行器将根据其上运行或调度的接收者被分配一个权重:
   *
   * - If a receiver is running on an executor, it contributes 1.0 to the executor's weight.
   * - If a receiver is scheduled to an executor but has not yet run, it contributes
   * `1.0 / #candidate_executors_of_this_receiver` to the executor's weight.
   */
  private def convertReceiverTrackingInfoToExecutorWeights(
      receiverTrackingInfo: ReceiverTrackingInfo): Seq[(ExecutorCacheTaskLocation, Double)] = {
    receiverTrackingInfo.state match {
      case ReceiverState.INACTIVE => Nil
      case ReceiverState.SCHEDULED =>
        val scheduledLocations = receiverTrackingInfo.scheduledLocations.get
        // The probability that a scheduled receiver will run in an executor is
        // 1.0 / scheduledLocations.size
        scheduledLocations.filter(_.isInstanceOf[ExecutorCacheTaskLocation]).map { location =>
          location.asInstanceOf[ExecutorCacheTaskLocation] -> (1.0 / scheduledLocations.size)
        }
      case ReceiverState.ACTIVE => Seq(receiverTrackingInfo.runningExecutor.get -> 1.0)
    }
  }
}
