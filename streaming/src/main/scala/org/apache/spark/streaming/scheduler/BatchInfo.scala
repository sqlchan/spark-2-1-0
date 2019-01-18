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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.streaming.Time

/**
 * :: DeveloperApi ::
 * Class having information on completed batches.  类具有已完成批次的信息。
 * @param batchTime   Time of the batch
 * @param streamIdToInputInfo A map of input stream id to its input info 输入流id到其输入信息的映射
 * @param submissionTime  Clock time of when jobs of this batch was submitted to
 *                        the streaming scheduler queue  将此批作业提交到流调度器队列的时钟时间
 * @param processingStartTime Clock time of when the first job of this batch started processing
  *                            此批处理的第一个作业开始处理的时钟时间
 * @param processingEndTime Clock time of when the last job of this batch finished processing
 * @param outputOperationInfos The output operations in this batch  此批处理中的输出操作
 */
@DeveloperApi
case class BatchInfo(
    batchTime: Time,
    streamIdToInputInfo: Map[Int, StreamInputInfo],
    submissionTime: Long,
    processingStartTime: Option[Long],
    processingEndTime: Option[Long],
    outputOperationInfos: Map[Int, OutputOperationInfo]
  ) {

  /**
   * Time taken for the first job of this batch to start processing from the time this batch
   * was submitted to the streaming scheduler. Essentially, it is
   * `processingStartTime` - `submissionTime`.
    * 此批处理的第一个作业从该批处理提交给流调度器时开始处理所花费的时间。
    * 本质上，它是“processingStartTime”-“submissionTime”。
   */
  def schedulingDelay: Option[Long] = processingStartTime.map(_ - submissionTime)

  /**
   * Time taken for the all jobs of this batch to finish processing from the time they started
   * processing. Essentially, it is `processingEndTime` - `processingStartTime`.
    * 这批作业从开始处理到完成处理所花费的时间。本质上，它是“processingEndTime”—“processingStartTime”。
   */
  def processingDelay: Option[Long] = processingEndTime.zip(processingStartTime)
    .map(x => x._1 - x._2).headOption

  /**
   * Time taken for all the jobs of this batch to finish processing from the time they
   * were submitted.  Essentially, it is `processingDelay` + `schedulingDelay`.
    * 这批作业从提交之日起完成处理所需的时间。本质上，它是' processingDelay ' + ' schedulingDelay '。
   */
  def totalDelay: Option[Long] = schedulingDelay.zip(processingDelay)
    .map(x => x._1 + x._2).headOption

  /**
   * The number of recorders received by the receivers in this batch. 接收方在这批数据中接收到的记录器数量。
   */
  def numRecords: Long = streamIdToInputInfo.values.map(_.numRecords).sum

}
