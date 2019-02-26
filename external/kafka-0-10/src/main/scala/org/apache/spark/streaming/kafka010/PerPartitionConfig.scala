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

package org.apache.spark.streaming.kafka010

import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkConf
import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * Interface for user-supplied configurations that can't otherwise be set via Spark properties,
 * because they need tweaking on a per-partition basis,
  * 接口为用户提供的配置，否则无法通过Spark属性设置，因为它们需要在每个分区的基础上进行调整，
 */
@Experimental
abstract class PerPartitionConfig extends Serializable {
  /**
   *  Maximum rate (number of records per second) at which data will be read
   *  from each Kafka partition. 从每个Kafka分区读取数据的最大速率(每秒记录数)。
   */
  def maxRatePerPartition(topicPartition: TopicPartition): Long
}

/**
 * Default per-partition configuration 默认每个分区配置
 */
private class DefaultPerPartitionConfig(conf: SparkConf)
    extends PerPartitionConfig {
  val maxRate = conf.getLong("spark.streaming.kafka.maxRatePerPartition", 0)

  def maxRatePerPartition(topicPartition: TopicPartition): Long = maxRate
}
