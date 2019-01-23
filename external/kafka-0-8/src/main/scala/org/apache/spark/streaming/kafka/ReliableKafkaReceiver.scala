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

package org.apache.spark.streaming.kafka

import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, ThreadPoolExecutor}

import scala.collection.{mutable, Map}
import scala.reflect.{classTag, ClassTag}

import kafka.common.TopicAndPartition
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector, KafkaStream}
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import kafka.utils.{VerifiableProperties, ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.receiver.{BlockGenerator, BlockGeneratorListener, Receiver}
import org.apache.spark.util.ThreadUtils

/**
 * ReliableKafkaReceiver offers the ability to reliably store data into BlockManager without loss.
 * It is turned off by default and will be enabled when
 * spark.streaming.receiver.writeAheadLog.enable is true. The difference compared to KafkaReceiver
 * is that this receiver manages topic-partition/offset itself and updates the offset information
 * after data is reliably stored as write-ahead log. Offsets will only be updated when data is
 * reliably stored, so the potential data loss problem of KafkaReceiver can be eliminated.
  * ReliableKafkaReceiver提供了可靠地将数据存储到BlockManager而不会丢失的能力。
  * 默认情况下它是关闭的，当spark.stream.receiver.writeaheadlog 时将启用它。使是正确的。
  * 与KafkaReceiver的不同之处在于，这个接收器管理主题分区/偏移量本身，并在可靠地将数据存储为写前日志之后更新偏移量信息。
  * 偏移量只有在数据可靠存储时才会被更新，从而消除KafkaReceiver潜在的数据丢失问题。
 *
 * Note: ReliableKafkaReceiver will set auto.commit.enable to false to turn off automatic offset
 * commit mechanism in Kafka consumer. So setting this configuration manually within kafkaParams
 * will not take effect.
  * ReliableKafkaReceiver将设置auto.commit.enable false来关闭Kafka使用者中的自动偏移提交机制。因此，在kafkaParams中手动设置此配置将不会生效
 */
private[streaming]
class ReliableKafkaReceiver[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag](
    kafkaParams: Map[String, String],
    topics: Map[String, Int],
    storageLevel: StorageLevel)
    extends Receiver[(K, V)](storageLevel) with Logging {

  private val groupId = kafkaParams("group.id")
  private val AUTO_OFFSET_COMMIT = "auto.commit.enable"
  private def conf = SparkEnv.get.conf

  /** High level consumer to connect to Kafka.  高级消费者连接Kafka*/
  private var consumerConnector: ConsumerConnector = null

  /** zkClient to connect to Zookeeper to commit the offsets. */
  private var zkClient: ZkClient = null

  /**
   * A HashMap to manage the offset for each topic/partition, this HashMap is called in
   * synchronized block, so mutable HashMap will not meet concurrency issue.
    * HashMap用于管理每个主题/分区的偏移量，这个HashMap在synchronized块中调用，因此可变HashMap不会遇到并发问题。
   */
  private var topicPartitionOffsetMap: mutable.HashMap[TopicAndPartition, Long] = null

  /** A concurrent HashMap to store the stream block id and related offset snapshot.
    * 一个用于存储流块id和相关偏移快照的并发HashMap*/
  private var blockOffsetMap: ConcurrentHashMap[StreamBlockId, Map[TopicAndPartition, Long]] = null

  /**
   * Manage the BlockGenerator in receiver itself for better managing block store and offset
   * commit.
   */
  private var blockGenerator: BlockGenerator = null

  /** Thread pool running the handlers for receiving message from multiple topics and partitions.
    * 运行用于接收来自多个主题和分区的消息的处理程序的线程池。*/
  private var messageHandlerThreadPool: ThreadPoolExecutor = null

  override def onStart(): Unit = {
    logInfo(s"Starting Kafka Consumer Stream with group: $groupId")

    // Initialize the topic-partition / offset hash map. 初始化主题分区/偏移散列映射
    topicPartitionOffsetMap = new mutable.HashMap[TopicAndPartition, Long]

    // Initialize the stream block id / offset snapshot hash map.  初始化流块id /偏移快照散列映射。
    blockOffsetMap = new ConcurrentHashMap[StreamBlockId, Map[TopicAndPartition, Long]]()

    // Initialize the block generator for storing Kafka message. 初始化用于存储Kafka消息的块生成器。
    blockGenerator = supervisor.createBlockGenerator(new GeneratedBlockHandler)

    if (kafkaParams.contains(AUTO_OFFSET_COMMIT) && kafkaParams(AUTO_OFFSET_COMMIT) == "true") {
      logWarning(s"$AUTO_OFFSET_COMMIT should be set to false in ReliableKafkaReceiver, " +
        "otherwise we will manually set it to false to turn off auto offset commit in Kafka")
    }

    val props = new Properties()
    kafkaParams.foreach(param => props.put(param._1, param._2))
    // Manually set "auto.commit.enable" to "false" no matter user explicitly set it to true,
    // we have to make sure this property is set to false to turn off auto commit mechanism in
    // Kafka. 我们必须确保将此属性设置为false，以关闭Kafka中的自动提交机制。
    props.setProperty(AUTO_OFFSET_COMMIT, "false")

    val consumerConfig = new ConsumerConfig(props)

    assert(!consumerConfig.autoCommitEnable)

    logInfo(s"Connecting to Zookeeper: ${consumerConfig.zkConnect}")
    consumerConnector = Consumer.create(consumerConfig)
    logInfo(s"Connected to Zookeeper: ${consumerConfig.zkConnect}")

    zkClient = new ZkClient(consumerConfig.zkConnect, consumerConfig.zkSessionTimeoutMs,
      consumerConfig.zkConnectionTimeoutMs, ZKStringSerializer)

    messageHandlerThreadPool = ThreadUtils.newDaemonFixedThreadPool(
      topics.values.sum, "KafkaMessageHandler")

    blockGenerator.start()

    val keyDecoder = classTag[U].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(consumerConfig.props)
      .asInstanceOf[Decoder[K]]

    val valueDecoder = classTag[T].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(consumerConfig.props)
      .asInstanceOf[Decoder[V]]

    val topicMessageStreams = consumerConnector.createMessageStreams(
      topics, keyDecoder, valueDecoder)

    topicMessageStreams.values.foreach { streams =>
      streams.foreach { stream =>
        messageHandlerThreadPool.submit(new MessageHandler(stream))
      }
    }
  }

  override def onStop(): Unit = {
    if (messageHandlerThreadPool != null) {
      messageHandlerThreadPool.shutdown()
      messageHandlerThreadPool = null
    }

    if (consumerConnector != null) {
      consumerConnector.shutdown()
      consumerConnector = null
    }

    if (zkClient != null) {
      zkClient.close()
      zkClient = null
    }

    if (blockGenerator != null) {
      blockGenerator.stop()
      blockGenerator = null
    }

    if (topicPartitionOffsetMap != null) {
      topicPartitionOffsetMap.clear()
      topicPartitionOffsetMap = null
    }

    if (blockOffsetMap != null) {
      blockOffsetMap.clear()
      blockOffsetMap = null
    }
  }

  /** Store a Kafka message and the associated metadata as a tuple.  将Kafka消息和相关的元数据存储为一个元组*/
  private def storeMessageAndMetadata(
      msgAndMetadata: MessageAndMetadata[K, V]): Unit = {
    val topicAndPartition = TopicAndPartition(msgAndMetadata.topic, msgAndMetadata.partition)
    val data = (msgAndMetadata.key, msgAndMetadata.message)
    val metadata = (topicAndPartition, msgAndMetadata.offset)
    blockGenerator.addDataWithCallback(data, metadata)
  }

  /** Update stored offset   更新存储抵消*/
  private def updateOffset(topicAndPartition: TopicAndPartition, offset: Long): Unit = {
    topicPartitionOffsetMap.put(topicAndPartition, offset)
  }

  /**
   * Remember the current offsets for each topic and partition. This is called when a block is
   * generated.
   */
  private def rememberBlockOffsets(blockId: StreamBlockId): Unit = {
    // Get a snapshot of current offset map and store with related block id.
    val offsetSnapshot = topicPartitionOffsetMap.toMap
    blockOffsetMap.put(blockId, offsetSnapshot)
    topicPartitionOffsetMap.clear()
  }

  /**
   * Store the ready-to-be-stored block and commit the related offsets to zookeeper. This method
   * will try a fixed number of times to push the block. If the push fails, the receiver is stopped.
   */
  private def storeBlockAndCommitOffset(
      blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
    var count = 0
    var pushed = false
    var exception: Exception = null
    while (!pushed && count <= 3) {
      try {
        store(arrayBuffer.asInstanceOf[mutable.ArrayBuffer[(K, V)]])
        pushed = true
      } catch {
        case ex: Exception =>
          count += 1
          exception = ex
      }
    }
    if (pushed) {
      Option(blockOffsetMap.get(blockId)).foreach(commitOffset)
      blockOffsetMap.remove(blockId)
    } else {
      stop("Error while storing block into Spark", exception)
    }
  }

  /**
   * Commit the offset of Kafka's topic/partition, the commit mechanism follow Kafka 0.8.x's
   * metadata schema in Zookeeper.
    * 提交Kafka的主题/分区的偏移量，提交机制遵循Kafka 0.8。Zookeeper中的x元数据模式。
   */
  private def commitOffset(offsetMap: Map[TopicAndPartition, Long]): Unit = {
    if (zkClient == null) {
      val thrown = new IllegalStateException("Zookeeper client is unexpectedly null")
      stop("Zookeeper client is not initialized before commit offsets to ZK", thrown)
      return
    }

    for ((topicAndPart, offset) <- offsetMap) {
      try {
        val topicDirs = new ZKGroupTopicDirs(groupId, topicAndPart.topic)
        val zkPath = s"${topicDirs.consumerOffsetDir}/${topicAndPart.partition}"

        ZkUtils.updatePersistentPath(zkClient, zkPath, offset.toString)
      } catch {
        case e: Exception =>
          logWarning(s"Exception during commit offset $offset for topic" +
            s"${topicAndPart.topic}, partition ${topicAndPart.partition}", e)
      }

      logInfo(s"Committed offset $offset for topic ${topicAndPart.topic}, " +
        s"partition ${topicAndPart.partition}")
    }
  }

  /** Class to handle received Kafka message. 类处理接收的Kafka消息*/
  private final class MessageHandler(stream: KafkaStream[K, V]) extends Runnable {
    override def run(): Unit = {
      while (!isStopped) {
        try {
          val streamIterator = stream.iterator()
          while (streamIterator.hasNext) {
            storeMessageAndMetadata(streamIterator.next)
          }
        } catch {
          case e: Exception =>
            reportError("Error handling message", e)
        }
      }
    }
  }

  /** Class to handle blocks generated by the block generator. */
  private final class GeneratedBlockHandler extends BlockGeneratorListener {

    def onAddData(data: Any, metadata: Any): Unit = {
      // Update the offset of the data that was added to the generator
      if (metadata != null) {
        val (topicAndPartition, offset) = metadata.asInstanceOf[(TopicAndPartition, Long)]
        updateOffset(topicAndPartition, offset)
      }
    }

    def onGenerateBlock(blockId: StreamBlockId): Unit = {
      // Remember the offsets of topics/partitions when a block has been generated
      rememberBlockOffsets(blockId)
    }

    def onPushBlock(blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
      // Store block and commit the blocks offset
      storeBlockAndCommitOffset(blockId, arrayBuffer)
    }

    def onError(message: String, throwable: Throwable): Unit = {
      reportError(message, throwable)
    }
  }
}
