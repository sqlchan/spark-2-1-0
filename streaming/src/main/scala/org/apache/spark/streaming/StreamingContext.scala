package org.apache.spark.streaming

import java.io.{InputStream, NotSerializableException}
import java.util.Properties
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.Map
import scala.collection.mutable.Queue
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.commons.lang3.SerializationUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

import org.apache.spark._
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.input.FixedLengthBinaryInputFormat
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{RDD, RDDOperationScope}
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.serializer.SerializationDebugger
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContextState._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.{ExecutorAllocationManager, JobScheduler, StreamingListener}
import org.apache.spark.streaming.ui.{StreamingJobProgressListener, StreamingTab}
import org.apache.spark.util.{CallSite, ShutdownHookManager, ThreadUtils, Utils}

/**
  * Spark流功能的主要入口点。它提供了用于创建[org.apache.spark.stream.dstream]的方法。
  * 从各种输入来源。它既可以通过提供Spark主URL和appName创建，也可以通过org.apache.spark创建。
  * SparkConf配置(请参阅核心Spark文档)，或来自现有的org.apache.spark.SparkContext。
  * 可以使用“context.sparkContext”访问关联的SparkContext。
  * 在创建和转换DStreams之后，可以分别使用context.start()和context.stop()启动和停止流计算。
  * ' context. awaittermination() '允许当前线程通过' stop() '或异常等待上下文的终止。
 */
class StreamingContext private[streaming] (
    _sc: SparkContext,
    _cp: Checkpoint,
    _batchDur: Duration
  ) extends Logging {

  private[streaming] val sc: SparkContext
  private[streaming] val conf = sc.conf

  private[streaming] val env = sc.env

  private[streaming] val graph: DStreamGraph = {
    val newGraph = new DStreamGraph()
    newGraph.setBatchDuration(_batchDur)
    newGraph
  }

  private[streaming] val scheduler = new JobScheduler(this)

  /**
    * Start the execution of the streams. 启动流的执行。
    */
  def start(): Unit = synchronized { //在一个线程中启动jobscheduler
    state match {
      case INITIALIZED =>
        startSite.set(DStream.getCreationSite()) // 从创建DStream的堆栈跟踪中获取DStream的创建站点。
        // 在一个新线程中启动流调度器，以便在不影响当前线程的情况下重置线程本地属性(如调用站点和作业组)。
        ThreadUtils.runInNewThread("streaming-start") {
          scheduler.start() //启动jobscheduler
        }
    }
  }
}