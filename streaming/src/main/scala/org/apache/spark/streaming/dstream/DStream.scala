package org.apache.spark.streaming.dstream
import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable.HashMap
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.matching.Regex

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{BlockRDD, PairRDDFunctions, RDD, RDDOperationScope}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext.rddToFileName
import org.apache.spark.streaming.scheduler.Job
import org.apache.spark.streaming.ui.UIUtils
import org.apache.spark.util.{CallSite, Utils}

abstract class DStream[T: ClassTag] (
    @transient private[streaming] var ssc: StreamingContext
  ) extends Serializable with Logging {

  def slideDuration: Duration  // 滑动时间
  def dependencies: List[DStream[_]]   //  此DStream依赖的父DStreams的列表*
  def compute(validTime: Time): Option[RDD[T]]   //方法为给定的时间生成RDD

  // 所有DStreams上可用的方法和字段
  // =======================================================================
  private[streaming] var generatedRDDs = new HashMap[Time, RDD[T]]()

  private[streaming] var rememberDuration: Duration = null   //  DStream将记住所创建的每个RDD的持续时间

  private[streaming] var checkpointDuration: Duration = null
  private[streaming] val checkpointData = new DStreamCheckpointData(this)

  // Reference to whole DStream graph   引用整个DStream图
  private[streaming] var graph: DStreamGraph = null

  /**获取与给定时间对应的RDD, 要么从缓存中检索它，要么对其进行计算和缓存。*/
  private[streaming] final def getOrCompute(time: Time): Option[RDD[T]] = {

    generatedRDDs.get(time).orElse {
        val rddOption = null
        rddOption.foreach { case newRDD =>
          // 把生成的RDD保存到generateRDD 哈希map中，便于下次使用
          generatedRDDs.put(time, newRDD)
        }
        rddOption
    }
  }

  /**
    * 为给定的时间生成一个SparkStreaming作业。这是一个不应该直接调用的内部方法。
    * 此默认实现创建一个作业，该作业实现相应的RDD。DStream的子类可以覆盖它来生成它们自己的作业。
   */
  private[streaming] def generateJob(time: Time): Option[Job] = {
    // getorcompute方法生成RDD，
    getOrCompute(time) match {
      case Some(rdd) =>
        val jobFunc = () => {
          val emptyFunc = { (iterator: Iterator[T]) => {} }
          context.sparkContext.runJob(rdd, emptyFunc)
        }
        Some(new Job(time, jobFunc))
    }
  }

  /**
    * 清除此DStream的“memorberduration”之前的元数据
    * 这是一个不应该直接调用的内部方法。此默认实现清除旧生成的RDDs。
    * DStream的子类可以覆盖它来清除它们自己的元数据以及生成的rdd。
   */
  private[streaming] def clearMetadata(time: Time) {
    val oldRDDs = generatedRDDs.filter(_._1 <= (time - rememberDuration))

    generatedRDDs --= oldRDDs.keys
      oldRDDs.values.foreach { rdd =>
        rdd.unpersist(false)
        rdd match {
          case b: BlockRDD[_] =>
            logInfo(s"Removing blocks of RDD $b of time $time")
            b.removeBlocks()
        }
      }
    dependencies.foreach(_.clearMetadata(time))
  }

  /**
   * 刷新将与此流的检查点一起保存的检查点RDDs列表。这是一个不应该直接调用的内部方法。
    * 这是一个默认实现，它只将检查点RDDs的文件名保存到checkpointData。
    * DStream的子类(尤其是InputDStream的子类)可以覆盖此方法来保存自定义检查点数据。
   */
  private[streaming] def updateCheckpointData(currentTime: Time) {
    logDebug(s"Updating checkpoint data for time $currentTime")
    checkpointData.update(currentTime)
    dependencies.foreach(_.updateCheckpointData(currentTime))
    logDebug(s"Updated checkpoint data for time $currentTime: $checkpointData")
  }

  private[streaming] def clearCheckpointData(time: Time) {
    logDebug("Clearing checkpoint data")
    checkpointData.cleanup(time)
    dependencies.foreach(_.clearCheckpointData(time))
    logDebug("Cleared checkpoint data")
  }


  /**将此流注册为输出流。这将确保生成此DStream的RDDs。 */
  private[streaming] def register(): DStream[T] = {
    ssc.graph.addOutputStream(this)
    this
  }
}

object DStream {

}
