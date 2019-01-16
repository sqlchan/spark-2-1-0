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

package org.apache.spark.streaming.dstream

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.Time
import org.apache.spark.util.Utils

private[streaming]
class DStreamCheckpointData[T: ClassTag](dstream: DStream[T])
  extends Serializable with Logging {
  protected val data = new HashMap[Time, AnyRef]()

  // Mapping of the batch time to the checkpointed RDD file of that time
  // 将批处理时间映射到该时间的检查点RDD文件
  @transient private var timeToCheckpointFile = new HashMap[Time, String]
  // Mapping of the batch time to the time of the oldest checkpointed RDD
  // in that batch's checkpoint data
  // 将批处理时间映射到该批处理检查点数据中最古老的检查点RDD的时间
  @transient private var timeToOldestCheckpointFileTime = new HashMap[Time, Time]

  @transient private var fileSystem: FileSystem = null
  protected[streaming] def currentCheckpointFiles = data.asInstanceOf[HashMap[Time, String]]

  /**
   * Updates the checkpoint data of the DStream. This gets called every time
   * the graph checkpoint is initiated. Default implementation records the
   * checkpoint files at which the generated RDDs of the DStream have been saved.
    * 更新DStream的检查点数据。在每次启动图检查点时都会调用此方法。
    * 默认实现记录保存DStream生成的RDDs的检查点文件。
   */
  def update(time: Time) {

    // Get the checkpointed RDDs from the generated RDDs  从生成的RDDs中获取检查点RDDs
    val checkpointFiles = dstream.generatedRDDs.filter(_._2.getCheckpointFile.isDefined)
                                       .map(x => (x._1, x._2.getCheckpointFile.get))
    logDebug("Current checkpoint files:\n" + checkpointFiles.toSeq.mkString("\n"))

    // Add the checkpoint files to the data to be serialized
    // 将检查点文件添加到要序列化的数据中
    if (!checkpointFiles.isEmpty) {
      currentCheckpointFiles.clear()
      currentCheckpointFiles ++= checkpointFiles
      // Add the current checkpoint files to the map of all checkpoint files
      // This will be used to delete old checkpoint files
      // 将当前检查点文件添加到所有检查点文件的映射中。这将用于删除旧的检查点文件
      timeToCheckpointFile ++= currentCheckpointFiles
      // Remember the time of the oldest checkpoint RDD in current state
      // 请记住当前状态下最老的检查点RDD的时间
      timeToOldestCheckpointFileTime(time) = currentCheckpointFiles.keys.min(Time.ordering)
    }
  }

  /**
   * Cleanup old checkpoint data. This gets called after a checkpoint of `time` has been
   * written to the checkpoint directory.
    * 清理旧的检查点数据。在将一个“时间”检查点写入检查点目录后调用该方法。
   */
  def cleanup(time: Time) {
    // Get the time of the oldest checkpointed RDD that was written as part of the
    // checkpoint of `time`  获取作为“时间”检查点的一部分编写的最古老检查点RDD的时间
    timeToOldestCheckpointFileTime.remove(time) match {
      case Some(lastCheckpointFileTime) =>
        // Find all the checkpointed RDDs (i.e. files) that are older than `lastCheckpointFileTime`
        // This is because checkpointed RDDs older than this are not going to be needed
        // even after master fails, as the checkpoint data of `time` does not refer to those files
        // 查找所有大于“lastCheckpointFileTime”的检查点RDDs(即文件)，因为即使master失败，
        // 也不需要大于“lastCheckpointFileTime”的检查点RDDs，因为“time”的检查点数据不引用这些文件
        val filesToDelete = timeToCheckpointFile.filter(_._1 < lastCheckpointFileTime)
        logDebug("Files to delete:\n" + filesToDelete.mkString(","))
        filesToDelete.foreach {
          case (time, file) =>
            try {
              val path = new Path(file)
              if (fileSystem == null) {
                fileSystem = path.getFileSystem(dstream.ssc.sparkContext.hadoopConfiguration)
              }
              fileSystem.delete(path, true)
              timeToCheckpointFile -= time
              logInfo("Deleted checkpoint file '" + file + "' for time " + time)
            } catch {
              case e: Exception =>
                logWarning("Error deleting old checkpoint file '" + file + "' for time " + time, e)
                fileSystem = null
            }
        }
      case None =>
        logDebug("Nothing to delete")
    }
  }

  /**
   * Restore the checkpoint data. This gets called once when the DStream graph
   * (along with its output DStreams) is being restored from a graph checkpoint file.
   * Default implementation restores the RDDs from their checkpoint files.
    * 恢复检查点数据。当从图检查点文件还原DStream图(及其输出DStreams)时，将调用此函数一次。默认实现从检查点文件中恢复RDDs。
   */
  def restore() {
    // Create RDDs from the checkpoint data  从检查点数据创建RDDs
    currentCheckpointFiles.foreach {
      case(time, file) =>
        logInfo("Restoring checkpointed RDD for time " + time + " from file '" + file + "'")
        dstream.generatedRDDs += ((time, dstream.context.sparkContext.checkpointFile[T](file)))
    }
  }

  override def toString: String = {
    "[\n" + currentCheckpointFiles.size + " checkpoint files \n" +
      currentCheckpointFiles.mkString("\n") + "\n]"
  }

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    logDebug(this.getClass().getSimpleName + ".writeObject used")
    if (dstream.context.graph != null) {
      dstream.context.graph.synchronized {
        if (dstream.context.graph.checkpointInProgress) {
          oos.defaultWriteObject()
        } else {
          val msg = "Object of " + this.getClass.getName + " is being serialized " +
            " possibly as a part of closure of an RDD operation. This is because " +
            " the DStream object is being referred to from within the closure. " +
            " Please rewrite the RDD operation inside this DStream to avoid this. " +
            " This has been enforced to avoid bloating of Spark tasks " +
            " with unnecessary objects."
          throw new java.io.NotSerializableException(msg)
        }
      }
    } else {
      throw new java.io.NotSerializableException(
        "Graph is unexpectedly null when DStream is being serialized.")
    }
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    logDebug(this.getClass().getSimpleName + ".readObject used")
    ois.defaultReadObject()
    timeToOldestCheckpointFileTime = new HashMap[Time, Time]
    timeToCheckpointFile = new HashMap[Time, String]
  }
}
