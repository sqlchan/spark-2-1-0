
package org.apache.spark.streaming

import java.io._
import java.util.concurrent.{ArrayBlockingQueue, RejectedExecutionException,
  ThreadPoolExecutor, TimeUnit}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.streaming.scheduler.JobGenerator
import org.apache.spark.util.Utils


private[streaming]
object Checkpoint extends Logging {
  val PREFIX = "checkpoint-"
  val REGEX = (PREFIX + """([\d]+)([\w\.]*)""").r

  /** Get the checkpoint file for the given checkpoint time */
  def checkpointFile(checkpointDir: String, checkpointTime: Time): Path = {
    new Path(checkpointDir, PREFIX + checkpointTime.milliseconds)
  }

}


/*** 方便类来处理图形检查点到文件的写入*/
private[streaming]
class CheckpointWriter(
    jobGenerator: JobGenerator,
    conf: SparkConf,
    checkpointDir: String,
    hadoopConf: Configuration
  ) extends Logging {
  val MAX_ATTEMPTS = 3

  // Single-thread executor which rejects executions when a large amount have queued up.
  // This fails fast since this typically means the checkpoint store will never keep up, and
  // will otherwise lead to filling memory with waiting payloads of byte[] to write.
  /**
    * 单线程执行程序，当有大量的线程排队时，它拒绝执行。
    * 这种方法失败得很快，因为这通常意味着检查点存储将永远跟不上，否则将导致用等待写入的byte[]有效负载填充内存。
    */
  val executor = new ThreadPoolExecutor(
    1, 1,
    0L, TimeUnit.MILLISECONDS,
    new ArrayBlockingQueue[Runnable](1000))
  val compressionCodec = CompressionCodec.createCodec(conf)
  private var stopped = false
  @volatile private[this] var fs: FileSystem = null
  @volatile private var latestCheckpointTime: Time = null

  class CheckpointWriteHandler(
      checkpointTime: Time,
      bytes: Array[Byte],
      clearCheckpointDataLater: Boolean) extends Runnable {
    def run() {
      if (latestCheckpointTime == null || latestCheckpointTime < checkpointTime) {
        latestCheckpointTime = checkpointTime
      }

      var attempts = 0
      val startTime = System.currentTimeMillis()
      val tempFile = new Path(checkpointDir, "temp")
      // We will do checkpoint when generating a batch and completing a batch. When the processing
      // time of a batch is greater than the batch interval, checkpointing for completing an old
      // batch may run after checkpointing of a new batch. If this happens, checkpoint of an old
      // batch actually has the latest information, so we want to recovery from it. Therefore, we
      // also use the latest checkpoint time as the file name, so that we can recover from the
      // latest checkpoint file.
      //
      /**
        * 我们将在生成批处理和完成批处理时执行检查点。当批处理时间大于批处理间隔时，
        * 可以在新批处理的检查点之后运行用于完成旧批处理的检查点。如果发生这种情况，
        * 旧批处理的检查点实际上具有最新信息，因此我们希望从中恢复。
        * 因此，我们还使用最新的检查点时间作为文件名，以便从最新的检查点文件中恢复。
        */
      // about thread-safety. 只有一个线程在编写检查点文件，因此我们不需要担心线程安全性。
      val checkpointFile = Checkpoint.checkpointFile(checkpointDir, latestCheckpointTime)
      val backupFile = Checkpoint.checkpointBackupFile(checkpointDir, latestCheckpointTime)

      while (attempts < MAX_ATTEMPTS && !stopped) {
        attempts += 1
        try {
          logInfo(s"Saving checkpoint for time $checkpointTime to file '$checkpointFile'")

          // Write checkpoint to temp file
          fs.delete(tempFile, true) // just in case it exists  以防它存在
          val fos = fs.create(tempFile)
          fos.write(bytes)

          // If the checkpoint file exists, back it up 如果检查点文件存在，请备份它
          // If the backup exists as well, just delete it, otherwise rename will fail 如果备份也存在，只需删除它，否则重命名将失败
          if (fs.exists(checkpointFile)) {
            fs.delete(backupFile, true) // just in case it exists
          }

          // Delete old checkpoint files 删除旧检查点文件
          val allCheckpointFiles = Checkpoint.getCheckpointFiles(checkpointDir, Some(fs))
          if (allCheckpointFiles.size > 10) {
            allCheckpointFiles.take(allCheckpointFiles.size - 10).foreach { file =>
              logInfo(s"Deleting $file")
              fs.delete(file, true)
            }
          }

          // All done, print success 全部完成，打印成功
          val finishTime = System.currentTimeMillis()
          logInfo(s"Checkpoint for time $checkpointTime saved to file '$checkpointFile'" +
            s", took ${bytes.length} bytes and ${finishTime - startTime} ms")
          jobGenerator.onCheckpointCompletion(checkpointTime, clearCheckpointDataLater)
          return
        }
      }
    }
  }

}

