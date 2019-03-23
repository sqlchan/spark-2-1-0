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

package org.apache.spark.streaming

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.scheduler.Job
import org.apache.spark.util.Utils

final private[streaming] class DStreamGraph extends Serializable with Logging {

  private val inputStreams = new ArrayBuffer[InputDStream[_]]()
  private val outputStreams = new ArrayBuffer[DStream[_]]()

  var rememberDuration: Duration = null

  def start(time: Time) {
      outputStreams.foreach(_.remember(rememberDuration))
      inputStreams.par.foreach(_.start())
  }

  def generateJobs(time: Time): Seq[Job] = {
    val jobs = this.synchronized {
      outputStreams.flatMap { outputStream =>
        val jobOption = outputStream.generateJob(time)
        jobOption.foreach(_.setCallSite(outputStream.creationSite))
        jobOption
      }
    }
    jobs
  }

  def clearMetadata(time: Time) {outputStreams.foreach(_.clearMetadata(time))}

  def updateCheckpointData(time: Time) {outputStreams.foreach(_.updateCheckpointData(time))
  }

  def clearCheckpointData(time: Time) {outputStreams.foreach(_.clearCheckpointData(time))
  }

  def restoreCheckpointData() {outputStreams.foreach(_.restoreCheckpointData())
  }

}

