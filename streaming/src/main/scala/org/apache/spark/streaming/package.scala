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

package org.apache.spark

/**
 * Spark Streaming functionality. [[org.apache.spark.streaming.StreamingContext]] serves as the main
 * entry point to Spark Streaming, while [[org.apache.spark.streaming.dstream.DStream]] is the data
 * type representing a continuous sequence of RDDs, representing a continuous stream of data.
  * [[org.apache.spark.streaming。StreamingContext]]是Spark流的主要入口点，
  * 而[[org.apache. Spark . stream.dstream]]则是Spark流的主要入口点。
  * 是表示RDDs连续序列的数据类型，表示连续的数据流。
 *
 * In addition, [[org.apache.spark.streaming.dstream.PairDStreamFunctions]] contains operations
 * available only on DStreams
 * of key-value pairs, such as `groupByKey` and `reduceByKey`. These operations are automatically
 * available on any DStream of the right type (e.g. DStream[(Int, Int)] through implicit
 * conversions.
  * 此外,[[org.apache.spark.streaming.dstream。PairDStreamFunctions]]包含仅在键值对的DStreams上可用的操作，
  * 如“groupByKey”和“reduceByKey”。
  * 这些操作在任何正确类型的DStream(例如DStream[(Int, Int)])上通过隐式转换自动可用。
 *
 * For the Java API of Spark Streaming, take a look at the
 * [[org.apache.spark.streaming.api.java.JavaStreamingContext]] which serves as the entry point, and
 * the [[org.apache.spark.streaming.api.java.JavaDStream]] and the
 * [[org.apache.spark.streaming.api.java.JavaPairDStream]] which have the DStream functionality.
  * 对于Spark流的Java API，请查看[[org.apache.Spark.API.Java]]。它作为入口点，
  * 以及[[org.apache.spark.stream .api.java]]。和[[org.apache.spark.stream .api.java]]。
  * 具有DStream功能的JavaPairDStream]。
 */
package object streaming {
  // For package docs only
}
