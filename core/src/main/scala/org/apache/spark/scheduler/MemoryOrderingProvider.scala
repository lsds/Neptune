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

package org.apache.spark.scheduler

import org.apache.spark.executor.ExecutorMetrics

import scala.collection.mutable

class MemoryOrderingProvider(windowSize: Int,
                             bucketSizePercentage: Int,
                             bucketSizeMB: Int,
                             ignoreMissing: Boolean) {
  private[spark] val executorToExecutorWindows: mutable.HashMap[String, ExecutorMetricsWindow] = mutable.HashMap.empty[String, ExecutorMetricsWindow]
  private[spark] var ordering: List[String] = List.empty[String]

  def update(execId: String, executorMetrics: ExecutorMetrics): Unit = {
    val executorWindows = executorToExecutorWindows.getOrElseUpdate(execId,
      new ExecutorMetricsWindow(windowSize, bucketSizePercentage, bucketSizeMB, ignoreMissing))
    executorWindows.add(executorMetrics)
    setOrdering()
  }

  private[spark] def setOrdering(): Unit = {
    ordering = executorToExecutorWindows
      .iterator
      .map(keyAndValue => {
        (keyAndValue._2.get(), keyAndValue._1)
      })
      .toList
      .sorted
      .map(valueAndKey => valueAndKey._2)
  }

  def getOrdering(): List[String] = ordering
}
