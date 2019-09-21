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

import org.apache.spark.SparkFunSuite
import org.apache.spark.executor.ExecutorMetrics

class MemoryOrderingProviderSuite extends SparkFunSuite {

  test("Empty statistics returns empty ordering") {
    val executorStatistics = new MemoryOrderingProvider(5, 5, 5, false)

    val ordering = executorStatistics.getOrdering()

    assert(ordering.isEmpty)
  }

  test("After supplying heartbeats differing on bytes spilled the ordering of the executors is correct") {
    val executorStatistics = new MemoryOrderingProvider(5, 5, 5, false)

    val executor0MetricsMap = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 5242880L,
      "OnHeapExecutionMemory" -> 7L,
      "OnHeapStorageMemory" -> 2L,
      "OnHeapTotalMemory" -> 10L,
      "ExecutorAndTasksGCTime" -> 5L,
      "ExecutorAndTasksDuration" -> 10L
    )

    val executor1MetricsMap = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 1048576L,
      "OnHeapExecutionMemory" -> 7L,
      "OnHeapStorageMemory" -> 2L,
      "OnHeapTotalMemory" -> 10L,
      "ExecutorAndTasksGCTime" -> 5L,
      "ExecutorAndTasksDuration" -> 10L
    )

    val executor0Metrics = new ExecutorMetrics(executor0MetricsMap)
    val executor1Metrics = new ExecutorMetrics(executor1MetricsMap)

    executorStatistics.update("executor0", executor0Metrics)
    executorStatistics.update("executor1", executor1Metrics)

    val ordering = executorStatistics.getOrdering()

    assert(ordering.length == 2)
    assert(ordering(0) == "executor1")
    assert(ordering(1) == "executor0")
  }

  test("After supplying heartbeats differing on memory used the ordering of the executors is correct") {
    val executorStatistics = new MemoryOrderingProvider(5, 5, 5, false)

    val executor0MetricsMap = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 2097152L,
      "OnHeapExecutionMemory" -> 7L,
      "OnHeapStorageMemory" -> 2L,
      "OnHeapTotalMemory" -> 10L,
      "ExecutorAndTasksGCTime" -> 5L,
      "ExecutorAndTasksDuration" -> 10L
    )

    val executor1MetricsMap = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 1048576L,
      "OnHeapExecutionMemory" -> 6L,
      "OnHeapStorageMemory" -> 2L,
      "OnHeapTotalMemory" -> 10L,
      "ExecutorAndTasksGCTime" -> 5L,
      "ExecutorAndTasksDuration" -> 10L
    )

    val executor0Metrics = new ExecutorMetrics(executor0MetricsMap)
    val executor1Metrics = new ExecutorMetrics(executor1MetricsMap)

    executorStatistics.update("executor0", executor0Metrics)
    executorStatistics.update("executor1", executor1Metrics)

    val ordering = executorStatistics.getOrdering()

    assert(ordering.length == 2)
    assert(ordering(0) == "executor1")
    assert(ordering(1) == "executor0")
  }

  test("After supplying heartbeats differing on GC time the ordering of the executors is correct") {
    val executorStatistics = new MemoryOrderingProvider(5, 5, 5, false)

    val executor0MetricsMap = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 2097152L,
      "OnHeapExecutionMemory" -> 75L,
      "OnHeapStorageMemory" -> 15L,
      "OnHeapTotalMemory" -> 100L,
      "ExecutorAndTasksGCTime" -> 5L,
      "ExecutorAndTasksDuration" -> 10L
    )

    val executor1MetricsMap = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 1048576L,
      "OnHeapExecutionMemory" -> 71L,
      "OnHeapStorageMemory" -> 19L,
      "OnHeapTotalMemory" -> 100L,
      "ExecutorAndTasksGCTime" -> 4L,
      "ExecutorAndTasksDuration" -> 10L
    )

    val executor0Metrics = new ExecutorMetrics(executor0MetricsMap)
    val executor1Metrics = new ExecutorMetrics(executor1MetricsMap)

    executorStatistics.update("executor0", executor0Metrics)
    executorStatistics.update("executor1", executor1Metrics)

    val ordering = executorStatistics.getOrdering()

    assert(ordering.length == 2)
    assert(ordering(0) == "executor1")
    assert(ordering(1) == "executor0")
  }

  test("StrangeBehaviour") {
    val executor0MetricsMap = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 5242880L,
      "OnHeapExecutionMemory" -> 7L,
      "OnHeapStorageMemory" -> 2L,
      "OnHeapTotalMemory" -> 10L,
      "ExecutorAndTasksGCTime" -> 5L,
      "ExecutorAndTasksDuration" -> 10L
    )

    val executor1MetricsMap = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 1048576L,
      "OnHeapExecutionMemory" -> 2L,
      "OnHeapStorageMemory" -> 1L,
      "OnHeapTotalMemory" -> 10L,
      "ExecutorAndTasksGCTime" -> 0L,
      "ExecutorAndTasksDuration" -> 10L
    )

    val executor2MetricsMap = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 2097152L,
      "OnHeapExecutionMemory" -> 1L,
      "OnHeapStorageMemory" -> 1L,
      "OnHeapTotalMemory" -> 10L,
      "ExecutorAndTasksGCTime" -> 1L,
      "ExecutorAndTasksDuration" -> 10L
    )

    val executor3MetricsMap = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 3145728L,
      "OnHeapExecutionMemory" -> 1L,
      "OnHeapStorageMemory" -> 1L,
      "OnHeapTotalMemory" -> 10L,
      "ExecutorAndTasksGCTime" -> 0L,
      "ExecutorAndTasksDuration" -> 10L
    )

    val executor0Metrics = new ExecutorMetrics(executor0MetricsMap)
    val executor1Metrics = new ExecutorMetrics(executor1MetricsMap)
    val executor2Metrics = new ExecutorMetrics(executor2MetricsMap)
    val executor3Metrics = new ExecutorMetrics(executor3MetricsMap)

    val memoryOrderingProvider = new MemoryOrderingProvider(5, 5, 5, true)

    memoryOrderingProvider.update("executor0", executor0Metrics)
    memoryOrderingProvider.update("executor1", executor1Metrics)
    memoryOrderingProvider.update("executor2", executor2Metrics)
    memoryOrderingProvider.update("executor3", executor3Metrics)

    val expectedOrdering = "executor3" :: "executor2" :: "executor1" :: "executor0" :: Nil

    print(memoryOrderingProvider.getOrdering())

    assert(memoryOrderingProvider.getOrdering().equals(expectedOrdering))
  }

}
