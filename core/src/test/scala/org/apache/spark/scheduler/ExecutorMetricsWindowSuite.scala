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

class ExecutorMetricsWindowSuite extends SparkFunSuite {

  test("NOT Ignoring: Adding empty metrics does not cause a crash on startup") {
    val executorWindows = new ExecutorMetricsWindow(5, 5, 5, false)

    val executorMetricsMap = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 0L,
      "OnHeapExecutionMemory" -> 0L,
      "OnHeapStorageMemory" -> 0L,
      "OnHeapTotalMemory" -> 0L,
      "ExecutorAndTasksGCTime" -> 0L,
      "ExecutorAndTasksDuration" -> 0L
    )

    val executorMetrics = new ExecutorMetrics(executorMetricsMap)

    executorWindows.add(executorMetrics)
    val ((diskMean, diskStd), (memoryMean, memoryStd), (gcMean, gcStd)) = executorWindows.get()

    assert(diskMean == 0L)
    assert(diskStd == 0L)
    assert(memoryMean == 0L)
    assert(memoryStd == 0L)
    assert(gcMean == 0L)
    assert(gcStd == 0L)
  }

  test("Ignoring: Adding empty metrics does not cause a crash on startup") {
    val executorWindows = new ExecutorMetricsWindow(5, 5, 5, true)

    val executorMetricsMap = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 0L,
      "OnHeapExecutionMemory" -> 0L,
      "OnHeapStorageMemory" -> 0L,
      "OnHeapTotalMemory" -> 0L,
      "ExecutorAndTasksGCTime" -> 0L,
      "ExecutorAndTasksDuration" -> 0L
    )

    val executorMetrics = new ExecutorMetrics(executorMetricsMap)

    executorWindows.add(executorMetrics)
    val ((diskMean, diskStd), (memoryMean, memoryStd), (gcMean, gcStd)) = executorWindows.get()

    assert(diskMean == 0L)
    assert(diskStd == 0L)
    assert(memoryMean == 0L)
    assert(memoryStd == 0L)
    assert(gcMean == 0L)
    assert(gcStd == 0L)
  }

  test("NOT Ignoring: Adding empty metrics 2 does not cause a crash") {
    val executorWindows = new ExecutorMetricsWindow(5, 5, 5, false)

    val executorMetricsMap = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 0L,
      "OnHeapExecutionMemory" -> 0L,
      "OnHeapStorageMemory" -> 0L,
      "OnHeapTotalMemory" -> 0L,
      "ExecutorAndTasksGCTime" -> 0L,
      "ExecutorAndTasksDuration" -> 0L
    )

    val executorMetrics = new ExecutorMetrics(executorMetricsMap)

    executorWindows.add(executorMetrics)
    executorWindows.add(executorMetrics)
    val ((diskMean, diskStd), (memoryMean, memoryStd), (gcMean, gcStd)) = executorWindows.get()

    assert(diskMean == 0L)
    assert(diskStd == 0L)
    assert(memoryMean == 0L)
    assert(memoryStd == 0L)
    assert(gcMean == 0L)
    assert(gcStd == 0L)
  }

  test("Ignoring: Adding empty metrics 2 does not cause a crash") {
    val executorWindows = new ExecutorMetricsWindow(5, 5, 5, true)

    val executorMetricsMap = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 0L,
      "OnHeapExecutionMemory" -> 0L,
      "OnHeapStorageMemory" -> 0L,
      "OnHeapTotalMemory" -> 0L,
      "ExecutorAndTasksGCTime" -> 0L,
      "ExecutorAndTasksDuration" -> 0L
    )

    val executorMetrics = new ExecutorMetrics(executorMetricsMap)

    executorWindows.add(executorMetrics)
    executorWindows.add(executorMetrics)
    val ((diskMean, diskStd), (memoryMean, memoryStd), (gcMean, gcStd)) = executorWindows.get()

    assert(diskMean == 0L)
    assert(diskStd == 0L)
    assert(memoryMean == 0L)
    assert(memoryStd == 0L)
    assert(gcMean == 0L)
    assert(gcStd == 0L)
  }

  test("NOT Ignoring: Adding valid and invalid metrics repeats the last valid value") {
    val executorWindows = new ExecutorMetricsWindow(5, 5, 5, false)

    val executorMetricsMap0 = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 1048576L,
      "OnHeapExecutionMemory" -> 20L,
      "OnHeapStorageMemory" -> 20L,
      "OnHeapTotalMemory" -> 100L,
      "ExecutorAndTasksGCTime" -> 5L,
      "ExecutorAndTasksDuration" -> 10L
    )

    val executorMetrics0 = new ExecutorMetrics(executorMetricsMap0)

    executorWindows.add(executorMetrics0)

    val ((diskMean0, diskStd0), (memoryMean0, memoryStd0), (gcMean0, gcStd0)) = executorWindows.get()

    assert(diskMean0 == 0L)
    assert(diskStd0 == 0L)
    assert(memoryMean0 == 40L)
    assert(memoryStd0 == 0L)
    assert(gcMean0 == 50L)
    assert(gcStd0 == 0L)
    assert(executorWindows.diskSpillWindow.length == 1)
    assert(executorWindows.memoryWindow.length == 1)
    assert(executorWindows.gcWindow.length == 1)

    val executorMetricsMap1 = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 0L,
      "OnHeapExecutionMemory" -> 0L,
      "OnHeapStorageMemory" -> 0L,
      "OnHeapTotalMemory" -> 0L,
      "ExecutorAndTasksGCTime" -> 0L,
      "ExecutorAndTasksDuration" -> 0L
    )

    val executorMetrics1 = new ExecutorMetrics(executorMetricsMap1)

    executorWindows.add(executorMetrics1)

    val ((diskMean1, diskStd1), (memoryMean1, memoryStd1), (gcMean1, gcStd1)) = executorWindows.get()

    assert(diskMean1 == 0L)
    assert(diskStd1 == 0L)
    assert(memoryMean1 == 40L)
    assert(memoryStd1 == 0L)
    assert(gcMean1 == 50L)
    assert(gcStd1 == 0L)
    assert(executorWindows.diskSpillWindow.length == 2)
    assert(executorWindows.memoryWindow.length == 2)
    assert(executorWindows.gcWindow.length == 2)
  }

  test("NOT Ignoring: Adding valid and invalid metrics repeats the last valid value in window of 1") {
    val executorWindows = new ExecutorMetricsWindow(1, 1, 1, false)

    val executorMetricsMap0 = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 1048576L,
      "OnHeapExecutionMemory" -> 20L,
      "OnHeapStorageMemory" -> 20L,
      "OnHeapTotalMemory" -> 100L,
      "ExecutorAndTasksGCTime" -> 5L,
      "ExecutorAndTasksDuration" -> 10L
    )

    val executorMetrics0 = new ExecutorMetrics(executorMetricsMap0)

    executorWindows.add(executorMetrics0)

    val ((diskMean0, diskStd0), (memoryMean0, memoryStd0), (gcMean0, gcStd0)) = executorWindows.get()

    assert(diskMean0 == 1L)
    assert(diskStd0 == 0L)
    assert(memoryMean0 == 40L)
    assert(memoryStd0 == 0L)
    assert(gcMean0 == 50L)
    assert(gcStd0 == 0L)
    assert(executorWindows.diskSpillWindow.length == 1)
    assert(executorWindows.memoryWindow.length == 1)
    assert(executorWindows.gcWindow.length == 1)

    val executorMetricsMap1 = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 1048576L,
      "OnHeapExecutionMemory" -> 0L,
      "OnHeapStorageMemory" -> 0L,
      "OnHeapTotalMemory" -> 0L,
      "ExecutorAndTasksGCTime" -> 0L,
      "ExecutorAndTasksDuration" -> 0L
    )

    val executorMetrics1 = new ExecutorMetrics(executorMetricsMap1)

    executorWindows.add(executorMetrics1)

    val ((diskMean1, diskStd1), (memoryMean1, memoryStd1), (gcMean1, gcStd1)) = executorWindows.get()

    assert(diskMean1 == 0L)
    assert(diskStd1 == 0L)
    assert(memoryMean1 == 40L)
    assert(memoryStd1 == 0L)
    assert(gcMean1 == 50L)
    assert(gcStd1 == 0L)
    assert(executorWindows.diskSpillWindow.length == 1)
    assert(executorWindows.memoryWindow.length == 1)
    assert(executorWindows.gcWindow.length == 1)
  }

  test("NOT Ignoring: Adding invalid and valid metrics does not cause a crash") {
    val executorWindows = new ExecutorMetricsWindow(5, 5, 5, false)

    val executorMetricsMap0 = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 0L,
      "OnHeapExecutionMemory" -> 0L,
      "OnHeapStorageMemory" -> 0L,
      "OnHeapTotalMemory" -> 0L,
      "ExecutorAndTasksGCTime" -> 0L,
      "ExecutorAndTasksDuration" -> 0L
    )

    val executorMetrics0 = new ExecutorMetrics(executorMetricsMap0)

    executorWindows.add(executorMetrics0)

    assert(executorWindows.diskSpillWindow.length == 1)
    assert(executorWindows.memoryWindow.isEmpty)
    assert(executorWindows.gcWindow.isEmpty)

    val executorMetricsMap1 = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 1048576L,
      "OnHeapExecutionMemory" -> 20L,
      "OnHeapStorageMemory" -> 20L,
      "OnHeapTotalMemory" -> 100L,
      "ExecutorAndTasksGCTime" -> 5L,
      "ExecutorAndTasksDuration" -> 10L
    )

    val executorMetrics1 = new ExecutorMetrics(executorMetricsMap1)

    executorWindows.add(executorMetrics1)

    val ((diskMean, diskStd), (memoryMean, memoryStd), (gcMean, gcStd)) = executorWindows.get()

    assert(diskMean == 0L)
    assert(diskStd == 0L)
    assert(memoryMean == 40L)
    assert(memoryStd == 0L)
    assert(gcMean == 50L)
    assert(gcStd == 0L)
    assert(executorWindows.diskSpillWindow.length == 2)
    assert(executorWindows.memoryWindow.length == 1)
    assert(executorWindows.gcWindow.length == 1)
  }

  test("NOT Ignoring: Adding invalid Memory and GC metrics to empty windows keeps them empty") {
    val executorWindows = new ExecutorMetricsWindow(5, 5, 5, false)

    assert(executorWindows.diskSpillWindow.isEmpty)
    assert(executorWindows.memoryWindow.isEmpty)
    assert(executorWindows.gcWindow.isEmpty)

    val executorMetricsMap = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 0L,
      "OnHeapExecutionMemory" -> 0L,
      "OnHeapStorageMemory" -> 0L,
      "OnHeapTotalMemory" -> 0L,
      "ExecutorAndTasksGCTime" -> 0L,
      "ExecutorAndTasksDuration" -> 0L
    )

    val executorMetrics = new ExecutorMetrics(executorMetricsMap)

    executorWindows.add(executorMetrics)

    assert(!executorWindows.diskSpillWindow.isEmpty)
    assert(executorWindows.memoryWindow.isEmpty)
    assert(executorWindows.gcWindow.isEmpty)
  }

  test("Ignoring: Adding invalid Memory and GC metrics to empty windows keeps them empty") {
    val executorWindows = new ExecutorMetricsWindow(5, 5, 5, true)

    assert(executorWindows.diskSpillWindow.isEmpty)
    assert(executorWindows.memoryWindow.isEmpty)
    assert(executorWindows.gcWindow.isEmpty)

    val executorMetricsMap = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 0L,
      "OnHeapExecutionMemory" -> 0L,
      "OnHeapStorageMemory" -> 0L,
      "OnHeapTotalMemory" -> 0L,
      "ExecutorAndTasksGCTime" -> 0L,
      "ExecutorAndTasksDuration" -> 0L
    )

    val executorMetrics = new ExecutorMetrics(executorMetricsMap)

    executorWindows.add(executorMetrics)

    assert(!executorWindows.diskSpillWindow.isEmpty)
    assert(executorWindows.memoryWindow.isEmpty)
    assert(executorWindows.gcWindow.isEmpty)
  }

  test("NOT Ignoring: Providing 2 heartbeats causes means and standard deviations to update both times") {
    val executorWindows = new ExecutorMetricsWindow(5, 5, 5, false)

    var executorMetricsMap = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 4L,
      "OnHeapExecutionMemory" -> 4L,
      "OnHeapStorageMemory" -> 3L,
      "OnHeapTotalMemory" -> 10L,
      "ExecutorAndTasksGCTime" -> 3L,
      "ExecutorAndTasksDuration" -> 5L
    )

    val executorMetrics = new ExecutorMetrics(executorMetricsMap)

    executorWindows.add(executorMetrics)
    val ((diskMean, diskStd), (memoryMean, memoryStd), (gcMean, gcStd)) = executorWindows.get()

    assert(diskMean == 0L)
    assert(diskStd == 0L)
    assert(memoryMean == 70L)
    assert(memoryStd == 0L)
    assert(gcMean == 60L)
    assert(gcStd == 0L)

    val executorMetricsMap2 = Map(
      "ExecutorAndTaskDiskBytesSpilled" -> 7L,
      "OnHeapExecutionMemory" -> 2L,
      "OnHeapStorageMemory" -> 7L,
      "OnHeapTotalMemory" -> 10L,
      "ExecutorAndTasksGCTime" -> 6L,
      "ExecutorAndTasksDuration" -> 10L
    )

    val executorMetrics2 = new ExecutorMetrics(executorMetricsMap2)

    executorWindows.add(executorMetrics2)
    val ((diskMean2, diskStd2), (memoryMean2, memoryStd2), (gcMean2, gcStd2)) = executorWindows.get()

    assert(diskMean2 == 0L)
    assert(diskStd2 == 0L)
    assert(memoryMean2 == 80L)
    assert(memoryStd2 == 7L)
    assert(gcMean2 == 60L)
    assert(gcStd2 == 0L)
  }

}
