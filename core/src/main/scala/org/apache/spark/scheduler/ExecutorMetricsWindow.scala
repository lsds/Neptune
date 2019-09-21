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

import scala.collection.mutable.ArrayBuffer

private[spark]
class ExecutorMetricsWindow(windowSize: Int,
                            bucketSizePercentage: Int,
                            bucketSizeMB: Int,
                            ignoreMissing: Boolean) {

  private[spark] val diskSpillWindow: ArrayBuffer[Long] = ArrayBuffer.empty[Long]
  private[spark] val memoryWindow: ArrayBuffer[Long] = ArrayBuffer.empty[Long]
  private[spark] val gcWindow: ArrayBuffer[Long] = ArrayBuffer.empty[Long]

  private var previousDiskSpillMegabytes: Long = 0L
  private var previousGCTime: Long = 0L
  private var previousDuration: Long = 0L

  private var diskSpillMean: Long = 0L
  private var diskSpillStd: Long = 0L
  private var memoryMean: Long = 0L
  private var memoryStd: Long = 0L
  private var gcMean: Long = 0L
  private var gcStd: Long = 0L

  def get(): ((Long, Long), (Long, Long), (Long, Long)) = {
    ((diskSpillMean, diskSpillStd), (memoryMean, memoryStd), (gcMean, gcStd))
  }

  def add(executorMetrics: ExecutorMetrics): Unit = {
    bytesSpilled(executorMetrics)
    memoryUsed(executorMetrics)
    gc(executorMetrics)
  }

  private[spark] def bytesSpilled(executorMetrics: ExecutorMetrics): Unit = {
    val bytesSpilledToDisk = executorMetrics.getMetricValue("ExecutorAndTaskDiskBytesSpilled")
    val megaBytesSpilledToDisk = bytesSpilledToDisk / (1024 * 1024)

    if (diskSpillWindow.isEmpty) {
      previousDiskSpillMegabytes = megaBytesSpilledToDisk
      diskSpillWindow.+=(roundUpToNearestBucket(megaBytesSpilledToDisk, bucketSizeMB))
    } else {
      val diskSpillSinceLastHeartbeat = megaBytesSpilledToDisk - previousDiskSpillMegabytes
      previousDiskSpillMegabytes = megaBytesSpilledToDisk
      diskSpillWindow.+=(roundUpToNearestBucket(diskSpillSinceLastHeartbeat, bucketSizeMB))
      if (diskSpillWindow.length > windowSize) {
        diskSpillWindow.remove(0)
      }
    }

    diskSpillMean = mean(diskSpillWindow)
    diskSpillStd = std(diskSpillWindow)
  }

  private[spark] def memoryUsed(executorMetrics: ExecutorMetrics): Unit = {
    val onHeapExecutionMemory = executorMetrics.getMetricValue("OnHeapExecutionMemory")
    val onHeapStorageMemory = executorMetrics.getMetricValue("OnHeapStorageMemory")
    val onHeapTotalMemory = executorMetrics.getMetricValue("OnHeapTotalMemory")

    // If the window IS empty add an entry only if the divisor IS NOT zero.
    if (memoryWindow.isEmpty) {
      if (onHeapTotalMemory != 0) {
        val percentageOnHeapMemoryUsed = (onHeapExecutionMemory + onHeapStorageMemory) * 100L / onHeapTotalMemory

        memoryWindow.+=(roundUpToNearestBucket(percentageOnHeapMemoryUsed, bucketSizePercentage))

        memoryMean = mean(memoryWindow)
        memoryStd = std(memoryWindow)
      }
    } else {
      // If the window IS NOT empty check whether the divisor IS zero.
      if (onHeapTotalMemory != 0) {
        // If the divisor IS NOT zero check whether we need to remove something from the window before adding a new entry.
        val percentageOnHeapMemoryUsed = (onHeapExecutionMemory + onHeapStorageMemory) * 100L / onHeapTotalMemory

        memoryWindow.+=(roundUpToNearestBucket(percentageOnHeapMemoryUsed, bucketSizePercentage))

        if (memoryWindow.length > windowSize) {
          memoryWindow.remove(0)
        }

        memoryMean = mean(memoryWindow)
        memoryStd = std(memoryWindow)
      } else if (!ignoreMissing) {
        // If the divisor IS zero repeat the last entry ONLY IF we are not ignoring missing values.
        memoryWindow.+=(memoryWindow.last)

        if (memoryWindow.length > windowSize) {
          memoryWindow.remove(0)
        }

        memoryMean = mean(memoryWindow)
        memoryStd = std(memoryWindow)
      }
    }
  }

  private[spark] def gc(executorMetrics: ExecutorMetrics): Unit = {
    val timeSpentDoingGC = executorMetrics.getMetricValue("ExecutorAndTasksGCTime")
    val timeSpentDoingWork = executorMetrics.getMetricValue("ExecutorAndTasksDuration")

    // If the window IS empty add an entry only if the divisor is NOT zero.
    if (gcWindow.isEmpty) {
      if (timeSpentDoingWork > previousDuration) {
        val gcSinceLastHeartbeat = timeSpentDoingGC - previousGCTime
        val durationSinceLastHeartbeat = timeSpentDoingWork - previousDuration

        previousGCTime = timeSpentDoingGC
        previousDuration = timeSpentDoingWork

        val percentageTimeSpentDoingGC = gcSinceLastHeartbeat * 100L / durationSinceLastHeartbeat

        gcWindow.+=(roundUpToNearestBucket(percentageTimeSpentDoingGC, bucketSizePercentage))

        gcMean = mean(gcWindow)
        gcStd = std(gcWindow)
      }
    } else {
      // If the window IS NOT empty check whether the divisor IS zero.
      if (timeSpentDoingWork > previousDuration) {
        val gcSinceLastHeartbeat = timeSpentDoingGC - previousGCTime
        val durationSinceLastHeartbeat = timeSpentDoingWork - previousDuration

        previousGCTime = timeSpentDoingGC
        previousDuration = timeSpentDoingWork

        val percentageTimeSpentDoingGC = gcSinceLastHeartbeat * 100L / durationSinceLastHeartbeat

        gcWindow.+=(roundUpToNearestBucket(percentageTimeSpentDoingGC, bucketSizePercentage))

        if (gcWindow.length > windowSize) {
          gcWindow.remove(0)
        }

        gcMean = mean(gcWindow)
        gcStd = std(gcWindow)
      } else if (!ignoreMissing) {
        // If the divisor IS zero repeat the last entry ONLY IF we are not ignoring missing values.
        gcWindow.+=(gcWindow.last)
        if (gcWindow.length > windowSize) {
          gcWindow.remove(0)
        }

        gcMean = mean(gcWindow)
        gcStd = std(gcWindow)
      }
    }
  }

  private[spark] def mean(window: ArrayBuffer[Long]): Long = {
    var sum = 0L
    for (element <- window) {
      sum += element
    }
    sum / window.length
  }

  private[spark] def std(window: ArrayBuffer[Long]): Long = {
    val meanValue = mean(window)
    val numberOfElements = window.length

    var sum = 0.0

    for (element <- window) {
      sum = math.pow(math.abs(element - meanValue).toDouble, 2.0)
    }

    math.sqrt(sum / numberOfElements).toLong
  }

  private[spark] def roundUpToNearestBucket(value: Long, bucketSize: Int): Long = {
    (math.ceil(value / bucketSize) * bucketSize).toLong
  }
}
