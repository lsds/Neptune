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
package org.apache.spark.metrics

import java.lang.management.{BufferPoolMXBean, ManagementFactory}
import javax.management.ObjectName

import scala.collection.JavaConverters._
import org.apache.spark.SparkEnv
import org.apache.spark.executor.ProcfsMetricsGetter
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.memory.MemoryManager

import scala.collection.mutable

/**
 * Executor metric types for executor-level metrics stored in ExecutorMetrics.
 */
sealed trait ExecutorMetricType {
  private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long]
  private[spark] def names: Seq[String]
}

sealed trait SingleValueExecutorMetricType extends ExecutorMetricType {
  override private[spark] def names = {
    Seq(getClass().getName().
      stripSuffix("$").split("""\.""").last)
  }
  override private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long] = {
    val metrics = new Array[Long](1)
    metrics(0) = getMetricValue(memoryManager)
    metrics
  }
  private[spark] def getMetricValue(memoryManager: MemoryManager): Long
}

case object ProcessTreeMetrics extends ExecutorMetricType {
  override val names = Seq(
    "ProcessTreeJVMVMemory",
    "ProcessTreeJVMRSSMemory",
    "ProcessTreePythonVMemory",
    "ProcessTreePythonRSSMemory",
    "ProcessTreeOtherVMemory",
    "ProcessTreeOtherRSSMemory")

  override private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long] = {
    val allMetrics = ProcfsMetricsGetter.pTreeInfo.computeAllMetrics()
    val processTreeMetrics = new Array[Long](names.length)
    processTreeMetrics(0) = allMetrics.jvmVmemTotal
    processTreeMetrics(1) = allMetrics.jvmRSSTotal
    processTreeMetrics(2) = allMetrics.pythonVmemTotal
    processTreeMetrics(3) = allMetrics.pythonRSSTotal
    processTreeMetrics(4) = allMetrics.otherVmemTotal
    processTreeMetrics(5) = allMetrics.otherRSSTotal
    processTreeMetrics
  }
}

private[spark] abstract class MemoryManagerExecutorMetricType(f: MemoryManager => Long)
  extends SingleValueExecutorMetricType {

  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    f(memoryManager)
  }
}

private[spark] abstract class MBeanExecutorMetricType(mBeanName: String)
  extends SingleValueExecutorMetricType {

  private val bean = ManagementFactory.newPlatformMXBeanProxy(
    ManagementFactory.getPlatformMBeanServer,
    new ObjectName(mBeanName).toString, classOf[BufferPoolMXBean])

  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    bean.getMemoryUsed
  }
}

case object JVMHeapMemory extends SingleValueExecutorMetricType {

  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    ManagementFactory.getMemoryMXBean.getHeapMemoryUsage().getUsed()
  }
}

case object JVMOffHeapMemory extends SingleValueExecutorMetricType {

  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    ManagementFactory.getMemoryMXBean.getNonHeapMemoryUsage().getUsed()
  }
}

case object GarbageCollectionMetrics extends ExecutorMetricType with Logging {
  private var nonBuiltInCollectors: Seq[String] = Nil

  override val names = Seq(
    "MinorGCCount",
    "MinorGCTime",
    "MajorGCCount",
    "MajorGCTime"
  )

  /* We builtin some common GC collectors which categorized as young generation and old */
  private[spark] val YOUNG_GENERATION_BUILTIN_GARBAGE_COLLECTORS = Seq(
    "Copy",
    "PS Scavenge",
    "ParNew",
    "G1 Young Generation"
  )

  private[spark] val OLD_GENERATION_BUILTIN_GARBAGE_COLLECTORS = Seq(
    "MarkSweepCompact",
    "PS MarkSweep",
    "ConcurrentMarkSweep",
    "G1 Old Generation"
  )

  private lazy val youngGenerationGarbageCollector: Seq[String] = {
    SparkEnv.get.conf.get(config.EVENT_LOG_GC_METRICS_YOUNG_GENERATION_GARBAGE_COLLECTORS)
  }

  private lazy val oldGenerationGarbageCollector: Seq[String] = {
    SparkEnv.get.conf.get(config.EVENT_LOG_GC_METRICS_OLD_GENERATION_GARBAGE_COLLECTORS)
  }

  override private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long] = {
    val gcMetrics = new Array[Long](names.length) // minorCount, minorTime, majorCount, majorTime
    ManagementFactory.getGarbageCollectorMXBeans.asScala.foreach { mxBean =>
      if (youngGenerationGarbageCollector.contains(mxBean.getName)) {
        gcMetrics(0) = mxBean.getCollectionCount
        gcMetrics(1) = mxBean.getCollectionTime
      } else if (oldGenerationGarbageCollector.contains(mxBean.getName)) {
        gcMetrics(2) = mxBean.getCollectionCount
        gcMetrics(3) = mxBean.getCollectionTime
      } else if (!nonBuiltInCollectors.contains(mxBean.getName)) {
        nonBuiltInCollectors = mxBean.getName +: nonBuiltInCollectors
        // log it when first seen
        logWarning(s"To enable non-built-in garbage collector(s) " +
          s"$nonBuiltInCollectors, users should configure it(them) to " +
          s"${config.EVENT_LOG_GC_METRICS_YOUNG_GENERATION_GARBAGE_COLLECTORS.key} or " +
          s"${config.EVENT_LOG_GC_METRICS_OLD_GENERATION_GARBAGE_COLLECTORS.key}")
      } else {
        // do nothing
      }
    }
    gcMetrics
  }
}

case object GCAndDurationMetrics extends ExecutorMetricType {

  override val names = Seq(
    "ExecutorGCTime",
    "ExecutorDuration",
    "ExecutorAndTasksGCTime",
    "ExecutorAndTasksDuration",
    "ExecutorAndTaskMemoryBytesSpilled",
    "ExecutorAndTaskDiskBytesSpilled"
  )

  var executorGCTime = 0L
  var executorDuration = 0L
  var executorAndTaskGCTime = 0L
  var executorAndTaskDuration = 0L
  var executorAndTaskMemoryBytesSpilled = 0L
  var executorAndTaskDiskBytesSpilled = 0L

  override private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long] = {
    val metrics = new Array[Long](names.length)
    metrics(0) = executorGCTime
    metrics(1) = executorDuration
    metrics(2) = executorAndTaskGCTime
    metrics(3) = executorAndTaskDuration
    metrics(4) = executorAndTaskMemoryBytesSpilled
    metrics(5) = executorAndTaskDiskBytesSpilled
    metrics
  }

  def update(executorGCTime: Long,
             executorDuration: Long,
             executorAndTaskGCTime: Long,
             executorAndTaskDuration: Long,
             executorAndTaskMemoryBytesSpilled: Long,
             executorAndTaskDiskBytesSpilled: Long): Unit = {
    this.executorGCTime = executorGCTime
    this.executorDuration = executorDuration
    this.executorAndTaskGCTime = executorAndTaskGCTime
    this.executorAndTaskDuration = executorAndTaskDuration
    this.executorAndTaskMemoryBytesSpilled = executorAndTaskMemoryBytesSpilled
    this.executorAndTaskDiskBytesSpilled = executorAndTaskDiskBytesSpilled
  }

}

case object OnHeapExecutionMemory extends MemoryManagerExecutorMetricType(
  _.onHeapExecutionMemoryUsed)

case object OffHeapExecutionMemory extends MemoryManagerExecutorMetricType(
  _.offHeapExecutionMemoryUsed)

case object OnHeapStorageMemory extends MemoryManagerExecutorMetricType(
  _.onHeapStorageMemoryUsed)

case object OffHeapStorageMemory extends MemoryManagerExecutorMetricType(
  _.offHeapStorageMemoryUsed)

case object OnHeapUnifiedMemory extends MemoryManagerExecutorMetricType(
  (m => m.onHeapExecutionMemoryUsed + m.onHeapStorageMemoryUsed))

case object OffHeapUnifiedMemory extends MemoryManagerExecutorMetricType(
  (m => m.offHeapExecutionMemoryUsed + m.offHeapStorageMemoryUsed))

case object OnHeapTotalMemory extends MemoryManagerExecutorMetricType(
 _.onHeapTotalMemory
)

case object DirectPoolMemory extends MBeanExecutorMetricType(
  "java.nio:type=BufferPool,name=direct")

case object MappedPoolMemory extends MBeanExecutorMetricType(
  "java.nio:type=BufferPool,name=mapped")

private[spark] object ExecutorMetricType {
  // List of all executor metric types
  val metricGetters = IndexedSeq(
    JVMHeapMemory,
    JVMOffHeapMemory,
    OnHeapExecutionMemory,
    OffHeapExecutionMemory,
    OnHeapStorageMemory,
    OffHeapStorageMemory,
    OnHeapUnifiedMemory,
    OffHeapUnifiedMemory,
    DirectPoolMemory,
    MappedPoolMemory,
    ProcessTreeMetrics,
    GarbageCollectionMetrics,
    GCAndDurationMetrics,
    OnHeapTotalMemory
  )

  val (metricToOffset, numMetrics) = {
    var numberOfMetrics = 0
    val definedMetricsAndOffset = mutable.LinkedHashMap.empty[String, Int]
    metricGetters.foreach { m =>
      var metricInSet = 0
      (0 until m.names.length).foreach { idx =>
        definedMetricsAndOffset += (m.names(idx) -> (idx + numberOfMetrics))
      }
      numberOfMetrics += m.names.length
    }
    (definedMetricsAndOffset, numberOfMetrics)
  }

  def update(executorGCTime: Long,
             executorDuration: Long,
             executorAndTaskGCTime: Long,
             executorAndTaskDuration: Long,
             executorAndTaskMemoryBytesSpilled: Long,
             executorAndTaskDiskBytesSpilled: Long): Unit = {
    GCAndDurationMetrics.update(executorGCTime,
      executorDuration,
      executorAndTaskGCTime,
      executorAndTaskDuration,
      executorAndTaskMemoryBytesSpilled,
      executorAndTaskDiskBytesSpilled)
  }
}
