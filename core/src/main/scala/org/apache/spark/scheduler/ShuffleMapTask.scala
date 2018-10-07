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

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.Properties

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle._
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.ExternalSorter
import org.coroutines.{call, coroutine, yieldval, ~>, ~~>}

/**
 * A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
 * specified in the ShuffleDependency).
 *
 * See [[org.apache.spark.scheduler.Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param taskBinary broadcast version of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 * @param serializedTaskMetrics a `TaskMetrics` that is created and serialized on the driver side
 *                              and sent to executor side.
 *
 * The parameters below are optional:
 * @param jobId id of the job this task belongs to
 * @param appId id of the app this task belongs to
 * @param appAttemptId attempt id of the app this task belongs to
 */
private[spark] class ShuffleMapTask(
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation],
    localProperties: Properties,
    serializedTaskMetrics: Array[Byte],
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None,
    isPausable: Boolean = false)
  extends Task[MapStatus](stageId, stageAttemptId, partition.index, localProperties,
    serializedTaskMetrics, jobId, appId, appAttemptId, isPausable)
  with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int) {
    this(0, 0, null, new Partition { override def index: Int = 0 }, null, new Properties, null)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def runTask(context: TaskContext): MapStatus = {
    // Deserialize the RDD using the broadcast variable.
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTime = System.currentTimeMillis()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    val manager = SparkEnv.get.shuffleManager
    val records = rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]]
    if (!context.isPausable()) {
      var writer: ShuffleWriter[Any, Any] = null
      try {
        writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
        writer.write(records)
        writer.stop(success = true).get
      } catch {
        case e: Exception =>
          try {
            if (writer != null) {
              writer.stop(success = false)
            }
          } catch {
            case e: Exception =>
              log.debug("Could not stop writer", e)
          }
          throw e
      }
    } else {
      val shuffleBlockResolver: IndexShuffleBlockResolver = manager
        .shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver]
      val depLoc = dep.shuffleHandle.asInstanceOf[BaseShuffleHandle[Any, Any, Any]].dependency
      val blockManager = SparkEnv.get.blockManager
      var sorter: ExternalSorter[Any, Any, Any] = null
      var mapStatus: MapStatus = null
      val writeMetrics = context.taskMetrics().shuffleWriteMetrics


      val shuffleWriteCoFunc: (TaskContext, Any) ~> (Int, MapStatus) =
        coroutine { (context: TaskContext, t: Any) => {

          sorter = if (dep.mapSideCombine) {
            require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
            new ExternalSorter[Any, Any, Any](
              context, depLoc.aggregator, Some(dep.partitioner), depLoc.keyOrdering, dep.serializer)
          } else {
            // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
            // care whether the keys get sorted in each partition; that will be done on the reduce side
            // if the operation being run is sortByKey.
            new ExternalSorter[Any, Any, Any](
              context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
          }
          //  sorter.insertAll(records) Implementation
          if (context.isPaused()) {
            yieldval(0)
          }
          val shouldCombine = depLoc.aggregator.isDefined
          if (shouldCombine) {
            log.warn("[ShuffleWrite] NEPTUNE coarse-grained yield point")
            // Combine values in-memory first using our AppendOnlyMap
            val mergeValue = depLoc.aggregator.get.mergeValue
            val createCombiner = depLoc.aggregator.get.createCombiner
            var kv: Product2[Any, Any] = null
            val update = (hadValue: Boolean, oldValue: Any) => {
              if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
            }
            while (records.hasNext) {
              sorter.addElementsRead()
              kv = records.next()
              sorter.map.changeValue((sorter.getPartition(kv._1), kv._1), update)
              sorter.maybeSpillCollection(usingMap = true)
            }
          } else {
            // Stick values into our buffer
            while (records.hasNext) {
              if (context.isPaused()) {
                yieldval(0)
              }
              sorter.addElementsRead()
              val kv = records.next()
              sorter.buffer.insert(sorter.getPartition(kv._1), kv._1, kv._2.asInstanceOf[Any])
              sorter.maybeSpillCollection(usingMap = false)
            }
          }
          //  sorter.insertAll(records) Implementation Up to HERE
          val output = shuffleBlockResolver.getDataFile(dep.shuffleId, partitionId)
          val tmp = Utils.tempFileWith(output)
          try {
            val blockId = ShuffleBlockId(dep.shuffleId, partitionId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
            val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
            shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, partitionId, partitionLengths, tmp)
            mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
          } finally {
            if (tmp.exists() && !tmp.delete()) {
              logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
            }
          }
          try {
            // Stop Code
            try {
              ()
            } finally {
              // Clean up our sorter, which may have its own intermediate files
              if (sorter != null) {
                val startTime = System.nanoTime()
                sorter.stop()
                writeMetrics.incWriteTime(System.nanoTime - startTime)
                sorter = null
              }
            }
          } catch {
            case e: Exception =>
              try {
                ()
              } catch {
                case e: Exception =>
                  log.debug("Could not stop writer", e)
              }
              throw e
          }
          mapStatus
        }
       }
      if (context.getcoInstance() == null) {
        val shuffleWriteCoFuncCast = shuffleWriteCoFunc.asInstanceOf[(TaskContext, Any) ~> (Any, Any)]
        context.setCoInstance(call(shuffleWriteCoFuncCast(context, None.asInstanceOf[Any])))
      }
      // Run up to the next yield Point
      if(!context.getcoInstance().isCompleted && context.getcoInstance().pull) {
        return null
      } else {
        // Need to return the actual result used by the DAGScheduler
        return mapStatus
      }
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
