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

package org.apache.spark.shuffle.sort

import java.io.{File, FileInputStream, FileOutputStream}

import com.google.common.io.Closeables
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.scheduler.ShuffleMapTask.ShuffleCoroutineDeps
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleWriter}
import org.apache.spark.storage._
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.ExternalSorter
import org.coroutines.{coroutine, yieldval}

private[spark] class SortShuffleWriter[K, V, C](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, C],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency

  private val blockManager = SparkEnv.get.blockManager

  private var sorter: ExternalSorter[K, V, _] = null

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var mapStatus: MapStatus = null

  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics

  /** Write a bunch of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    sorter = if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }
    sorter.insertAll(records)

    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).
    val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
    val tmp = Utils.tempFileWith(output)
    try {
      val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
      val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
      shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
      }
    }
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        return Option(mapStatus)
      } else {
        return None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        val startTime = System.nanoTime()
        sorter.stop()
        writeMetrics.incWriteTime(System.nanoTime - startTime)
        sorter = null
      }
    }
  }
}

private[spark] object SortShuffleWriter {
  def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {
    // We cannot bypass sorting if we need to do map-side aggregation.
    if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      false
    } else {
      val bypassMergeThreshold: Int = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
      dep.partitioner.numPartitions <= bypassMergeThreshold
    }
  }

  val sortShuffleWriter = coroutine { (context: TaskContext, shuffleCoDeps: ShuffleCoroutineDeps) =>
    val log = shuffleCoDeps.log
    var mapStatus: MapStatus = null
    val rdd = shuffleCoDeps.rdd
    val dep = shuffleCoDeps.dep
    val partition = shuffleCoDeps.partition
    val partitionId = shuffleCoDeps.partitionId

    val records = rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]]
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    val shuffleBlockResolver: IndexShuffleBlockResolver = SparkEnv.get.shuffleManager
      .shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver]
    val blockManager = SparkEnv.get.blockManager
    val depLoc = dep.shuffleHandle.asInstanceOf[BaseShuffleHandle[Any, Any, Any]].dependency
    var sorter: ExternalSorter[Any, Any, Any] = if (dep.mapSideCombine) {
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
      log.warn("[Neptune] SortShuffleWriter coarse-grained Aggregation yield point")
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
        sorter.buffer.insert(sorter.getPartition(kv._1), kv._1, kv._2)
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
        log.error(s"While deleting temp file ${tmp.getAbsolutePath}")
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
            log.error(s"Could not stop writer ${e}")
        }
        throw e
    }
    mapStatus
  }


  val bypassMergeSortShuffleWriter = coroutine { (context: TaskContext, shuffleCoDeps: ShuffleCoroutineDeps) =>
    val log = shuffleCoDeps.log
    val rdd = shuffleCoDeps.rdd
    val dep = shuffleCoDeps.dep
    val partition = shuffleCoDeps.partition
    val partitionId = shuffleCoDeps.partitionId

    val records = rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]]
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    val shuffleBlockResolver: IndexShuffleBlockResolver = SparkEnv.get.shuffleManager
      .shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver]
    val blockManager = SparkEnv.get.blockManager

    var partitionWriters: Array[DiskBlockObjectWriter] = null
    val partitionLengths: Array[Long] = new Array[Long](dep.partitioner.numPartitions)

    assert(partitionWriters == null)
    if (!records.hasNext) {
      shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, partitionId, partitionLengths, null)
      MapStatus(blockManager.shuffleServerId, partitionLengths)
    } else {
      val serInstance = dep.serializer.newInstance()
      //    var openStartTime = System.nanoTime()
      partitionWriters = new Array[DiskBlockObjectWriter](dep.partitioner.numPartitions)
      val partitionWriterSegments = new Array[FileSegment](dep.partitioner.numPartitions)
      val fileBufferSize = SparkEnv.get.conf.getSizeAsKb("spark.shuffle.file.buffer", "32k") * 1024
      val transferToEnabled = SparkEnv.get.conf.getBoolean("spark.file.transferTo", true)
      var i = 0
      while (i < dep.partitioner.numPartitions) {
        if (context.isPaused()) {
          yieldval(0)
        }
        val tempShuffleBlockIdPlusFile = blockManager.diskBlockManager.createTempShuffleBlock
        val file = tempShuffleBlockIdPlusFile._2
        val blockId = tempShuffleBlockIdPlusFile._1
        partitionWriters(i) = blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize.toInt, writeMetrics)
        i += 1
      }
      //    writeMetrics.incWriteTime(System.nanoTime() - openStartTime)

      while (records.hasNext) {
        if (context.isPaused()) {
          yieldval(0)
        }
        val record = records.next()
        val key = record._1
        partitionWriters(dep.partitioner.getPartition(key)).write(key, record._2)
      }

      i = 0
      while (i < dep.partitioner.numPartitions) {
        if (context.isPaused()) {
          yieldval(0)
        }
        val writer = partitionWriters(i)
        partitionWriterSegments(i) = writer.commitAndGet
        writer.close()
        i += 1
      }

      val output = shuffleBlockResolver.getDataFile(dep.shuffleId, partitionId)
      val tmp = Utils.tempFileWith(output)
      try {
        //      partitionLengths = writePartitionedFile(tmp)
        // We were NOT passed an empty iterator
        if (partitionWriters != null) {
          val out = new FileOutputStream(tmp, true)
          //        var writeStartTime = System.nanoTime()
          var threwException = false
          try {
            i = 0
            while (i < dep.partitioner.numPartitions) {
              if (context.isPaused()) {
                yieldval(0)
              }
              val file: File = partitionWriterSegments(i).file
              if (file.exists()) {
                val in: FileInputStream = new FileInputStream(file)
                var copyThrewException = true
                try {
                  partitionLengths(i) = Utils.copyStream(in, out, false, transferToEnabled)
                  copyThrewException = false;
                } finally {
                  Closeables.close(in, copyThrewException)
                }
                if (!file.delete()) {
                  log.error("Unable to delete file for partition {}", i);
                }
              }
              i += 1
            }
            threwException = false;
          } finally {
            Closeables.close(out, threwException);
            //          writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
          }
        }
        shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, partitionId, partitionLengths, tmp)
      }
      finally {
        if (tmp.exists() && !tmp.delete()) {
          log.error(s"Error while deleting temp file ${tmp.getAbsolutePath}")
        }
      }
      MapStatus(blockManager.shuffleServerId, partitionLengths)
    }
  }

}
