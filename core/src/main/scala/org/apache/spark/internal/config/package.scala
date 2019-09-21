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

package org.apache.spark.internal

import java.util.concurrent.TimeUnit

import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.metrics.GarbageCollectionMetrics
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.util.Utils

package object config {

  private[spark] val DRIVER_CLASS_PATH =
    ConfigBuilder(SparkLauncher.DRIVER_EXTRA_CLASSPATH).stringConf.createOptional

  private[spark] val DRIVER_JAVA_OPTIONS =
    ConfigBuilder(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS).stringConf.createOptional

  private[spark] val DRIVER_LIBRARY_PATH =
    ConfigBuilder(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH).stringConf.createOptional

  private[spark] val DRIVER_USER_CLASS_PATH_FIRST =
    ConfigBuilder("spark.driver.userClassPathFirst").booleanConf.createWithDefault(false)

  private[spark] val DRIVER_MEMORY = ConfigBuilder("spark.driver.memory")
    .doc("Amount of memory to use for the driver process, in MiB unless otherwise specified.")
    .bytesConf(ByteUnit.MiB)
    .createWithDefaultString("1g")

  private[spark] val DRIVER_MEMORY_OVERHEAD = ConfigBuilder("spark.driver.memoryOverhead")
    .doc("The amount of off-heap memory to be allocated per driver in cluster mode, " +
      "in MiB unless otherwise specified.")
    .bytesConf(ByteUnit.MiB)
    .createOptional

  private[spark] val EVENT_LOG_COMPRESS =
    ConfigBuilder("spark.eventLog.compress")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EVENT_LOG_BLOCK_UPDATES =
    ConfigBuilder("spark.eventLog.logBlockUpdates.enabled")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EVENT_LOG_TESTING =
    ConfigBuilder("spark.eventLog.testing")
      .internal()
      .booleanConf
      .createWithDefault(false)

  private[spark] val EVENT_LOG_OUTPUT_BUFFER_SIZE = ConfigBuilder("spark.eventLog.buffer.kb")
    .doc("Buffer size to use when writing to output streams, in KiB unless otherwise specified.")
    .bytesConf(ByteUnit.KiB)
    .createWithDefaultString("100k")

  private[spark] val EVENT_LOG_STAGE_EXECUTOR_METRICS =
    ConfigBuilder("spark.eventLog.logStageExecutorMetrics.enabled")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EVENT_LOG_PROCESS_TREE_METRICS =
    ConfigBuilder("spark.eventLog.logStageExecutorProcessTreeMetrics.enabled")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EVENT_LOG_GC_METRICS_YOUNG_GENERATION_GARBAGE_COLLECTORS =
    ConfigBuilder("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors")
      .doc("Names of supported young generation garbage collector. A name usually is " +
        " the return of GarbageCollectorMXBean.getName. The built-in young generation garbage " +
        s"collectors are ${GarbageCollectionMetrics.YOUNG_GENERATION_BUILTIN_GARBAGE_COLLECTORS}")
      .stringConf
      .toSequence
      .createWithDefault(GarbageCollectionMetrics.YOUNG_GENERATION_BUILTIN_GARBAGE_COLLECTORS)

  private[spark] val EVENT_LOG_GC_METRICS_OLD_GENERATION_GARBAGE_COLLECTORS =
    ConfigBuilder("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors")
      .doc("Names of supported old generation garbage collector. A name usually is " +
        "the return of GarbageCollectorMXBean.getName. The built-in old generation garbage " +
        s"collectors are ${GarbageCollectionMetrics.OLD_GENERATION_BUILTIN_GARBAGE_COLLECTORS}")
      .stringConf
      .toSequence
      .createWithDefault(GarbageCollectionMetrics.OLD_GENERATION_BUILTIN_GARBAGE_COLLECTORS)

  private[spark] val EVENT_LOG_OVERWRITE =
    ConfigBuilder("spark.eventLog.overwrite").booleanConf.createWithDefault(false)

  private[spark] val EXECUTOR_CLASS_PATH =
    ConfigBuilder(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH).stringConf.createOptional

  private[spark] val EXECUTOR_JAVA_OPTIONS =
    ConfigBuilder(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS).stringConf.createOptional

  private[spark] val EXECUTOR_LIBRARY_PATH =
    ConfigBuilder(SparkLauncher.EXECUTOR_EXTRA_LIBRARY_PATH).stringConf.createOptional

  private[spark] val EXECUTOR_USER_CLASS_PATH_FIRST =
    ConfigBuilder("spark.executor.userClassPathFirst").booleanConf.createWithDefault(false)

  private[spark] val EXECUTOR_MEMORY = ConfigBuilder("spark.executor.memory")
    .doc("Amount of memory to use per executor process, in MiB unless otherwise specified.")
    .bytesConf(ByteUnit.MiB)
    .createWithDefaultString("1g")

  private[spark] val EXECUTOR_MEMORY_OVERHEAD = ConfigBuilder("spark.executor.memoryOverhead")
    .doc("The amount of off-heap memory to be allocated per executor in cluster mode, " +
      "in MiB unless otherwise specified.")
    .bytesConf(ByteUnit.MiB)
    .createOptional

  private[spark] val MEMORY_OFFHEAP_ENABLED = ConfigBuilder("spark.memory.offHeap.enabled")
    .doc("If true, Spark will attempt to use off-heap memory for certain operations. " +
      "If off-heap memory use is enabled, then spark.memory.offHeap.size must be positive.")
    .withAlternative("spark.unsafe.offHeap")
    .booleanConf
    .createWithDefault(false)

  private[spark] val MEMORY_OFFHEAP_SIZE = ConfigBuilder("spark.memory.offHeap.size")
    .doc("The absolute amount of memory in bytes which can be used for off-heap allocation. " +
      "This setting has no impact on heap memory usage, so if your executors' total memory " +
      "consumption must fit within some hard limit then be sure to shrink your JVM heap size " +
      "accordingly. This must be set to a positive value when spark.memory.offHeap.enabled=true.")
    .bytesConf(ByteUnit.BYTE)
    .checkValue(_ >= 0, "The off-heap memory size must not be negative")
    .createWithDefault(0)

  private[spark] val IS_PYTHON_APP = ConfigBuilder("spark.yarn.isPython").internal()
    .booleanConf.createWithDefault(false)

  private[spark] val CPUS_PER_TASK = ConfigBuilder("spark.task.cpus").intConf.createWithDefault(1)

  private[spark] val DYN_ALLOCATION_MIN_EXECUTORS =
    ConfigBuilder("spark.dynamicAllocation.minExecutors").intConf.createWithDefault(0)

  private[spark] val DYN_ALLOCATION_INITIAL_EXECUTORS =
    ConfigBuilder("spark.dynamicAllocation.initialExecutors")
      .fallbackConf(DYN_ALLOCATION_MIN_EXECUTORS)

  private[spark] val DYN_ALLOCATION_MAX_EXECUTORS =
    ConfigBuilder("spark.dynamicAllocation.maxExecutors").intConf.createWithDefault(Int.MaxValue)

  private[spark] val LOCALITY_WAIT = ConfigBuilder("spark.locality.wait")
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefaultString("3s")

  private[spark] val SHUFFLE_SERVICE_ENABLED =
    ConfigBuilder("spark.shuffle.service.enabled").booleanConf.createWithDefault(false)

  private[spark] val KEYTAB = ConfigBuilder("spark.yarn.keytab")
    .doc("Location of user's keytab.")
    .stringConf.createOptional

  private[spark] val PRINCIPAL = ConfigBuilder("spark.yarn.principal")
    .doc("Name of the Kerberos principal.")
    .stringConf.createOptional

  private[spark] val EXECUTOR_INSTANCES = ConfigBuilder("spark.executor.instances")
    .intConf
    .createOptional

  private[spark] val PY_FILES = ConfigBuilder("spark.yarn.dist.pyFiles")
    .internal()
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val MAX_TASK_FAILURES =
    ConfigBuilder("spark.task.maxFailures")
      .intConf
      .createWithDefault(4)

  // Blacklist confs
  private[spark] val BLACKLIST_ENABLED =
    ConfigBuilder("spark.blacklist.enabled")
      .booleanConf
      .createOptional

  private[spark] val MAX_TASK_ATTEMPTS_PER_EXECUTOR =
    ConfigBuilder("spark.blacklist.task.maxTaskAttemptsPerExecutor")
      .intConf
      .createWithDefault(1)

  private[spark] val MAX_TASK_ATTEMPTS_PER_NODE =
    ConfigBuilder("spark.blacklist.task.maxTaskAttemptsPerNode")
      .intConf
      .createWithDefault(2)

  private[spark] val MAX_FAILURES_PER_EXEC =
    ConfigBuilder("spark.blacklist.application.maxFailedTasksPerExecutor")
      .intConf
      .createWithDefault(2)

  private[spark] val MAX_FAILURES_PER_EXEC_STAGE =
    ConfigBuilder("spark.blacklist.stage.maxFailedTasksPerExecutor")
      .intConf
      .createWithDefault(2)

  private[spark] val MAX_FAILED_EXEC_PER_NODE =
    ConfigBuilder("spark.blacklist.application.maxFailedExecutorsPerNode")
      .intConf
      .createWithDefault(2)

  private[spark] val MAX_FAILED_EXEC_PER_NODE_STAGE =
    ConfigBuilder("spark.blacklist.stage.maxFailedExecutorsPerNode")
      .intConf
      .createWithDefault(2)

  private[spark] val BLACKLIST_TIMEOUT_CONF =
    ConfigBuilder("spark.blacklist.timeout")
      .timeConf(TimeUnit.MILLISECONDS)
      .createOptional

  private[spark] val BLACKLIST_KILL_ENABLED =
    ConfigBuilder("spark.blacklist.killBlacklistedExecutors")
      .booleanConf
      .createWithDefault(false)

  private[spark] val BLACKLIST_LEGACY_TIMEOUT_CONF =
    ConfigBuilder("spark.scheduler.executorTaskBlacklistTime")
      .internal()
      .timeConf(TimeUnit.MILLISECONDS)
      .createOptional

  private[spark] val BLACKLIST_FETCH_FAILURE_ENABLED =
    ConfigBuilder("spark.blacklist.application.fetchFailure.enabled")
      .booleanConf
      .createWithDefault(false)
  // End blacklist confs

  private[spark] val UNREGISTER_OUTPUT_ON_HOST_ON_FETCH_FAILURE =
    ConfigBuilder("spark.files.fetchFailure.unRegisterOutputOnHost")
      .doc("Whether to un-register all the outputs on the host in condition that we receive " +
        " a FetchFailure. This is set default to false, which means, we only un-register the " +
        " outputs related to the exact executor(instead of the host) on a FetchFailure.")
      .booleanConf
      .createWithDefault(false)

  private[spark] val LISTENER_BUS_EVENT_QUEUE_CAPACITY =
    ConfigBuilder("spark.scheduler.listenerbus.eventqueue.capacity")
      .intConf
      .checkValue(_ > 0, "The capacity of listener bus event queue must not be negative")
      .createWithDefault(10000)

  private[spark] val LISTENER_BUS_METRICS_MAX_LISTENER_CLASSES_TIMED =
    ConfigBuilder("spark.scheduler.listenerbus.metrics.maxListenerClassesTimed")
      .internal()
      .intConf
      .createWithDefault(128)

  // This property sets the root namespace for metrics reporting
  private[spark] val METRICS_NAMESPACE = ConfigBuilder("spark.metrics.namespace")
    .stringConf
    .createOptional

  private[spark] val PYSPARK_DRIVER_PYTHON = ConfigBuilder("spark.pyspark.driver.python")
    .stringConf
    .createOptional

  private[spark] val PYSPARK_PYTHON = ConfigBuilder("spark.pyspark.python")
    .stringConf
    .createOptional

  // To limit how many applications are shown in the History Server summary ui
  private[spark] val HISTORY_UI_MAX_APPS =
    ConfigBuilder("spark.history.ui.maxApplications").intConf.createWithDefault(Integer.MAX_VALUE)

  private[spark] val UI_SHOW_CONSOLE_PROGRESS = ConfigBuilder("spark.ui.showConsoleProgress")
    .doc("When true, show the progress bar in the console.")
    .booleanConf
    .createWithDefault(false)

  private[spark] val IO_ENCRYPTION_ENABLED = ConfigBuilder("spark.io.encryption.enabled")
    .booleanConf
    .createWithDefault(false)

  private[spark] val IO_ENCRYPTION_KEYGEN_ALGORITHM =
    ConfigBuilder("spark.io.encryption.keygen.algorithm")
      .stringConf
      .createWithDefault("HmacSHA1")

  private[spark] val IO_ENCRYPTION_KEY_SIZE_BITS = ConfigBuilder("spark.io.encryption.keySizeBits")
    .intConf
    .checkValues(Set(128, 192, 256))
    .createWithDefault(128)

  private[spark] val IO_CRYPTO_CIPHER_TRANSFORMATION =
    ConfigBuilder("spark.io.crypto.cipher.transformation")
      .internal()
      .stringConf
      .createWithDefaultString("AES/CTR/NoPadding")

  private[spark] val DRIVER_HOST_ADDRESS = ConfigBuilder("spark.driver.host")
    .doc("Address of driver endpoints.")
    .stringConf
    .createWithDefault(Utils.localCanonicalHostName())

  private[spark] val DRIVER_BIND_ADDRESS = ConfigBuilder("spark.driver.bindAddress")
    .doc("Address where to bind network listen sockets on the driver.")
    .fallbackConf(DRIVER_HOST_ADDRESS)

  private[spark] val BLOCK_MANAGER_PORT = ConfigBuilder("spark.blockManager.port")
    .doc("Port to use for the block manager when a more specific setting is not provided.")
    .intConf
    .createWithDefault(0)

  private[spark] val DRIVER_BLOCK_MANAGER_PORT = ConfigBuilder("spark.driver.blockManager.port")
    .doc("Port to use for the block manager on the driver.")
    .fallbackConf(BLOCK_MANAGER_PORT)

  private[spark] val IGNORE_CORRUPT_FILES = ConfigBuilder("spark.files.ignoreCorruptFiles")
    .doc("Whether to ignore corrupt files. If true, the Spark jobs will continue to run when " +
      "encountering corrupted or non-existing files and contents that have been read will still " +
      "be returned.")
    .booleanConf
    .createWithDefault(false)

  private[spark] val APP_CALLER_CONTEXT = ConfigBuilder("spark.log.callerContext")
    .stringConf
    .createOptional

  private[spark] val FILES_MAX_PARTITION_BYTES = ConfigBuilder("spark.files.maxPartitionBytes")
    .doc("The maximum number of bytes to pack into a single partition when reading files.")
    .longConf
    .createWithDefault(128 * 1024 * 1024)

  private[spark] val FILES_OPEN_COST_IN_BYTES = ConfigBuilder("spark.files.openCostInBytes")
    .doc("The estimated cost to open a file, measured by the number of bytes could be scanned in" +
      " the same time. This is used when putting multiple files into a partition. It's better to" +
      " over estimate, then the partitions with small files will be faster than partitions with" +
      " bigger files.")
    .longConf
    .createWithDefault(4 * 1024 * 1024)

  private[spark] val HADOOP_RDD_IGNORE_EMPTY_SPLITS =
    ConfigBuilder("spark.hadoopRDD.ignoreEmptySplits")
      .internal()
      .doc("When true, HadoopRDD/NewHadoopRDD will not create partitions for empty input splits.")
      .booleanConf
      .createWithDefault(false)

  private[spark] val SECRET_REDACTION_PATTERN =
    ConfigBuilder("spark.redaction.regex")
      .doc("Regex to decide which Spark configuration properties and environment variables in " +
        "driver and executor environments contain sensitive information. When this regex matches " +
        "a property key or value, the value is redacted from the environment UI and various logs " +
        "like YARN and event logs.")
      .regexConf
      .createWithDefault("(?i)secret|password|url|user|username".r)

  private[spark] val STRING_REDACTION_PATTERN =
    ConfigBuilder("spark.redaction.string.regex")
      .doc("Regex to decide which parts of strings produced by Spark contain sensitive " +
        "information. When this regex matches a string part, that string part is replaced by a " +
        "dummy value. This is currently used to redact the output of SQL explain commands.")
      .regexConf
      .createOptional

  private[spark] val NETWORK_AUTH_ENABLED =
    ConfigBuilder("spark.authenticate")
      .booleanConf
      .createWithDefault(false)

  private[spark] val SASL_ENCRYPTION_ENABLED =
    ConfigBuilder("spark.authenticate.enableSaslEncryption")
      .booleanConf
      .createWithDefault(false)

  private[spark] val NETWORK_ENCRYPTION_ENABLED =
    ConfigBuilder("spark.network.crypto.enabled")
      .booleanConf
      .createWithDefault(false)

  private[spark] val BUFFER_WRITE_CHUNK_SIZE =
    ConfigBuilder("spark.buffer.write.chunkSize")
      .internal()
      .doc("The chunk size in bytes during writing out the bytes of ChunkedByteBuffer.")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(_ <= Int.MaxValue, "The chunk size during writing out the bytes of" +
        " ChunkedByteBuffer should not larger than Int.MaxValue.")
      .createWithDefault(64 * 1024 * 1024)

  private[spark] val CHECKPOINT_COMPRESS =
    ConfigBuilder("spark.checkpoint.compress")
      .doc("Whether to compress RDD checkpoints. Generally a good idea. Compression will use " +
        "spark.io.compression.codec.")
      .booleanConf
      .createWithDefault(false)

  private[spark] val SHUFFLE_ACCURATE_BLOCK_THRESHOLD =
    ConfigBuilder("spark.shuffle.accurateBlockThreshold")
      .doc("Threshold in bytes above which the size of shuffle blocks in " +
        "HighlyCompressedMapStatus is accurately recorded. This helps to prevent OOM " +
        "by avoiding underestimating shuffle block size when fetch shuffle blocks.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(100 * 1024 * 1024)

  private[spark] val SHUFFLE_REGISTRATION_TIMEOUT =
    ConfigBuilder("spark.shuffle.registration.timeout")
      .doc("Timeout in milliseconds for registration to the external shuffle service.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(5000)

  private[spark] val SHUFFLE_REGISTRATION_MAX_ATTEMPTS =
    ConfigBuilder("spark.shuffle.registration.maxAttempts")
      .doc("When we fail to register to the external shuffle service, we will " +
        "retry for maxAttempts times.")
      .intConf
      .createWithDefault(3)

  private[spark] val REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS =
    ConfigBuilder("spark.reducer.maxBlocksInFlightPerAddress")
      .doc("This configuration limits the number of remote blocks being fetched per reduce task " +
        "from a given host port. When a large number of blocks are being requested from a given " +
        "address in a single fetch or simultaneously, this could crash the serving executor or " +
        "Node Manager. This is especially useful to reduce the load on the Node Manager when " +
        "external shuffle is enabled. You can mitigate the issue by setting it to a lower value.")
      .intConf
      .checkValue(_ > 0, "The max no. of blocks in flight cannot be non-positive.")
      .createWithDefault(Int.MaxValue)

  private[spark] val MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM =
    ConfigBuilder("spark.maxRemoteBlockSizeFetchToMem")
      .doc("Remote block will be fetched to disk when size of the block is above this threshold " +
        "in bytes. This is to avoid a giant request takes too much memory. We can enable this " +
        "config by setting a specific value(e.g. 200m). Note this configuration will affect " +
        "both shuffle fetch and block manager remote block fetch. For users who enabled " +
        "external shuffle service, this feature can only be worked when external shuffle" +
        "service is newer than Spark 2.2.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(Long.MaxValue)

  private[spark] val TASK_METRICS_TRACK_UPDATED_BLOCK_STATUSES =
    ConfigBuilder("spark.taskMetrics.trackUpdatedBlockStatuses")
      .doc("Enable tracking of updatedBlockStatuses in the TaskMetrics. Off by default since " +
        "tracking the block statuses can use a lot of memory and its not used anywhere within " +
        "spark.")
      .booleanConf
      .createWithDefault(false)

  private[spark] val SHUFFLE_FILE_BUFFER_SIZE =
    ConfigBuilder("spark.shuffle.file.buffer")
      .doc("Size of the in-memory buffer for each shuffle file output stream, in KiB unless " +
        "otherwise specified. These buffers reduce the number of disk seeks and system calls " +
        "made in creating intermediate shuffle files.")
      .bytesConf(ByteUnit.KiB)
      .checkValue(v => v > 0 && v <= Int.MaxValue / 1024,
        s"The file buffer size must be greater than 0 and less than ${Int.MaxValue / 1024}.")
      .createWithDefaultString("32k")

  private[spark] val SHUFFLE_UNSAFE_FILE_OUTPUT_BUFFER_SIZE =
    ConfigBuilder("spark.shuffle.unsafe.file.output.buffer")
      .doc("The file system for this buffer size after each partition " +
        "is written in unsafe shuffle writer. In KiB unless otherwise specified.")
      .bytesConf(ByteUnit.KiB)
      .checkValue(v => v > 0 && v <= Int.MaxValue / 1024,
        s"The buffer size must be greater than 0 and less than ${Int.MaxValue / 1024}.")
      .createWithDefaultString("32k")

  private[spark] val SHUFFLE_DISK_WRITE_BUFFER_SIZE =
    ConfigBuilder("spark.shuffle.spill.diskWriteBufferSize")
      .doc("The buffer size, in bytes, to use when writing the sorted records to an on-disk file.")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(v => v > 0 && v <= Int.MaxValue,
        s"The buffer size must be greater than 0 and less than ${Int.MaxValue}.")
      .createWithDefault(1024 * 1024)

  private[spark] val UNROLL_MEMORY_CHECK_PERIOD =
    ConfigBuilder("spark.storage.unrollMemoryCheckPeriod")
      .internal()
      .doc("The memory check period is used to determine how often we should check whether "
        + "there is a need to request more memory when we try to unroll the given block in memory.")
      .longConf
      .createWithDefault(16)

  private[spark] val UNROLL_MEMORY_GROWTH_FACTOR =
    ConfigBuilder("spark.storage.unrollMemoryGrowthFactor")
      .internal()
      .doc("Memory to request as a multiple of the size that used to unroll the block.")
      .doubleConf
      .createWithDefault(1.5)

  private[spark] val FORCE_DOWNLOAD_SCHEMES =
    ConfigBuilder("spark.yarn.dist.forceDownloadSchemes")
      .doc("Comma-separated list of schemes for which files will be downloaded to the " +
        "local disk prior to being added to YARN's distributed cache. For use in cases " +
        "where the YARN service does not support schemes that are supported by Spark, like http, " +
        "https and ftp.")
      .stringConf
      .toSequence
      .createWithDefault(Nil)

  private[spark] val UI_X_XSS_PROTECTION =
    ConfigBuilder("spark.ui.xXssProtection")
      .doc("Value for HTTP X-XSS-Protection response header")
      .stringConf
      .createWithDefaultString("1; mode=block")

  private[spark] val UI_X_CONTENT_TYPE_OPTIONS =
    ConfigBuilder("spark.ui.xContentTypeOptions.enabled")
      .doc("Set to 'true' for setting X-Content-Type-Options HTTP response header to 'nosniff'")
      .booleanConf
      .createWithDefault(true)

  private[spark] val UI_STRICT_TRANSPORT_SECURITY =
    ConfigBuilder("spark.ui.strictTransportSecurity")
      .doc("Value for HTTP Strict Transport Security Response Header")
      .stringConf
      .createOptional

  private[spark] val EXTRA_LISTENERS = ConfigBuilder("spark.extraListeners")
    .doc("Class names of listeners to add to SparkContext during initialization.")
    .stringConf
    .toSequence
    .createOptional

  private[spark] val SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD =
    ConfigBuilder("spark.shuffle.spill.numElementsForceSpillThreshold")
      .internal()
      .doc("The maximum number of elements in memory before forcing the shuffle sorter to spill. " +
        "By default it's Integer.MAX_VALUE, which means we never force the sorter to spill, " +
        "until we reach some limitations, like the max page size limitation for the pointer " +
        "array in the sorter.")
      .intConf
      .createWithDefault(Integer.MAX_VALUE)

  private[spark] val SHUFFLE_MAP_OUTPUT_PARALLEL_AGGREGATION_THRESHOLD =
    ConfigBuilder("spark.shuffle.mapOutput.parallelAggregationThreshold")
      .internal()
      .doc("Multi-thread is used when the number of mappers * shuffle partitions is greater than " +
        "or equal to this threshold. Note that the actual parallelism is calculated by number of " +
        "mappers * shuffle partitions / this threshold + 1, so this threshold should be positive.")
      .intConf
      .checkValue(v => v > 0, "The threshold should be positive.")
      .createWithDefault(10000000)

}
