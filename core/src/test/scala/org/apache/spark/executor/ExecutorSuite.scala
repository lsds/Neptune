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

package org.apache.spark.executor

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.lang.Thread.UncaughtExceptionHandler
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.coroutines.{~>, _}

import scala.collection.mutable
import scala.collection.mutable.Map
import scala.concurrent.duration._
import scala.language.postfixOps
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.{any, eq => meq}
import org.mockito.Mockito.{inOrder, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.concurrent.Eventually
import org.scalatest.mockito.MockitoSugar
import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.memory.MemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler._
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.UninterruptibleThread

class ExecutorSuite extends SparkFunSuite with LocalSparkContext with MockitoSugar with Eventually {

  test("SPARK-15963: Catch `TaskKilledException` correctly in Executor.TaskRunner") {
    // mock some objects to make Executor.launchTask() happy
    val conf = new SparkConf
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)
    val serializedTask = serializer.newInstance().serialize(new FakeTask(0, 0))
    val taskDescription = createFakeTaskDescription(serializedTask)

    // we use latches to force the program to run in this order:
    // +-----------------------------+---------------------------------------+
    // |      main test thread       |      worker thread                    |
    // +-----------------------------+---------------------------------------+
    // |    executor.launchTask()    |                                       |
    // |                             | TaskRunner.run() begins               |
    // |                             |          ...                          |
    // |                             | execBackend.statusUpdate  // 1st time |
    // | executor.killAllTasks(true) |                                       |
    // |                             |          ...                          |
    // |                             |  task = ser.deserialize               |
    // |                             |          ...                          |
    // |                             | execBackend.statusUpdate  // 2nd time |
    // |                             |          ...                          |
    // |                             |   TaskRunner.run() ends               |
    // |       check results         |                                       |
    // +-----------------------------+---------------------------------------+

    val executorSuiteHelper = new ExecutorSuiteHelper

    val mockExecutorBackend = mock[ExecutorBackend]
    when(mockExecutorBackend.statusUpdate(any(), any(), any()))
      .thenAnswer(new Answer[Unit] {
        var firstTime = true
        override def answer(invocationOnMock: InvocationOnMock): Unit = {
          if (firstTime) {
            executorSuiteHelper.latch1.countDown()
            // here between latch1 and latch2, executor.killAllTasks() is called
            executorSuiteHelper.latch2.await()
            firstTime = false
          }
          else {
            // save the returned `taskState` and `testFailedReason` into `executorSuiteHelper`
            val taskState = invocationOnMock.getArguments()(1).asInstanceOf[TaskState]
            executorSuiteHelper.taskState = taskState
            val taskEndReason = invocationOnMock.getArguments()(2).asInstanceOf[ByteBuffer]
            executorSuiteHelper.testFailedReason =
              serializer.newInstance().deserialize(taskEndReason)
            // let the main test thread check `taskState` and `testFailedReason`
            executorSuiteHelper.latch3.countDown()
          }
        }
      })

    var executor: Executor = null
    try {
      executor = new Executor("id", "localhost", env, userClassPath = Nil, isLocal = true)
      // the task will be launched in a dedicated worker thread
      executor.launchTask(mockExecutorBackend, taskDescription)

      if (!executorSuiteHelper.latch1.await(5, TimeUnit.SECONDS)) {
        fail("executor did not send first status update in time")
      }
      // we know the task will be started, but not yet deserialized, because of the latches we
      // use in mockExecutorBackend.
      executor.killAllTasks(true, "test")
      executorSuiteHelper.latch2.countDown()
      if (!executorSuiteHelper.latch3.await(5, TimeUnit.SECONDS)) {
        fail("executor did not send second status update in time")
      }

      // `testFailedReason` should be `TaskKilled`; `taskState` should be `KILLED`
      assert(executorSuiteHelper.testFailedReason === TaskKilled("test"))
      assert(executorSuiteHelper.taskState === TaskState.KILLED)
    }
    finally {
      if (executor != null) {
        executor.stop()
      }
    }
  }

  /**
   * ::Neptune::
   */
  test("Neptune ResultTask expected result") {
    val conf = new SparkConf().setMaster("local").setAppName("executor suite test")
    sc = new SparkContext(conf)
    val serializer = SparkEnv.get.closureSerializer.newInstance()
    val resultFunc = (context: TaskContext, itr: Iterator[Int]) => itr.toList

    val rdd1 = sc.parallelize(1 to 10, 1)
    val rdd2 = rdd1.filter(_ % 2 == 0)
    val rdd3 = rdd2.map(_ + 1)
    // Should be: 3, 5, 7, 9, 11
    // Action needed for the lazy-evaluation!!
    rdd3.collect()


    val taskBinary = sc.broadcast(serializer.serialize((rdd3, resultFunc)).array())
    val serializedTaskMetrics = serializer.serialize(TaskMetrics.registered).array()
    val task = new ResultTask(
      stageId = 1,
      stageAttemptId = 0,
      taskBinary = taskBinary,
      partition = rdd3.partitions(0),
      locs = Seq(),
      outputId = 0,
      localProperties = new Properties(),
      serializedTaskMetrics = serializedTaskMetrics
    )

    val serTask = serializer.serialize(task)
    val taskDescription = createFakeTaskDescription(serTask)

    // Expected results
    val expectedResult = List(3, 5, 7, 9, 11)
    // Now run Task
    runTaskExpectedHandler(taskDescription, expectedResult)
  }

  test("Neptune coroutine ResultTask") {
    val conf = new SparkConf().setMaster("local").setAppName("executor suite test")
    sc = new SparkContext(conf)
    val serializer = SparkEnv.get.closureSerializer.newInstance()

    val rdd1 = sc.parallelize(1 to 10, 1)
    val rdd2 = rdd1.filter(_ % 2 == 0)
    val rdd3 = rdd2.map(_ + 1)
    // Should be: 3, 5, 7, 9, 11
    // Action needed for the lazy-evaluation!!
    rdd3.collect()

    val resultFunc: (TaskContext, Iterator[Int]) ~> (Int, Any) =
      coroutine { (context: TaskContext, itr: Iterator[Int]) => {
        val result = new mutable.ArrayBuffer[Any]
        while (itr.hasNext) {
          result.append(itr.next)
          if (context.isPaused()) {
            yieldval(0)
          }
        }
        result.toArray.asInstanceOf[Any]
      }
      }

    val taskBinary = sc.broadcast(serializer.serialize((rdd3, resultFunc)).array())
    val serializedTaskMetrics = serializer.serialize(TaskMetrics.registered).array()
    val task = new ResultTask(
      stageId = 1,
      stageAttemptId = 0,
      taskBinary = taskBinary,
      partition = rdd3.partitions(0),
      locs = Seq(),
      outputId = 0,
      localProperties = new Properties(),
      serializedTaskMetrics = serializedTaskMetrics,
      isPausable = true // calls coroutine deserializer
    )

    val serTask = serializer.serialize(task)
    val taskDescription = createFakeTaskDescription(serTask)

    // Expected results
    val expectedResult = Array(3, 5, 7, 9, 11)
    // Run the coroutine ResultTask bypassing the DAGScheduler
    runTaskExpectedHandler(taskDescription, expectedResult)
  }

  test("test Neptune coroutine performance") {
    val conf = new SparkConf().setMaster("local").setAppName("executor suite test")
    val numTrials = 100
    val partitions = 4
    val numElems = 1000000L

    conf.enableNeptuneCoroutines()
    sc = new SparkContext(conf)
    warmup(sc)

    val begin = System.nanoTime
    for (j <- 0 until numTrials) {
      val rdd1 = sc.parallelize(1L to numElems, partitions)
      val rdd2 = rdd1.filter(_ % 2 == 0)
      val rdd3 = rdd2.map(_ + 1)
      // Should be: 3, 5, 7, 9, 11
      // Action needed for the lazy-evaluation!!
      val res = rdd3.collect()
    }
    // scalastyle:off
    println(s"Neptune Took ${((System.nanoTime - begin) / 1e6) / numTrials} ms")
  }

  test("test Spark performance") {
    val conf = new SparkConf().setMaster("local").setAppName("executor suite test")
    val numTrials = 100
    val partitions = 4
    val numElems = 1000000L

    sc = new SparkContext(conf)
    warmup(sc)

    val begin = System.nanoTime
    for (j <- 0 until numTrials) {
      val rdd1 = sc.parallelize(1L to numElems, partitions)
      val rdd2 = rdd1.filter(_ % 2 == 0)
      val rdd3 = rdd2.map(_ + 1)
      // Should be: 3, 5, 7, 9, 11
      // Action needed for the lazy-evaluation!!
      val res = rdd3.collect()
    }
    // scalastyle:off
    println(s"Spark Took ${((System.nanoTime - begin) / 1e6) / numTrials } ms")

  }

  def warmup(sc: SparkContext): Unit = {
    // Let all the executors join
    Thread.sleep(10000)
    // Warm up the JVM and copy the JAR out to all the machines etc.
    sc.parallelize(0 until sc.getExecutorMemoryStatus.size,
      sc.getExecutorMemoryStatus.size).foreach { x =>
      Thread.sleep(1)
    }
  }

  test("SPARK-19276: Handle FetchFailedExceptions that are hidden by user exceptions") {
    val conf = new SparkConf().setMaster("local").setAppName("executor suite test")
    sc = new SparkContext(conf)
    val serializer = SparkEnv.get.closureSerializer.newInstance()
    val resultFunc = (context: TaskContext, itr: Iterator[Int]) => itr.size

    // Submit a job where a fetch failure is thrown, but user code has a try/catch which hides
    // the fetch failure.  The executor should still tell the driver that the task failed due to a
    // fetch failure, not a generic exception from user code.
    val inputRDD = new FetchFailureThrowingRDD(sc)
    val secondRDD = new FetchFailureHidingRDD(sc, inputRDD, throwOOM = false)
    val taskBinary = sc.broadcast(serializer.serialize((secondRDD, resultFunc)).array())
    val serializedTaskMetrics = serializer.serialize(TaskMetrics.registered).array()
    val task = new ResultTask(
      stageId = 1,
      stageAttemptId = 0,
      taskBinary = taskBinary,
      partition = secondRDD.partitions(0),
      locs = Seq(),
      outputId = 0,
      localProperties = new Properties(),
      serializedTaskMetrics = serializedTaskMetrics
    )

    val serTask = serializer.serialize(task)
    val taskDescription = createFakeTaskDescription(serTask)

    val failReason = runTaskAndGetFailReason(taskDescription)
    assert(failReason.isInstanceOf[FetchFailed])
  }

  test("Executor's worker threads should be UninterruptibleThread") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("executor thread test")
      .set("spark.ui.enabled", "false")
    sc = new SparkContext(conf)
    val executorThread = sc.parallelize(Seq(1), 1).map { _ =>
      Thread.currentThread.getClass.getName
    }.collect().head
    assert(executorThread === classOf[UninterruptibleThread].getName)
  }

  test("SPARK-19276: OOMs correctly handled with a FetchFailure") {
    // when there is a fatal error like an OOM, we don't do normal fetch failure handling, since it
    // may be a false positive.  And we should call the uncaught exception handler.
    val conf = new SparkConf().setMaster("local").setAppName("executor suite test")
    sc = new SparkContext(conf)
    val serializer = SparkEnv.get.closureSerializer.newInstance()
    val resultFunc = (context: TaskContext, itr: Iterator[Int]) => itr.size

    // Submit a job where a fetch failure is thrown, but then there is an OOM.  We should treat
    // the fetch failure as a false positive, and just do normal OOM handling.
    val inputRDD = new FetchFailureThrowingRDD(sc)
    val secondRDD = new FetchFailureHidingRDD(sc, inputRDD, throwOOM = true)
    val taskBinary = sc.broadcast(serializer.serialize((secondRDD, resultFunc)).array())
    val serializedTaskMetrics = serializer.serialize(TaskMetrics.registered).array()
    val task = new ResultTask(
      stageId = 1,
      stageAttemptId = 0,
      taskBinary = taskBinary,
      partition = secondRDD.partitions(0),
      locs = Seq(),
      outputId = 0,
      localProperties = new Properties(),
      serializedTaskMetrics = serializedTaskMetrics
    )

    val serTask = serializer.serialize(task)
    val taskDescription = createFakeTaskDescription(serTask)

    val (failReason, uncaughtExceptionHandler) =
      runTaskGetFailReasonAndExceptionHandler(taskDescription)
    // make sure the task failure just looks like a OOM, not a fetch failure
    assert(failReason.isInstanceOf[ExceptionFailure])
    val exceptionCaptor = ArgumentCaptor.forClass(classOf[Throwable])
    verify(uncaughtExceptionHandler).uncaughtException(any(), exceptionCaptor.capture())
    assert(exceptionCaptor.getAllValues.size === 1)
    assert(exceptionCaptor.getAllValues.get(0).isInstanceOf[OutOfMemoryError])
  }

  test("Gracefully handle error in task deserialization") {
    val conf = new SparkConf
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)
    val serializedTask = serializer.newInstance().serialize(new NonDeserializableTask)
    val taskDescription = createFakeTaskDescription(serializedTask)

    val failReason = runTaskAndGetFailReason(taskDescription)
    failReason match {
      case ef: ExceptionFailure =>
        assert(ef.exception.isDefined)
        assert(ef.exception.get.getMessage() === NonDeserializableTask.errorMsg)
      case _ =>
        fail(s"unexpected failure type: $failReason")
    }
  }

  private def createMockEnv(conf: SparkConf, serializer: JavaSerializer): SparkEnv = {
    val mockEnv = mock[SparkEnv]
    val mockRpcEnv = mock[RpcEnv]
    val mockMetricsSystem = mock[MetricsSystem]
    val mockMemoryManager = mock[MemoryManager]
    when(mockEnv.conf).thenReturn(conf)
    when(mockEnv.serializer).thenReturn(serializer)
    when(mockEnv.serializerManager).thenReturn(mock[SerializerManager])
    when(mockEnv.rpcEnv).thenReturn(mockRpcEnv)
    when(mockEnv.metricsSystem).thenReturn(mockMetricsSystem)
    when(mockEnv.memoryManager).thenReturn(mockMemoryManager)
    when(mockEnv.closureSerializer).thenReturn(serializer)
    SparkEnv.set(mockEnv)
    mockEnv
  }

  private def createFakeTaskDescription(serializedTask: ByteBuffer): TaskDescription = {
    new TaskDescription(
      taskId = 0,
      attemptNumber = 0,
      executorId = "",
      name = "",
      index = 0,
      addedFiles = Map[String, Long](),
      addedJars = Map[String, Long](),
      properties = new Properties,
      serializedTask)
  }

  private def runTaskAndGetFailReason(taskDescription: TaskDescription): TaskFailedReason = {
    runTaskGetFailReasonAndExceptionHandler(taskDescription)._1
  }


  /**
   * ::Neptune::
   * Run a given Task in a mockedExecutor environment and assert the intercepted result
   * @param taskDescription
   * @param taskResult
   */
  private def runTaskExpectedHandler(taskDescription: TaskDescription, taskResult: Any): Unit = {
    val mockBackend = mock[ExecutorBackend]
    var executor: Executor = null
    try {
      executor = new Executor("id", "localhost", SparkEnv.get, userClassPath = Nil, isLocal = true)
      // the task will be launched in a dedicated worker thread
      executor.launchTask(mockBackend, taskDescription)
      eventually(timeout(10.seconds), interval(10.milliseconds)) {
        assert(executor.numRunningTasks === 0)
      }
    } finally {
      if (executor != null) {
        executor.stop()
      }
    }
    // Capture all the updates send from the Executor in order
    val orderedMock = inOrder(mockBackend)
    val statusCaptor = ArgumentCaptor.forClass(classOf[ByteBuffer])
    orderedMock.verify(mockBackend)
      .statusUpdate(meq(0L), meq(TaskState.RUNNING), statusCaptor.capture())
    orderedMock.verify(mockBackend)
      .statusUpdate(meq(0L), meq(TaskState.FINISHED), statusCaptor.capture())

    // first statusUpdate for RUNNING has empty data
    assert(statusCaptor.getAllValues().get(0).remaining() === 0)

    val taskSer = SparkEnv.get.closureSerializer.newInstance()

    import collection.JavaConverters._
    for( finishedTaskData <- statusCaptor.getAllValues.asScala) {
      if (finishedTaskData.limit() !== 0) {
        // first statusUpdate for FINISHED has expected data
        // val finishedTaskResult = SparkEnv.get.closureSerializer.newInstance().deserialize[DirectTaskResult[_]](finishedTaskData)
        taskSer.deserialize[TaskResult[_]](finishedTaskData) match {
          case directResult: DirectTaskResult[_] =>
            assert(directResult.value(taskSer)  === taskResult)
        }
      }
    }
  }

  private def runTaskGetFailReasonAndExceptionHandler(
      taskDescription: TaskDescription): (TaskFailedReason, UncaughtExceptionHandler) = {
    val mockBackend = mock[ExecutorBackend]
    val mockUncaughtExceptionHandler = mock[UncaughtExceptionHandler]
    var executor: Executor = null
    try {
      executor = new Executor("id", "localhost", SparkEnv.get, userClassPath = Nil, isLocal = true,
        uncaughtExceptionHandler = mockUncaughtExceptionHandler)
      // the task will be launched in a dedicated worker thread
      executor.launchTask(mockBackend, taskDescription)
      eventually(timeout(5.seconds), interval(10.milliseconds)) {
        assert(executor.numRunningTasks === 0)
      }
    } finally {
      if (executor != null) {
        executor.stop()
      }
    }
    val orderedMock = inOrder(mockBackend)
    val statusCaptor = ArgumentCaptor.forClass(classOf[ByteBuffer])
    orderedMock.verify(mockBackend)
      .statusUpdate(meq(0L), meq(TaskState.RUNNING), statusCaptor.capture())
    orderedMock.verify(mockBackend)
      .statusUpdate(meq(0L), meq(TaskState.FAILED), statusCaptor.capture())
    // first statusUpdate for RUNNING has empty data
    assert(statusCaptor.getAllValues().get(0).remaining() === 0)
    // second update is more interesting
    val failureData = statusCaptor.getAllValues.get(1)
    val failReason =
      SparkEnv.get.closureSerializer.newInstance().deserialize[TaskFailedReason](failureData)
    (failReason, mockUncaughtExceptionHandler)
  }
}

class FetchFailureThrowingRDD(sc: SparkContext) extends RDD[Int](sc, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[Int] = {
    new Iterator[Int] {
      override def hasNext: Boolean = true
      override def next(): Int = {
        throw new FetchFailedException(
          bmAddress = BlockManagerId("1", "hostA", 1234),
          shuffleId = 0,
          mapId = 0,
          reduceId = 0,
          message = "fake fetch failure"
        )
      }
    }
  }
  override protected def getPartitions: Array[Partition] = {
    Array(new SimplePartition)
  }
}

class SimplePartition extends Partition {
  override def index: Int = 0
}

class FetchFailureHidingRDD(
    sc: SparkContext,
    val input: FetchFailureThrowingRDD,
    throwOOM: Boolean) extends RDD[Int](input) {
  override def compute(split: Partition, context: TaskContext): Iterator[Int] = {
    val inItr = input.compute(split, context)
    try {
      Iterator(inItr.size)
    } catch {
      case t: Throwable =>
        if (throwOOM) {
          throw new OutOfMemoryError("OOM while handling another exception")
        } else {
          throw new RuntimeException("User Exception that hides the original exception", t)
        }
    }
  }

  override protected def getPartitions: Array[Partition] = {
    Array(new SimplePartition)
  }
}

// Helps to test("SPARK-15963")
private class ExecutorSuiteHelper {

  val latch1 = new CountDownLatch(1)
  val latch2 = new CountDownLatch(1)
  val latch3 = new CountDownLatch(1)

  @volatile var taskState: TaskState = _
  @volatile var testFailedReason: TaskFailedReason = _
}

private class NonDeserializableTask extends FakeTask(0, 0) with Externalizable {
  def writeExternal(out: ObjectOutput): Unit = {}
  def readExternal(in: ObjectInput): Unit = {
    throw new RuntimeException(NonDeserializableTask.errorMsg)
  }
}

private object NonDeserializableTask {
  val errorMsg = "failure in deserialization"
}
