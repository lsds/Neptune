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

package org.apache.spark.rdd

import java.io.{File, IOException, ObjectInputStream, ObjectOutputStream}

import com.esotericsoftware.kryo.KryoException
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, TextInputFormat}
import org.apache.spark._
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDDSuiteUtils._
import org.apache.spark.util.{ThreadUtils, Utils}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.reflect.ClassTag

class RDDCoroutineSuite extends SparkFunSuite {
  var tempDir: File = _
  var conf : SparkConf = _
  var sc : SparkContext = _

  override def beforeAll(): Unit = {
    tempDir = Utils.createTempDir()
    conf = new SparkConf().setMaster("local").setAppName("RDD coroutine suite test")
    conf.enableNeptuneCoroutines()
    sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterAll()
    }
  }

  test("Neptune reduce example") {
    val plusFunc = (x: Int, y: Int) => {
      // scalastyle:off
      println(s"compared $x to $y")
      x + y
    }
    // Same as:
    // val reducePartition: Iterator[Int] => Option[Int] = iter => {
    val reducePartition = (iter: Iterator[Int]) => {
      if (iter.hasNext) {
        println("called")
        Some(iter.reduceLeft(plusFunc))
      } else {
        None
      }
    }

    val intValues = Array(20, 12, 6, 15, 2, 9)
    // scalastyle:off
    println(reducePartition(intValues.iterator))
  }

  test("Neptune Coroutine basic operations") {
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    assert(nums.getNumPartitions === 2)
    // collect action implementation
    assert(nums.collect().toList === List(1, 2, 3, 4))
    // toLocalIterator implementation
    assert(nums.toLocalIterator.toList === List(1, 2, 3, 4))
    // count implementation
    val dups = sc.makeRDD(Array(1, 1, 2, 2, 3, 3, 4, 4), 2)
    assert(dups.distinct().count() === 4)
    assert(dups.distinct().count === 4)  // Can distinct and count be called without parentheses?
    assert(dups.distinct().collect === dups.distinct().collect)
    assert(nums.reduce(_ + _) === 10)
    assert(nums.fold(0)(_ + _) === 10)
    assert(nums.fold(10)(_ + _) === 40)
    assert(nums.map(_.toString).collect().toList === List("1", "2", "3", "4"))
    assert(nums.filter(_ > 2).collect().toList === List(3, 4))
    assert(nums.flatMap(x => 1 to x).collect().toList === List(1, 1, 2, 1, 2, 3, 1, 2, 3, 4))
    assert(nums.union(nums).collect().toList === List(1, 2, 3, 4, 1, 2, 3, 4))
    assert(nums.glom().map(_.toList).collect().toList === List(List(1, 2), List(3, 4)))
    assert(nums.collect({ case i if i >= 3 => i.toString }).collect().toList === List("3", "4"))
    assert(nums.keyBy(_.toString).collect().toList === List(("1", 1), ("2", 2), ("3", 3), ("4", 4)))
    assert(!nums.isEmpty())
  }
}
