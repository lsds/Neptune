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

package org.apache.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.Benchmark

/**
 * Benchmark for Dataset typed operations comparing with DataFrame and RDD versions.
 * Used Neptune Coroutines to make sure performance is the same
 */
object DatasetBenchmarkCo {

  case class Data(l: Long, s: String)

  def backToBackMapLong(spark: SparkSession, numRows: Long, numChains: Int): Benchmark = {
    import spark.implicits._

    val rdd = spark.sparkContext.range(0, numRows)
    val ds = spark.range(0, numRows)
    val df = ds.toDF("l")
    val func = (l: Long) => l + 1

    val benchmark = new Benchmark("back-to-back map long", numRows)

    benchmark.addCase("RDD") { iter =>
      var res = rdd
      var i = 0
      while (i < numChains) {
        res = res.map(func)
        i += 1
      }
      res.foreach(_ => Unit)
    }

    benchmark.addCase("DataFrame") { iter =>
      var res = df
      var i = 0
      while (i < numChains) {
        res = res.select($"l" + 1 as "l")
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("Dataset") { iter =>
      var res = ds.as[Long]
      var i = 0
      while (i < numChains) {
        res = res.map(func)
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark
  }

  def backToBackMap(spark: SparkSession, numRows: Long, numChains: Int): Benchmark = {
    import spark.implicits._

    val df = spark.range(1, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))
    val benchmark = new Benchmark("back-to-back map", numRows)
    val func = (d: Data) => Data(d.l + 1, d.s)

    val rdd = spark.sparkContext.range(1, numRows).map(l => Data(l, l.toString))
    benchmark.addCase("RDD") { iter =>
      var res = rdd
      var i = 0
      while (i < numChains) {
        res = res.map(func)
        i += 1
      }
      res.foreach(_ => Unit)
    }

    benchmark.addCase("DataFrame") { iter =>
      var res = df
      var i = 0
      while (i < numChains) {
        res = res.select($"l" + 1 as "l", $"s")
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("Dataset") { iter =>
      var res = df.as[Data]
      var i = 0
      while (i < numChains) {
        res = res.map(func)
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark
  }

  def backToBackFilterLong(spark: SparkSession, numRows: Long, numChains: Int): Benchmark = {
    import spark.implicits._

    val rdd = spark.sparkContext.range(1, numRows)
    val ds = spark.range(1, numRows)
    val df = ds.toDF("l")
    val func = (l: Long) => l % 2L == 0L

    val benchmark = new Benchmark("back-to-back filter Long", numRows)

    benchmark.addCase("RDD") { iter =>
      var res = rdd
      var i = 0
      while (i < numChains) {
        res = res.filter(func)
        i += 1
      }
      res.foreach(_ => Unit)
    }

    benchmark.addCase("DataFrame") { iter =>
      var res = df
      var i = 0
      while (i < numChains) {
        res = res.filter($"l" % 2L === 0L)
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("Dataset") { iter =>
      var res = ds.as[Long]
      var i = 0
      while (i < numChains) {
        res = res.filter(func)
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark
  }

  def backToBackFilter(spark: SparkSession, numRows: Long, numChains: Int): Benchmark = {
    import spark.implicits._

    val df = spark.range(1, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))
    val benchmark = new Benchmark("back-to-back filter", numRows)
    val func = (d: Data, i: Int) => d.l % (100L + i) == 0L
    val funcs = 0.until(numChains).map { i =>
      (d: Data) => func(d, i)
    }

    val rdd = spark.sparkContext.range(1, numRows).map(l => Data(l, l.toString))
    benchmark.addCase("RDD") { iter =>
      var res = rdd
      var i = 0
      while (i < numChains) {
        res = res.filter(funcs(i))
        i += 1
      }
      res.foreach(_ => Unit)
    }

    benchmark.addCase("DataFrame") { iter =>
      var res = df
      var i = 0
      while (i < numChains) {
        res = res.filter($"l" % (100L + i) === 0L)
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("Dataset") { iter =>
      var res = df.as[Data]
      var i = 0
      while (i < numChains) {
        res = res.filter(funcs(i))
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark
  }

  object ComplexAggregator extends Aggregator[Data, Data, Long] {
    override def zero: Data = Data(0, "")

    override def reduce(b: Data, a: Data): Data = Data(b.l + a.l, "")

    override def finish(reduction: Data): Long = reduction.l

    override def merge(b1: Data, b2: Data): Data = Data(b1.l + b2.l, "")

    override def bufferEncoder: Encoder[Data] = Encoders.product[Data]

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

  def aggregate(spark: SparkSession, numRows: Long): Benchmark = {
    import spark.implicits._

    val df = spark.range(1, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))
    val benchmark = new Benchmark("aggregate", numRows)

    val rdd = spark.sparkContext.range(1, numRows).map(l => Data(l, l.toString))
    benchmark.addCase("RDD sum") { iter =>
      rdd.aggregate(0L)(_ + _.l, _ + _)
    }

    benchmark.addCase("DataFrame sum") { iter =>
      df.select(sum($"l")).queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("Dataset sum using Aggregator") { iter =>
      df.as[Data].select(typed.sumLong((d: Data) => d.l)).queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("Dataset complex Aggregator") { iter =>
      df.as[Data].select(ComplexAggregator.toColumn).queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Neptune Dataset benchmark")
    conf.enableNeptuneCoroutines()

    val sc = new SparkContext(conf)
    val spark = new SparkSession(sc)

    val numRows = 100000000
    val numChains = 10

    val benchmark0 = backToBackMapLong(spark, numRows, numChains)
    val benchmark1 = backToBackMap(spark, numRows, numChains)
    val benchmark2 = backToBackFilterLong(spark, numRows, numChains)
    val benchmark3 = backToBackFilter(spark, numRows, numChains)
    val benchmark4 = aggregate(spark, numRows)

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.6
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz

    back-to-back map long:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD                                           8962 / 9488         11.2          89.6       1.0X
    DataFrame                                     1599 / 1616         62.5          16.0       5.6X
    Dataset                                       2139 / 2238         46.8          21.4       4.2X

    Neptune ==>
    back-to-back map long:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD                                           7142 / 7144         14.0          71.4       1.0X
    DataFrame                                     1262 / 1443         79.2          12.6       5.7X
    Dataset                                       1762 / 1766         56.7          17.6       4.1X
    */
    benchmark0.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.6
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz

    back-to-back map:                        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD                                         14462 / 16030          6.9         144.6       1.0X
    DataFrame                                    8126 / 10520         12.3          81.3       1.8X
    Dataset                                     17283 / 17997          5.8         172.8       0.8X

    Neptune ==>
    back-to-back map:                        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD                                         11327 / 11354          8.8         113.3       1.0X
    DataFrame                                     7341 / 7453         13.6          73.4       1.5X
    Dataset                                     15964 / 16230          6.3         159.6       0.7X
    */
    benchmark1.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.6
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz

    back-to-back filter Long:                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD                                           3057 / 3118         32.7          30.6       1.0X
    DataFrame                                     2038 / 2089         49.1          20.4       1.5X
    Dataset                                       3513 / 3625         28.5          35.1       0.9X

    Neptune ==>
    back-to-back filter Long:                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD                                           2976 / 3190         33.6          29.8       1.0X
    DataFrame                                      996 / 1008        100.4          10.0       3.0X
    Dataset                                       2894 / 2948         34.6          28.9       1.0X
    */
    benchmark2.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.6
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz

    back-to-back filter:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD                                           5460 / 5931         18.3          54.6       1.0X
    DataFrame                                      401 /  544        249.6           4.0      13.6X
    Dataset                                     10700 / 11330          9.3         107.0       0.5X

    Neptune ==>
    back-to-back filter:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD                                           4447 / 4469         22.5          44.5       1.0X
    DataFrame                                      161 /  200        620.4           1.6      27.6X
    Dataset                                       9018 / 9290         11.1          90.2       0.5X
    */
    benchmark3.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.6
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz

    aggregate:                               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD sum                                       4328 / 4356         23.1          43.3       1.0X
    DataFrame sum                                   67 /  104       1498.4           0.7      64.9X
    Dataset sum using Aggregator                  8820 / 9703         11.3          88.2       0.5X
    Dataset complex Aggregator                  12567 / 13258          8.0         125.7       0.3X

    Neptune ==>
    aggregate:                               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD sum                                       3326 / 3331         30.1          33.3       1.0X
    DataFrame sum                                   66 /  106       1519.1           0.7      50.5X
    Dataset sum using Aggregator                  8661 / 8675         11.5          86.6       0.4X
    Dataset complex Aggregator                  11913 / 11990          8.4         119.1       0.3X
    */
    benchmark4.run()
  }
}
