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
package org.apache.spark.sql.execution.streaming

import scala.collection.mutable.ListBuffer

class AdaptiveQueue[A](max: Int) {
   /*
    * LIFO implementation using a List - used for adaptation History.
    * The smaller the List the more aggressive the adaptation.
    */
  val list: ListBuffer[A] = ListBuffer()

  def append(elem: A) {
    if (list.size == max) {
      list.trimStart(1)
    }
    list.append(elem)
  }

  def toAdapt(toClean: Boolean = false): Boolean = {
    val toReturn: Int = {
      if (list.isEmpty || list.size!=max) {
        0
      }
      else {
        if (list.count( _ == 1 ) > max/2) {
          1
        }
        else {
          0
        }
      }
    }
    if ((toReturn == 1) && (toClean == true)) {
      list.trimStart(max)
    }
    toReturn == 1
  }
}
