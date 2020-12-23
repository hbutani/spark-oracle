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

package org.apache.spark.sql.oracle.util

import org.apache.spark.internal.Logging

trait TimeIt { self : Logging =>

  def timeIt[U](action : String)(actionFn : => U) : U = {
    if (log.isInfoEnabled) {
      val sTime = System.currentTimeMillis()
      val r = actionFn
      val eTime = System.currentTimeMillis()
      logInfo(s"${Thread.currentThread().getName} " +
        s"$action = ${eTime - sTime}, " +
        s"Memory: ${Runtime.getRuntime.freeMemory() / (1000 * 1000)}")
      r
    } else {
      actionFn
    }
  }

}
