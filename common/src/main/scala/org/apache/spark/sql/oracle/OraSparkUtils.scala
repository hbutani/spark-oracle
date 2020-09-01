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

package org.apache.spark.sql.oracle

import java.util.Locale

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.oracle.util.CrossProductIterator
import org.apache.spark.sql.types.{DataType, NumericType}
import org.apache.spark.util.Utils

import scala.util.Random

object OraSparkUtils {

  def getLocalDir(conf: SparkConf): String = {
    Utils.getLocalDir(conf)
  }

  def isNumeric(dt: DataType) = NumericType.acceptsType(dt)


  def setLogLevel(logLevel: String): Unit = {
    val upperCased = logLevel.toUpperCase(Locale.ENGLISH)
    org.apache.spark.util.Utils.setLogLevel(org.apache.log4j.Level.toLevel(logLevel))
  }

  def currentSparkSession : SparkSession = {
    var spkSessionO = SparkSession.getActiveSession
    if (!spkSessionO.isDefined) {
      spkSessionO = SparkSession.getDefaultSession
    }
    spkSessionO.getOrElse(???)
  }

  def currentSparkContext : SparkContext = currentSparkSession.sparkContext

  def currentSQLConf : SQLConf = {
    var spkSessionO = SparkSession.getActiveSession
    if (!spkSessionO.isDefined) {
      spkSessionO = SparkSession.getDefaultSession
    }

    spkSessionO.map(_.sqlContext.conf).getOrElse {
      val sprkConf = SparkEnv.get.conf
      val sqlConf = new SQLConf
      sprkConf.getAll.foreach { case (k, v) =>
        sqlConf.setConfString(k, v)
      }
      sqlConf
    }
  }

  def throwAnalysisException[T](msg: => String): T = {
    throw new AnalysisException(msg)
  }

  private val r = new Random()

  def nextRandomInt(n : Int) = r.nextInt(n)

  /**
   * from fpinscala book
   *
   * @param a
   * @tparam A
   * @return
   */
  def sequence[A](a: List[Option[A]]): Option[List[A]] =
    a match {
      case Nil => Some(Nil)
      case h :: t => h flatMap (hh => sequence(t) map (hh :: _))
    }

  def crossProduct[T](seqs : Seq[Seq[T]]) : Iterator[Seq[T]] = {
    val rseqs = seqs.reverse
    rseqs.tail.foldLeft(CrossProductIterator[T](None, rseqs.head)) {
      case (currItr, nSeq) => CrossProductIterator[T](Some(currItr),nSeq)
    }
  }

}
