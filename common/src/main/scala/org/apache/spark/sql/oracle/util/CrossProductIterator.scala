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

case class CrossProductIterator[T](val nextItr : Option[CrossProductIterator[T]],
                                   val mySeq : Seq[T]) extends Iterator[Seq[T]] {

  private var itr = mySeq.iterator
  private var v : Option[T] = None

  init

  private def setV : Unit = {
    v = if (itr.hasNext) {
      Some(itr.next)
    } else None
  }

  private def init : Unit = {
    if (nextItr.isDefined) {
      nextItr.get.init
    }
    itr = mySeq.iterator
    setV
  }

  // for testing only
  def getState : Seq[Option[T]] = nextItr match {
    case Some(nItr) => v +: nItr.getState
    case None => Seq(v)
  }

  private def advance : Unit =  nextItr match {
    case Some(nItr) => {
      nItr.advance
      if (!nItr.hasNext) {
        v = if (itr.hasNext) {
          nItr.init
          Some(itr.next)
        } else None
      }
    }
    case None => {
      v = if (itr.hasNext) {
        Some(itr.next)
      } else None
    }
  }

  override def hasNext: Boolean = nextItr match {
    case Some(nItr) => nItr.hasNext && v.isDefined
    case None => v.isDefined
  }

  private def _next(): Seq[T] = nextItr match {
    case Some(nItr)  => {
      v.get +: nItr._next
    }
    case None => {
      Seq(v.get)
    }
  }

  override def next(): Seq[T] = {
    val s = _next()
    advance
    s
  }
}


