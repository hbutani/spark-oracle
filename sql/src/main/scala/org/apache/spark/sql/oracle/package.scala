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

import org.apache.spark.sql.catalyst.trees.TreeNode

package object oracle {

  trait UnSupportedActionHelper[T <: TreeNode[T]] {

    /*
     * Such as 'Illegal' or 'Unsupported'
     */
    def unsupportVerb: String

    /*
     * Such as 'OraPlan build action'
     */
    def actionKind: String

    def apply(action: String, node: T, reason: Option[String] = None): Nothing = {
      val reasonStr = if (reason.isDefined) {
        s"\n  reason: ${reason.get}\n"
      } else ""

      val nodeStr = if (node != null) {
        s" on\n  node: ${node}"
      } else ""

      throw new UnsupportedOperationException(
        s"${unsupportVerb} ${actionKind}: ${action}${nodeStr}${reasonStr}")
    }

    def apply(action: String, node: T, reason: String): Nothing =
      apply(action, node, Some(reason))

  }
}
