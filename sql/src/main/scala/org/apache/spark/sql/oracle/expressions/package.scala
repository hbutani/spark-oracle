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

package object expressions {

  private[expressions] val MINUS = "-"
  private[expressions] val PLUS = "+"
  private[expressions] val MULTIPLY = "*"
  private[expressions] val DIVIDE = "/"
  private[expressions] val ABS = "ABS"
  private[expressions] val TRUNC = "TRUNC"
  private[expressions] val REMAINDER = "REMAINDER"
  private[expressions] val LEAST = "LEAST"
  private[expressions] val GREATEST = "GREATEST"
  private[expressions] val MOD = "MOD"
  private[expressions] val NOT = "NOT"
  private[oracle] val AND = "AND"
  private[expressions] val OR = "OR"
  private[expressions] val DECODE = "DECODE"
  private[oracle] val EQ = "="
  private[expressions] val ISNULL = "IS NULL"
  private[expressions] val ISNOTNULL = "IS NOT NULL"
}
