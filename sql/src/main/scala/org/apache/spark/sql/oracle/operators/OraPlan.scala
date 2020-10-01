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

package org.apache.spark.sql.oracle.operators

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraTable
import org.apache.spark.sql.oracle.expressions.OraExpression
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

/**
 * Represents an Oracle Query, captured as an Operator tree that can be
 * manipulated. In general an [[OraPlan]] is mapped to the catalyst
 * [[LogicalPlan]] it was mapped from, but for a Scan this is setup
 * from information in an
 * [[org.apache.spark.sql.connector.read.oracle.OraScanBuilder]] or a
 * [[org.apache.spark.sql.connector.read.oracle.OraScan]].
 * The reason for this is we have a chicken-and-egg problem: a
 * [[org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation]]
 * is built from a [[Scan]] and an
 * [[org.apache.spark.sql.connector.read.oracle.OraScan]] needs a [[LogicalPlan]].
 *
 */
abstract class OraPlan extends TreeNode[OraPlan] with PlanHelper with ScanBuilder {

  /*
   * Notes:
   * - catalystOp: in general OraPlan should be associated with one
   *   - but in case of a orascan Op there is no associated LogPlan
   *     this is built from an OraTable
   *     Here we have chicken-and-egg issue: DSv2Scan is built from to a OraScan
   *     which needs a reference to a LogPlan since it is a OraPlan
   *     So make catalystOp optional
   * - do we need to resolve AttrRef; how to convert AttrRef
   *   - can assume Attr in expressions are resolved
   *   - so conversion is to an OraAttr that references the underlying AttrRef
   */

  def catalystOp: Option[LogicalPlan]

  def catalystOutputSchema: AttributeSet

  /**
   * generate a parameterized query; convert literals in query into bind values.
   * @param sqlBldr
   * @param params
   */
  def genOraSQL(sqlBldr: StringBuilder, params: ArrayBuffer[Any]): Unit

  override def simpleStringWithNodeId(): String = {
    val operatorId = catalystOp
      .flatMap(_.getTagValue(QueryPlan.OP_ID_TAG))
      .map(id => s"$id")
      .getOrElse("unknown")
    s"$nodeName ($operatorId)".trim
  }

  override def verboseString(maxFields: Int): String = simpleString(maxFields)

}

object OraPlan {

  def generateOraSQL(oraPlan: OraPlan): (String, Array[Any]) = {
    val sqlBldr = new StringBuilder
    val params = ArrayBuffer[Any]()
    oraPlan.genOraSQL(sqlBldr, params)
    (sqlBldr.toString(), params.toArray)
  }

  def filter(
      oraPlan: OraPlan,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): OraPlan = {

    var plan = oraPlan

    if (dataFilters.nonEmpty) {
      val datFils = OraExpression.convert(dataFilters, oraPlan.catalystOutputSchema)
      plan = plan.filter(datFils, false)
    }

    if (partitionFilters.nonEmpty) {
      val partFils = OraExpression.convert(partitionFilters, oraPlan.catalystOutputSchema)
      plan = plan.filter(partFils, true)
    }
    plan
  }

  def buildOraPlan(
      table: OraTable,
      requiredSchema: StructType,
      pushedFilters: Array[Filter]): OraPlan = {

    // TODO and oraExpressions
    val pushedOraExpressions: Array[OraExpression] =
      pushedFilters.flatMap(OraExpression.convert(_, table).toSeq)

    val attrs = requiredSchema.toAttributes
    val attrSet = AttributeSet(attrs)
    val oraProjs = attrs.map(OraExpression.convert(_, attrSet))

    OraTableScan(
      table,
      None,
      attrSet,
      oraProjs,
      if (pushedOraExpressions.nonEmpty) Some(pushedOraExpressions.head) else None,
      None)
  }

  /**
   * If Plan is such that most of the processing is on a single
   * table, then use its stats as an estimate for the Plan
   * @param oraPlan
   * @return
   */
  def useTableStatsForPlan(oraPlan: OraPlan): Option[OraTable] = {
    // TODO
    None
  }
}
