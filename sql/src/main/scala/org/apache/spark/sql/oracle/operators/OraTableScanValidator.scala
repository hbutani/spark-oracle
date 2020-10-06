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

import java.io.PrintStream

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.read.oracle.OraFileScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

/**
 * Utility class that provides and can check
 * [[org.apache.spark.sql.oracle.operators.OraTableScanValidator.ScanDetails]]
 * in a plan.
 *
 * @param plan
 */
case class OraTableScanValidator(plan: LogicalPlan) {

  import OraTableScanValidator.ScanDetails

  private lazy val tableScans: Seq[(String, OraTableScan)] =
    plan.collect {
      case DataSourceV2ScanRelation(
          t,
          OraFileScan(_, _, _, _, _, oraPlan: OraTableScan, _, _, _),
          _) =>
        t.name() -> oraPlan
    }

  private lazy val scanDetails: Seq[(String, ScanDetails)] =
    tableScans.map {
      case (nm, oraPlan) => (nm, new ScanDetails(oraPlan))
    }

  def dumpTableScans(out: PrintStream): Unit = {

    for ((tbl, scanD) <- scanDetails) {
      // scalastyle:off println
      out.println(s"Table ${tbl} : ${scanD.code}")
      // scalastyle:on println
    }
  }

  def validateScans(reqdScans: Map[String, ScanDetails]): Unit = {

    val planScans = scanDetails.toMap

    val errorReqdScans = ArrayBuffer[(String, ScanDetails, ScanDetails)]()
    val missingReqdScans = ArrayBuffer[(String, ScanDetails)]()
    val missingPlanScans = ArrayBuffer[(String, ScanDetails)]()

    for ((tNm, reqdScan) <- reqdScans) {
      val planScan = planScans.get(tNm)
      (reqdScan, planScan) match {
        case (rP, None) => missingPlanScans += ((tNm, rP))
        case (rP, Some(sP)) if rP != sP =>
          errorReqdScans += ((tNm, rP, sP))
        case _ => ()
      }
    }

    for ((tNm, planScan) <- planScans) {
      val reqdScan = reqdScans.get(tNm)
      (reqdScan, planScan) match {
        case (None, sP) => missingReqdScans += ((tNm, sP))
        case _ => ()
      }
    }

    val errMsg = new StringBuilder

    if (errorReqdScans.nonEmpty) {
      errMsg.append("Error Matching Scans:\n")
      for ((tblNm, reqdScan, planScan) <- errorReqdScans) {
        errMsg.append(s""" Table ${tblNm} :
             |  Required Scan : ${reqdScan}
             |  Plan Scan : ${planScan}""".stripMargin)
      }
    }

    if (missingReqdScans.nonEmpty) {
      if (errMsg.nonEmpty) {
        errMsg.append("\n")
      }
      errMsg.append("Error Plan Missing Scans:\n")
      for ((tblNm, reqdScan) <- missingReqdScans) {
        errMsg.append(s""" Table ${tblNm} :
             |  Required Scan : ${reqdScan}""".stripMargin)
      }
    }

    if (missingPlanScans.nonEmpty) {
      if (errMsg.nonEmpty) {
        errMsg.append("\n")
      }
      errMsg.append("Error Plan has additional Scans:\n")
      for ((tblNm, planScan) <- missingPlanScans) {
        errMsg.append(s""" Table ${tblNm} :
             |  Plan Scan : ${planScan}""".stripMargin)
      }
    }
    assert(errMsg.isEmpty, errMsg.toString())
  }

}

object OraTableScanValidator {

  case class ScanDetails(
      projections: Seq[String],
      filter: Option[String],
      filterBindValues: Seq[Literal],
      partitionFilter: Option[String],
      partitionFilterBindValues: Seq[Literal]) {

    def this(oraPlan: OraTableScan) =
      this(
        oraPlan.projections.map(_.orasql.sql),
        oraPlan.filter.map(_.orasql.sql),
        oraPlan.filter.toSeq.flatMap(_.orasql.params),
        oraPlan.partitionFilter.map(_.orasql.sql),
        oraPlan.partitionFilter.toSeq.flatMap(_.orasql.params))

    lazy val code: String = {

      def literalCode(l: Literal) = s"Literal(${l})"

      val projCode = s"""List(${projections.map(p => s""""${p}"""").mkString(", ")})"""
      val filCode = filter.map(f => s"""Some("$f")""").getOrElse("None")
      val filterBindValuesCode =
        s"List(${filterBindValues.map(l => s"${literalCode(l)}").mkString(", ")})"
      val partitionFilterCode = partitionFilter.map(f => s"""Some("$f")""").getOrElse("None")
      val partitionFilterBindValuesCode =
        s"List(${partitionFilterBindValues.map(l => s"${literalCode(l)}").mkString(", ")})"

      s"""ScanDetails(
         |  $projCode,
         |  $filCode,
         |  $filterBindValuesCode,
         |  $partitionFilterCode,
         |  $partitionFilterBindValuesCode
         |)""".stripMargin

    }
  }
}
