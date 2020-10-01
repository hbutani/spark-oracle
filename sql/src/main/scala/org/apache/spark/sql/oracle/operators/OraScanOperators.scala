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

import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraTable
import org.apache.spark.sql.oracle.expressions.OraExpression

trait ScanBuilder { self: OraPlan =>

  def filter(filter: OraExpression, isPartFilter: Boolean): OraPlan = {

    def setFil(os: OraTableScan, fil: OraExpression) =
      if (!isPartFilter) {
        os.copy(filter = Some(fil))
      } else {
        os.copy(partitionFilter = Some(fil))
      }

    self match {
      case os @ OraTableScan(_, _, _, _, None, _) => setFil(os, filter)
      case os @ OraTableScan(_, _, _, _, Some(oFil), _) =>
        // TODO and oFil and filter
        setFil(os, filter)
      case _ =>
        illegalPlanBuild(
          "filter",
          self,
          "on a OraPlan that is not a scan, provide associated catalyst filter op")
    }
  }

  def filter(catalystFil: Filter): OraPlan = {
    val oraFilExpr = OraExpression.convert(catalystFil.condition, catalystFil.inputSet)
    self match {
      case os: OraTableScan => filter(oraFilExpr, false)
      case _ => OraFilter(self, oraFilExpr, catalystFil)
    }
  }

  def project(catalystProj: Project): OraPlan = {
    val oraProjs = catalystProj.projectList.map(OraExpression.convert(_, catalystProj.inputSet))
    self match {
      case os: OraTableScan =>
        os.copy(
          projections = oraProjs,
          catalystOp = Some(catalystProj),
          catalystOutputSchema = catalystProj.outputSet)
      case _ => OraProject(self, oraProjs, catalystProj)
    }
  }

}

case class OraTableScan(
    oraTable: OraTable,
    catalystOp: Option[LogicalPlan],
    catalystOutputSchema: AttributeSet,
    projections: Seq[OraExpression],
    filter: Option[OraExpression],
    partitionFilter: Option[OraExpression])
    extends OraPlan {

  val children: Seq[OraPlan] = Seq.empty

  override def genOraSQL(sqlBldr: StringBuilder, params: ArrayBuffer[Any]): Unit = {
    // TODO
    ???
  }
}

case class OraFilter(child: OraPlan, filter: OraExpression, catalystFil: Filter) extends OraPlan {

  val children: Seq[OraPlan] = Seq(child)
  val catalystOp: Option[LogicalPlan] = Some(catalystFil)
  val catalystOutputSchema: AttributeSet = catalystFil.outputSet

  override def genOraSQL(sqlBldr: StringBuilder, params: ArrayBuffer[Any]): Unit = {
    // TODO
    ???
  }
}

case class OraProject(child: OraPlan, projections: Seq[OraExpression], catalystProj: Project)
    extends OraPlan {

  val children: Seq[OraPlan] = Seq(child)
  val catalystOp: Option[LogicalPlan] = Some(catalystProj)
  val catalystOutputSchema: AttributeSet = catalystProj.outputSet

  override def genOraSQL(sqlBldr: StringBuilder, params: ArrayBuffer[Any]): Unit = {
    // TODO
    ???
  }
}
