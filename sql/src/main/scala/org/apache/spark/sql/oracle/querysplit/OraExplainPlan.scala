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
package org.apache.spark.sql.oracle.querysplit

import scala.util.Try

import oracle.spark.{DataSourceKey, ORAMetadataSQLs}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.oracle.OraSparkUtils.sequence
import org.apache.spark.sql.oracle.expressions.OraLiterals
import org.apache.spark.sql.oracle.operators.OraPlan


/**
 * Invoke [[ORAMetadataSQLs#queryPlan]] to get the oracle plan in xml form.
 * Extract overall plan stats and when `extractTabAccess = true` extract
 * ''Table Access Operations'' and build [[TableAccessOperation]] structs.
 * Return the [[PlanInfo]] for given [[OraPlan]]
 */
object OraExplainPlan extends Logging {

  import scala.xml.{Node, NodeSeq}
  import scala.xml.XML._

  private def oraExplainPlanXML(dsKey: DataSourceKey, oraPlan: OraPlan): String = {
    val oSQL = oraPlan.orasql
    ORAMetadataSQLs.queryPlan(dsKey, oSQL.sql, ps => {
      OraLiterals.bindValues(ps, oSQL.params)
    })
  }

  def constructPlanInfo(dsKey: DataSourceKey,
                        oraPlan: OraPlan,
                        extractTabAccess: Boolean): Option[PlanInfo] = {
    val pO = PlanXMLReader.parsePlan(
      oraExplainPlanXML(dsKey, oraPlan),
      extractTabAccess
    )

    if (!pO.isDefined) {
      logWarning(
        s"""Failed to construct PlanInfo for generated sql:
           |${oraPlan.orasql.sql}
           |""".stripMargin
      )
    }
    pO
  }

  private object PlanXMLReader {

    def textValue(nd: NodeSeq): Option[String] = {
      if (nd.nonEmpty) Some(nd.text) else None
    }

    def intValue(nd: NodeSeq): Option[Int] = {
      textValue(nd).flatMap(v => Try { v.toInt }.toOption)
    }

    def longValue(nd: NodeSeq): Option[Long] = {
      textValue(nd).flatMap(v => Try { v.toLong }.toOption)
    }

    def doubleValue(nd: NodeSeq): Option[Double] = {
      textValue(nd).flatMap(v => Try { v.toDouble }.toOption)
    }

    case class OpCost(row_count: Long, bytes : Long)

    def operations(root: NodeSeq): NodeSeq = root \ "plan" \ "operation"

    def opCost(nd: Node): Option[OpCost] = {

      for (row_count <- longValue(nd \ "card");
           bytes <- longValue(nd \ "bytes")
           ) yield {
        OpCost(row_count, bytes)
      }
    }

    def operation(nd: Node): Option[TableAccessOperation] = {
      for (tbNm <- textValue(nd \ "object");
           qBlk <- textValue(nd \ "qblock");
           obj_alias <- textValue(nd \ "object_alias");
           row_count <- longValue(nd \ "card");
           bytes <- longValue(nd \ "bytes");
           partition = nd \ "partition") yield {

        val pRng : Option[(Int, Int)] = if (partition.nonEmpty) {
          for (startS <- partition.head.attribute("start");
               start <- intValue(NodeSeq.fromSeq(startS));
               endS <- partition.head.attribute("stop");
               end <- intValue(NodeSeq.fromSeq(endS))) yield {
            (start, end)
          }
        } else None

        val alias = obj_alias.replaceAll("\\\"", "").split("@").head
        TableAccessOperation(tbNm, qBlk, obj_alias, alias, row_count, bytes, pRng)
      }
    }

    def tableAccessOperations(nd : NodeSeq) : Option[Seq[TableAccessOperation]] = {
      sequence(
      nd.filter { nd =>
        val nmS = nd.attribute("name")
        val nm = nmS.flatMap(s => textValue(NodeSeq.fromSeq(s)))
        nm.isDefined && nm.get == "TABLE ACCESS"
      }.map(operation)
      )
    }

    def parsePlan(xml: String, extractTabAccess: Boolean): Option[PlanInfo] = {

      val root = loadString(xml)
      val ops = operations(root)

      for(
        rootOpCost <- opCost(ops.head);
        tblOps <- if (extractTabAccess) tableAccessOperations(ops) else Some(Seq.empty)
      ) yield {
        PlanInfo(
          rootOpCost.row_count,
          rootOpCost.bytes,
          tblOps
        )
      }
    }
  }
}
