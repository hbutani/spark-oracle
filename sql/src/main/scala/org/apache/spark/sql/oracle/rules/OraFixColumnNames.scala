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
package org.apache.spark.sql.oracle.rules

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.expressions.Named.{OraAlias, OraColumnRef, OraNamedExpression, QualFixedColNm, UnQualFixedColNm}
import org.apache.spark.sql.oracle.expressions.OraExpression
import org.apache.spark.sql.oracle.expressions.Subquery.OraSubqueryExpression
import org.apache.spark.sql.oracle.operators.{OraPlan, OraQueryBlock, OraTableScan}

/**
 * Ensure 'correct' column names used in oracle-sql. This entails 2 things:
 *  - where possible use the catalog name of a column instead of the case insensitive
 *    name used in Spark.
 *  - When there are multiple
 *    [[org.apache.spark.sql.catalyst.expressions.AttributeReference attributes]]
 *    with the same name apply de-dup strategies of qualifying names or generating
 *    new names.
 *
 * '''Column Name case:'''
 *
 * In ''Spark SQL'' the default and preferred behavior is that of '''case insensitive'''
 * resolution of column names. Spark documenation says:
 * 'highly discouraged to turn on case sensitive mode.'
 * This means that if you have a column defined with the mixed case name 'aA', in
 * Spark SQL it is ok to refer to it as aa or AA... Further the optimized Logical Plan
 * will carry an [[AttributeReference]] with that name(aa or AA..) used in the sql statement.
 *
 * Whereas in oracle sql unquoted names are treated to mean upper-case names. So unquoted
 * names such as aa or AA .. used in a sql statement are resolved to mean the column 'AA'.
 *
 * So when generating oracle sql from a Spark Optimized Logical Plan we need to
 * walk down the Spark Plan to find the Catalog column that an [[AttributeReference]]
 * is for and use that name.
 *
 * '''Column Qualification:'''
 *
 * In [[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan Optimized Spark Plans]] columns
 * are identified by [[org.apache.spark.sql.catalyst.expressions.ExprId expr_id]]; so it is
 * possible to end up with Operator shapes that contain multiple columns with the
 * same name. When generating `Oracle SQL` from these plans we need to disambiguate
 * duplicate names.
 *
 * We will operate on a [[OraPlan translated oracle plan]] that is output from the
 * [[OraSQLPushdownRule]] and apply the following disambiguation logic.
 *  - if there are multiple attributes with the same name among the inputs
 *    of a [[OraQueryBlock]], we disambiguate these attributes by qualifying each
 *    by the input they are from. Inputs to the QueryBlock are assigned ''qualifiers''.
 *  - if the output of a [[OraPlan]] have multiple attributes with the same name.
 *    We generate new aliases form duplicate attributes.
 *
 * '''Example:'''
 *
 * In the following query `C_INT` is a column in 4 table references. The optimized spark plan
 * contains the [[org.apache.spark.sql.catalyst.expressions.AttributeReference]] `C_INT`
 * for all of them. With the disambiguated oracle-sql is also listed below. `C_INT`
 * references in the top query block are qualified and in the top select the projections
 * have new aliases associated.
 * {{{
 * // SQL
 * select a.c_int, b.c_int, c.c_int, d.c_int
 * from sparktest.unit_test a,
 *      sparktest.unit_test_partitioned b,
 *      sparktest.unit_test c,
 *      sparktest.unit_test_partitioned d
 * where a.c_int = b.c_int and b.c_int = c.c_int and c.c_int = d.c_int
 *
 * // Spark optimized plan without oracle pushdown
 * !Join Inner, (c_int#46 = c_int#63)
 * !:- Join Inner, (c_int#27 = c_int#46)
 * !:  :- Join Inner, (c_int#10 = c_int#27)
 * !:  :  :- Filter isnotnull(C_INT#10)
 * !:  :  :  +- RelationV2[C_INT#10] SPARKTEST.UNIT_TEST
 * !:  :  +- Filter isnotnull(C_INT#27)
 * !:  :     +- RelationV2[C_INT#27] SPARKTEST.UNIT_TEST_PARTITIONED
 * !:  +- Filter isnotnull(C_INT#46)
 * !:     +- RelationV2[C_INT#46] SPARKTEST.UNIT_TEST
 * !+- Filter isnotnull(C_INT#63)
 * !   +- RelationV2[C_INT#63] SPARKTEST.UNIT_TEST_PARTITIONED
 *
 * // oracle-sql generated
 * SELECT "sparkora_0"."C_INT" AS "C_INT_1_sparkora",
 *        "sparkora_1"."C_INT" AS "C_INT_2_sparkora",
 *        "sparkora_2"."C_INT" AS "C_INT_3_sparkora",
 *        "sparkora_3"."C_INT" AS "C_INT_4_sparkora"
 * FROM SPARKTEST.UNIT_TEST "sparkora_0"
 * JOIN SPARKTEST.UNIT_TEST_PARTITIONED "sparkora_1"
 *          ON ("sparkora_0"."C_INT" = "sparkora_1"."C_INT")
 * JOIN SPARKTEST.UNIT_TEST "sparkora_2"
 *          ON ("sparkora_1"."C_INT" = "sparkora_2"."C_INT")
 * JOIN SPARKTEST.UNIT_TEST_PARTITIONED "sparkora_3"
 *          ON ("sparkora_2"."C_INT" = "sparkora_3"."C_INT")
 * WHERE ((("sparkora_0"."C_INT" IS NOT NULL
 *          AND "sparkora_1"."C_INT" IS NOT NULL)
 *         AND "sparkora_2"."C_INT" IS NOT NULL)
 *        AND "sparkora_3"."C_INT" IS NOT NULL)
 * }}}
 */
object OraFixColumnNames extends OraLogicalRule with Logging {

  val ORA_FIXED_NAMES_TAG = TreeNodeTag[Boolean]("fixedOraColumnNames")

  override def _apply(plan: LogicalPlan)(implicit sparkSession: SparkSession): LogicalPlan =
    plan transformUp {
      case dsv2@DataSourceV2ScanRelation(_, oraScan: OraScan, _) =>
        val oraPlan = oraScan.oraPlan
        val namesFixed = oraPlan.getTagValue(ORA_FIXED_NAMES_TAG).getOrElse(false)
        if (!namesFixed) {
          fixNames(oraPlan)
          oraPlan.setTagValue(ORA_FIXED_NAMES_TAG, true)
        }
        dsv2
    }

  def fixNames(oraPlan: OraPlan): Unit =
    FixPlan(oraPlan, 0).execute

  private type SourcePos = Int
  private val NAME_TAG = "sparkora"


  sealed trait FixPlan {
    def oraPlan: OraPlan
    def projectList : Seq[OraExpression]
    def numSources: Int
    def pos: SourcePos
    lazy val qualifier: String = s"${NAME_TAG}_${pos}"
    def source(id: SourcePos): FixPlan

    case class QualCol(srcPos: SourcePos, name: String) {
      lazy val fixedName = QualFixedColNm(source(srcPos).qualifier, name)
    }

    case class InputDetails(inEIdMap : Map[ExprId, QualCol]) {
      val dupNames: Map[String, Seq[ExprId]] = {
        (for ((eId, qNm) <- inEIdMap.toSeq) yield {
          (qNm.name, eId)
        }).groupBy(_._1).
          filter(t => t._2.size > 1).
          mapValues(s => s.map(_._2))
      }

      val qualifiedExprIds = dupNames.values.flatten.toSet

      val qualifiedInputs : Set[SourcePos] = inEIdMap.filter {
        case (eId, _) => qualifiedExprIds.contains(eId)
      }.map(t => t._2.srcPos).toSet

      def fixName(oc: OraColumnRef): Unit = {
        val exprId = oc.catalystExpr.exprId
        if (qualifiedExprIds.contains(exprId)) {
          oc.setOraFixedNm(inEIdMap(exprId).fixedName)
        } else {
          val inNm = inEIdMap(exprId).name
          if (inNm != oc.outNmInOraSQL) {
            oc.setOraFixedNm(UnQualFixedColNm(inNm))
          }
        }
      }
    }

    lazy val inDetails = {
      val inEIdMap : Map[ExprId, QualCol] = (
          for (srcPos <- (0 until numSources))  yield {
          source(srcPos).outEIdMap.mapValues(s => QualCol(srcPos, s))
          }
        ).reduceLeft(_ ++ _)

      InputDetails(inEIdMap)
    }

    protected def fixInternals : Unit

    def outEIdMap : Map[ExprId, String]

    def fixProjectList : Unit = projectList.foreach {
      case oNE : OraNamedExpression =>
        val outNm = outEIdMap(oNE.catalystExpr.exprId)
        if (oNE.outNmInOraSQL != outNm) {
          oNE.setOraFixedAlias(outNm)
        }
      case _ => ()
    }

    def execute : Unit = {
      fixInternals
      fixProjectList
    }
  }

  case class TableScanFixPlan(oraPlan : OraTableScan,
                              pos: SourcePos) extends FixPlan {

    override def projectList: Seq[OraExpression] = oraPlan.projections
    override def numSources: SourcePos = 0
    override def source(id: SourcePos): FixPlan = null
    override protected def fixInternals : Unit = ()

    override def outEIdMap: Map[ExprId, String] =
      oraPlan.catalystAttributes.map(a => a.exprId -> a.name).toMap
  }

  case class QBlkFixPlan(oraPlan : OraQueryBlock,
                         pos: SourcePos) extends FixPlan {
    val projectList: Seq[OraExpression] = oraPlan.select
    val numSources: SourcePos = 1 + oraPlan.joins.size

    val childPlansMap: Map[SourcePos, FixPlan] = {
      val childPlans = FixPlan(oraPlan.source, 0) +:
        (for ((jn, i) <- oraPlan.joins.zipWithIndex) yield {
          FixPlan(jn.joinSrc, i + 1)
        })

      (for (cP <- childPlans) yield {
        (cP.pos, cP)
      }).toMap
    }

    case class OutputDetails(projectList : Seq[OraExpression]) {
      /*
       * In Sub-queries the projectList entry may not be OraNamedExpression.
       * We can ignore these; the sub-query predicate's output doesn't apply to its
       * outer query
       */
      val eIdMap : Map[ExprId, String] = (projectList.collect {
        case oNE : OraNamedExpression => oNE.catalystExpr.exprId -> oNE.outNmInOraSQL
      }).toMap

      val dupNames: Map[String, Seq[ExprId]] = {
        (for ((eId, nm) <- eIdMap.toSeq) yield {
          (nm, eId)
        }).groupBy(_._1).
          filter(t => t._2.size > 1).
          mapValues(s => s.map(_._2))
      }

      val dupExprIds = dupNames.values.flatten.toSet

      val outEIdMap : Map[ExprId, String] = {
        var i: Int = 0
        for((eId, nm) <- eIdMap) yield {
          val oNm = if (dupExprIds.contains(eId)) {
            i += 1
            s"${nm}_${i}_${NAME_TAG}"
          } else nm
          eId -> oNm
        }
      }
    }

    lazy val outDetails = OutputDetails(projectList)
    lazy val outEIdMap : Map[ExprId, String] = outDetails.outEIdMap

    override def source(id: SourcePos): FixPlan = childPlansMap(id)

    override protected def fixInternals: Unit = {
      def fixOE(oE : OraExpression) : Unit = {
        oE.foreachUp {
          case oc : OraColumnRef => inDetails.fixName(oc)
          case _ => ()
        }
      }

      /* 0. fix subquery expressions */
      for (oE <- oraPlan.where) {
        val subQueries = oE.collect {
          case oSE : OraSubqueryExpression => oSE.oraPlan
        }
        for(sQ <- subQueries) {
          OraFixColumnNames.fixNames(sQ)
        }
      }

      /* 1. source and joins */
      if (inDetails.qualifiedInputs.contains(0)) {
        val child = childPlansMap(0)
        oraPlan.setSourceAlias(child.qualifier)
      }

      for ((jc, i) <- oraPlan.joins.zipWithIndex) yield {
        val childPos = i + 1
        if (inDetails.qualifiedInputs.contains(childPos)) {
          val child = childPlansMap(childPos)
          jc.setJoinAlias(child.qualifier)
        }
        fixOE(jc.onCondition)
      }

      /* select */
      oraPlan.select.foreach(fixOE)

      /* where */
      oraPlan.where.foreach(fixOE)

      /* groupBys */
      oraPlan.groupBy.foreach(gBys => gBys.foreach(fixOE))

    }
  }

  object FixPlan {
    def apply(oraPlan: OraPlan,
              pos: SourcePos) : FixPlan = oraPlan match {
      case oT : OraTableScan => TableScanFixPlan(oT, pos)
      case qBlk : OraQueryBlock => QBlkFixPlan(qBlk, pos)
      case _ => null
    }
  }
}
