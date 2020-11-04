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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Expression, IsNotNull, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.{OraSparkUtils, SQLSnippet}
import org.apache.spark.sql.oracle.expressions.{AND, OraBinaryOpExpression, OraExpression, OraExpressions}
import org.apache.spark.sql.oracle.expressions.Predicates.OraSubQueryFilter
import org.apache.spark.sql.oracle.operators.OraQueryBlock

/**
 * '''Left Semi Joins:'''
 *
 * `in subquery` and `exists correlated subquery` predicates in Spark SQL
 * get translated into a `Left Semi-Join` operation.
 * We translate these into `in subquery` when generating Oracle SQL.
 *
 * ''Example 1:''
 * For the Spark SQL query:
 * {{{
 * select c_long
 * from sparktest.unit_test
 * where c_int in (select c_int
 *                 from sparktest.unit_test_partitioned
 *                 where c_long = sparktest.unit_test.c_long
 *                 )
 * }}}
 * the Spark Plan is:
 * {{{
 * !Project [C_LONG#13L]
 * !+- Join LeftSemi, ((C_INT#12 = C_INT#29) AND (c_long#30L = C_LONG#13L))
 * !   :- RelationV2[C_INT#12, C_LONG#13L] SPARKTEST.UNIT_TEST
 * !   +- RelationV2[C_INT#29, C_LONG#30L] SPARKTEST.UNIT_TEST_PARTITIONED
 * }}}
 * This gets translated to the following Oracle SQL:
 * {{{
 * select "C_LONG"
 * from SPARKTEST.UNIT_TEST
 * where  ("C_INT", "C_LONG") in
 *    ( select "C_INT", "C_LONG" from SPARKTEST.UNIT_TEST_PARTITIONED )
 * }}}
 *
 * ''Example 2:''
 * For the Spark SQL query:
 * {{{
 * with ssales as
 * (select ss_item_sk
 * from store_sales
 * where ss_customer_sk = 8
 * ),
 * ssales_other as
 * (select ss_item_sk
 * from store_sales
 * where ss_customer_sk = 10
 * )
 * select ss_item_sk
 * from ssales
 * where exists (select ssales_other.ss_item_sk
 *               from ssales_other
 *               where ssales_other.ss_item_sk = ssales.ss_item_sk
 *               )
 * }}}
 * the Spark Plan is:
 * {{{
 * !Join LeftSemi, (ss_item_sk#183 = SS_ITEM_SK#160)
 * !:- Project [SS_ITEM_SK#160]
 * !:  +- Filter (isnotnull(SS_CUSTOMER_SK#161) AND (SS_CUSTOMER_SK#161 = 8.000000000000000000))
 * !:     +- RelationV2[SS_ITEM_SK#160, SS_CUSTOMER_SK#161] TPCDS.STORE_SALES
 * !+- RelationV2[SS_ITEM_SK#183] TPCDS.STORE_SALES
 * }}}
 * This gets translated to the following Oracle SQL:
 * {{{
 * select "SS_ITEM_SK"
 * from TPCDS.STORE_SALES
 * where (("SS_CUSTOMER_SK" IS NOT NULL AND ("SS_CUSTOMER_SK" = ?)) AND  "SS_ITEM_SK" in ( select "SS_ITEM_SK"
 * from TPCDS.STORE_SALES
 * where ("SS_CUSTOMER_SK" IS NOT NULL AND ("SS_CUSTOMER_SK" = ?)) ))
 * }}}
 *
 * '''Left Anti Joins (not in):'''
 *
 * `not in subquery`  predicates in Spark SQL
 * get translated into a `Left Anti-Join` operation with a
 * 'is null' equality conjunction. See [[NotInJoinPattern]] for more details.
 * We translate these into `not in subquery` when generating Oracle SQL.
 *
 * For the Spark SQL query:
 * {{{
 * select c_long
 * from sparktest.unit_test
 * where c_int not in (select c_int
 *                 from sparktest.unit_test_partitioned
 *                 where c_long = sparktest.unit_test.c_long
 *                 )
 * }}}
 * the Spark Plan is:
 * {{{
 * !Project [C_LONG#309L]
 * !+- Join LeftAnti, (((C_INT#308 = C_INT#325) OR isnull((C_INT#308 = C_INT#325))) AND (c_long#326L = C_LONG#309L))
 * !   :- RelationV2[C_INT#308, C_LONG#309L] SPARKTEST.UNIT_TEST
 * !   +- RelationV2[C_INT#325, C_LONG#326L] SPARKTEST.UNIT_TEST_PARTITIONED
 * }}}
 * - the join condition on the [[LeftAnti]] join outputs `unit_test` rows with null 'c_int' values.
 * This gets translated to the following Oracle SQL:
 * {{{
 * select "C_LONG"
 * from SPARKTEST.UNIT_TEST
 * where  ("C_LONG", "C_INT") NOT IN ( select "C_LONG", "C_INT"
 * from SPARKTEST.UNIT_TEST_PARTITIONED )
 * }}}
 * - we translate into an oracle sql not in subquery and rely on no-in smenatics in Oracle.
 *
 * '''Left Anti Joins (not exists):'''
 * For the Spark SQL query:
 * {{{
 * select c_long
 * from sparktest.unit_test
 * where not exists (select c_int
 *                 from sparktest.unit_test_partitioned
 *                 where c_long = sparktest.unit_test.c_long and
 *                       c_int = sparktest.unit_test.c_int
 *                 )
 * }}}
 * the Spark Plan is:
 * {{{
 * !Project [C_LONG#187L]
 * !+- Join LeftAnti, ((c_long#204L = C_LONG#187L) AND (c_int#203 = C_INT#186))
 * !   :- RelationV2[C_INT#186, C_LONG#187L] SPARKTEST.UNIT_TEST
 * !   +- RelationV2[C_INT#203, C_LONG#204L] SPARKTEST.UNIT_TEST_PARTITIONED
 * }}}
 * This gets translated to the following Oracle SQL:
 * {{{
 *   select "C_LONG"
 * from SPARKTEST.UNIT_TEST
 * where  ("C_LONG", "C_INT") NOT IN ( select "C_INT", "C_LONG"
 * from SPARKTEST.UNIT_TEST_PARTITIONED
 * where ("c_long" IS NOT NULL AND "c_int" IS NOT NULL) )
 * }}}
 * Since we don't translate into a not exists Oracle subquery,
 * we push not null checks against joining keys.
 * See [[https://tipsfororacle.blogspot.com/2016/09/not-in-vs-not-exists.html difference between not in and not exists]]
 * semantics in oracle sql.
 *
 * @param inDSScan
 * @param leftOraScan
 * @param leftQBlk
 * @param rightQBlk
 * @param pushdownCatalystOp
 * @param joinType
 * @param leftKeys
 * @param rightKeys
 * @param joinCond
 * @param sparkSession
 */
case class SemiAntiJoinPushDown(inDSScan: DataSourceV2ScanRelation,
                                leftOraScan: OraScan,
                                leftQBlk: OraQueryBlock,
                                rightQBlk: OraQueryBlock,
                                pushdownCatalystOp: Join,
                                joinType: JoinType,
                                leftKeys: Seq[Expression],
                                rightKeys: Seq[Expression],
                                joinCond: Option[Expression],
                                sparkSession: SparkSession
                               )
  extends OraPushdown with PredicateHelper {
  override val inOraScan: OraScan = leftOraScan
  override val inQBlk: OraQueryBlock = leftQBlk

  private def pushSemiJoin : Option[OraQueryBlock] = {
    val joinOp = pushdownCatalystOp

    for (leftOraExprs <- OraExpressions.unapplySeq(leftKeys)) yield {
      val oraExpression: OraExpression = OraSubQueryFilter(
        joinOp,
        leftOraExprs,
        SQLSnippet.IN,
        rightQBlk)
      val newFil = currQBlk.where.map(f =>
        OraBinaryOpExpression(AND,
          And(f.catalystExpr, oraExpression.catalystExpr),
          f, oraExpression
        )
      ).getOrElse(oraExpression)

      currQBlk.copy(
        where = Some(newFil),
        catalystOp = Some(joinOp),
        catalystProjectList = joinOp.output
      )
    }
  }

  private def pushNotExists : Option[OraQueryBlock] = {
    val joinOp = pushdownCatalystOp
    val rightNullChecks : Expression = rightKeys.map(IsNotNull(_)).reduceLeft(And)
    for (
      leftOraExprs <- OraExpressions.unapplySeq(leftKeys);
      rightOraNotNullExprs <- OraExpression.unapply(rightNullChecks)
    ) yield {

      /*
       * 1. Push is not null checks into right Query Block.
       * - the `rightKeys` are [[AttributeReference]]s on rightQBlock
       * - so pushing conditions w/o any checks or alias substitution.
       */
      val newRFil = rightQBlk.where.map(f =>
        OraBinaryOpExpression(AND, And(f.catalystExpr, rightNullChecks), f, rightOraNotNullExprs)
      ).getOrElse(rightOraNotNullExprs)

      val newRQBlk = rightQBlk.copy(where = Some(newRFil))

      /*
       * 2. Add NOT IN predicate to `currQBlk`
       */
      val oraExpression: OraExpression = OraSubQueryFilter(
        joinOp,
        leftOraExprs,
        SQLSnippet.NOT_IN,
        newRQBlk)
      val newFil = currQBlk.where.map(f =>
        OraBinaryOpExpression(AND,
          And(f.catalystExpr, oraExpression.catalystExpr),
          f, oraExpression
        )
      ).getOrElse(oraExpression)

      currQBlk.copy(
        where = Some(newFil),
        catalystOp = Some(joinOp),
        catalystProjectList = joinOp.output
      )
    }

  }

  private def pushNotIn : Option[OraQueryBlock] = {
    val joinOp = pushdownCatalystOp
    val notInConjuncts = splitConjunctivePredicates(joinCond.get)
    val notInJoinPattern = NotInJoinPattern(joinOp)
    val notInJoinKeys : Option[Seq[(Expression, Expression)]] =
      OraSparkUtils.sequence(notInConjuncts.map(notInJoinPattern.unapply(_)))

    if (notInJoinKeys.isDefined) {
      val (notInLKeys, notInRkeys) = notInJoinKeys.get.unzip
      for (
        leftOraExprs <- OraExpressions.unapplySeq(leftKeys ++ notInLKeys);
        rightOraExprs <- OraExpressions.unapplySeq(leftKeys ++ notInRkeys)
      ) yield {
        val newRQBlk = rightQBlk.copy(select = rightOraExprs)

        val oraExpression: OraExpression = OraSubQueryFilter(
          joinOp,
          leftOraExprs,
          SQLSnippet.NOT_IN,
          newRQBlk)
        val newFil = currQBlk.where.map(f =>
          OraBinaryOpExpression(AND,
            And(f.catalystExpr, oraExpression.catalystExpr),
            f, oraExpression
          )
        ).getOrElse(oraExpression)

        currQBlk.copy(
          where = Some(newFil),
          catalystOp = Some(joinOp),
          catalystProjectList = joinOp.output
        )
      }
    } else None
  }

  private[rules] def pushdownSQL: Option[OraQueryBlock] = {
    if (currQBlk.canApply(pushdownCatalystOp) &&
      leftKeys.size > 0) {

      joinType match {
        case LeftSemi if !joinCond.isDefined => pushSemiJoin
        case LeftAnti if !joinCond.isDefined => pushNotExists
        case LeftAnti => pushNotIn
        case _ => None
      }
    } else None
  }
}
