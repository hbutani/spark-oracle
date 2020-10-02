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

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}

/**
 * Represents a sql string with bind values.
 * Provides a composition API to compose component snippets into larger snippets.
 * Based on `SQLSyntax` in [[http://scalikejdbc.org/]]
 *
 * @param sql
 * @param params
 */
class SQLSnippet private (val sql: String, val params: Seq[Literal]) {

  import OraSQLImplicits._
  import SQLSnippet._

  override def equals(that: Any): Boolean = {
    that match {
      case sqlSnip: SQLSnippet =>
        sql == sqlSnip.sql && params == sqlSnip.params
      case _ =>
        false
    }
  }

  override def hashCode: Int = (sql, params).##

  override def toString(): String = s"SQLSnippet(value: ${sql}, parameters: ${params})"

  def append(syntax: SQLSnippet): SQLSnippet = osql"${this} ${syntax}"
  def +(syntax: SQLSnippet): SQLSnippet = this.append(syntax)

  def groupBy(columns: SQLSnippet*): SQLSnippet =
    if (columns.isEmpty) this else osql"${this} group by ${csv(columns: _*)}"

  def having(condition: SQLSnippet): SQLSnippet = osql"${this} having ${condition}"

  def orderBy(columns: SQLSnippet*): SQLSnippet =
    if (columns.isEmpty) this else osql"${this} order by ${csv(columns: _*)}"

  def asc: SQLSnippet = osql"${this} asc"
  def desc: SQLSnippet = osql"${this} desc"
  def limit(n: Int): SQLSnippet = osql"${this} limit ${n}"
  def offset(n: Int): SQLSnippet = osql"${this} offset ${n}"

  def where: SQLSnippet = osql"${this} where"
  def where(where: SQLSnippet): SQLSnippet = osql"${this} where ${where}"
  def where(whereOpt: Option[SQLSnippet]): SQLSnippet = whereOpt.fold(this)(where(_))

  def and: SQLSnippet = osql"${this} and"
  def and(sqlSnip: SQLSnippet): SQLSnippet = osql"$this and ($sqlSnip)"
  def and(andOpt: Option[SQLSnippet]): SQLSnippet = andOpt.fold(this)(and(_))
  def or: SQLSnippet = osql"${this} or"
  def or(sqlSnip: SQLSnippet): SQLSnippet = osql"$this or ($sqlSnip)"
  def or(orOpt: Option[SQLSnippet]): SQLSnippet = orOpt.fold(this)(or(_))

  /*
   * Decided not to refactor eq, neq, gt, ... into calling a common helper
   * method predicate because generating sql in this common method
   * would require calling [[SQLSnippet.join]]; which would incur
   * cost of building q Seq.
   *
   */

  private def sqlSnippet[T](v: T)(implicit lb: OraSQLLiteralBuilder[T]): SQLSnippet = v match {
    case sqlSnip: SQLSnippet => sqlSnip
    case l: Literal => osql"${lb(v)}"
  }

  def eq[A](v: A)(implicit lb: OraSQLLiteralBuilder[A]): SQLSnippet =
    osql"${this} = ${sqlSnippet(v)}"

  def neq[A](v: A)(implicit lb: OraSQLLiteralBuilder[A]): SQLSnippet =
    osql"${this} != ${sqlSnippet(v)}"

  def `<`[A](v: A)(implicit lb: OraSQLLiteralBuilder[A]): SQLSnippet =
    osql"${this} < ${sqlSnippet(v)}"

  def `<=`[A](v: A)(implicit lb: OraSQLLiteralBuilder[A]): SQLSnippet =
    osql"${this} <= ${sqlSnippet(v)}"

  def `>`[A](v: A)(implicit lb: OraSQLLiteralBuilder[A]): SQLSnippet =
    osql"${this} > ${sqlSnippet(v)}"

  def `>=`[A](v: A)(implicit lb: OraSQLLiteralBuilder[A]): SQLSnippet =
    osql"${this} >= ${sqlSnippet(v)}"

  def isNull: SQLSnippet = osql"${this} is null"
  def isNotNull: SQLSnippet = osql"${this} is not null"

  def between[A, B](a: A, b: B)(
      implicit lbA: OraSQLLiteralBuilder[A],
      lbB: OraSQLLiteralBuilder[B]): SQLSnippet =
    osql"${this} between ${sqlSnippet(a)} and ${sqlSnippet(b)}"

  def not: SQLSnippet = osql"not ${this}"

  def in[A](as: A*)(implicit lb: OraSQLLiteralBuilder[A]): SQLSnippet = {
    val inlist = join(as.map(sqlSnippet(_)), comma, false)
    osql"${this} in (${inlist})"
  }

  def notIn[A](as: A*)(implicit lb: OraSQLLiteralBuilder[A]): SQLSnippet = {
    val inlist = join(as.map(sqlSnippet(_)), comma, false)
    osql"${this} not in (${inlist})"
  }

}

/**
 * an entity that has a [[SQLSnippet]] representation.
 */
trait SQLSnippetProvider {

  def orasql: SQLSnippet
}

trait OraSQLImplicits {

  import SQLSnippet.SQLSnippetInterpolationString

  /**
   * Enables sql interpolation.
   *
   * {{{
   *   osql"select * from sales"
   *   val whereClause = osql"where quantity > ${qVal}"
   *   osql"select * from sales ${whereClause}"
   * }}}
   *
   * But important:
   * - the params of interpolation are treated as bind values, so cannot use params
   *   to inject sql. Use the compositional methods of [[SQLSnippet]] to build up sql.
   *   - For example:
   *     this `val verb = "select"; osql"${verb} 1 from dual` will generate
   *     the sql: `? 1 from dual` with a bind param of `Seq(Literal("select"))`
   *     which is not valid sql.
   */
  @inline implicit def oraSQLInterpolation(s: StringContext): SQLSnippetInterpolationString =
    new SQLSnippetInterpolationString(s)

}

object OraSQLImplicits extends OraSQLImplicits

object SQLSnippet {

  import OraSQLImplicits._

  val empty: SQLSnippet = osql""
  val comma: SQLSnippet = osql","

  def unapply(snippet: SQLSnippet): Option[(String, Seq[Literal])] =
    Some((snippet.sql, snippet.params))

  private def apply(sql: String, params: Seq[Literal]) =
    new SQLSnippet(sql, params)

  def join(
      parts: collection.Seq[SQLSnippet],
      delimiter: SQLSnippet,
      spaceBeforeDelimiter: Boolean = true): SQLSnippet = {

    val sep = if (spaceBeforeDelimiter) {
      s" ${delimiter.sql} "
    } else {
      s"${delimiter.sql} "
    }

    val value = parts.collect { case p if p.sql.nonEmpty => p.sql }.mkString(sep)

    val parameters = if (delimiter.params.isEmpty) {
      parts.flatMap(_.params)
    } else {
      parts.tail.foldLeft(parts.headOption.fold(collection.Seq.empty[Literal])(_.params)) {
        case (params, part) => params ++ delimiter.params ++ part.params
      }
    }
    apply(value, parameters)
  }

  def csv(parts: SQLSnippet*): SQLSnippet = join(parts, osql",", false)

  /**
   * A [[SQLSnippet]] generator build from a scala String Interpolation
   * expression of the form `osql"..."`
   * @param s
   */
  class SQLSnippetInterpolationString(private val s: StringContext) extends AnyVal {

    /**
     * - The `params` can be [[Literal]], a [[SQLSnippet]], a [[SQLSnippetProvider]]
     *   or 'any value'
     *   - any values are converted to [[Literal]]; but we only support certain
     *     dataTypes. See [[OraSQLLiteralBuilder.isSupportedForBind]]
     *   - [[SQLSnippetProvider]] is replaced by its [[SQLSnippet]]
     * - generates a [[SQLSnippet]]
     *   - by gathering all parts from the [[StringContext] and snippets
     *     from any [[SQLSnippet]] params into a new snippet.
     *   - gathering all lietral params and flattening params from [[SQLSnippet]]
     *     params into a new param list.
     * @param params
     * @return
     */
    def osql(params: Any*): SQLSnippet = {
      val normalizedParams = normalizeValues(params.map(util.makeImmutable)).toSeq
      SQLSnippet(buildQuery(normalizedParams), buildParams(normalizedParams))
    }

    private def buildQuery(params: Iterable[Any]): String = {
      val sb = new StringBuilder

      def addPlaceholder(param: Any): StringBuilder = param match {
        case l: Literal => sb += '?'
        case SQLSnippet(sql, _) => sb ++= sql
        case LastParam => sb
      }

      for ((qp, param) <- s.parts.zipAll(params, "", LastParam)) {
        sb ++= qp
        addPlaceholder(sb, param)
      }
      sb.result()
    }

    /**
     * - Replace [[SQLSnippetProvider]] with its [[SQLSnippet]]
     * - Replace `any value` with a [[Literal]] representation.
     */
    private def normalizeValues(params: Traversable[Any]): Traversable[Any] = {
      for (p <- params) yield {
        p match {
          case l: Literal => OraSQLLiteralBuilder.literalOf(l)
          case ss: SQLSnippet => ss
          case ssp: SQLSnippetProvider => ssp.orasql
          case p => OraSQLLiteralBuilder.literalOf(p)
        }
      }
    }

    private def buildParams(params: Traversable[Any]): Seq[Literal] = {
      val lBuf = ArrayBuffer[Literal]()

      def add(p: Any): Unit = p match {
        case l: Literal => lBuf += l
        case ss: SQLSnippet => lBuf ++= ss.params
      }

      for (p <- params) {
        add(p)
      }
      lBuf
    }
  }

  private case object LastParam

}
