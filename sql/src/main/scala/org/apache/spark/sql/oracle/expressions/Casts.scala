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
package org.apache.spark.sql.oracle.expressions

import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.expressions.{Cast, CheckOverflow, Expression, Literal, PromotePrecision}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.catalog.oracle.OraDataType
import org.apache.spark.sql.oracle.{OraSQLImplicits, SQLSnippet}
import org.apache.spark.sql.types._

/**
 * translate [[Cast]], [[PromotePrecision]] and [[CheckOverflow]]
 *
 *  - for [[PromotePrecision]] just return child translation.
 *  - for [[CheckOverflow]]
 *    - if `nullOnOverflow` is true add a case check
 *    - if `nullOnOverflow` is false: do nothing? translated oExpr will throw
 */
object Casts extends OraSQLImplicits {

  case class OraCast(catalystExpr : Cast,
                     children: Seq[OraExpression],
                     orasql: SQLSnippet) extends OraExpression {

  }

  object OraCast {
    def apply(catalystExpr : Cast, child: OraExpression, orasql: SQLSnippet) : OraCast =
      new OraCast(catalystExpr, Seq(child), orasql)
  }

  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {
      case CheckOverflow(PromotePrecision(cE@Cast(OraExpression(oE), _, _)), _, nullOnOverflow) =>
        Casting(cE, oE, nullOnOverflow).translate
      case CheckOverflow(cE@Cast(OraExpression(oE), _, _), _, nullOnOverflow) =>
        Casting(cE, oE, nullOnOverflow).translate
      case PromotePrecision(cE@Cast(OraExpression(oE), _, _)) => Casting(cE, oE, false).translate
      case cE@Cast(OraExpression(oE), _, _) => Casting(cE, oE, false).translate
      case _ => null
    })

  object CastingType extends Enumeration {
    val NUMERIC_CONV = Value
    val TO_STRING = Value
    val FROM_STRING = Value
    val TO_BOOLEAN = Value
    val FROM_BOOLEAN = Value
    val TO_DATE = Value
    val FROM_DATE = Value
    val TO_TIMESTAMP = Value
    val FROM_TIMESTAMP = Value

    def apply(castExpr : Cast) : Value = (castExpr.child.dataType, castExpr.dataType) match {
      case (l : NumericType, r : NumericType) => NUMERIC_CONV
      case (StringType, _) => FROM_STRING
      case (_, StringType) => TO_STRING
      case (BooleanType, _) => FROM_BOOLEAN
      case (_, BooleanType) => TO_BOOLEAN
      case (DateType, _) => FROM_DATE
      case (_, DateType) => TO_DATE
      case (TimestampType, _) => FROM_TIMESTAMP
      case (_, TimestampType) => TO_TIMESTAMP
      case _ => ???
    }
  }

  val epochTS = {
    // osql"to_timestamp_tz('1970-01-01 00:00:00 00:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM')"
    osql"from_tz(to_timestamp('1970-01-01', 'YYYY-MM-DD'), 'UTC')"
  }
  val epochDt = osql"date '1970-01-01'"

  val epochTSAtSessionTZ = osql"to_timestamp('1970-01-01', 'YYYY-MM-DD')"

  val true_bool_TS =
    osql"from_tz(to_timestamp('1970-01-01', 'YYYY-MM-DD'), 'UTC') + interval '0.001' second(0,3)"

  /**
   *  - if there is no [[catalystExpr#zoneId]] then generate `cast(${oraE} as timestamp)`
   *  - else generate `cast(${oraE} as timestamp) at time zone ${zoneOE}`
   *
   * @param oraE is the date value expression being converted
   * @param catalystExpr original Spark expression.
   * @return
   */
  def dtToTimestamp(oraE : OraExpression,
                    catalystExpr : Cast) : OraCast = {
    val zoneId = catalystExpr.timeZoneId.map(DateTimeUtils.getZoneId)
    val orasql = if (zoneId.isDefined) {
      val zoneOE = OraLiteral(Literal(zoneId.get.getId)).toLiteralSql
      osql"cast(${oraE} as timestamp) at time zone ${zoneOE}"
    } else {
      osql"cast(${oraE} as timestamp)"
    }
    OraCast(catalystExpr, oraE, orasql)
  }

  /**
   *  - if there is no [[catalystExpr#zoneId]] then
   *    - cast input to a timestamp and set its timezone; then cast the result to a date
   *    - Translation expression: `cast(from_tz(cast({oraE} as timestamp), {zoneOE}) as date)`
   *  - Otherwise translation expression is `cast({oraE} as date)`
   *
   * @param oraE is the timestamp value expression being converted
   * @param catalystExpr original Spark expression.
   * @return
   */
  def timestampToDt(oraE : OraExpression,
                    catalystExpr : Cast) : OraCast = {
    val zoneId = catalystExpr.timeZoneId.map(DateTimeUtils.getZoneId)
    val orasql = if (zoneId.isDefined) {
      val zoneOE = OraLiteral(Literal(zoneId.get.getId)).toLiteralSql
      osql"cast(from_tz(cast(${oraE} as timestamp), ${zoneOE}) as date)"
    } else {
      osql"cast(${oraE} as date)"
    }
    OraCast(catalystExpr, oraE, orasql)
  }

  /**
   * Translation logic is:
   * {{{
   * millisToInterval = numtodsinterval({oraE}/1000, 'SECOND')
   * millisToIntervalWithTZOffset = {millisToInterval} + {epochTS} - {epochTSAtSessionTZ}
   * result = epochTSAtSessionTZ} + ({millisToIntervalWitTZOffset})
   * }}}
   *
   * For example for `oraE = 1603425037802`, sql is:
   * {{{
   *   to_timestamp('1970-01-01', 'YYYY-MM-DD') +
   *        (numtodsinterval(1603425037802/1000, 'SECOND') +
   *         from_tz(to_timestamp('1970-01-01', 'YYYY-MM-DD')), 'UTC') -
   *         to_timestamp('1970-01-01', 'YYYY-MM-DD')
   *        )
   * }}}
   *
   * @param oraE is numeric value interpreted as millis since epoch
   * @param catalystExpr spark's [[Cast]] expression
   * @return
   */
  def epochToTimestamp(oraE : OraExpression,
                       catalystExpr : Cast) : OraCast = {
    val orasql : SQLSnippet = osql"" //FIXME

    OraCast(catalystExpr, oraE, orasql)
  }

  /**
   * Translation logic is:
   * {{{
   * millisToInterval = numtodsinterval({oraE}/1000, 'SECOND')
   * millisToIntervalWithTZOffset = {millisToInterval} + {epochTS} - {epochTSAtSessionTZ}
   * epoch_ts = {epochTSAtSessionTZ} + {millisToIntervalWitTZOffset}
   * result = trunc({epoch_ts}, 'DD')
   * }}}
   *
   * For example for `oraE = 1603425037802`, sql is:
   * {{{
   *   trunc(
   *     to_timestamp('1970-01-01', 'YYYY-MM-DD') +
   *        (numtodsinterval(1603425037802/1000, 'SECOND') +
   *         from_tz(to_timestamp('1970-01-01', 'YYYY-MM-DD')), 'UTC') -
   *         to_timestamp('1970-01-01', 'YYYY-MM-DD')
   *        ),
   *      'DD'
   *     )
   * }}}
   *
   * @param oraE is numeric value interpreted as millis since epoch
   * @param catalystExpr spark's [[Cast]] expression
   * @return
   */
  def epochToDate(oraE : OraExpression,
                  catalystExpr : Cast) : OraCast = {
    val orasql : SQLSnippet = osql"" //FIXME

    OraCast(catalystExpr, oraE, orasql)
  }

  /**
   * Translation logic is:
   * {{{
   *   // using ora date arithmetic: ora_ts - ora_ts -> ora_interval
   *   days = extract(day from ({oraE} - {epochTS})) * 24 * 60 * 60
   *   hours = extract(hour from ({oraE} - {epochTS})) * 60 * 60
   *   mins = extract(minute from ({oraE} - {epochTS})) * 60 * 60
   *   secs = extract(second from ({oraE} - {epochTS})) * 60 * 60
   *   result = ({days} + {hours} + {mins} + {secs}) * 1000
   * }}}
   * For example for `oraE = systimestamp`, sql is:
   * {{{
   *   extract(day from (systimestamp - from_tz(to_timestamp('1970-01-01', 'YYYY-MM-DD')), 'UTC'))) * 24 * 60 * 60 +
   *  extract(hour from (systimestamp - from_tz(to_timestamp('1970-01-01', 'YYYY-MM-DD')), 'UTC'))) * 60 * 60 +
   *  extract(minute from (systimestamp - from_tz(to_timestamp('1970-01-01', 'YYYY-MM-DD')), 'UTC'))) * 60 +
   *  extract(second from (systimestamp - from_tz(to_timestamp('1970-01-01', 'YYYY-MM-DD')), 'UTC')))
   * ) * 1000
   * }}}
   *
   * @param oraE is timestamp value as timestamp or timestamp with tz or timestamp with local tz
   * @param catalystExpr spark's [[Cast]] expression
   * @return
   */
  def timestampToEpoch(oraE : OraExpression,
                       catalystExpr : Cast) : OraCast = {

    val days = osql"extract(day from (${oraE} - ${epochTS})) * 24 * 60 * 60"
    val hours = osql"extract(hour from (${oraE} - ${epochTS})) * 60 * 60"
    val mins = osql"extract(minute from (${oraE} - ${epochTS})) * 60 * 60"
    val secs = osql"extract(second from (${oraE} - ${epochTS})) * 60 * 60"

    val orasql : SQLSnippet =
      osql"(${days} + ${hours} + ${mins} + ${secs}) * 1000"

    OraCast(catalystExpr, oraE, orasql)
  }

  /**
   * Translation logic is:
   * {{{
   *   trunc_to_days = trunc(sysdate, 'DD')
   *   // using ora date arithmetic: ora_date - ora_ts -> ora_interval
   *   interval_from_epoch = trunc_to_days - epoch_ts
   *   num_hours = extract(day from interval_from_epoch) * 24 +
   *               extract(hour from interval_from_epoch)
   *   result = num_hours * 60 * 60 * 1000
   * }}}
   *
   * For example, for sysdate:
   * {{{
   *   (extract(day from(trunc(sysdate, 'DD') - from_tz(to_timestamp('1970-01-01', 'YYYY-MM-DD')), 'UTC'))) * 24 +
   *    extract(hour from(trunc(sysdate, 'DD') - from_tz(to_timestamp('1970-01-01', 'YYYY-MM-DD')), 'UTC')))
   *   ) * 60 * 60 * 1000
   * }}}
   *
   * @param oraE is a date value
   * @param catalystExpr
   * @return
   */
  def dateToEpoch(oraE : OraExpression,
                  catalystExpr : Cast) : OraCast = {

    val days = osql"extract( day from (trunc(sysdate, 'DD') - ${epochTS})"
    val hours = osql"extract( hour from (trunc(sysdate, 'DD') - ${epochTS})"

    val orasql = osql"(${days} * 24 + ${hours}) * 60 * 60 * 1000"

    OraCast(catalystExpr, oraE, orasql)
  }

  trait CastingBase {
    val castExpr : Cast
    val childOE : OraExpression
    val nullOnOverFlow : Boolean

    lazy val inputExpr = castExpr.child
    lazy val castingType = CastingType(castExpr)


  }

  /**
   * '''for widening Cast:'''
   * do nothing, return 'childOE'
   *
   * '''for narrowing Cast:'''
   * use the following sql expression template:
   *
   * {{{
   * case when {childOE} > {toDT.MinValue} and {childOE} < {toDT.MaxValue}
   *          then cast({childOE} as {toDT})
   *      else null
   * end
   * }}}
   */
  trait NumericCasting { self : CastingBase =>
    def isDataTypeWidening(fromDT : NumericType, toDT : NumericType) : Boolean = {
      val tType = TypeCoercion.findTightestCommonType(fromDT, toDT)
      tType.isDefined && tType.get == toDT
    }

    def num_conv : OraExpression = {
      val fromDT = inputExpr.dataType.asInstanceOf[NumericType]
      val toDT = castExpr.dataType.asInstanceOf[NumericType]
      if (isDataTypeWidening(fromDT, toDT) || !nullOnOverFlow) {
        childOE
      } else {
        val (minV, maxV) = OraLiterals.dataTypeMinMaxRange(toDT)
        val oraDTE : OraExpression = {
          val oraDT = OraDataType.toOraDataType(toDT)
          OraLiteral(Literal(oraDT.oraTypeString)).toLiteralSql
        }
        val orasql = osql"case when ${childOE} >= ${minV} and ${childOE} <= ${maxV}" +
          osql" then cast(${childOE} as ${oraDTE}) else null"
        OraCast(castExpr, childOE, orasql)
      }
    }
  }

  /**
   * '''from string:'''
   *
   *  - '''to numeric:'''
   *    - apply `TO_NUMBER` [[https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/TO_NUMBER.html#GUID-D4807212-AFD7-48A7-9AED-BEC3E8809866 oracle function]])
   *    - so sql template is `to_number({childOE})`
   *  - '''to date:'''
   *    - Spark uses [[DateTimeUtils.stringToDate]]; this tries a bunch of Date formats
   *    - When translating we will use the default date format of Oracle Connection.
   *      From Oracle `To_Date` [[https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/TO_DATE.html#GUID-D226FA7C-F7AD-41A0-BB1D-BD8EF9440118 oracle function]]:
   *      The default date format is determined implicitly by the NLS_TERRITORY initialization
   *      parameter or can be set explicitly by the NLS_DATE_FORMAT parameter.
   *    - so translation is `to_date({childOE})`
   *  - '''to timestamp:'''
   *    - Spark uses [[org.joda.time.DateTimeUtils.stringToTimestamp]]; this tries a
   *      bunch of Date formats.
   *    - When translating we will use the default timestamp format of Oracle Connection.
   *      From Oracle `To_Timestamp` [[https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/TO_TIMESTAMP.html#GUID-57E09334-E3CC-4CA2-809E-F0909458BCFA oracle function]]
   *      The default format of the TIMESTAMP data type, which is determined by
   *      the NLS_TIMESTAMP_FORMAT initialization parameter.
   *    - so translation is `to_timestamp({childOE})`
   *  - '''to boolean:'''
   *    - Spark uses
   *      - [[StringUtils.isTrueString]] to translate to `true`
   *      - [[StringUtils.isFalseString]] to translate to `false`
   *      - else `null`
   *    - sql template:
   *     {{{
   *       (case when ${childOE} in ('t', 'true', 'y', 'yes', '1') then 1
   *            when ${childOE} in ('f', 'false', 'n', 'no', '0') then 0
   *            else null
   *        end) = 1
   *     }}}
   *    - So below example shows boolean translations:
   *    {{{
   *      -- This returns 1 row
   *      select 1
   *      from dual
   *      where (case when't' in ('t', 'true', 'y', 'yes', '1') then 1
   *                  when 't' in ('f', 'false', 'n', 'no', '0') then 0
   *                  else null
   *             end) = 1;
   *
   *     -- These return 0 rows:
   *     select 1
   *     from dual
   *     where (case when'f' in ('t', 'true', 'y', 'yes', '1') then 1
   *                 when 'f' in ('f', 'false', 'n', 'no', '0') then 0
   *                 else null
   *            end) = 1;
   *
   *     select 1
   *     from dual
   *     where (case when'u' in ('t', 'true', 'y', 'yes', '1') then 1
   *                 when 'u' in ('f', 'false', 'n', 'no', '0') then 0
   *                 else null
   *            end) = 1;
   *    }}}
   *
   * '''to string:'''
   *
   *  - '''from numeric:'''
   *    - Spark applies `UTF8String.fromString({childExpr.val}.toString)`
   *    - translate using `TO_CHAR(number)` [[https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/TO_CHAR-number.html#GUID-00DA076D-2468-41AB-A3AC-CC78DBA0D9CB oracle function]]
   *    - so translation template is `to_char({childOE})`
   *    - For example `to_char(12345678912345.345678900000)` returns
   *      `12345678912345.345678900000`
   *  - '''from date:'''
   *    - Spark uses [[DateFormatter]] for the `timeZoneId` of the `castExpr`
   *      - date pattern used is `defaultPattern: String = "yyyy-MM-dd"`
   *    - translate to sql template using TO_CHAR(date) [[https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/TO_CHAR-datetime.html#GUID-0C3EEFD1-AE3D-452D-BF23-2FC95664E78F oracle function]].
   *      - template is `to_char({childOE})`.
   *      - This uses the default date format of the Oracle connection.
   *        Which can be changed in Oracle by setting the the 'NLS_TERRITORY' initialization
   *        parameter or can be set explicitly by the 'NLS_DATE_FORMAT' parameter.
   *  - '''from timestamp:'''
   *    - Spark uses [[FractionTimestampFormatter]] for the `timeZoneId` of the `castExpr`
   *      - timetsamp pattern used is `formatter = DateTimeFormatterHelper.fractionFormatter`
   *        Which parses/formats timestamps according to the pattern `yyyy-MM-dd HH:mm:ss.[..fff..]`
   *    - translate to sql template using TO_CHAR(date) [[https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/TO_CHAR-datetime.html#GUID-0C3EEFD1-AE3D-452D-BF23-2FC95664E78F oracle function]].
   *      - template is `to_char({childOE})`.
   *      - This uses the default date format of the Oracle connection.
   *        Which can be changed in Oracle by setting the the 'NLS_TERRITORY' initialization
   *        parameter or can be set explicitly by the 'NLS_TIMESTAMP_FORMAT' parameter.
   *      - For example: `to_char('22-OCT-20 01.16.32.740812 PM')`
   *  - '''from boolean:'''
   *    - use template: `case when {childOE} then 'true' else 'false' end`
   */
  trait StringCasting { self : CastingBase =>
    def to_str : OraExpression = ???
    def from_str : OraExpression = ???
  }

  /**
   * '''from boolean:'''
   *
   * - '''to numeric:'''
   * - '''to string:'''
   * - '''to date:'''
   * - '''to timestamp:'''
   *
   * '''to boolean:'''
   *
   * - '''from numeric:'''
   * - '''from string:'''
   * - '''from date:'''
   * - '''from timestamp:'''
   *
   */

  trait BooleanCasting { self : CastingBase =>
    def to_boolean : OraExpression = ???
    def from_boolean : OraExpression = ???
  }

  /**
   * '''from date:'''
   *
   *  - '''to numeric:'''
   *    - In Spark: num_of_days since epoch.
   *    - translate to: `{oraE} - {epochDt}`.
   *      Based on oracle's date arithmetic(`oraDt - oraDt -> number`)
   *      this represents the number of days since start of epoch.
   *  - '''to string:'''
   *    - Sames as `Date -> String` in [[StringCasting]]
   *  - '''to timestamp:'''
   *    - In Spark: `DatetimeUtils.daysToMicros(d, zoneId)`
   *      - Converts days since `1970-01-01 `at the given zone ID to microseconds
   *        since 1970-01-01 00:00:00Z.
   *    - translate to: `cast({oraE} as timestamp)`
   *      with additional ` at time zone {castE.timeZoneId}`.
   *      See [[dtToTimestamp()]] method.
   *  - '''to boolean:'''
   *    - In Spark: `null`
   *    - translate to: `null`
   *
   * '''to date:'''
   *
   *  - '''from numeric:'''
   *    - In Spark it is undefined
   *    - translate to: `{epochDt} + {oraE}`
   *      Based on oracle's date arithmetic this represents the `date` that is `{oraE}`
   *      days from epoch.
   *  - '''from string:'''
   *    - same as `String -> Date` in [[StringCasting]]
   *  - '''from timestamp:'''
   *    - In Spark: convert timestamp at given tz to date
   *    - translate to: `cast({oraE} as date)`;
   *      if `{castE.timeZoneId}` is specified first convert to timestamp in timeZone.
   *      See [[timestampToDt()]]
   *  - '''from boolean:'''
   *    - In Spark it is undefined
   *    - we throw during translation.
   */
  trait DateCasting { self : CastingBase =>
    def to_date : OraExpression = ???
    def from_date : OraExpression = ???
  }

  /**
   * '''from timestamp:'''
   *
   *  - '''to numeric:'''
   *    - In Spark: convert to `millis_since_epoch`
   *    - translate to: see [[timestampToEpoch()]]
   *  - '''to string:''' Sames as `Timestamp -> String` in [[StringCasting]]
   *  - '''to date:''' Same as `Timestamp -> Date` in  [[DateCasting]]
   *  - '''to boolean:'''
   *    - In Spark: `millis_since_epoch != 0`
   *      translate to: `timestampToEpoch({oraE}) != 0`. See [[timestampToEpoch()]]
   *
   * '''to timestamp:'''
   *
   *  - '''from numeric:'''
   *    - In Spark it is undefined
   *    - translate to: See [[epochToTimestamp()]]
   *  - '''from string:''' same as `String -> Date` in [[StringCasting]]
   *  - '''from date:'''
   *    - In Spark: convert timestamp at given tz to date
   *    - translate to: See [[timestampToDt()]]
   *  - '''from boolean:'''
   *    - In Spark: `true` is interpreted as `1L millis_since_epoch`,
   *      and `false` is `0L millis_since_epoch`.
   *    - translate to: `case when {oraE} then ${epochTS} else ${true_bool_TS} end`
   */
  trait TimestampCasting { self : CastingBase =>
    def to_timestamp : OraExpression = ???
    def from_timestamp : OraExpression = ???
  }

  case class Casting(castExpr : Cast,
                    childOE : OraExpression,
                     nullOnOverFlow : Boolean
                    ) extends CastingBase
    with NumericCasting with StringCasting
    with BooleanCasting with DateCasting
    with TimestampCasting {

    import CastingType._

    def translate : OraExpression = castingType match {
      case  NUMERIC_CONV => num_conv
      case TO_STRING => to_str
      case FROM_STRING => from_str
      case TO_BOOLEAN => to_boolean
      case FROM_BOOLEAN => from_boolean
      case TO_DATE => to_date
      case FROM_DATE => from_date
      case TO_TIMESTAMP => to_timestamp
      case FROM_TIMESTAMP => from_timestamp
    }
  }

  /*
   * TODO:
   * 1. Tests
   * - c_int + c_long + c_byte < c_decimal_scale_5 * 5 + c_decimal_scale_8
   * - c_long + "5" > c_int + c_decimal_scale_8
   * - c_date < c_timestamp
   * - c_date + 10 < c_timestamp
   * - c_date < cast('' as date) and c_timestamp > cast('' as timestamp)
   *
   * 2. Coding tasks
   * - Implement StringCasting
   * - Implement DateCasting
   * - Implement TimestampCasting
   * - BooleanCasting
   * - misc
   *   - fixme in  CastingType apply
   *   - implement orasql in epochToTimestamp
   *   - implement orasql in epochToDate
   * - wire into OraExpressions.unapply
   */
}
