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

import java.math.BigDecimal
import java.sql._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, LegacyDateFormats, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.getZoneId
import org.apache.spark.sql.oracle.{OraSparkUtils, OraSQLLiteralBuilder, SQLSnippet}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class OraLiteral(catalystExpr: Literal) extends OraExpression with OraLeafExpression {
  override def orasql: SQLSnippet = osql"${catalystExpr}"

  def toLiteralSql: OraLiteralSql =
    new OraLiteralSql(OraLiterals.toOraLiteralSql(catalystExpr).get)

}

case class OraLiteralSql(catalystExpr: Literal) extends OraExpression with OraLeafExpression {
  assert(catalystExpr.dataType == StringType)
  override def orasql: SQLSnippet =
    SQLSnippet.literalSnippet(catalystExpr)

  def this(s: String) = this(Literal(UTF8String.fromString(s), StringType))
}

/**
 * There are 2 kinds of literal conversions:
 * - typically a literal is converted to a [[OraLiteral]], which encapsulates the
 *   spark literal and is set in the PreparedStatement as a bind value.
 * - sometimes you may need to convert the spark literal into a oracle literal value.
 *   This must be explicitly asked for by invoking [[OraLiterals.toOraLiteralSql]]
 *   Conversion is attempted using literal representation rules in the Oracle SQL Reference.
 */
object OraLiterals {

  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {
      case l: Literal if OraSQLLiteralBuilder.isSupportedForBind(l.dataType) =>
        OraLiteral(l)
      case _ => null
    })

  def toOraLiteralSql(l: Literal): Option[String] =
    ORA_LITERAL_CONV.ora_literal(l)

  /*
   * Based on https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Literals.html#GUID-192417E8-A79D-4A1D-9879-68272D925707
   *
   * Passed in values mustn't be null.
   *
   * Date Times:
   * Convert to oracle data and timestamp literals
   * - timestamp -> convert to a UTC timestamp. This works w/o knowledge of DB timeZone
   * - date -> convert to a UTC timestamp in oracle and then truncate.
   *   This works w/o knowledge of DB timeZone
   *
   * String: escape single "'" with "''"
   */
  private object ORA_LITERAL_CONV {

    val spk_dt_fmt = "yyyy-MM-dd"
    val ora_dt_format = "YYYY-MM-DD"

    val UTC = getZoneId("+00:00")
    val spk_ts_format = "yyyy-MM-dd HH:mm:ss.SSSSSS"
    val spk_ts_fmtter =
      TimestampFormatter(
        spk_ts_format,
        UTC,
        LegacyDateFormats.FAST_DATE_FORMAT,
        isParsing = false)

    val ora_ts_format = "YYYY-MM-DD HH24:MI:SS:FF6 00:00"

    def to_ora_ts_literal(value: Long): String =
      s"TIMESTAMP '${spk_ts_fmtter.format(value)}'"

    def to_ora_dt_literal(value: Int): String = {
      val jvTS = new Timestamp(DateTimeUtils.toJavaDate(value).getTime)
      s"TRUNC(TIMESTAMP '${spk_ts_fmtter.format(jvTS)}')"
    }

    def to_string_literal(s: String): String = {
      def fn(s: String) =
        s.split("''")
          .map(_.replaceAll("'", "''"))
          .mkString("''")

      val r = s match {
        case "''" => "''"
        case _ => fn(s)
      }

      s"'${r}'"
    }

    /**
     * The [[Decimal]] value is converted to [[java.math.BigDecimal]]
     * Then [[java.math.BigDecimal:toString]] method is called.
     * This returns a string representation based on Decimal's precision and scale.
     *
     * @param d
     * @return
     */
    def to_decimal_literal(d: Decimal): String = d.toString

    def ora_literal(l: Literal): Option[String] =
      Option(l match {
        case Literal(null, _) => "null" // TODO: does this work in all cases?
        case Literal(s, StringType) => ORA_LITERAL_CONV.to_string_literal(s.toString)
        case Literal(d, DateType) => ORA_LITERAL_CONV.to_ora_dt_literal(d.asInstanceOf[Int])
        case Literal(t, TimestampType) => ORA_LITERAL_CONV.to_ora_ts_literal(t.asInstanceOf[Long])
        case Literal(n, _: IntegralType) => n.toString
        case Literal(f, FloatType) => s"${f}f"
        case Literal(f, DoubleType) => s"${f}d"
        case Literal(d: Decimal, dt: DecimalType) => to_decimal_literal(d)
        case _ => null
      })
  }

  trait JDBCGetSet[T] {

    val sqlType: Int

    protected def readResultSet(rs: ResultSet, pos: Int): T
    protected def setIRow(oRow: InternalRow, pos: Int, v: T): Unit
    protected def readIRow(iRow: InternalRow, pos: Int): T
    protected def readLiteral(lit: Literal): T
    protected def setPrepStat(ps: PreparedStatement, pos: Int, v: T): Unit

    /**
     * Read a value from the [[ResultSet]] and set it in the [[InternalRow]]
     * @param rs
     * @param pos
     * @param row
     */
    final def readValue(rs: ResultSet, pos: Int, row: InternalRow): Unit = {
      val v: T = readResultSet(rs, pos + 1)
      if (rs.wasNull()) {
        row.setNullAt(pos)
      } else {
        setIRow(row, pos, v)
      }
    }

    /**
     * Read a value from the [[InternalRow]] and set it in the [[PreparedStatement]]
     * @param row
     * @param ps
     * @param pos
     */
    final def setValue(row: InternalRow, ps: PreparedStatement, pos: Int): Unit = {
      if (row.isNullAt(pos)) {
        ps.setNull(pos + 1, sqlType)
      } else {
        val v: T = readIRow(row, pos)
        setPrepStat(ps, pos + 1, v)
      }
    }

    final def setValue(lit: Literal, ps: PreparedStatement, pos: Int): Unit = {
      if (lit.value == null) {
        ps.setNull(pos + 1, sqlType)
      } else {
        val v: T = readLiteral(lit)
        setPrepStat(ps, pos + 1, v)
      }
    }
  }

  private object StringGetSet extends JDBCGetSet[String] {
    override val sqlType: Int = Types.VARCHAR

    @inline protected def readResultSet(rs: ResultSet, pos: Int) = rs.getString(pos)
    @inline protected def setIRow(iRow: InternalRow, pos: Int, v: String) =
      iRow.update(pos, UTF8String.fromString(v))

    @inline protected def readIRow(iRow: InternalRow, pos: Int) = iRow.getString(pos)
    @inline protected def setPrepStat(ps: PreparedStatement, pos: Int, v: String) =
      ps.setString(pos, v)

    @inline override protected def readLiteral(lit: Literal): String =
      lit.value.asInstanceOf[UTF8String].toString
  }

  private object ByteGetSet extends JDBCGetSet[Byte] {
    override val sqlType: Int = Types.TINYINT

    @inline protected def readResultSet(rs: ResultSet, pos: Int) = rs.getByte(pos)
    @inline protected def setIRow(iRow: InternalRow, pos: Int, v: Byte) =
      iRow.setByte(pos, v)

    @inline protected def readIRow(iRow: InternalRow, pos: Int) = iRow.getByte(pos)
    @inline protected def setPrepStat(ps: PreparedStatement, pos: Int, v: Byte) =
      ps.setByte(pos, v)
    @inline override protected def readLiteral(lit: Literal): Byte =
      lit.value.asInstanceOf[Byte]
  }

  private object ShortGetSet extends JDBCGetSet[Short] {
    override val sqlType: Int = Types.SMALLINT

    @inline protected def readResultSet(rs: ResultSet, pos: Int) = rs.getShort(pos)
    @inline protected def setIRow(iRow: InternalRow, pos: Int, v: Short) =
      iRow.setShort(pos, v)

    @inline protected def readIRow(iRow: InternalRow, pos: Int) = iRow.getShort(pos)
    @inline protected def setPrepStat(ps: PreparedStatement, pos: Int, v: Short) =
      ps.setShort(pos, v)
    @inline override protected def readLiteral(lit: Literal): Short =
      lit.value.asInstanceOf[Short]
  }

  private object IntGetSet extends JDBCGetSet[Int] {
    override val sqlType: Int = Types.INTEGER

    @inline protected def readResultSet(rs: ResultSet, pos: Int) = rs.getInt(pos)
    @inline protected def setIRow(iRow: InternalRow, pos: Int, v: Int) =
      iRow.setInt(pos, v)

    @inline protected def readIRow(iRow: InternalRow, pos: Int) = iRow.getInt(pos)
    @inline protected def setPrepStat(ps: PreparedStatement, pos: Int, v: Int) =
      ps.setInt(pos, v)
    @inline override protected def readLiteral(lit: Literal): Int =
      lit.value.asInstanceOf[Int]
  }

  private object LongGetSet extends JDBCGetSet[Long] {
    override val sqlType: Int = Types.BIGINT

    @inline protected def readResultSet(rs: ResultSet, pos: Int) = rs.getLong(pos)
    @inline protected def setIRow(iRow: InternalRow, pos: Int, v: Long) =
      iRow.setLong(pos, v)

    @inline protected def readIRow(iRow: InternalRow, pos: Int) = iRow.getLong(pos)
    @inline protected def setPrepStat(ps: PreparedStatement, pos: Int, v: Long) =
      ps.setLong(pos, v)
    @inline override protected def readLiteral(lit: Literal): Long =
      lit.value.asInstanceOf[Long]
  }

  private class DecimalGetSet(val dt: DecimalType) extends JDBCGetSet[BigDecimal] {
    override val sqlType: Int = Types.NUMERIC

    @inline protected def readResultSet(rs: ResultSet, pos: Int) = rs.getBigDecimal(pos)

    @inline protected def setIRow(iRow: InternalRow, pos: Int, v: BigDecimal) =
      iRow.update(pos, Decimal(v, dt.precision, dt.scale))

    @inline protected def readIRow(iRow: InternalRow, pos: Int) =
      iRow.getDecimal(pos, dt.precision, dt.scale).toJavaBigDecimal

    @inline protected def setPrepStat(ps: PreparedStatement, pos: Int, v: BigDecimal) =
      ps.setBigDecimal(pos, v)
    @inline override protected def readLiteral(lit: Literal): BigDecimal =
      lit.value.asInstanceOf[Decimal].toJavaBigDecimal
  }

  private object FloatGetSet extends JDBCGetSet[Float] {
    override val sqlType: Int = Types.FLOAT

    @inline protected def readResultSet(rs: ResultSet, pos: Int) = rs.getFloat(pos)
    @inline protected def setIRow(iRow: InternalRow, pos: Int, v: Float) =
      iRow.setFloat(pos, v)

    @inline protected def readIRow(iRow: InternalRow, pos: Int) = iRow.getFloat(pos)
    @inline protected def setPrepStat(ps: PreparedStatement, pos: Int, v: Float) =
      ps.setFloat(pos, v)
    @inline override protected def readLiteral(lit: Literal): Float =
      lit.value.asInstanceOf[Float]
  }

  private object DoubleGetSet extends JDBCGetSet[Double] {
    override val sqlType: Int = Types.DOUBLE

    @inline protected def readResultSet(rs: ResultSet, pos: Int) = rs.getDouble(pos)
    @inline protected def setIRow(iRow: InternalRow, pos: Int, v: Double) =
      iRow.setDouble(pos, v)

    @inline protected def readIRow(iRow: InternalRow, pos: Int) = iRow.getDouble(pos)
    @inline protected def setPrepStat(ps: PreparedStatement, pos: Int, v: Double) =
      ps.setDouble(pos, v)
    @inline override protected def readLiteral(lit: Literal): Double =
      lit.value.asInstanceOf[Double]
  }

  private object DateGetSet extends JDBCGetSet[Date] {
    override val sqlType: Int = Types.DATE

    @inline protected def readResultSet(rs: ResultSet, pos: Int) = rs.getDate(pos)
    @inline protected def setIRow(iRow: InternalRow, pos: Int, v: Date) =
      iRow.setInt(pos, DateTimeUtils.fromJavaDate(v))

    @inline protected def readIRow(iRow: InternalRow, pos: Int) =
      DateTimeUtils.toJavaDate(iRow.getInt(pos))
    @inline protected def setPrepStat(ps: PreparedStatement, pos: Int, v: Date) =
      ps.setDate(pos, v)
    @inline override protected def readLiteral(lit: Literal): Date =
      DateTimeUtils.toJavaDate(lit.value.asInstanceOf[Int])
  }

  private object TimestampGetSet extends JDBCGetSet[Timestamp] {
    override val sqlType: Int = Types.TIMESTAMP

    @inline protected def readResultSet(rs: ResultSet, pos: Int) = rs.getTimestamp(pos)
    @inline protected def setIRow(iRow: InternalRow, pos: Int, v: Timestamp) =
      iRow.setLong(pos, DateTimeUtils.fromJavaTimestamp(v))

    @inline protected def readIRow(iRow: InternalRow, pos: Int) =
      DateTimeUtils.toJavaTimestamp(iRow.getLong(pos))
    @inline protected def setPrepStat(ps: PreparedStatement, pos: Int, v: Timestamp) =
      ps.setTimestamp(pos, v)
    @inline override protected def readLiteral(lit: Literal): Timestamp =
      DateTimeUtils.toJavaTimestamp(lit.value.asInstanceOf[Long])
  }

  def jdbcGetSet(dt: DataType): JDBCGetSet[_] = dt match {
    case StringType => StringGetSet
    case ByteType => ByteGetSet
    case ShortType => ShortGetSet
    case IntegerType => IntGetSet
    case LongType => LongGetSet
    case dt: DecimalType => new DecimalGetSet(dt)
    case FloatType => FloatGetSet
    case DoubleType => DoubleGetSet
    case DateType => DateGetSet
    case TimestampType => TimestampGetSet
    case _ =>
      OraSparkUtils.throwAnalysisException(
        s"Currently Unsupported DataType for reading/writing values from oracle: ${dt}")
  }

  def dataTypeMinMaxRange(dt : NumericType) : (OraExpression, OraExpression) = {
    val (minV, maxV) = dt match {
      case ByteType => (Literal(Byte.MinValue), Literal(Byte.MaxValue))
      case ShortType => (Literal(Short.MinValue), Literal(Short.MaxValue))
      case IntegerType => (Literal(Int.MinValue), Literal(Int.MaxValue))
      case LongType => (Literal(Long.MinValue), Literal(Long.MaxValue))
      case FloatType => (Literal(Float.MinValue), Literal(Float.MaxValue))
      case DoubleType => (Literal(Double.MinValue), Literal(Double.MaxValue))
      case dt : DecimalType =>
        val pVal = "9" * (dt.precision - dt.scale)
        val sVal = "9" * dt.scale
        val v = s"${pVal}.${sVal}"
        (s"-${v}", v)
    }
    (OraLiteral(Literal(minV)).toLiteralSql, OraLiteral(Literal(maxV)).toLiteralSql)
  }

  def bindValues(ps: PreparedStatement,
                         bindValues: Seq[Literal]): Unit = {
    val setters: Seq[JDBCGetSet[_]] =
      bindValues.map { lit =>
        jdbcGetSet(lit.dataType)
      }
    for (((bV, setter), i) <- bindValues.zip(setters).zipWithIndex) {
      setter.setValue(bV, ps, i)
    }
  }

}
