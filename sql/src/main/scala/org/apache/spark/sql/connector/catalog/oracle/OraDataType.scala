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

package org.apache.spark.sql.connector.catalog.oracle

import java.sql.{JDBCType, Types}
import java.util.Locale

import org.apache.spark.sql.oracle.OraSparkUtils
import org.apache.spark.sql.types.{
  ByteType,
  DataType,
  DateType,
  DecimalType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  ShortType,
  StringType,
  TimestampType
}

/**
 * References:
 * - https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-7B72E154-677A-4342-A1EA-C74C1EA928E6
 * - https://docs.oracle.com/en/database/oracle/oracle-database/19/jjdbc/accessing-and-manipulating-Oracle-data.html#GUID-231E827F-77EE-4648-B0C4-98651F9CE03F
 * - [[org.apache.spark.sql.execution.datasources.jdbc]] package
 */
trait OraDataType {
  def sqlType: Int
  def catalystType: DataType
}

case class OraChar(size: Int, inChars: Boolean) extends OraDataType {
  override def sqlType: Int = Types.CHAR
  override def catalystType: DataType = OraDataType.toCatalystType(sqlType)
}

case class OraVarchar2(size: Int, inChars: Boolean) extends OraDataType {
  override def sqlType: Int = Types.VARCHAR
  override def catalystType: DataType = OraDataType.toCatalystType(sqlType)
}

case class OraNChar(size: Int) extends OraDataType {
  override def sqlType: Int = Types.NCHAR
  override def catalystType: DataType = OraDataType.toCatalystType(sqlType)
}

case class OraNVarchar2(size: Int) extends OraDataType {
  override def sqlType: Int = Types.NVARCHAR
  override def catalystType: DataType = OraDataType.toCatalystType(sqlType)
}

case class OraNumber(precision: Option[Int], scale: Option[Int]) extends OraDataType {

  OraNumber.checkSupported(precision, scale)

  private def precisionToSQLType(precision: Int): Int =
    if (precision <= 2) {
      Types.TINYINT
    } else if (precision <= 4) {
      Types.SMALLINT
    } else if (precision <= 9) {
      Types.INTEGER
    } else if (precision <= 18) {
      Types.BIGINT
    } else {
      Types.BIGINT
    }

  override def sqlType: Int = ((precision, scale): @unchecked) match {
    case (Some(p), Some(0)) => precisionToSQLType(p)
    case (Some(p), None) => precisionToSQLType(p)
    case (Some(_), _) => Types.NUMERIC
    case (None, Some(_)) => Types.NUMERIC
    case (None, None) => Types.NUMERIC
  }

  override def catalystType: DataType = OraDataType.toCatalystType(sqlType, precision, scale)
}

object OraNumber {
  import OracleMetadata.checkDataType

  def toOraTypeNm(precision: Option[Int], scale: Option[Int]): String =
    ((precision, scale): @unchecked) match {
      case (None, None) => "NUMBER"
      case (None, Some(0)) => "NUMBER"
      case (Some(p), None) => s"NUMBER(${p})"
      case (Some(p), Some(s)) => if (s > 0) s"NUMBER(${p}, ${s})" else (s"NUMBER(${p})")
    }
  /*
=======
  def toOraTypeNm(precision: Option[Int], scale: Option[Int]): String = (precision, scale) match {
    case (None, None) => "NUMBER"
    case (Some(p), None) => s"NUMBER(${p})"
    case (None, Some(s)) => "NUMBER"
    case (Some(p), Some(s)) => s"NUMBER(${p}, ${s})"
  }
>>>>>>> 1f96e8f... XMLReader Modified
   */

  // Precision Defined && Scale Defined -> Check if Scale is smaller that precision.
  // Precision Not Defined && Scale Not Defined -> Allowed (NUMBER)
  // Precision Not Defined && Scale Defined -> Not Allowed.
  // Precision Defined && Scale Not Defined -> Allowed.
  def checkSupported(precision: Option[Int], scale: Option[Int]): Unit = {
    if (!precision.isDefined) {
      checkDataType(
        scale.isDefined && scale.get != 0,
        toOraTypeNm(precision, scale),
        s"scale cannot be defined if precision is undefined")
    } else {
      if (scale.isDefined) {
        val p = precision.get
        val s = scale.get
        checkDataType(s < 0, toOraTypeNm(precision, scale), "scale cannot be negative")
        checkDataType(
          s > p,
          toOraTypeNm(precision, scale),
          "scale cannot be greater than precision")
      }
    }
  }
}

case class OraFloat(precision: Option[Int]) extends OraDataType {
  override def sqlType: Int = Types.FLOAT
  override def catalystType: DataType = OraDataType.toCatalystType(sqlType, precision)
}

case object OraLong extends OraDataType {
  override def sqlType: Int = Types.LONGNVARCHAR
  override def catalystType: DataType = OraDataType.toCatalystType(sqlType)
}

case object OraBinaryFloat extends OraDataType {
  override def sqlType: Int = Types.FLOAT
  override def catalystType: DataType = OraDataType.toCatalystType(sqlType)
}

case object OraBinaryDouble extends OraDataType {
  override def sqlType: Int = Types.DOUBLE
  override def catalystType: DataType = OraDataType.toCatalystType(sqlType)
}

case object OraDate extends OraDataType {
  override def sqlType: Int = Types.DATE
  override def catalystType: DataType = OraDataType.toCatalystType(sqlType)
}

trait OraTimestampTypes extends OraDataType {
  def frac_secs_prec: Option[Int]
}
case class OraTimestamp(frac_secs_precision: Option[Int]) extends OraTimestampTypes {
  override def sqlType: Int = Types.TIMESTAMP
  override def catalystType: DataType = OraDataType.toCatalystType(sqlType)
  override def frac_secs_prec: Option[Int] = frac_secs_precision
}

case class OraTimestampWithTZ(frac_secs_precision: Option[Int]) extends OraTimestampTypes {
  override def sqlType: Int = Types.TIMESTAMP_WITH_TIMEZONE
  override def catalystType: DataType = OraDataType.toCatalystType(sqlType)
  override def frac_secs_prec: Option[Int] = frac_secs_precision
}

case class OraTimestampWithLocalTZ(frac_secs_precision: Option[Int]) extends OraTimestampTypes {
  override def sqlType: Int = Types.TIMESTAMP_WITH_TIMEZONE
  override def catalystType: DataType = OraDataType.toCatalystType(sqlType)
  override def frac_secs_prec: Option[Int] = frac_secs_precision
}

case class OraIntervalYearToMonth(pres: Option[Int], scale : Option[Int]) extends OraDataType {
  // TODO have to find the Types.
  override def sqlType: Int = Types.TIMESTAMP_WITH_TIMEZONE
  override def catalystType: DataType = OraDataType.toCatalystType(sqlType)
}

case class OraIntervalDayToSec(pres: Option[Int], scale : Option[Int]) extends OraDataType {
  // TODO have to find the Types.
  override def sqlType: Int = Types.TIMESTAMP_WITH_TIMEZONE
  override def catalystType: DataType = OraDataType.toCatalystType(sqlType)
}

object OraDataType {

  def typeString(
      s: String,
      length: Option[Int],
      precision: Option[Int],
      scale: Option[Int]): String = {
    var r: String = s

    val pDefined = precision.isDefined
    val sDefined = scale.isDefined
    val lDefined = length.isDefined
    val addBrackets = pDefined || sDefined
    val addComma = pDefined && sDefined

    def add(cond: Boolean, s: => String): Unit = {
      if (cond) {
        r += s
      }
    }

    add(addBrackets, "(")
    add(pDefined, precision.get.toString)
    add(addComma, ", ")
    add(sDefined, scale.get.toString)
    add(addBrackets, ")")
    add(lDefined, length.get.toString)

    r
  }

  def unapply(
      s: String,
      length: Option[Int],
      precision: Option[Int],
      scale: Option[Int]): OraDataType =
    (s.toUpperCase(Locale.ROOT), length, precision, scale) match {
      case ("CHAR", Some(l), None, None) => OraChar(l, true)
      case ("VARCHAR2", Some(l), None, None) => OraVarchar2(l, true)
      case ("NCHAR", Some(l), None, None) => OraNChar(l)
      case ("NVARCHAR2", Some(l), None, None) => OraNVarchar2(l)
      case ("NUMBER", None, p, s) =>
        OraNumber.checkSupported(p, s)
        OraNumber(p, s)
      case ("FLOAT", None, p, None) => OraFloat(p)
      case ("LONG", None, None, None) => OraLong
      case ("BINARY_FLOAT", None, None, None) => OraBinaryFloat
      case ("BINARY_DOUBLE", None, None, None) => OraBinaryDouble
      case ("DATE", None, None, None) => OraDate
      // case ("TIMESTAMP", p, None) => OraTimestamp(p)
      case ("TIMESTAMP", None, None, s) => OraTimestamp(s)
      case ("TIMESTAMP_WITH_TIMEZONE", None, None, s) => OraTimestampWithTZ(s)
      case ("TIMESTAMP_WITH_LOCAL_TIMEZONE", None, None, s) => OraTimestampWithLocalTZ(s)
      case ("INTERVAL_YEAR_TO_MONTH", None, p, s) => OraIntervalYearToMonth(p, s)
      case ("INTERVAL_DAY_TO_SECOND", None, p, s) => OraIntervalDayToSec(p, s)
      case _ => OracleMetadata.unsupportedOraDataType(typeString(s, length, precision, scale))
    }

    def toOracleType(oraDataType: OraDataType) : String = {
      oraDataType match {
        case OraChar(l, _) => s"CHAR(${l})"
        case OraVarchar2(l, _) => s"VARCHAR($l)"
        case OraNChar(l) => s"NCHAR($l)"
        case OraNChar(l) => s"NVARCHAR($l)"
        case OraNumber(p, s) => OraNumber.toOraTypeNm(p, s)
        case OraDate => s"DATE"
        case OraFloat(Some(p)) => s"Float($p)"
        case OraTimestamp(Some(s)) => s"TIMESTAMP(${s})"
        case _ => s"UNKNOWN"
      }
 }

  def toCatalystType(
      sqlType: Int,
      precision: Option[Int] = None,
      scale: Option[Int] = None): DataType = (sqlType, precision, scale) match {
      case (Types.CHAR, _, _) => StringType
      case (Types.VARCHAR, _, _) => StringType
      case (Types.NCHAR, _, _) => StringType
      case (Types.NVARCHAR, _, _) => StringType
      case (Types.TINYINT, _, _) => ByteType
      case (Types.SMALLINT, _, _) => ShortType
      case (Types.INTEGER, _, _) => IntegerType
      case (Types.BIGINT, Some(p), _) if p <= 18 => LongType
      case (Types.BIGINT, Some(p), _) => DecimalType(p, 0)
      case (Types.NUMERIC, Some(p), Some(s)) if p < DecimalType.MAX_PRECISION => DecimalType(p, s)
      case (Types.NUMERIC, None, None) => IntegerType
      case (Types.NUMERIC, _, _) => DecimalType.SYSTEM_DEFAULT
      case (Types.FLOAT, _, _) => FloatType
      case (Types.DOUBLE, _, _) => DoubleType
      case (Types.LONGNVARCHAR, _, _) => StringType
      case (Types.DATE, _, _) => DateType
      case (Types.TIMESTAMP, _, _) => TimestampType
      case (Types.TIMESTAMP_WITH_TIMEZONE, _, _) => TimestampType
      case _ =>
        OraSparkUtils.throwAnalysisException(
          s"Unsupported SqlType: " +
            s"${JDBCType.valueOf(sqlType).getName}")
    }
}
