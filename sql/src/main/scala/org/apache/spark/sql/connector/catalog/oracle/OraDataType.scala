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

import org.apache.spark.sql.{types, SparkSession}
import org.apache.spark.sql.oracle.{OraSparkConfig, OraSparkUtils}
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, NumericType, ShortType, StringType, TimestampType}

/**
 * References:
 * - https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-7B72E154-677A-4342-A1EA-C74C1EA928E6
 * - https://docs.oracle.com/en/database/oracle/oracle-database/19/jjdbc/accessing-and-manipulating-Oracle-data.html#GUID-231E827F-77EE-4648-B0C4-98651F9CE03F
 * - [[org.apache.spark.sql.execution.datasources.jdbc]] package
 */
trait OraDataType {
  def sqlType: Int
  def catalystType: DataType

  def oraTypeString: String
}

case class OraChar(size: Int, inChars: Boolean) extends OraDataType {
  @transient lazy val sqlType: Int = Types.CHAR
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String = s"CHAR(${size}${if (!inChars) "BYTE" else ""})"
}

case class OraVarchar2(size: Int, inChars: Boolean) extends OraDataType {
  @transient lazy val sqlType: Int = Types.VARCHAR
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String = s"VARCHAR(${size}${if (!inChars) "BYTE" else ""})"
}

case class OraNChar(size: Int) extends OraDataType {
  @transient lazy val sqlType: Int = Types.NCHAR
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String = s"NCHAR(${size})"
}

case class OraNVarchar2(size: Int) extends OraDataType {
  @transient lazy val sqlType: Int = Types.NVARCHAR
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String = s"NVARCHAR2(${size})"
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

  @transient lazy val sqlType: Int = ((precision, scale): @unchecked) match {
    case (Some(p), Some(0)) => precisionToSQLType(p)
    case (Some(p), None) => precisionToSQLType(p)
    case (Some(_), _) => Types.NUMERIC
    case (None, Some(_)) => Types.NUMERIC
    case (None, None) => Types.NUMERIC
  }

  @transient lazy val catalystType: DataType =
    OraDataType.toCatalystType(sqlType, precision, scale)

  @transient lazy val oraTypeString: String = OraNumber.toOraTypeNm(precision, scale)
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
  @transient lazy val sqlType: Int = precision match {
    case None => Types.NUMERIC
    case Some(p) if p <= 7 => Types.FLOAT
    case Some(p) if p <= 15 => Types.DOUBLE
    case _ => Types.NUMERIC
  }

  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType, precision)
  @transient lazy val oraTypeString: String =
    s"FLOAT${if (precision.isDefined) "(" + precision.get + ")" else ""}"
}

case object OraLong extends OraDataType {
  @transient lazy val sqlType: Int = Types.LONGNVARCHAR
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String = "LONG"
}

case object OraBinaryFloat extends OraDataType {
  @transient lazy val sqlType: Int = Types.FLOAT
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String = "BINARY_FLOAT"
}

case object OraBinaryDouble extends OraDataType {
  @transient lazy val sqlType: Int = Types.DOUBLE
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String = "BINARY_DOUBLE"
}

case object OraDate extends OraDataType {
  @transient lazy val sqlType: Int = Types.DATE
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String = "DATE"
}

trait OraTimestampTypes extends OraDataType {
  def frac_secs_prec: Option[Int]
}

case class OraTimestamp(frac_secs_prec: Option[Int]) extends OraTimestampTypes {
  @transient lazy val sqlType: Int = Types.TIMESTAMP
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String =
    s"TIMESTAMP${if (frac_secs_prec.isDefined) "(" + frac_secs_prec.get + ")" else ""}"
}

case class OraTimestampWithTZ(frac_secs_prec: Option[Int]) extends OraTimestampTypes {
  @transient lazy val sqlType: Int = Types.TIMESTAMP_WITH_TIMEZONE
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String =
    s"TIMESTAMP${if (frac_secs_prec.isDefined) "(" + frac_secs_prec.get + ")" else ""}" +
      s"WITH TIME ZONE"
}

case class OraTimestampWithLocalTZ(frac_secs_prec: Option[Int]) extends OraTimestampTypes {
  @transient lazy val sqlType: Int = Types.TIMESTAMP_WITH_TIMEZONE
  @transient lazy val catalystType: DataType = OraDataType.toCatalystType(sqlType)
  @transient lazy val oraTypeString: String =
    s"TIMESTAMP${if (frac_secs_prec.isDefined) "(" + frac_secs_prec.get + ")" else ""}" +
      s"WITH LOCAL TIME ZONE"
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

  def create(
      s: String,
      length: Option[Int],
      precision: Option[Int],
      scale: Option[Int]): OraDataType =
    (s.toUpperCase(Locale.ROOT), length, precision, scale) match {
      case ("CHAR", Some(l), None, None) => OraChar(l, true)
      case ("VARCHAR2", Some(l), None, None) => OraVarchar2(l, true)
      case ("VARCHAR2", None, None, None) =>
        OraVarchar2(OraSparkConfig.getConf(OraSparkConfig.VARCHAR2_MAX_LENGTH), true)
      case ("NCHAR", Some(l), None, None) => OraNChar(l)
      case ("NVARCHAR2", Some(l), None, None) => OraNVarchar2(l)
      case ("NVARCHAR2", None, None, None) =>
        OraNVarchar2(OraSparkConfig.getConf(OraSparkConfig.VARCHAR2_MAX_LENGTH))
      case ("NUMBER", _, p, s) =>
        OraNumber.checkSupported(p, s)
        OraNumber(p, s)
      case ("FLOAT", None, p, None) => OraFloat(p)
      case ("LONG", None, None, None) => OraLong
      case ("BINARY_INTEGER", None, None, None) => OraNumber(Some(9), None)
      case ("BINARY_FLOAT", None, None, None) => OraBinaryFloat
      case ("BINARY_DOUBLE", None, None, None) => OraBinaryDouble
      case ("DATE", None, None, None) => OraDate
      // case ("TIMESTAMP", p, None) => OraTimestamp(p)
      case ("TIMESTAMP", None, None, s) => OraTimestamp(s)
      // TODO TIMESTAMP WITH TZ, TIMESTAMP WITh LOCAL TZ
      case _ => OracleMetadata.unsupportedOraDataType(typeString(s, length, precision, scale))
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

  def toOraDataType(dataType: DataType)
                   (implicit sparkSession : SparkSession = OraSparkUtils.currentSparkSession)
  : OraDataType = dataType match {
    case BooleanType => OraNumber(Some(1), None)
    case ByteType => OraNumber(Some(3), None)
    case ShortType => OraNumber(Some(5), None)
    case IntegerType => OraNumber(Some(10), None)
    case LongType => OraNumber(Some(19), None)
    // based on [[DecimalType#FloatDecimal]] definition
    case FloatType => OraNumber(Some(14), Some(7))
    // based on [[DecimalType#DoubleDecimal]] definition
    case DoubleType => OraNumber(Some(30), Some(15))
    case decT : DecimalType => OraNumber(Some(decT.precision), Some(decT.scale))
    case StringType => OraVarchar2(OraSparkConfig.getConf(OraSparkConfig.VARCHAR2_MAX_LENGTH), true)
    case DateType => OraDate
    case types.TimestampType => OraTimestamp(None)
    case _ => OraSparkUtils.throwAnalysisException(
      s"Unsupported dataType translation: ${dataType}")
  }
}
