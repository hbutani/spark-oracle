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
  val sqlType: Int
  val catalystType: DataType
}

case class OraChar(size: Int, inChars: Boolean) extends OraDataType {
  val sqlType: Int = Types.CHAR
  val catalystType: DataType = OraDataType.toCatalystType(sqlType)
}

case class OraVarchar2(size: Int, inChars: Boolean) extends OraDataType {
  val sqlType: Int = Types.VARCHAR
  val catalystType: DataType = OraDataType.toCatalystType(sqlType)
}

case class OraNChar(size: Int) extends OraDataType {
  val sqlType: Int = Types.NCHAR
  val catalystType: DataType = OraDataType.toCatalystType(sqlType)
}

case class OraNVarchar2(size: Int) extends OraDataType {
  val sqlType: Int = Types.NVARCHAR
  val catalystType: DataType = OraDataType.toCatalystType(sqlType)
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

  val sqlType: Int = (precision, scale) match {
    case (Some(p), Some(0)) => precisionToSQLType(p)
    case (Some(p), None) => precisionToSQLType(p)
    case (Some(_), _) => Types.NUMERIC
    case (None, None) => Types.NUMERIC
  }

  val catalystType: DataType =
    OraDataType.toCatalystType(sqlType, precision, scale)

}

object OraNumber {
  import OracleMetadata.checkDataType

  def toOraTypeNm(precision: Option[Int], scale: Option[Int]): String = (precision, scale) match {
    case (None, None) => "NUMBER"
    case (Some(p), None) => s"NUMBER(${p})"
    case (Some(p), Some(s)) => s"NUMBER(${p}, ${s})"
  }

  def checkSupported(precision: Option[Int], scale: Option[Int]): Unit = {
    if (!precision.isDefined) {
      checkDataType(
        scale.isDefined,
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
  val sqlType: Int = precision match {
    case None => Types.NUMERIC
    case Some(p) if p <= 7 => Types.FLOAT
    case Some(p) if p <= 15 => Types.DOUBLE
    case _ => Types.NUMERIC
  }

  val catalystType: DataType = OraDataType.toCatalystType(sqlType, precision)

}

case object OraLong extends OraDataType {
  val sqlType: Int = Types.LONGNVARCHAR
  val catalystType: DataType = OraDataType.toCatalystType(sqlType)
}

case object OraBinaryFloat extends OraDataType {
  val sqlType: Int = Types.FLOAT
  val catalystType: DataType = OraDataType.toCatalystType(sqlType)
}

case object OraBinaryDouble extends OraDataType {
  val sqlType: Int = Types.DOUBLE
  val catalystType: DataType = OraDataType.toCatalystType(sqlType)
}

case object OraDate extends OraDataType {
  val sqlType: Int = Types.DATE
  val catalystType: DataType = OraDataType.toCatalystType(sqlType)
}

trait OraTimestampTypes extends OraDataType {
  val frac_secs_prec: Option[Int]
}
case class OraTimestamp(frac_secs_prec: Option[Int]) extends OraTimestampTypes {
  val sqlType: Int = Types.TIMESTAMP
  val catalystType: DataType = OraDataType.toCatalystType(sqlType)
}

case class OraTimestampWithTZ(frac_secs_prec: Option[Int]) extends OraTimestampTypes {
  val sqlType: Int = Types.TIMESTAMP_WITH_TIMEZONE
  val catalystType: DataType = OraDataType.toCatalystType(sqlType)
}

case class OraTimestampWithLocalTZ(frac_secs_prec: Option[Int]) extends OraTimestampTypes {
  val sqlType: Int = Types.TIMESTAMP_WITH_TIMEZONE
  val catalystType: DataType = OraDataType.toCatalystType(sqlType)
}

object OraDataType {

  def typeString(s: String, precision: Option[Int], scale: Option[Int]): String = {
    var r: String = s

    val pDefined = precision.isDefined
    val sDefined = scale.isDefined
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

    r
  }

  def unapply(s: String, precision: Option[Int], scale: Option[Int]): OraDataType =
    (s.toUpperCase(Locale.ROOT), precision, scale) match {
      case ("CHAR", Some(p), None) => OraChar(p, true)
      case ("VARCHAR2", Some(p), None) => OraVarchar2(p, true)
      case ("NCHAR", Some(p), None) => OraNChar(p)
      case ("NVARCHAR2", Some(p), None) => OraNVarchar2(p)
      case ("NUMBER", p, s) =>
        OraNumber.checkSupported(p, s)
        OraNumber(p, s)
      case ("FLOAT", p, None) => OraFloat(p)
      case ("LONG", None, None) => OraLong
      case ("BINARY_FLOAT", None, None) => OraBinaryFloat
      case ("BINARY_DOUBLE", None, None) => OraBinaryDouble
      case ("DATE", None, None) => OraDate
      case ("TIMESTAMP", p, None) => OraTimestamp(p)
      // TODO TIMESTAMP WITH TZ, TIMESTAMP WITh LOCAL TZ
      case _ => OracleMetadata.unsupportedOraDataType(typeString(s, precision, scale))
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
    case (Types.NUMERIC, _, _) => DecimalType.SYSTEM_DEFAULT
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
