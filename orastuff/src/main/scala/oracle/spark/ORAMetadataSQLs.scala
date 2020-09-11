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

package oracle.spark

import java.sql
import java.sql.{ResultSet, SQLException, Types}

import scala.collection.mutable.{ArrayBuffer, Map => MMap}

object ORAMetadataSQLs {

  import ORASQLUtils._

  private val TABLE_METADATA_SQL = s"""
         declare 
           r_xml clob;
           r_sxml clob;
         begin
           SELECT DBMS_METADATA.get_xml ('TABLE', ?, USER) into r_xml from dual;
           SELECT DBMS_METADATA.get_sxml ('TABLE', ?, USER) into r_sxml from dual;
           ? := r_xml;
           ? := r_sxml;
         end;""".stripMargin

  /**
   * Return the DBMS_METADATA.get_xml and DBMS_METADATA.get_sxml
   * output for the given table.
   *
   * @param dsKey
   * @param schema
   * @param table
   * @return
   */
  def tableMetadata(dsKey: DataSourceKey, schema: String, table: String): (String, String) = {
    var xml: String = null
    var sxml: String = null
    performDSCall(dsKey, TABLE_METADATA_SQL, s"get table metadata for ${schema}.${table}", { cs =>
      cs.setString(1, "TIMES")
      cs.setString(2, "TIMES")
      cs.registerOutParameter(3, Types.CLOB)
      cs.registerOutParameter(4, Types.CLOB)
    }, { cs =>
      xml = cs.getString(3)
      sxml = cs.getString(4)
    })
    (xml, sxml)
  }

  @throws[sql.SQLException]
  def validateConnection(dsKey: DataSourceKey): Unit = {
    perform[Unit](dsKey, "check instance is setup for spark-oracle") { conn =>
      val meta = conn.getMetaData
      var res: ResultSet = null
      try {
        res = meta.getTables(dsKey.userName, null, "PLAN_TABLE", null)
        if (!res.next()) {
          throw new SQLException(s"${dsKey} is not setup for spark-oracle: missing PLAN_TABLE")
        }
      } finally {
        if (res != null) {
          res.close()
        }
      }
    }
  }

  def listAccessibleUsers(dsKey: DataSourceKey): Array[String] = {
    performDSQuery(
      dsKey,
      """
        |select username, all_shard
        |from all_users
        |where ORACLE_MAINTAINED = 'N'
        |""".stripMargin,
      "list non oracle maintained users") { rs =>
      val buf = ArrayBuffer[String]()
      while (rs.next()) {
        buf += rs.getString(1)
      }
      buf.toArray
    }
  }

  def listAllTables(dsKey: DataSourceKey): Map[String, Array[String]] = {
    performDSQuery(
      dsKey,
      """
        |select owner, table_name
        |from ALL_TABLES
        |where owner in (select username from all_users where ORACLE_MAINTAINED = 'N')
        |order by OWNER, TABLE_NAME
        |""".stripMargin,
      "list non oracle maintained users",
    ) { rs =>
      val m = MMap[String, Array[String]]()
      var currOwner: String = null
      var currBuf = ArrayBuffer[String]()

      while (rs.next()) {
        val nOwner = rs.getString(1)

        if (currOwner == null || currOwner != nOwner) {
          if (currBuf.nonEmpty) {
            m(currOwner) = currBuf.toArray
          }
          currOwner = nOwner
          currBuf = ArrayBuffer[String]()
        }
        currBuf += rs.getString(2)
      }
      m.toMap
    }
  }

}
