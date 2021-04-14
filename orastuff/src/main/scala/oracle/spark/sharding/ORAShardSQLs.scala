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

package oracle.spark.sharding

import java.sql.{Connection, ResultSet, SQLException}

import oracle.spark.DataSourceKey
import oracle.spark.ORASQLUtils.{performDSQuery, performQuery}

object ORAShardSQLs {

  private val IS_SHARDED_INSTANCE_QUERY = "select name from gsmadmin_internal.database"

  private val LIST_SHARD_INSTANCES =
    """
      |select name, connect_string
      |from gsmadmin_internal.database""".stripMargin

  // MLOG$_<tNm>, RUPD$_<tNm>
  private val LIST_REPLICATED_TABLES =
  """with tlist as
    |(select owner,
    |       case
    |           when table_name like 'RUPD$_%' then substr(table_name, 7)
    |           when table_name like 'MLOG$_%' then substr(table_name, 7)
    |           else table_name
    |           end tname
    |from ALL_TABLES
    |where owner in (select username from all_users where ORACLE_MAINTAINED = 'N')
    |)
    |select owner, tname, count(*)
    |from tlist
    |group by owner, tname
    |having count(*) = 3
    |order by 1, 2""".stripMargin

  val LIST_TABLE_FAMILIES =
    """
      |select TABFAM_ID, TABLE_NAME, SCHEMA_NAME,
      |       GROUP_TYPE, GROUP_COL_NUM,
      |       SHARD_TYPE, SHARD_COL_NUM,
      |       DEF_VERSION
      |from local_chunk_types
      |order by TABFAM_ID
      |
      |-- local_chunk_types table
      |-- list the table family and root table
      | Name					   Null?    Type
      | ----------------------------------------- -------- ----------------------------
      | TABFAM_ID				   NOT NULL NUMBER
      | TABLE_NAME				   NOT NULL VARCHAR2(128)
      | SCHEMA_NAME					    VARCHAR2(128)
      | GROUP_TYPE					    VARCHAR2(5)
      | GROUP_COL_NUM					    NUMBER
      | SHARD_TYPE					    VARCHAR2(5)
      | SHARD_COL_NUM					    NUMBER
      | DEF_VERSION				   NOT NULL NUMBER
      | SHARDGROUP_NAME				    VARCHAR2(4000)
      |
      |""".stripMargin

  val LIST_TABLE_FAMILY_COLUMNS =
  """
      |select tabfam_id, shard_level, col_name, COL_IDX_IN_KEY
      |from local_chunk_columns
      |order by tabfam_id, shard_level, COL_IDX_IN_KEY
      |
      |describe local_chunk_columns;
      | Name					   Null?    Type
      | ----------------------------------------- -------- ----------------------------
      | TABFAM_ID					    NUMBER
      | SHARD_LEVEL				   NOT NULL NUMBER(1)
      | COL_NAME				   NOT NULL VARCHAR2(128)
      | COL_IDX_IN_KEY 			   NOT NULL NUMBER
      | EFF_TYPE				   NOT NULL NUMBER
      | CHARACTER_SET					    NUMBER
      | COL_TYPE				   NOT NULL NUMBER
      | COL_SIZE				   NOT NULL NUMBER
      |""".stripMargin


  def isShardedInstance(conn: Connection): Boolean = {
    try {
      performQuery(conn, IS_SHARDED_INSTANCE_QUERY) { rs =>
        rs.next()
      }
    } catch {
      case ex: SQLException => false
    }
  }

  def listShardInstances[V](dsKey: DataSourceKey)(action: ResultSet => V) : V = {
    performDSQuery(dsKey, LIST_SHARD_INSTANCES, "list shard instances")(action)
  }

  def listReplicatedTables[V](dsKey: DataSourceKey)(action: ResultSet => V) : V = {
    performDSQuery(dsKey, LIST_REPLICATED_TABLES, "list replicated tables")(action)
  }

}
