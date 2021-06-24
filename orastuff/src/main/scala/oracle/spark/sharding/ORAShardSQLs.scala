/*
  Copyright (c) 2021, Oracle and/or its affiliates.

  This software is dual-licensed to you under the Universal Permissive License
  (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
  2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
  either license.

  If you elect to accept the software under the Apache License, Version 2.0,
  the following applies:

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
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
      |select TABFAM_ID, TABLE_NAME, SCHEMA_NAME, GROUP_TYPE,
      |       GROUP_COL_NUM, SHARD_TYPE, SHARD_COL_NUM, DEF_VERSION
      |from LOCAL_CHUNK_TYPES
      |""".stripMargin

  val LIST_TABLE_FAMILY_COLUMNS =
  """
      |select SHARD_LEVEL, COL_IDX_IN_KEY, COL_NAME
      |from LOCAL_CHUNK_COLUMNS
      |where tabFam_id = ?
      |order by SHARD_LEVEL, COL_IDX_IN_KEY
      |""".stripMargin

  val LIST_CHUNKS =
    """
      |select shard_name, shard_key_low, shard_key_high,
      |       group_key_low, group_key_high,
      |       chunk_id, grp_id, chunk_unique_id,
      |       chunk_name, priority, state
      |from local_chunks c
      |where tabfam_id = ?
      |order by grp_id, chunk_id""".stripMargin


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

  def listTableFamilies[V](dsKey: DataSourceKey)(action: ResultSet => V) : V = {
    performDSQuery(dsKey, LIST_TABLE_FAMILIES, "list table families")(action)
  }

  def listTableFamilyColumns[V](dsKey: DataSourceKey, tFamId : Int)(action: ResultSet => V) : V = {
    performDSQuery(dsKey, LIST_TABLE_FAMILY_COLUMNS,
      "list table family columns",
      ps => {
        ps.setInt(1, tFamId)
      }
    )(action)
  }

  def listTableFamilyChunks[V](dsKey: DataSourceKey, tFamId : Int)(action: ResultSet => V) : V = {
    performDSQuery(dsKey, LIST_CHUNKS,
      "list table family chunks",
      ps => {
        ps.setInt(1, tFamId)
      }
    )(action)
  }
}
