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

package org.apache.spark.sql.hive.test.oracle

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.config
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH

object OracleTestConf {

  private lazy val commonConf = new SparkConf()
    .set("spark.sql.test", "")
    .set(SQLConf.CODEGEN_FALLBACK.key, "false")
    .set(
      HiveUtils.HIVE_METASTORE_BARRIER_PREFIXES.key,
      "org.apache.spark.sql.hive.execution.PairSerDe")
    .set(WAREHOUSE_PATH.key, TestHiveContext.makeWarehouseDir().toURI.getPath)
    // SPARK-8910
    .set(UI_ENABLED, false)
    .set(config.UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
    // Hive changed the default of hive.metastore.disallow.incompatible.col.type.changes
    // from false to true. For details, see the JIRA HIVE-12320 and HIVE-17764.
    .set("spark.hadoop.hive.metastore.disallow.incompatible.col.type.changes", "false")
    // Disable ConvertToLocalRelation for better test coverage. Test cases built on
    // LocalRelation will exercise the optimization rules better by disabling it as
    // this rule may potentially block testing of other optimization rules such as
    // ConstantPropagation etc.
    .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)
    .set("spark.sql.extensions", "org.apache.spark.sql.oracle.SparkSessionExtensions")
    .set(
      "spark.kryo.registrator",
      "org.apache.spark.sql.connector.catalog.oracle.OraKryoRegistrator")
    /*
    Uncomment to see Plan rewrites
    .set("spark.sql.planChangeLog.level", "ERROR")
    .set(
      "spark.sql.planChangeLog.rules",
      "org.apache.spark.sql.execution.datasources.PruneFileSourcePartitions," +
        "org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown")
     */
    .set(
      "spark.sql.catalog.oracle",
      "org.apache.spark.sql.connector.catalog.oracle.OracleCatalog")
    .set("spark.sql.catalog.oracle.use_metadata_cache_only", "true")
    .set("spark.sql.catalog.oracle.metadata_cache_loc", "sql/src/test/resources/metadata_cache")
    .set("spark.sql.catalog.oracle.log_and_time_sql.enabled", "true")
    .set("spark.sql.catalog.oracle.log_and_time_sql.log_level", "info")
    .set("spark.sql.catalog.oracle.use_resultset_cache", "true")
    .set("spark.sql.catalog.oracle.resultset_cache_loc", "sql/src/test/resources/resultset_cache")
    // .set("spark.sql.oracle.max_string_size", "32767")

  lazy val localConf: SparkConf = {

    var conf = commonConf

    assert(
      System.getProperty(SPARK_ORACLE_DB_INSTANCE) != null,
      s"Running test requires setting ${SPARK_ORACLE_DB_INSTANCE} system property")

    System.getProperty(SPARK_ORACLE_DB_INSTANCE) match {
      case "local_hb" =>
        conf = local_hb(conf)
      case "local_sc" =>
        conf = local_sc(conf)
      case "mammoth_medium" =>
        assert(
          System.getProperty(SPARK_ORACLE_DB_WALLET_LOC) != null,
          s"Use of mammoth instance requires setting ${SPARK_ORACLE_DB_WALLET_LOC} system property")
        conf = mammoth_medium(conf)
      // scalastyle:off
      case _ => ???
      // scalastyle:on
    }
    conf
  }

  def local_hb(conf: SparkConf): SparkConf =
    conf
      .set("spark.sql.catalog.oracle.url", "jdbc:oracle:thin:@hbutani-Mac:1521/orclpdb1")
      .set("spark.sql.catalog.oracle.user", "sh")
      .set("spark.sql.catalog.oracle.password", "welcome123")

  def local_sc(conf: SparkConf): SparkConf =
    conf
      .set("spark.sql.catalog.oracle.url", "jdbc:oracle:thin:@//localhost:1521/ORCLCDB")
      .set("spark.sql.catalog.oracle.user", "sparktest")
      .set("spark.sql.catalog.oracle.password", "sparktest")

  def mammoth_medium(conf: SparkConf): SparkConf =
    conf
      .set("spark.sql.catalog.oracle.authMethod", "ORACLE_WALLET")
      .set("spark.sql.catalog.oracle.url", "jdbc:oracle:thin:@mammoth_medium")
      .set("spark.sql.catalog.oracle.user", "tpcds")
      .set("spark.sql.catalog.oracle.password", "Performance_1234")
      .set(
        "spark.sql.catalog.oracle.net.tns_admin",
        System.getProperty(SPARK_ORACLE_DB_WALLET_LOC))
      .set("spark.sql.catalog.oracle.oci_credential_name", "OS_EXT_OCI")

  def testMaster: String = "local[*]"

  val SPARK_ORACLE_DB_INSTANCE = "spark.oracle.test.db_instance"
  val SPARK_ORACLE_DB_WALLET_LOC = "spark.oracle.test.db_wallet_loc"

}

object TestOracleHive
    extends TestHiveContext(
      new SparkContext(
        System.getProperty("spark.sql.test.master", OracleTestConf.testMaster),
        "TestSQLContext",
        OracleTestConf.localConf),
      false)
