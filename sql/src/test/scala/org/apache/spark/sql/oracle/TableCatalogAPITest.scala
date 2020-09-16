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

import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.UnsupportedAction
import org.apache.spark.sql.connector.catalog.oracle.OraMetadataMgrInternalTest
import org.apache.spark.sql.hive.test.oracle.TestOracleHive

class TableCatalogAPITest extends AbstractTest with OraMetadataMgrInternalTest {

  test("describeTables") { td =>
    for ((ns, tbls) <- catalogTableMap;
         tbl <- tbls) {
      TestOracleHive.sql(s"describe extended ${ns}.${tbl}").show(1000, false)
    }
  }

  test("createTable") { td =>
    /*
    1. On Spark, don't specify table as external.
       Parse fails because external tables not supported for DS v2.
     */
    intercept[ParseException] {
      TestOracleHive.sql(s"""
           |create external table t2(id long, p string) 
           |using parquet 
           |partitioned by (p)
           |location "/tmp"""".stripMargin)
    }

    /*
     * 2. Valid table creation, but no yet implemented
     */
    var ex = intercept[UnsupportedAction] {
      TestOracleHive.sql(s"""
           |create table t2(id long, p string) 
           |using parquet 
           |partitioned by (p)
           |location "https://objectstorage.us-ashburn-1.oraclecloud.com/n/idlxex3qf8sf/b/SparkTest/o/t1"""".stripMargin)
    }

    assert(
      ex.getMessage ==
        """Unsupported Action on Oracle Catalog: Cannot create table
          | Method is supported but hasn't been implemented yet;""".stripMargin)

    /*
     * 3. Valid table creation, but no yet implemented
     */
    ex = intercept[UnsupportedAction] {
      TestOracleHive.sql(s"""
                          |create table t2(id long, p string) 
                          |using parquet
                          |location "https://objectstorage.us-ashburn-1.oraclecloud.com/n/idlxex3qf8sf/b/SparkTest/o/t1"""".stripMargin)
    }

    assert(
      ex.getMessage ==
        """Unsupported Action on Oracle Catalog: Cannot create table
        | Method is supported but hasn't been implemented yet;""".stripMargin)

    /*
     * 4. InValid table creation, not object store location
     */
    ex = intercept[UnsupportedAction] {
      TestOracleHive.sql(s"""
                            |create table t2(id long, p string) 
                            |using parquet
                            |location "/tmp"""".stripMargin)
    }

    assert(
      ex.getMessage ==
        """Unsupported Action on Oracle Catalog: Cannot create table
          | Currently only object store resident tables of parquet format can be created
          | via Spark SQL. For other cases, create table using Oracle DDL;""".stripMargin)

  }

  test("dropTable") { td =>
    }

  test("alterTable") { td =>
    }

  test("renameTable") { td =>
    }

}
