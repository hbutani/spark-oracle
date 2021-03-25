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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.oracle.OracleCatalog

/**
 * Functions available on a [[SparkSession]] that has a loaded
 * [[OracleCatalog]]
 *
 * @param sparkSession
 */
case class OraSparkSessionExts(sparkSession : SparkSession) {

  private val ORACLE_CATALOG_NAME = "oracle"

  private lazy val catalogManager = sparkSession.sessionState.catalogManager

  private lazy val hasOracleCatalog = catalogManager.isCatalogRegistered(ORACLE_CATALOG_NAME)

  private lazy val oraCatalog : OracleCatalog = if (hasOracleCatalog) {
      catalogManager.catalog(ORACLE_CATALOG_NAME).asInstanceOf[OracleCatalog]
    } else {
      null
    }

  def ensureOracleCatalog : Unit = {
    if (oraCatalog == null) {
      throw new IllegalAccessException(s"Attempt to perform an Oracle Catalog action" +
        s" on a SparkSession with no loaded OracleCatalog")
    }
  }


  def registerOracleFunction(packageName : Option[String],
                             funcName : String,
                             sparkFuncName : Option[String] = None) : String = {
    ensureOracleCatalog
    oraCatalog.registerOracleFunction(packageName, funcName, sparkFuncName)(sparkSession)
  }

  def registerOracleFunctions(packageName : Option[String],
                              fnSpecs : AnyRef*) : String = {
    ensureOracleCatalog
    oraCatalog.registerOracleFunctions(packageName, fnSpecs : _*)(sparkSession)
  }

  def registerOraType(schema: String, typName: String) : Unit = {
    ensureOracleCatalog
    oraCatalog.registerOraType(schema, typName)
  }

}
