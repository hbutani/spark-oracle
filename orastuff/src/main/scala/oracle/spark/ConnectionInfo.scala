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

import java.util.Properties

case class ConnectionInfo(
                           url : String,
                           username : String,
                           password : Option[String],
                           sunPrincipal: Option[String],
                           kerberosCallback : Option[String],
                           krb5Conf : Option[String],
                           tnsAdmin : Option[String],
                           authMethod : Option[String]
                         ) {

  def this(url : String,
           username : String,
           password : String) =
    this(url, username, Some(password), None, None, None, None, None)

  private [oracle] def dump : String = {
    def toString(v : Any) : String = v match {
      case Some(x) => x.toString
      case _ => v.toString
    }
    val values = productIterator
    val m = getClass.getDeclaredFields.map( _.getName -> toString(values.next) ).toMap
    m.toSeq.map(t =>s"${t._1} = ${t._2}").mkString("\t", "\n\t", "\n")
  }

  lazy val asConnectionProperties: Properties = {
    val properties = new Properties()
    import ConnectionInfo._

    properties.setProperty(ORACLE_URL, url)
    properties.setProperty(ORACLE_JDBC_USER, username)

    for(p <- password) {
      properties.setProperty(ORACLE_JDBC_PASSWORD, p)
    }

    for(sp <- sunPrincipal) {
      properties.setProperty(SUN_SECURITY_KRB5_PRINCIPAL, sp)
    }
    for(kc <- kerberosCallback) {
      properties.setProperty(KERB_AUTH_CALLBACK, kc)
    }
    for(kc <- krb5Conf) {
      properties.setProperty(JAVA_SECURITY_KRB5_CONF, kc)
    }
    for(ta <- tnsAdmin) {
      properties.setProperty(ORACLE_NET_TNS_ADMIN, ta)
    }
    for(am <- authMethod) {
      properties.setProperty(ORACLE_JDBC_AUTH_METHOD, am)
    }

    properties
  }

}

object ConnectionInfo {
  private val connOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    connOptionNames += name.toLowerCase
    name
  }

  val ORACLE_URL = newOption("url")
  val ORACLE_JDBC_TABLE_NAME = newOption("dbtable")
  val ORACLE_JDBC_USER = newOption("user")
  val ORACLE_JDBC_PASSWORD = newOption("password")
  val ORACLE_JDBC_MAX_PARTITIONS = newOption("maxPartitions")
  val ORACLE_JDBC_PARTITIONER_TYPE = newOption("partitionerType")
  val ORACLE_FETCH_SIZE = newOption("fetchSize")
  val SUN_SECURITY_KRB5_PRINCIPAL = newOption("sun.security.krb5.principal")
  val KERB_AUTH_CALLBACK = newOption("kerbCallback")
  val JAVA_SECURITY_KRB5_CONF = newOption("java.security.krb5.conf")
  val ORACLE_NET_TNS_ADMIN = newOption("oracle.net.tns_admin")
  val ORACLE_JDBC_IS_CHUNK_SPLITTER = newOption("isChunkSplitter")
  val ORACLE_JDBC_AUTH_METHOD = newOption("authMethod")
  val ORACLE_CUSTOM_CHUNK_SQL = newOption("customPartitionSQL")
  val ORACLE_PARALLELISM = newOption("useOracleParallelism")

  val DEFAULT_MAX_SPLITS = 1
}

