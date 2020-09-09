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

import scala.xml.{Node, NodeSeq}

import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.{
  OraPartitionType,
  OraTablePartition,
  TablePartitionScheme
}

object XMLReader {

  import scala.xml.XML._

  private val NAME_TAG = "NAME"
  private val SCHEMA_TAG = "NAME"
  private val VALUES_TAG = "VALUES"
  private val SUBPARTITION_LIST_TAG = "SUBPARTITION_LIST"
  private val PARTITION_LIST_TAG = "PARTITION_LIST"
  private val PARTITION_LIST_ITEM_TAG = "PARTITION_LIST_ITEM"
  private val SUBPARTITION_LIST_ITEM_TAG = "SUBPARTITION_LIST_ITEM"

  def tablePartition(nd: Node, idx: Int): OraTablePartition = {

    val pNm = (nd \ NAME_TAG).text
    val values = (nd \ VALUES_TAG).text
    val subParts =
      for ((sNd, sIdx) <- (nd \ SUBPARTITION_LIST_TAG \ SUBPARTITION_LIST_ITEM_TAG).zipWithIndex)
        yield {
          tablePartition(sNd, sIdx)
        }
    OraTablePartition(pNm, idx, values, subParts.toArray)
  }

  def partitionScheme(nd: Node): TablePartitionScheme = {

    def partCols(nd: NodeSeq): Array[String] =
      (nd \ "COL_LIST" \ "COL_LIST_ITEM" \ NAME_TAG).map(_.text).toArray

    val cols = partCols(nd)
    val partType = OraPartitionType(nd.label)

    val rng_subPart = nd \ "RANGE_SUBPARTITIONING"
    val list_subPart = nd \ "LIST_SUBPARTITIONING"
    val hash_subPart = nd \ "HASH_SUBPARTITIONING"

    var subParScheme: Option[TablePartitionScheme] = None

    if (rng_subPart.nonEmpty) {
      val subCols = partCols(rng_subPart)
      subParScheme = Some(TablePartitionScheme(subCols, OraPartitionType.RANGE, None))
    } else if (list_subPart.nonEmpty) {
      val subCols = partCols(list_subPart)
      subParScheme = Some(TablePartitionScheme(subCols, OraPartitionType.LIST, None))
    } else if (hash_subPart.nonEmpty) {
      val subCols = partCols(hash_subPart)
      subParScheme = Some(TablePartitionScheme(subCols, OraPartitionType.HASH, None))
    }

    TablePartitionScheme(cols, partType, subParScheme)
  }

  private def relational_table(nd: NodeSeq) =
    RelationTableCls(nd \ "RELATIONAL_TABLE")

  private case class RelationTableCls(nd: NodeSeq) {
    lazy val name = (nd \ NAME_TAG).text
    lazy val schema = (nd \ SCHEMA_TAG).text

    private[XMLReader] def table_properties =
      TablePropertiesCls(nd \ "TABLE_PROPERTIES")
  }

  private case class TablePropertiesCls(nd: NodeSeq) {
    lazy val name = (nd \ NAME_TAG).text
    lazy val schema = (nd \ SCHEMA_TAG).text

    private[XMLReader] def range_partitioning =
      RangePartitioningCls(nd \ "RANGE_PARTITIONING")
  }

  private case class RangePartitioningCls(nd: NodeSeq) {
    lazy val col_list = nd \ "COL_LIST"
    lazy val list_subpartitioning_info = ListSubpartitioningCls(nd \ "LIST_SUBPARTITIONING")
    lazy val partitions = nd \ PARTITION_LIST_TAG \ PARTITION_LIST_ITEM_TAG
  }

  private case class ListSubpartitioningCls(nd: NodeSeq) {
    lazy val col_list = nd \ "COL_LIST"
  }

  def testMain: Unit = {
    // scalastyle:off println
    // scalastyle:off line.size.limit

    val s = scala.io.Source
      .fromFile(
        "/Users/hbutani/spark/spark3.1/sql/core/src/test/scala/org/rhb/experiments/partitioning/composite_part_rng_list_sxml.xml")
      .mkString

    val tbl_xml = relational_table(loadString(s))
    val tbl_parts = tbl_xml.table_properties.range_partitioning.partitions
    for ((p, i) <- tbl_parts.zipWithIndex) {
      val tP = tablePartition(p, i)
      println(tP)
      for (sP <- tP.subPartitions) {
        println("  " + sP)
      }
    }

  }

}
