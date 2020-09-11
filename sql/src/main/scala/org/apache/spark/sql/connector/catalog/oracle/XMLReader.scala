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

import java.io.File
import java.util

import scala.xml.{Node, NodeSeq}
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.{NotNullConstraint, OraColumn, OraForeignKey, OraPartitionType, OraPrimaryKey, OraTablePartition, TablePartitionScheme}

import scala.jdk.CollectionConverters.mapAsJavaMapConverter

case class ListSubpartitioningCls(nd: NodeSeq) {
  lazy val col_list = nd \ "COL_LIST"
}

object XMLReader {

  import scala.xml.XML._

  private val NAME_TAG = "NAME"
  private val SCHEMA_TAG = "NAME"
  private val COL_LIST_ITEM_TAG = "COL_LIST_ITEM"
  private val DATATYPE_TAG = "DATATYPE"
  private val COLLATE_NAME_TAG = "COLLATE_NAME"
  private val PRECISION_TAG = "PRECISION"
  private val LENGTH_TAG = "LENGTH"
  private val SCALE_TAG = "SCALE"
  private val NOT_NULL_TAG = "NOT_NULL"
  private val VALUES_TAG = "VALUES"
  private val SUBPARTITION_LIST_TAG = "SUBPARTITION_LIST"
  private val PARTITION_LIST_TAG = "PARTITION_LIST"
  private val PARTITION_LIST_ITEM_TAG = "PARTITION_LIST_ITEM"
  private val SUBPARTITION_LIST_ITEM_TAG = "SUBPARTITION_LIST_ITEM"
  private val PRIMARY_KEY_CONSTRAINT_LIST_TAG = "PRIMARY_KEY_CONSTRAINT_LIST"
  private val FOREIGN_KEY_CONSTRAINT_LIST_TAG = "FOREIGN_KEY_CONSTRAINT_LIST"
  private val FOREIGN_KEY_CONSTRAINT_LIST_ITEM_TAG = "FOREIGN_KEY_CONSTRAINT_LIST_ITEM"

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

  def getIntVal(x: String) : Option[Int] = {
    if (x == null || x.isEmpty) None
    else Some(x.toInt)
  }

  def getStringVal(x: String) : Option[String] = {
    if (x == null || x.isEmpty) None
    else Some(x)
  }

  def getNotNull(x : NodeSeq) : NotNullConstraint = {
    if (!x.isEmpty) {
      val name = (x \ NAME_TAG).text
      NotNullConstraint(true, Some(name))
    } else {
      NotNullConstraint(false, None)
    }
  }

  def tableSchema(nd: Node) : OraColumn = {
    val tc = TableColumnSchema(nd)

    OraColumn(tc.name,
      OraDataType.unapply(tc.datatype, getIntVal(tc.length),
        getIntVal(tc.precision), getIntVal(tc.scale)),
      getStringVal(tc.collateName),
      getNotNull(tc.notNULL))
  }



  def partitionScheme(nd: NodeSeq): TablePartitionScheme = {

    def partCols(nd: NodeSeq): Array[String] =
      (nd \ "COL_LIST" \ "COL_LIST_ITEM" \ NAME_TAG).map(_.text).toArray

    val cols = partCols(nd)
    val partType = OraPartitionType(nd.apply(0).label)

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

    private[XMLReader] def table_schema =
      TableSchemaCls(nd \ "COL_LIST")

    private[XMLReader] def table_properties =
      TablePropertiesCls(nd \ "TABLE_PROPERTIES")

    private[XMLReader] def primaryKey =
      (nd \ PRIMARY_KEY_CONSTRAINT_LIST_TAG)

    private[XMLReader] def foreignKey =
      (nd \ FOREIGN_KEY_CONSTRAINT_LIST_TAG)

    private[XMLReader] def physical_properties =
      (nd \ "PHYSICAL_PROPERTIES")
  }

  private case class TableSchemaCls(nd : NodeSeq) {
    lazy val name = (nd \ NAME_TAG).text
    lazy val schema = (nd \ SCHEMA_TAG).text

    private[XMLReader] def colSchema =
      TableColumnSchema(nd \ "COL_LIST_ITEM")
  }

  private case class TableColumnSchema(nd : NodeSeq) {
    lazy val name = (nd \ NAME_TAG).text
    lazy val datatype = (nd \ DATATYPE_TAG).text
    lazy val collateName = (nd \ COLLATE_NAME_TAG).text
    lazy val length = (nd \ LENGTH_TAG).text
    lazy val precision = (nd \ PRECISION_TAG).text
    lazy val scale = (nd \ SCALE_TAG).text
    lazy val notNULL = (nd \ NOT_NULL_TAG)
  }

  private case class TablePropertiesCls(nd: NodeSeq) {
    lazy val name = (nd \ NAME_TAG).text
    lazy val schema = (nd \ SCHEMA_TAG).text

    def partType : Option[PartitionCls] = {
      if (!(nd \ "RANGE_PARTITIONING").isEmpty) {
        Some(range_partitioning)
      } else if (!(nd \ "LIST_PARTITIONING").isEmpty) {
        Some(list_partitioning)
      } else if (!(nd \ "HASH_PARTITIONING").isEmpty) {
        Some(hash_partitioning)
      } else {
        None
      }
    }

    private[XMLReader] def range_partitioning =
      RangePartitioningCls(nd \ "RANGE_PARTITIONING")

    private[XMLReader] def list_partitioning =
      ListPartitioningCls(nd \ "LIST_PARTITIONING")

    private[XMLReader] def hash_partitioning =
      HashPartitioningCls(nd \ "HASH_PARTITIONING")
  }

  trait PartitionCls {
    def  getcol_list : NodeSeq
    def  gets_subpartition_inf : ListSubpartitioningCls
    def  getpartition : NodeSeq
    def  partNode : NodeSeq
    def  physicalPropertyNode : NodeSeq
  }

  private case class RangePartitioningCls(nd: NodeSeq) extends PartitionCls {
    lazy val col_list = nd \ "COL_LIST"
    lazy val list_subpartitioning_info = ListSubpartitioningCls(nd \ "LIST_SUBPARTITIONING")
    lazy val partitions = nd \ PARTITION_LIST_TAG \ PARTITION_LIST_ITEM_TAG

    override def getcol_list: NodeSeq = col_list

    override def gets_subpartition_inf: ListSubpartitioningCls = list_subpartitioning_info

    override def getpartition: NodeSeq = partitions

    override def partNode: NodeSeq = nd

    override def physicalPropertyNode: NodeSeq =
      (nd \ "DEFAULT_PHYSICAL_PROPERTIES")
  }

  private case class ListPartitioningCls(nd: NodeSeq) extends PartitionCls {
    lazy val col_list = nd \ "COL_LIST"
    lazy val list_subpartitioning_info = ListSubpartitioningCls(nd \ "LIST_SUBPARTITIONING")
    lazy val partitions = nd \ PARTITION_LIST_TAG \ PARTITION_LIST_ITEM_TAG

    override def getcol_list: NodeSeq = col_list

    override def gets_subpartition_inf: ListSubpartitioningCls = list_subpartitioning_info

    override def getpartition: NodeSeq = partitions

    override def partNode: NodeSeq = nd

    override def physicalPropertyNode: NodeSeq =
      (nd \ "DEFAULT_PHYSICAL_PROPERTIES")
  }

  private case class HashPartitioningCls(nd: NodeSeq) extends PartitionCls {
    lazy val col_list = nd \ "COL_LIST"
    lazy val list_subpartitioning_info = ListSubpartitioningCls(nd \ "LIST_SUBPARTITIONING")
    lazy val partitions = nd \ PARTITION_LIST_TAG \ PARTITION_LIST_ITEM_TAG

    override def getcol_list: NodeSeq = col_list

    override def gets_subpartition_inf: ListSubpartitioningCls = list_subpartitioning_info

    override def getpartition: NodeSeq = partitions

    override def partNode: NodeSeq = nd

    override def physicalPropertyNode: NodeSeq =
      (nd \ "DEFAULT_PHYSICAL_PROPERTIES")
  }



  val testFiles: List[String] = {
    val d = new File("/Users/sounchak/metadata/")
    var l = List[String]()
    for(x <- d.listFiles().filter(p => p.getName.endsWith(".xml"))) {
      l ++= List( x.getAbsolutePath)
    }
    l
  }


  def getSchema(tbl_xml: RelationTableCls): Array[OraColumn] = {
    val user_schema = tbl_xml.table_schema.nd
    var sc: List[OraColumn] = List[OraColumn]()
    for {p <- user_schema \ COL_LIST_ITEM_TAG} {
      sc = sc :+ tableSchema(p)
    }
    sc.toArray
  }

  def getPartitions(tbl_xml: RelationTableCls): Array[OraTablePartition] = {
    var tp: List[OraTablePartition] = List[OraTablePartition]()
    tbl_xml.table_properties.partType match {
      case Some(a) =>
        for ((p, i) <- a.getpartition.zipWithIndex) {
          tp = tp :+ tablePartition(p, i)
        }
        tp.toArray
      case _ => tp.toArray
    }
  }

  def getPartSchema(tbl_xml: RelationTableCls): Option[TablePartitionScheme] = {
    tbl_xml.table_properties.partType match {
      case Some(p) =>
        // val x = (p \ "RANGE_PARTITIONING")
        Some(partitionScheme(p.partNode))
      // Some(partitionScheme(p.partNode))
      case _ => None
    }
  }

  def getPrimaryKey(tbl_xml : RelationTableCls) : Option[OraPrimaryKey] = {
    def partCols(nd: NodeSeq): Array[String] =
      (nd \ "COL_LIST" \ "COL_LIST_ITEM" \ NAME_TAG).map(_.text).toArray

    val primaryKeyNode = tbl_xml.primaryKey

    if (!primaryKeyNode.isEmpty) {
      val cols = partCols(primaryKeyNode)
      Some(OraPrimaryKey(cols))
    } else {
      None
    }
  }

  def getForeignKey(tbl_xml : RelationTableCls) : Option[Array[OraForeignKey]] = {
    def partCols(nd: NodeSeq): Array[String] =
      (nd \ "COL_LIST" \ "COL_LIST_ITEM" \ NAME_TAG).map(_.text).toArray

    def referencedTable(nd : NodeSeq) : (String, String, Array[String]) = {
      // Pass the Schema and the Name and the Col List
      ((nd \ SCHEMA_TAG).text, (nd \ NAME_TAG).text, partCols(nd \ "REFERENCES"))
    }

    val foreignKeyNode = tbl_xml.foreignKey

    if (foreignKeyNode.nonEmpty) {
      val foreignKeyList = (foreignKeyNode \ FOREIGN_KEY_CONSTRAINT_LIST_ITEM_TAG)
      var fKeyList : List[OraForeignKey] = List[OraForeignKey]()
      for((p, i) <- foreignKeyList.zipWithIndex) {
        val cols = partCols(p)
        val (fschema, fname, fcols) = referencedTable(foreignKeyNode)
        fKeyList = fKeyList :+ OraForeignKey(cols, (fschema, fname), fcols)
      }
      Some(fKeyList.toArray)
    } else {
      None
    }
  }

  def getXternalTableProperties(tbl_xml : RelationTableCls) : Map[String, String] = {

    val nd = tbl_xml.table_properties.partType match {
      case Some(a) => (a.physicalPropertyNode \ "EXTERNAL_TABLE")
      case _ => (tbl_xml.physical_properties \ "EXTERNAL_TABLE")
    }

    var extProp : Map[String, String] = Map()

    if ((nd \ "ACCESS_DRIVER_TYPE").nonEmpty) {
      extProp += ("ACCESS_DRIVER_TYPE" -> (nd \ "ACCESS_DRIVER_TYPE").text)
    }
    if ((nd \ "DEFAULT_DIRECTORY").nonEmpty) {
      extProp += ("DEFAULT_DIRECTORY" -> (nd \ "DEFAULT_DIRECTORY").text)
    }
    if ((nd \ "ACCESS_PARAMETERS").nonEmpty) {
      extProp += ("ACCESS_PARAMETERS" -> (nd \ "ACCESS_PARAMETERS").text)
    }
    if ((nd \ "LOCATION").nonEmpty) {
      val locList = (nd \ "LOCATION" \ "LOCATION_ITEM" \ NAME_TAG).map(_.text).mkString(" : ")
      extProp += ("LOCATION" -> locList)
    }
    if ((nd \ "REJECT_LIMIT").nonEmpty) {
      extProp += ("REJECT_LIMIT" -> (nd \ "REJECT_LIMIT").text)
    }
    extProp
  }

  def getIs_External(tbl_xml : RelationTableCls) : Boolean = {
    // EXTERNAL TABLE Information will be present in
    // a. Partitioned Table -> TABLE_PROPERTIES / PARTITIONING / DEFAULT_PHYSICAL_PROPERTIES
    // b. Non Partitioned Table -> PHYSICAL PROPERTIES
    tbl_xml.table_properties.partType match {
      case Some(a) => (a.physicalPropertyNode \ "EXTERNAL_TABLE").nonEmpty
      case _ => (tbl_xml.physical_properties \ "EXTERNAL_TABLE").nonEmpty
    }
  }

  def getProperties(tbl_xml : RelationTableCls) : util.Map[String, String] = {
    var properties : Map[String, String] = Map()
    // As of Now placing the EXTERNAL_TABLE info in Properties.
    if (getIs_External(tbl_xml)) {
      properties ++= getXternalTableProperties(tbl_xml)
    }
    properties.asJava
    // TODO Have to Add for INDEX_ORGANIZED_TABLE
    // TODO Have to Add for HEAP_TABLE
  }

  def testMain: Unit = {
    // scalastyle:off println
    // scalastyle:off line.size.limit

    val fileList = testFiles
    for (x <- fileList) {
      println(s"Processing for file $x")

      val s = scala.io.Source.fromFile(x).mkString
      val tbl_xml = relational_table(loadString(s))
      val name : String = tbl_xml.name
      val tblschema : String = tbl_xml.schema
      val schema : Array[OraColumn] = getSchema(tbl_xml)
      val partSchema : Option[TablePartitionScheme] = getPartSchema(tbl_xml)
      val partitions: Array[OraTablePartition] = getPartitions(tbl_xml)

      // For Printing Schema
      println(schema.mkString("\n"))
      partitions.foreach { p =>
        println(p)
        p.subPartitions.foreach(println(_))
      }
    }
  }
}
