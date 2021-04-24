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

package org.apache.spark.sql.connector.catalog.oracle.sharding.routing

import scala.language.implicitConversions

import oracle.spark.datastructs.{Interval, QResult, RedBlackIntervalTree}
import oracle.spark.sharding.KggHashGenerator
import oracle.sql.{Datum, NUMBER}

import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata
import org.apache.spark.sql.connector.catalog.oracle.sharding._
import org.apache.spark.sql.oracle.expressions.{OraLiteral, OraLiterals}


trait RoutingKeyRanges { self : RoutingTable =>

  trait RoutingKeyRange extends Interval[RoutingKey]

  case class ConsistentHashRange(start : SingleLevelKey, end : SingleLevelKey)
    extends RoutingKeyRange

  case class MultiLevelKeyRange(gRange : RoutingKeyRange,
                                sRange : RoutingKeyRange)
    extends RoutingKeyRange {

    val start = MultiLevelRKey(gRange.start, sRange.start)
    val end = MultiLevelRKey(gRange.end, sRange.end)


  }

  def createRoutingKeyRange(cInfo : ChunkInfo) : RoutingKeyRange = {
    val sKeyRange = ConsistentHashRange(
      OraDatumKey(new NUMBER(cInfo.shardKeyLo)),
      OraDatumKey(new NUMBER(cInfo.shardKeyHi))
      )

    tableFamily.superShardType match {
      case NONE_SHARDING => sKeyRange
      case _ => MultiLevelKeyRange(
        ConsistentHashRange(
          OraDatumKey(new NUMBER(cInfo.groupKeyLo)),
          OraDatumKey(new NUMBER(cInfo.groupKeyHi))
        ),
        sKeyRange
      )
    }
  }

  def createRoutingRange(lowKey : RoutingKey, hiKey : RoutingKey) : RoutingKeyRange = {
    null
  }
}

trait RoutingKeys extends RoutingKeyRanges { self : RoutingTable =>

  type ConsistentHash = Long // an unsigned value

  sealed trait RoutingKey extends Ordered[RoutingKey]

  trait SingleLevelKey extends RoutingKey {
    def consistentHash : ConsistentHash

    override def compare(that: RoutingKey): Int = that match {
      case sKey : SingleLevelKey => this.consistentHash.compareTo(sKey.consistentHash)
      case _ => -(that.compareTo(this))
    }
  }

  trait SingleColumnKey extends SingleLevelKey {
    def bytes : Array[Byte]
    val consistentHash: ConsistentHash = Integer.toUnsignedLong(KggHashGenerator.hash(bytes))
  }

  case class OraDatumKey(datum : Datum) extends SingleColumnKey {
    override def bytes : Array[Byte] = datum.getBytes
  }

  case object MinimumSingleKey extends SingleLevelKey {
    val consistentHash : ConsistentHash = 0L
  }

  case object MaximumSingleKey extends SingleLevelKey {
    val consistentHash : ConsistentHash = 0xffffffffL
  }

  case class MultiColumnRKey(colKeys : Seq[SingleLevelKey]) extends SingleLevelKey {
    val consistentHash: ConsistentHash = colKeys.map(_.consistentHash).sum
  }

  case class MultiLevelRKey(gKey : RoutingKey, sKey : RoutingKey) extends RoutingKey {
    val keys = (gKey, sKey)
    override def compare(that: RoutingKey): Int = {

      that match {
        case mLvlKey : MultiLevelRKey =>
          Ordering[(RoutingKey, RoutingKey)].compare(keys, mLvlKey.keys)
        case _ : SingleLevelKey => 1
      }
    }
  }

  implicit def rkeyToLong(rKey : SingleLevelKey) : ConsistentHash = rKey.consistentHash

  private def validateOraLiterals(oraLiterals : Seq[OraLiteral]) : Unit = {
    if (oraLiterals.size != routingColumnDTs.size) {
      OracleMetadata.invalidAction(s"Invalid OraLiterals list ${oraLiterals.mkString(", ")}", None)
    }

    for((oDT, oLit) <- routingColumnDTs.zip(oraLiterals)) {
      if (oDT.catalystType != oLit.catalystExpr.dataType) {
        OracleMetadata.invalidAction(s"Invalid OraLiteral ${oLit};" +
          s" need literal of type ${oDT.catalystType}", None)
      }
    }
  }

  private def multiColumnKey(cols : Seq[SingleLevelKey]) : SingleLevelKey = {
    if (cols.size == 1) {
      cols(0)
    } else {
      MultiColumnRKey(cols)
    }
  }

  def createRoutingKey(oraLiterals : OraLiteral*) : RoutingKey = {
    validateOraLiterals(oraLiterals)

    val gRKeys = oraLiterals.zip(gLvlKeyConstructors).map {
      case (oLit, cons) => cons(oLit)
    }

    val sRKeys = oraLiterals.drop(gLvlKeyConstructors.size).zip(sLvlKeyConstructors).map {
      case (oLit, cons) => cons(oLit)
    }

    val sKey = multiColumnKey(sRKeys)

    (tableFamily.superShardType) match {
      case NONE_SHARDING => sKey
      case _ => MultiLevelRKey(multiColumnKey(gRKeys), sKey)
    }
  }

  def minMaxKey(endKey : SingleLevelKey) : RoutingKey = {
    val sKey = multiColumnKey(Seq.fill(sColumnDTs.size)(endKey))
    (tableFamily.superShardType) match {
      case NONE_SHARDING => sKey
      case _ => MultiLevelRKey(multiColumnKey(Seq.fill(gColumnDTs.size)(endKey)), sKey)
    }
  }

}

trait RoutingQueryInterface { self : RoutingTable =>

  private def resultStreamToShardSet(qRes : Stream[QResult[RoutingKey, Array[Int]]])
  : Set[ShardInstance] = {
    val s = scala.collection.mutable.Set[Int]()
    val itr = qRes.iterator
    while(itr.hasNext) {
      val sInstances = itr.next().value
      sInstances.foreach(s.add(_))
    }
    Set[Int](s.toSeq : _*).map(shardCluster(_))
  }

  def lookupShardsLT(lits : OraLiteral*) : Set[ShardInstance] = {
    val rKey = createRoutingKey(lits: _*)
    resultStreamToShardSet(chunkRoutingIntervalTree.lt(rKey))
  }

  def lookupShardsLTE(lits : OraLiteral*) : Set[ShardInstance]
  def lookupShardsEQ(lits : OraLiteral*) : Set[ShardInstance]
  def lookupShardsNEQ(lits : OraLiteral*) : Set[ShardInstance]
  def lookupShardsGT(lits : OraLiteral*) : Set[ShardInstance]
  def lookupShardsGTE(lits : OraLiteral*) : Set[ShardInstance]
  def lookupShardsIN(lits : Array[Array[OraLiteral]]) : Set[ShardInstance]
  def lookupShardsNOTIN(lits : Array[Array[OraLiteral]]) : Set[ShardInstance]

}

trait RoutingTable extends RoutingQueryInterface with RoutingKeys {

  val tableFamily : TableFamily
  val shardCluster : Array[ShardInstance]
  val chunks : Array[ChunkInfo]

  val rootTable = tableFamily.rootTable
  val gColumnDTs = rootTable.superKeyColumns.map(_.dataType)
  val sColumnDTs = rootTable.keyColumns.map(_.dataType)
  val routingColumnDTs = gColumnDTs ++ sColumnDTs

  private lazy val shardNameToIdxMap : Map[String, Int] =
    (for ((s, i) <- shardCluster.zipWithIndex) yield {
      (s.name, i)
    }).toMap

  private[routing] lazy val gLvlKeyConstructors : Array[OraLiteral => SingleLevelKey] = {
    val jdbcGetSets = gColumnDTs.map(oDT => OraLiterals.jdbcGetSet(oDT.catalystType))
    for (jGS <- jdbcGetSets) yield {
      (oLit : OraLiteral) => OraDatumKey(jGS.toDatum(oLit.catalystExpr))
    }
  }

  private[routing] lazy val sLvlKeyConstructors : Array[OraLiteral => SingleLevelKey] = {
    val jdbcGetSets = sColumnDTs.map(oDT => OraLiterals.jdbcGetSet(oDT.catalystType))
    for (jGS <- jdbcGetSets) yield {
      (oLit : OraLiteral) => OraDatumKey(jGS.toDatum(oLit.catalystExpr))
    }
  }

  private[routing] lazy val minimumRoutingKey : RoutingKey = minMaxKey(MinimumSingleKey)

  private[routing] lazy val maximumRoutingKey : RoutingKey = minMaxKey(MaximumSingleKey)


  lazy val chunkRoutingIntervalTree = {
    val m : Map[Interval[RoutingKey], Array[Int]] =
      chunks.map(c => (createRoutingKeyRange(c), c.shardName)).
        groupBy(t => t._1).
        map {
          case (rrng : RoutingKeyRange, shardNames : Array[(RoutingKeyRange, String)]) =>
            (rrng, shardNames.map(s => shardNameToIdxMap(s._2)))
        }

    RedBlackIntervalTree.create[RoutingKey, Interval[RoutingKey], Array[Int]](m.toIndexedSeq)

  }
}