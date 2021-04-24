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

/**
 * Oracle Hash Function Implementation consistent with ORA_HASH
 * Copied from [[oracle.jdbc.pool.KggHashGenerator]]
 */
object KggHashGenerator {

  def hash(var0: Array[Byte]): Int = hash(var0, 0, var0.length, 0)

  def hash(var0: Array[Byte], var1: Int, var2: Int, var3: Int) : Int = {
    val var4 = var2 - var1
    var var5 = var4
    var var6 = var1
    var var7 = -1640531527
    var var8 = -1640531527
    var var9 = -1640531527
    var var10 = 0
    var10 = var3
    while ( {
      var5 >= 0
    }) {
      var var11 = 0
      var var12 = 0
      var var13 = 0
      var var14 = 0
      val var15 = var5 & -16
      val var16 = if (var15 != 0) 16
      else var5 & 15
      var16 match {
        case 16 =>
          var14 |= (var0(var6 + 15) & 255) << 24
        case 15 =>
          var14 |= (var0(var6 + 14) & 255) << 16
        case 14 =>
          var14 |= (var0(var6 + 13) & 255) << 8
        case 13 =>
          var14 |= var0(var6 + 12) & 255
        case 12 =>
          var13 |= (var0(var6 + 11) & 255) << 24
        case 11 =>
          var13 |= (var0(var6 + 10) & 255) << 16
        case 10 =>
          var13 |= (var0(var6 + 9) & 255) << 8
        case 9 =>
          var13 |= var0(var6 + 8) & 255
        case 8 =>
          var12 |= (var0(var6 + 7) & 255) << 24
        case 7 =>
          var12 |= (var0(var6 + 6) & 255) << 16
        case 6 =>
          var12 |= (var0(var6 + 5) & 255) << 8
        case 5 =>
          var12 |= var0(var6 + 4) & 255
        case 4 =>
          var11 |= (var0(var6 + 3) & 255) << 24
        case 3 =>
          var11 |= (var0(var6 + 2) & 255) << 16
        case 2 =>
          var11 |= (var0(var6 + 1) & 255) << 8
        case 1 =>
          var11 |= var0(var6 + 0) & 255
      }
      var10 += (if (var15 == 0) var4 + (var14 << 8) else var14)
      var7 += var11
      var8 += var12
      var9 += var13
      var7 += var10
      var10 += var7
      var7 ^= var7 >>> 7
      var8 += var7
      var7 += var8
      var8 ^= var8 << 13
      var9 += var8
      var8 += var9
      var9 ^= var9 >>> 17
      var10 += var9
      var9 += var10
      var10 ^= var10 << 9
      var7 += var10
      var10 += var7
      var7 ^= var7 >>> 3
      var8 += var7
      var7 += var8
      var8 ^= var8 << 7
      var9 += var8
      var8 += var9
      var9 ^= var9 >>> 15
      var10 += var9
      var9 += var10
      var10 ^= var10 << 11
      var6 += 16

      var5 -= 16
    }
    var10
  }

}
