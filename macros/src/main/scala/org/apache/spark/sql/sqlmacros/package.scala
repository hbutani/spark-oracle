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
package org.apache.spark.sql

/**
 * Spark SQL Macros provides a capability to register custom functions into a [[SparkSession]].
 * This is similar to [[UDFRegistration]]. The difference being SQL Macro attempts to generate
 * an equivalent [[Expression]] for the function body.
 *
 * Given a function registration:
 * {{{
 *   spark.udf.register("intUDF", (i: Int) => {
       val j = 2
       i + j
      })
 * }}}
 * The following query(assuming `sparktest.unit_test` has a column `c_int : Int`):
 * {{{
 *   select intUDF(c_int)
 *   from sparktest.unit_test
 *   where intUDF(c_int) < 0
 * }}}
 * generates the following physical plan:
 * {{{
 *
|== Physical Plan ==
Project (3)
+- * Filter (2)
   +- BatchScan (1)


(1) BatchScan
Output [1]: [C_INT#2271]
OraPlan: 00 OraSingleQueryBlock [C_INT#2271], [oracolumnref(C_INT#2271)]
01 +- OraTableScan SPARKTEST.UNIT_TEST, [C_INT#2271]
ReadSchema: struct<C_INT:int>
dsKey: DataSourceKey(jdbc:oracle:thin:@den02ads:1531/cdb1_pdb7.regress.rdbms.dev.us.oracle.com,tpcds)
oraPushdownSQL: select "C_INT"
from SPARKTEST.UNIT_TEST

(2) Filter [codegen id : 1]
Input [1]: [C_INT#2271]
Condition : (if (isnull(C_INT#2271)) null else intUDF(knownnotnull(C_INT#2271)) < 0)

(3) Project [codegen id : 1]
Output [1]: [if (isnull(C_INT#2271)) null else intUDF(knownnotnull(C_INT#2271)) AS intUDF(c_int)#2278]
Input [1]: [C_INT#2271]
 * }}}
 * The `intUDF` is invoked in the `Filter operator` for evaluating the `intUDF(c_int) < 0` predicate;
 * and in the `Project operator` to evaluate the projection `intUDF(c_int)`
 *
 * But the `intUDF` is a trivial function that just adds `2` to its argument.
 * With Spark SQL Macros you can register the function as a macro like this:
 * {{{
 *
import org.apache.spark.sql.defineMacros._

spark.registerMacro("intUDM", spark.udm((i: Int) => {
   val j = 2
   i + j
  }))
 * }}}
 * The query:
 * {{{
 *   select intUDM(c_int)
 *   from sparktest.unit_test
 *   where intUDM(c_int) < 0
 * }}}
 * generates the following physical plan:
 * {{{
 *
|== Physical Plan ==
 * Project (2)
+- BatchScan (1)


(1) BatchScan
Output [1]: [(c_int + 2)#2316]
OraPlan: 00 OraSingleQueryBlock [(C_INT#2309 + 2) AS (c_int + 2)#2316], [oraalias((C_INT#2309 + 2) AS (c_int + 2)#2316)], orabinaryopexpression((((C_INT#2309 + 2) < 0) AND isnotnull(C_INT#2309)))
01 +- OraTableScan SPARKTEST.UNIT_TEST, [C_INT#2309]
ReadSchema: struct<(c_int + 2):int>
dsKey: DataSourceKey(jdbc:oracle:thin:@den02ads:1531/cdb1_pdb7.regress.rdbms.dev.us.oracle.com,tpcds)
oraPushdownBindValues: 2, 0
oraPushdownSQL: select ("C_INT" + 2) AS "(c_int + 2)"
from SPARKTEST.UNIT_TEST
where ((("C_INT" + ?) < ?) AND "C_INT" IS NOT NULL)

(2) Project [codegen id : 1]
Output [1]: [(c_int + 2)#2316]
Input [1]: [(c_int + 2)#2316]
 * }}}
 * The predicate `intUDM(c_int) < 0` becomes `("C_INT" + ?) < ?`
 * (the literal in a predicate is converted to a bind value); and the
 * projection `intUDM(c_int)` becomes `"C_INT" + 2`.
 * And the entire query is pushed down.
 */
package object sqlmacros {}
