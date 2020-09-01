/* $Header: hadoop/jsrc/oracle/hcat/db/split/OracleDBPartitionSplit.java ratiwary_bug-27617356/5 2018/03/15 03:09:49 ratiwary Exp $ */

/* Copyright (c) 2018, Oracle and/or its affiliates. All rights reserved.*/

 /*
   DESCRIPTION
    <short description of component this file declares/defines>

   PRIVATE CLASSES
    <list of private classes defined - with one-line descriptions>

   NOTES
    <other useful comments, qualifications, etc.>

   MODIFIED    (MM/DD/YY)
    ratiwary    03/09/18 - Creation
 */
package oracle.hcat.db.split;

/**
 * Split that maps to a single partition of an Oracle Table
 *
 * @version $Header: OracleDBPartitionSplit.java 09-mar-2018.01:58:11 ratiwary
 * Exp $
 * @author ratiwary
 * @since release specific (what release of product did this appear in)
 */
public class OracleDBPartitionSplit extends OracleDBSplit {

  String partition;

  //for the de-serializer
  public OracleDBPartitionSplit() {
    super();
  }

  public OracleDBPartitionSplit(String part, long scn, String tableName,
          String predicate, String shardURL) {
    super(scn, "PARTITION_SPLITTER", tableName, predicate, shardURL);
    partition = part;
  }

  @Override
  public Object[] getBinds() {
    return null;
  }

  @Override
  public String getSqlStringToAppendBeforeWhere() {
    return String.format(" PARTITION(%s)", partition);
  }
  
  public String getPartition() {
    return partition;
  }

  public String toString() {
    return getClass().getName() + "(partition=" + partition + ", scn="
            + scn + ")";
  }

}
