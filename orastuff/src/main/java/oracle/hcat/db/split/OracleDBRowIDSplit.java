/* $Header: hadoop/jsrc/oracle/hcat/db/split/OracleDBRowIDSplit.java ratiwary_bug-27617356/5 2018/03/15 03:09:49 ratiwary Exp $ */

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
 * @version $Header: hadoop/jsrc/oracle/hcat/db/split/OracleDBRowIDSplit.java ratiwary_bug-27617356/5 2018/03/15 03:09:49 ratiwary Exp $
 * @author ratiwary
 * @since release specific (what release of product did this appear in)
 */

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 * A split that maps to ROWID bounds within an Oracle Table
 *
 * @version $Header: hadoop/jsrc/oracle/hcat/db/split/OracleDBRowIDSplit.java ratiwary_bug-27617356/5 2018/03/15 03:09:49 ratiwary Exp $
 * @author ratiwary
 * @since release specific (what release of product did this appear in)
 */
public class OracleDBRowIDSplit extends OracleDBSplit {

  String[] binds = new String[2];

  public OracleDBRowIDSplit() {
    super();
  }

  public OracleDBRowIDSplit(String start, String end, long scn, String tableName,
          String predicate, String shardURL) {
    super(scn, "ROWID_SPLITTER", tableName, predicate, shardURL);
    binds[0] = start;
    binds[1] = end;
  }// end of OracleRownumSplit  

  @Override
  public Object[] getBinds() {
    return binds;
  }

  @Override
  public String getSqlStringToAppendAfterWhere(String conditions) {
    // Avoiding concatenation here, since that could lead to SQL Injection
    // also it is bad to have SQL with literal binds in terms of performance
    return (((conditions != null && conditions.length() != 0) ? " AND"
            : " WHERE") + " rowid BETWEEN ? AND ?");
  }

  public String toString() {
    return getClass().getName() + "(range="
            + java.util.Arrays.toString(binds) + ")";
  }
}
