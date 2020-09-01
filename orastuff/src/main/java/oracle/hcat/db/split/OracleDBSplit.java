/* $Header: hadoop/jsrc/oracle/hcat/db/split/OracleDBSplit.java ratiwary_bug-27617356/5 2018/03/15 03:09:49 ratiwary Exp $ */

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
/**
 *  @version $Header: hadoop/jsrc/oracle/hcat/db/split/OracleDBSplit.java ratiwary_bug-27617356/5 2018/03/15 03:09:49 ratiwary Exp $
 * @author ratiwary
 * @since release specific (what release of product did this appear in)
 */
package oracle.hcat.db.split;

import java.io.Serializable;

/**
 * A basic split that maps to an entire oracle Table
 *
 * @version $Header: hadoop/jsrc/oracle/hcat/osh/OracleDBSplit.java /main/3
 * 2016/03/29 23:49:26 ratiwary Exp $
 * @author ashivaru
 * @since release specific (what release of product did this appear in)
 */
public class OracleDBSplit implements Serializable{

  // SCN is required for consistant reads
  long scn;
  String splitKind = "SINGLE_SPLITTER";
  String tableName;
  String predicate;
  String shardURL;

  // need for deserialization
  public OracleDBSplit() {
    scn = -1L;
    splitKind = null;
    tableName = null;
    predicate = null;
    shardURL = null;
  }

  /**
   * Default Constructor.
   *
   * @param scn The SCN for the
   */
  public OracleDBSplit(long scn, String tableName, String predicate, 
          String shardURL) {
    this.scn = scn;
    this.tableName = tableName;
    this.predicate = predicate;
    this.shardURL = shardURL;
  }
  
  /**
   * Default Constructor.
   *
   * @param scn The SCN for the
   */
  public OracleDBSplit(long scn, String splitKind, String tableName, 
          String predicate, String shardURL) {
    this.scn = scn;
    this.splitKind = splitKind;
    this.tableName = tableName;
    this.predicate = predicate;
    this.shardURL = shardURL;
  }

  /**
   * Split specific SQL that has to be appended to the SQL before WHERE clause
   * to obtain the results.
   *
   * @return
   */
  public String getSqlStringToAppendBeforeWhere() {
    // This could be included as InputQuery in DBConfiguration. 
    return "";
  }

  /**
   * Split specific SQL that has to be appended to the SQL after WHERE clause to
   * obtain the results.
   *
   * @param conditions Conditions used in the statement
   * @return
   */
  public String getSqlStringToAppendAfterWhere(String conditions) {
    // This could be included as InputQuery in DBConfiguration. 
    return "";
  }

  /**
   * Split specific bind parameters. This must be used in conjunction with
   * {@link #getSqlStringToAppend(String)}
   *
   * @return An array of binds. Returns null for no binds.
   */
  public Object[] getBinds() {
    return null;
  }

  public long getSCN() {
    return scn;
  }

  public String getTableName() {
    return tableName;
  }

  public String getPredicate() {
    return predicate;
  }

  public boolean hasPredicate() {
    return predicate != null && predicate.length() > 0;
  }
  
  public String getSplitKind() {
    return splitKind;
  }
  
  public String getShardURL() {
    return shardURL;
  }

  public String toString() {
    return getClass().getName() + "(scn=" + scn + ")";
  }
}
