/* $Header: hadoop/jsrc/oracle/hcat/db/split/OracleDBSplitUtil.java ratiwary_bug-27617356/8 2018/03/14 14:10:45 ratiwary Exp $ */

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static oracle.hcat.db.split.OracleDBSplitGenerator.DEFAULT_MAX_PARTITION_SPLITS;
import static oracle.hcat.db.split.OracleDBSplitGenerator.ORACLE_USE_CHUNK_SPLIT;
import static oracle.hcat.db.split.OracleDBSplitGenerator.SplitGeneratorKind.*;

/**
 * Utility methods for Oracle DB Splits
 *
 * @version $Header: hadoop/jsrc/oracle/hcat/db/split/OracleDBSplitUtil.java ratiwary_bug-27617356/8 2018/03/14 14:10:45 ratiwary Exp $
 * @author ratiwary
 * @since release specific (what release of product did this appear in)
 */
public class OracleDBSplitUtil implements Serializable {

  private static final Logger LOG
          = LoggerFactory.getLogger(OracleDBSplitUtil.class);

  private static final short MIN_SQL_ROW_SPLITTER_VERSION = 12100;

  /**
   * Returns MAX_SPLITS from the Properties object created from Table Properties
   *
   * @param prop table properties
   * @return max splits for this table
   */
  public static int getMaxSplitsFromProperties(Properties prop) {

    int maxSplits = OracleDBSplitGenerator.DEFAULT_MAX_SPLITS;
    String temp = prop.getProperty(OracleDBSplitGenerator.MAX_SPLITS);

    if (temp != null && Integer.valueOf(temp) > 0) {
      maxSplits = Integer.valueOf(temp);
    }
    return maxSplits;
  }

  private static final String IS_PARTITIONED_QUERY_SQL
          = "SELECT /*+ no_parallel */ table_name FROM USER_TAB_PARTITIONS "
          + "WHERE table_name = :1 and ROWNUM=1";

  public static OracleDBSplitGenerator.SplitGeneratorKind validateSplitKind(
          OracleDBSplitGenerator.SplitGeneratorKind splitKind, Connection 
          connection, String tableName, Properties prop)
          throws SQLException {
    
    // Set defaults for useChunkSplitter
    boolean useChunkSplitter = true;
    String useChunkSplitterString = prop.getProperty(ORACLE_USE_CHUNK_SPLIT);
    
    if(useChunkSplitterString != null) {
      useChunkSplitter = Boolean.getBoolean(useChunkSplitterString);      
    } else {
      prop.setProperty(ORACLE_USE_CHUNK_SPLIT, "true");
    }

    // If no splitter kind is specified, set splitter kind to partitioned 
    // splitter if the underlying table is partitioned.
    // Also when BLOCK_SPLLITTER is used, we must first check if the table is 
    // partitioned. If so, we override splitKind to PARITION_SPLLITTER
    if (splitKind == null || splitKind == BLOCK_SPLITTER || splitKind
            == CUSTOM_SPLITTER) {
      try (PreparedStatement pstmt = connection
              .prepareStatement(IS_PARTITIONED_QUERY_SQL)) {
        pstmt.setString(1, tableName);
        try (ResultSet rs = pstmt.executeQuery()) {
          // Sufficient to check if rs in not empty
          if (rs.next()) {
            // Set splitter kind and default max partitions for partitioned
            // table.
            LOG.warn("Table is partitioned. Using PARTITION_SPLITTER");
            splitKind = PARTITION_SPLITTER;
            prop.setProperty(OracleDBSplitGenerator.MAX_SPLITS,
                    Integer.toString(DEFAULT_MAX_PARTITION_SPLITS));
          } else if (isIndexOrganizedTable(connection, tableName)) {
            // IOT isn't allowed in ROWID based Block or Custom Splitters
            LOG.warn("Table is index organized. Using SINGLE_SPLITTER");
            splitKind = SINGLE_SPLITTER;
          }
        }
      }
    }
    
    // Default max splits for non partitioned underlying table
    int maxSplits = getMaxSplitsFromProperties(prop);
    
    // For a non partitioned underlying table, if maxSplits is set to 1 or if 
    // no splitter kind is specified, then SINGLE_SPLITTER must be used 
    // irrespective of any other property.
    if (maxSplits == 1 || splitKind == null) {
      return SINGLE_SPLITTER;
    }

    // Row based chunk splitter doesn't work with index organized tables.
    // Row based non chunk splitter doesn't work with DB version < 12.1
    // Setting meaningful defaults if any of the above cases arise.

    if (splitKind == ROW_SPLITTER) {
      boolean isIndexOrganized = OracleDBSplitUtil.isIndexOrganizedTable(connection,
              tableName);
      if (isIndexOrganized && useChunkSplitter == true) {
        LOG.warn("Using chunk based row splitter with index organized "
                + "table is not supported. Switching to no chunk based"
                + "row splitter");
        prop.setProperty(ORACLE_USE_CHUNK_SPLIT, "false");
      } else if (useChunkSplitter == false) {
        oracle.jdbc.internal.OracleConnection internalConnection
                = connection.unwrap(oracle.jdbc.internal.OracleConnection.class);
        short versionNumber
                = internalConnection.getVersionNumber();
        if (versionNumber < MIN_SQL_ROW_SPLITTER_VERSION) {

          if (!isIndexOrganized) {
            LOG.warn("SQL based row splitter needs Oracle DB 12.1 or above. "
                    + "Switching to chunk based row splitter");
            prop.setProperty(ORACLE_USE_CHUNK_SPLIT, "true");
          } else {
            LOG.warn("SQL based row splitter needs Oracle DB 12.1 or above. "
                    + "Switching to single splitter for IOT");
            splitKind = SINGLE_SPLITTER;
          }
        }
      }

    }
    
    return splitKind;
  }

  private static final String IS_IOT_QUERY_SQL = "SELECT /*+ no_parallel */ "
          + "table_name FROM ALL_TABLES WHERE table_name = :1 and "
          + "IOT_TYPE IS NOT NULL and ROWNUM=1";

  private static boolean isIndexOrganizedTable(Connection conn,
          String tableName) throws SQLException {
    try (PreparedStatement pstmt = conn.prepareStatement(IS_IOT_QUERY_SQL)) {
      pstmt.setString(1, tableName);
      try (ResultSet rs = pstmt.executeQuery()) {
        // Sufficient to check if rs in not empty
        if (rs.next()) {
          return true;
        }
      }
    }
    return false;
  }
  
  public static OracleDBSplitGenerator getSplitGenFromsplitKind(
          OracleDBSplitGenerator.SplitGeneratorKind splitKind,
          String tableName, int maxSplits,
          boolean useChunkSplitter) throws SQLException {

    OracleDBSplitGenerator dbSplitGen = null;
    
    switch (splitKind) {
      case SINGLE_SPLITTER:
        dbSplitGen = new OracleDBSimpleSplitGenerator();
        break;
      case ROW_SPLITTER:
        if (useChunkSplitter) {      
            dbSplitGen = new OracleDBRowChunkSplitGenerator();          
        } else {          
            dbSplitGen = new OracleDBRowSplitGenerator();
        }
        break;
      case BLOCK_SPLITTER:
        if (useChunkSplitter) {
          dbSplitGen = new OracleDBBlockChunkSplitGenerator();
        } else {
          dbSplitGen = new OracleDBBlockSplitGenerator();
        }
        break;
      case PARTITION_SPLITTER:
        dbSplitGen = new OracleDBPartitionSplitGenerator();
        break;
      case CUSTOM_SPLITTER:
        dbSplitGen = new OracleDBCustomChunkSplitGenerator();
        break;
      default:
        throw new AssertionError("Invalid Splitter Kind");
    }
    return dbSplitGen;
  }
  
}
