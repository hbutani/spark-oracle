/* $Header: hadoop/jsrc/oracle/hcat/db/split/OracleDBRowChunkSplitGenerator.java ratiwary_bug-27617356/4 2018/03/14 14:10:45 ratiwary Exp $ */

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

import oracle.hcat.db.conn.OracleDBConnectionCacheUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Create splits using chunks created by DBMS_PARALLEL_EXECUTE package. Chunks
 * will be created using number of rows provided as chunk size.
 *
 * @version $Header: OracleDBRowChunkSplitGenerator.java 09-mar-2018.01:58:11
 * ratiwary Exp $
 * @author ratiwary
 * @since release specific (what release of product did this appear in)
 */
public class OracleDBRowChunkSplitGenerator extends
        OracleDBChunkSplitGenerator {

  private static final Logger LOG
          = LoggerFactory.getLogger(OracleDBRowChunkSplitGenerator.class);

  @Override
  OracleDBSplit[] createChunksAndGetSplits(Connection conn, String shardURL,
          Properties prop, String inputConditions) throws SQLException {

    String temp = prop
            .getProperty(NUMBER_OF_ROWS_PER_SPLIT);
    long rowsPerSplit = DEFAULT_ROWS_PER_SPLIT;
    long tempRowsPerSplit = 0;

    if (temp != null) {
      try {
        tempRowsPerSplit = Long.valueOf(temp);
      } catch (NumberFormatException ex) {
        LOG.error("Property " + NUMBER_OF_ROWS_PER_SPLIT
                + "must be a number");
        throw new SQLException(ex.getMessage(), ex);
      }
    }

    if (tempRowsPerSplit > 0) {
      rowsPerSplit = tempRowsPerSplit;
    } else {
      LOG.info("Using default value for oracle.hcat.osh.rowsPerSplit: "
              + rowsPerSplit);
    }

    long rowCount = OracleDBConnectionCacheUtil.getRowCount(conn, tableName);
    if (rowCount == -1 || rowCount < rowsPerSplit) {

      long scn = OracleDBConnectionCacheUtil.getCurrentOracleSCN(conn);

      return new OracleDBSplit[]{new OracleDBSplit(scn,
        tableName, inputConditions, shardURL)};
    }

    // Number of splits should not exceed maxSplits property for security.
    // If needed, override value of rowsPerSplit to ensure this.    
    int numSplits = (int) Math.ceil(rowCount / (double) rowsPerSplit);
    if (numSplits > maxSplits) {
      numSplits = maxSplits;
      LOG.debug("Over riding rowsPerSplit specified for table");
    }
    rowsPerSplit = (int) Math.ceil(rowCount / (double) numSplits);
    LOG.info("Setting rowsPerSplit = " + rowsPerSplit);

    try (CallableStatement cstmt = conn.prepareCall(CHUNK_TASK_CREATE_CHUNKS)) {
      cstmt.setString(1, taskName);
      cstmt.setString(2, ownerName);
      cstmt.setString(3, tableName);
      cstmt.setInt(4, 1);
      cstmt.setLong(5, rowsPerSplit);
      cstmt.executeUpdate();
    } catch (SQLException se) {
      throw new SQLException(se.getMessage(), se);
    }
    return getSplitsFromChunks(conn, numSplits, tableName, inputConditions, 
            shardURL);
  }

}
