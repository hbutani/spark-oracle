/* $Header: hadoop/jsrc/oracle/hcat/db/split/OracleDBBlockChunkSplitGenerator.java ratiwary_bug-27617356/4 2018/03/14 14:10:45 ratiwary Exp $ */

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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Create splits using chunks created by DBMS_PARALLEL_EXECUTE package. Chunks
 * will be created using number of blocks provided as chunk size.
 *
 * @version $Header: OracleDBBlockChunkSplitGenerator.java 09-mar-2018.01:58:11
 * ratiwary Exp $
 * @author ratiwary
 * @since release specific (what release of product did this appear in)
 */
public class OracleDBBlockChunkSplitGenerator extends
        OracleDBChunkSplitGenerator {

  private static final Logger LOG
          = LoggerFactory.getLogger(OracleDBBlockChunkSplitGenerator.class);

  static final String NUM_TABLE_BLOCKS_QUERY = "select blocks from "
          + " dba_extents where OWNER=? AND segment_name=?";

  @Override
  OracleDBSplit[] createChunksAndGetSplits(Connection conn, String shardURL,
          Properties prop, String inputConditions) throws SQLException {

    // Retrieve all blocks from the table. maxSplits limit will be imposed in
    // getsplitsFromChunks
    int blocksPerSplit = 1;

    try (CallableStatement cstmt = conn.prepareCall(CHUNK_TASK_CREATE_CHUNKS)) {
      cstmt.setString(1, taskName);
      cstmt.setString(2, ownerName);
      cstmt.setString(3, tableName);
      cstmt.setInt(4, 0);
      cstmt.setInt(5, blocksPerSplit);
      cstmt.executeUpdate();
    } catch (SQLException se) {
      throw new SQLException(se.getMessage(), se);
    }
    return getSplitsFromChunks(conn, maxSplits, tableName, inputConditions, 
            shardURL);
  }

}
