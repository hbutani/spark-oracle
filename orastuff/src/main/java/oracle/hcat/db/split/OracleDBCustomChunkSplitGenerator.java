/* $Header: hadoop/jsrc/oracle/hcat/db/split/OracleDBCustomChunkSplitGenerator.java ratiwary_bug-27617356/4 2018/03/14 14:10:45 ratiwary Exp $ */

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
 * will be created using custom SQL provided as part of table properties.
 *
 * @version $Header: OracleDBCustomChunkSplitGenerator.java 09-mar-2018.01:58:11
 * ratiwary Exp $
 * @author ratiwary
 * @since release specific (what release of product did this appear in)
 */
public class OracleDBCustomChunkSplitGenerator extends
        OracleDBChunkSplitGenerator {

  private static final Logger LOG
          = LoggerFactory.getLogger(OracleDBCustomChunkSplitGenerator.class);
// static final String CHUNK_TASK_CREATE_CHUNKS_CUSTOM="call DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_SQL(?,?,TRUE)";
  static final String CHUNK_TASK_CREATE_CHUNKS_CUSTOM = "{call "
          + "DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_SQL(?,?,par_bool => (CASE ? WHEN 1 THEN TRUE ELSE FALSE END))}";

  @Override
  OracleDBSplit[] createChunksAndGetSplits(Connection conn, String shardURL,
          Properties prop, String inputConditions) throws SQLException {

    //TODO: Since chunks are generated using custom sql, maxsplits aren't
    // controlled in this splitter. We will either have to doc this or find a 
    // way to control max splits with custom splitters.
    String customSQL = prop.getProperty(OracleDBSplitGenerator.CUSTOM_CHUNK_SQL);
    if (customSQL == null) {
      throw new SQLException("Missing " + OracleDBSplitGenerator.CUSTOM_CHUNK_SQL + " property");
    }
    try (CallableStatement cstmt = conn.prepareCall(
            CHUNK_TASK_CREATE_CHUNKS_CUSTOM)) {
      cstmt.setString(1, taskName);
      cstmt.setString(2, customSQL);
      // We only support custom SQL to create ranges based on ROWID. So this 
      // argument is always true.
      cstmt.setInt(3, 1);
      cstmt.executeUpdate();
    }
    return getSplitsFromChunks(conn, maxSplits, tableName, inputConditions, 
            shardURL);
  }
}
