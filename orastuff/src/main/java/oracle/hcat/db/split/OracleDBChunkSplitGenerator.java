/* $Header: hadoop/jsrc/oracle/hcat/db/split/OracleDBChunkSplitGenerator.java ratiwary_bug-27617356/4 2018/03/14 14:10:45 ratiwary Exp $ */

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
 *  @version $Header: hadoop/jsrc/oracle/hcat/db/split/OracleDBChunkSplitGenerator.java ratiwary_bug-27617356/4 2018/03/14 14:10:45 ratiwary Exp $
 * @author ratiwary
 * @since release specific (what release of product did this appear in)
 */

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package oracle.hcat.db.split;

import oracle.hcat.db.conn.OracleDBConnectionCacheUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Create splits using chunks created by DBMS_PARALLEL_EXECUTE package
 *
 * @version $Header:
 * hadoop/jsrc/oracle/hcat/osh/OracleDBChunkSplitGenerator.java /main/3
 * 2017/06/15 02:54:31 ratiwary Exp $
 * @author ratiwary
 * @since release specific (what release of product did this appear in)
 */
public abstract class OracleDBChunkSplitGenerator implements
        OracleDBSplitGenerator {

  private static final Logger LOG
          = LoggerFactory.getLogger(OracleDBChunkSplitGenerator.class);

  static final String CHUNK_TASK_NAME_SQL
          = "select DBMS_PARALLEL_EXECUTE.GENERATE_TASK_NAME('OTA4H') from dual";
  static final String CHUNK_TASK_CREATE_TASK_SQL
          = "call DBMS_PARALLEL_EXECUTE.CREATE_TASK(?,'Dummy task for "
          + "OTA4H splitter')";
  static final String CHUNK_TASK_DROP_CHUNKS = "call "
          + "DBMS_PARALLEL_EXECUTE.DROP_CHUNKS(?)";
  static final String CHUNK_TASK_DROP_TASK = "call "
          + "DBMS_PARALLEL_EXECUTE.DROP_TASK(?)";
  static final String CHUNK_TASK_CREATE_CHUNKS = "{call "
          + "DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID(?,?,?,CASE WHEN ?=1 "
          + "THEN TRUE ELSE FALSE END,?)}";
  static final String ROWID_CHUNKS_QUERY = "select START_ROWID, END_ROWID from "
          + "user_parallel_execute_chunks where task_name=? order by "
          + "START_ROWID";

  String taskName;
  String ownerName;
  String tableName;
  int maxSplits;

  @Override
  public OracleDBSplit[] getDBInputSplits(Connection connection, String shardURL,
          Properties prop, String tableNameParameter, String inputConditions,
          String[] fields) throws SQLException {
    
    String completeName[];
    try {
      completeName = OracleDBConnectionCacheUtil.
              splitTableName(tableNameParameter);
    } catch (ParseException ex) {
      LOG.error("Invalid Table name");
      throw new SQLException(ex.getMessage(), ex);
    }

    ownerName = OracleDBConnectionCacheUtil.normalizeSQLName(completeName[0]);
    tableName = OracleDBConnectionCacheUtil.normalizeSQLName(completeName[1]);

    if (ownerName == null) {
      ownerName = connection.getMetaData().getUserName();
    }

    maxSplits = OracleDBSplitUtil.getMaxSplitsFromProperties(prop);

    try (PreparedStatement pstmt = connection.prepareStatement(
            CHUNK_TASK_NAME_SQL)) {
      try (ResultSet rs = pstmt.executeQuery()) {
        rs.next();
        taskName = rs.getString(1);
      }
    } catch (SQLException se) {
      throw new SQLException(se.getMessage(), se);
    }

    try (PreparedStatement pstmt = connection.prepareStatement(
            CHUNK_TASK_CREATE_TASK_SQL)) {
      pstmt.setString(1, taskName);
      try (ResultSet rs = pstmt.executeQuery()) {
      }
    } catch (SQLException se) {
      throw new SQLException(se.getMessage(), se);
    }

    OracleDBSplit[] returnValue = createChunksAndGetSplits(connection, shardURL, 
            prop, inputConditions);

    // Cleanup chunks information
    try (PreparedStatement pstmt = connection.prepareStatement(
            CHUNK_TASK_DROP_CHUNKS)) {
      pstmt.setString(1, taskName);
      try (ResultSet rs = pstmt.executeQuery()) {
      }
    } catch (SQLException se) {
      throw new SQLException(se.getMessage(), se);
    }

    try (PreparedStatement pstmt = connection.prepareStatement(
            CHUNK_TASK_DROP_TASK)) {
      pstmt.setString(1, taskName);
      try (ResultSet rs = pstmt.executeQuery()) {
      }
    } catch (SQLException se) {
      throw new SQLException(se.getMessage(), se);
    }
    return returnValue;
  }

  /**
   * Creates chunks using DBMS_PARALLEL_EXECUTE package and returns an array
   * containing splits created from the chunks.
   */
  abstract OracleDBSplit[] createChunksAndGetSplits(Connection conn, String shardURL,
          Properties prop, String inputConditions) throws SQLException;

  /**
   * Returns an array representing splits created from Oracle Table chunks. The
   * table chunks must already be created before calling this method.
   */
  OracleDBSplit[] getSplitsFromChunks(Connection conn, int maxSplits, 
          String tableNameParam, String inputConditions, String shardURL)
          throws SQLException {

    List<String> startRowID = new ArrayList<>();
    List<String> endRowID = new ArrayList<>();
    try (PreparedStatement pstmt = conn.prepareStatement(ROWID_CHUNKS_QUERY)) {
      pstmt.setString(1, taskName);
      // As per DBMS_PARALLEL_EXECUTE documentation, the actual number of chunks
      // and rows per chunk may not be exactly equal to the value specified as 
      // argument to CREATE_CHUNKS_BY_ROWID procedure. We must ensure here 
      // that the number of splits is always less than maxSplits.
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          startRowID.add(rs.getString(1));
          endRowID.add(rs.getString(2));
        }
      }
    }
    int numChunks = startRowID.size();
    int numSplits = numChunks;
    int numLargeSplits = 0;
    int normalSplitSize = 1;
    if (maxSplits < numSplits) {
      // When numSplits is greater than maxSplits, we divide them into groups.
      // normalSplitSize is the size of splits when numSplits can be divided
      // into maxSplits of equal size. If this isn't the case then 
      // numLargeSplits will be larger than other splits by a factor of 1
      normalSplitSize = numSplits / maxSplits;
      numLargeSplits = numSplits % maxSplits;
      numSplits = maxSplits;
    }
    int splitStartIndex = 0;
    // splitEndIndex is incremented by at least 1 inside the below for loop 
    // before it is used
    int splitEndIndex = -1;
    int currentSplitSize;
    OracleDBSplit[] returnValue = new OracleDBSplit[numSplits];
    for (int i = 0; i < numSplits; i++) {
      long scn = OracleDBConnectionCacheUtil.getCurrentOracleSCN(conn);
      currentSplitSize = i < numLargeSplits
              ? normalSplitSize + 1 : normalSplitSize;
      splitEndIndex = splitEndIndex + currentSplitSize;
      returnValue[i] = new OracleDBRowIDSplit(startRowID.get(splitStartIndex),
              endRowID.get(splitEndIndex), scn, tableNameParam,
              inputConditions, shardURL);
      splitStartIndex = splitStartIndex + currentSplitSize;
    }
    return returnValue;
  }
}
