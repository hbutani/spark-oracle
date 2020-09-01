/* $Header: hadoop/jsrc/oracle/hcat/db/split/OracleDBPartitionSplitGenerator.java ratiwary_bug-27617356/4 2018/03/14 14:10:45 ratiwary Exp $ */

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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Generates splits based on partitions.
 *
 * @version $Header: OracleDBPartitionSplitGenerator.java 09-mar-2018.01:58:11
 * ratiwary Exp $
 * @author ratiwary
 * @since release specific (what release of product did this appear in)
 */
public class OracleDBPartitionSplitGenerator implements OracleDBSplitGenerator {

  private static final Logger LOG = LoggerFactory.
          getLogger(OracleDBPartitionSplitGenerator.class);

  @Override
  public OracleDBSplit[] getDBInputSplits(Connection connection, String shardURL,
          Properties prop, String tableName, String inputConditions,
          String[] fields) throws SQLException {
    
    int maxSplits = OracleDBSplitUtil.getMaxSplitsFromProperties(prop);
    String[] partitions = getPartitions(connection, maxSplits, tableName,
            inputConditions, fields);
    
    long scn = OracleDBConnectionCacheUtil.getCurrentOracleSCN(connection);
    if (partitions == null || partitions.length == 0) {
      // No partitions were found. Use SINGLE_SPLITTER.
      LOG.error("PARTITION_SPLITTER used over a Table that is not partitioned. "
              + "Using SINGLE_SPLITTER instead");
      return new OracleDBSplit[]{new OracleDBSplit(scn,
        tableName, inputConditions, shardURL)};
    }
    int partitionCount = partitions.length;
    OracleDBSplit[] returnValue = new OracleDBSplit[partitionCount];
    for (int i = 0; i < returnValue.length; ++i) {
      returnValue[i] = new OracleDBPartitionSplit(partitions[i], scn,
              tableName, inputConditions, shardURL);
    }

    return returnValue;
  }

  private static final String PARTITION_QUERY_SQL = "SELECT /*+ no_parallel */ "
          + "PARTITION_NAME FROM "
          + "USER_TAB_PARTITIONS WHERE table_name = :1";
  private static final String HIVE_QUERY_STRING = "hive.query.string";

  private String[] getPartitions(Connection connection,
          int maxSplits, String tableName, String inputConditions, String[] fields) throws SQLException {
    String[] returnValue;

    String inputQuery = generateInputQuery(tableName, inputConditions, fields);

    LOG.info("input-query: " + inputQuery);
    if (inputQuery != null) {
      String conditions = inputConditions;
      if (conditions != null && (conditions = conditions.trim()).length() > 0) {
        inputQuery += " WHERE " + inputConditions;
      }
    }

    returnValue = getPartitionListWithExplainPlan(connection, inputQuery);
    if (returnValue == null || returnValue.length == 0) {
      returnValue = getAllPartitionsForTable(connection, tableName);
    }

    // Number of splits should not exceed maxSplits property for security.
    // If needed, group partitions together into a split. To do this create
    // another array where each entry shall contain string representing
    // at most partitionsPerSplit partitions.
    int partitionCount = returnValue.length;
    if (partitionCount > maxSplits) {
      int partitionsPerSplit
              = (int) Math.ceil(partitionCount / (double) maxSplits);
      String[] tempPartitions = new String[maxSplits];
      int i = 0;  // For original partition array (partitions)  
      int j = 0;  // For compressed partition array (tempPartitions) 
      while (j < maxSplits) {
        StringBuilder tempString = new StringBuilder();
        tempString.append(returnValue[i++]);
        int k = 1;
        while (i < partitionCount && k++ < partitionsPerSplit) {
          tempString.append(",");
          tempString.append(returnValue[i++]);
        }
        tempPartitions[j++] = tempString.toString();
      }
      returnValue = tempPartitions;
    }

    return returnValue;
  }

  private String generateInputQuery(String tableName, String inputConditions,
          String[] fields) {
    StringBuilder query = new StringBuilder();

    query.append("SELECT /*+ no_parallel */ ");

    if (fields != null) {
      for (int i = 0; i < fields.length; i++) {
        query.append(fields[i]);
        if (i != fields.length - 1) {
          query.append(", ");
        }
      }
    }

    query.append(" FROM ").append(tableName);

    LOG.info("PartitionSplitGenerator: Query: " + query);
    return query.toString();
  }

  /**
   * Returns all the partitions for a given table name
   *
   * @param connection - To the schema where the table exists
   * @param tableName - table name
   * @return An array of partitions
   * @throws SQLException if there was an exception while obtaining the list of
   * partitions
   */
  private String[] getAllPartitionsForTable(Connection connection,
          String tableName) throws SQLException {
    LOG.debug("Getting all partitions for table " + tableName);
    List<String> partitionList = new ArrayList<String>();
    try (PreparedStatement pstmt = connection
            .prepareStatement(PARTITION_QUERY_SQL)) {
      pstmt.setString(1, tableName);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          partitionList.add(rs.getString(1));
        }
      }
    }
    String[] returnValue = new String[partitionList.size()];
    partitionList.toArray(returnValue);
    return returnValue;
  }// end of getPartitions

  /**
   * Obtain partition list for a given Query.
   *
   * @param conn - connection to the schema where query can be executed
   * @param sql - SQL for which partition list is obtained
   * @return an array of partitions.
   * @throws SQLException - if there was an error while obtaining the list of
   * partitions.
   */
  private String[] getPartitionListWithExplainPlan(Connection conn,
          String sql) throws SQLException {
    //TODO: Need to use size to combine or split partitions between splits
    List<String> partitionList = new ArrayList<String>();

    // Generate a unique ID for the explain plan
    String id = new java.math.BigInteger(130, new java.security.SecureRandom())
            .toString(32);
    LOG.info("ID: " + id + ", sql=" + sql);
    // execute explain plan
    try (PreparedStatement pstmt = conn
            .prepareStatement("EXPLAIN PLAN " + "  SET STATEMENT_ID = '" + id
                    + "'  FOR " + conn.nativeSQL(sql))) {
      pstmt.execute();
    }// auto-close pstmt

    // Obtain all the partitions that are relevant to the query
    try (PreparedStatement pstmt = conn
            .prepareStatement(GET_ALL_RELEVANT_PARTITIONS)) {
      pstmt.setString(1, id);
      pstmt.setString(2, id);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          LOG.info("partitions: " + rs.getString(1));
          partitionList.add(rs.getString(1));
        }
      } catch (SQLException ex) {
        // When SQL compiler can identify start and stop position of partition 
        // range required for this query plan, PARTITION_STOP and 
        // PARTITION_START in plan_table are varchars representing numbers. 
        // Otherwise they contain 'INVALID' repersenting empty range or strings 
        // representing that range will be calculated at run time. When these 
        // fields don't represent a number, we get "ORA-01722: invalid number" 
        // here after executing GET_ALL_RELEVANT_PARTITIONS. In this case 
        // consume the exception and return null to indicate that partition list
        // should be created from getAllPartitionsForTable().
        if (ex.getErrorCode() == 1722) {
          return null;
        } else {
          throw new SQLException(ex.getMessage(), ex);
        }
      }
    }

    String[] returnValue = new String[partitionList.size()];
    partitionList.toArray(returnValue);
    return returnValue;
  }// end of doExplainPlanAndFrameQuery

  // Since PQ is disabled on all queries. No need to check for PQ OPERATIONS in
  // Query Plan
  private static final String GET_ALL_RELEVANT_PARTITIONS = "SELECT "
          + "/*+ no_parallel */ part.PARTITION_NAME "
          + "FROM USER_TAB_PARTITIONS part,PLAN_TABLE plan "
          + "WHERE "
          + "     STATEMENT_ID = :id "
          + " AND (plan.OPERATION LIKE '%PARTITION%')"
          + " AND part.TABLE_NAME = ( "
          + "     SELECT /*+ no_parallel */ OBJECT_NAME FROM PLAN_TABLE "
          + "     WHERE STATEMENT_ID=:id "
          + "       AND OPERATION = 'TABLE ACCESS') "
          + " AND part.PARTITION_POSITION  >= plan.PARTITION_START "
          + " AND part.PARTITION_POSITION  <= plan.PARTITION_STOP ";

}
