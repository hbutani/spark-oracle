/* $Header: hadoop/jsrc/oracle/hcat/db/split/OracleDBSplitGenerator.java ratiwary_bug-27617356/5 2018/03/15 03:09:49 ratiwary Exp $ */

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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * All Splits generator implement this interface and an entry for the new
 * implementor is entered in SplitGeneratorKind.
 *
 * @version $Header: OracleDBSplitGenerator.java 09-mar-2018.01:58:11 ratiwary
 * Exp $
 * @author ratiwary
 * @since release specific (what release of product did this appear in)
 */
public interface OracleDBSplitGenerator {

  /**
   * InputSplits can be of various types. This enumeration lists all the
   * possible splits. All splits generators must implement OracleSplitGenerator
   * interface.
   *
   * @author ratiwary
   *
   */
  public enum SplitGeneratorKind {

    /**
     * Generates one Split.
     */
    SINGLE_SPLITTER,
    /**
     * Generates split based on the number of columns specified by the property
     * {@link OracleSplitGenerator.NUMBER_OF_ROWS_PER_SPLIT}. Default number is
     * 1000.
     */
    ROW_SPLITTER,
    /**
     * Generates splits based on underlying storage of data blocks in Oracle
     * Database. Uses {@link OracleSplitGenerator.MAX_SPLITS} to control the
     * maximum number of splits generated
     */
    BLOCK_SPLITTER,
    /**
     * Generates one split per partition for a partitioned Oracle Table
     */
    PARTITION_SPLITTER,
    /**
     * Generates splits based on custom SQL specified by the user
     */
    CUSTOM_SPLITTER;

  };// end of enum SplitGeneratorKind

  /**
   * Max number of splits for any splitter.
   */
  public static final String MAX_SPLITS = "oracle.hcat.osh.maxSplits";

  /**
   * Default value for max splits. This will be used if
   * oracle.db.split.maxSplits is not set in table properties
   */
  public static final int DEFAULT_MAX_SPLITS = 1;

  /**
   * Default value for max splits in case of a partitioned table. This will be
   * used if oracle.db.split.maxSplits is not set in table properties
   */
  public static final int DEFAULT_MAX_PARTITION_SPLITS = 64;

  /**
   * Number of rows per split for ROW_SPLITTER. Default is 1000.
   */
  public static final String NUMBER_OF_ROWS_PER_SPLIT
          = "oracle.hcat.osh.rowsPerSplit";

  static final int DEFAULT_ROWS_PER_SPLIT = 1000;

  /**
   * Use DBMS_PARALLEL_EXECUTE to create table chunks for splits.
   */
  public static final String ORACLE_USE_CHUNK_SPLIT
          = "oracle.hcat.osh.useChunkSplitter";

  /**
   * SQL to generate chunk ranges for splitters
   */
  public static final String CUSTOM_CHUNK_SQL = "oracle.hcat.osh.chunkSQL";

  /**
   * Generate an array of {@link OracleDBSplit}.
   *
   * @param connection - Connection to the database
   * @param prop - Properties for the splitter
   * @return an array of OracleDBSplit(s)
   * @throws SQLException If there was an exception while creating splits.
   */
  public OracleDBSplit[] getDBInputSplits(Connection connection, 
          String shardURL, Properties prop, String tableName, 
          String inputConditions, String[] fields)
          throws SQLException;
}
