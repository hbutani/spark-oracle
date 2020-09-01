/* $Header: hadoop/jsrc/oracle/hcat/db/split/OracleDBRowSplitGenerator.java ratiwary_bug-27617356/4 2018/03/14 14:10:45 ratiwary Exp $ */

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
import java.sql.SQLException;
import java.util.Properties;

/**
 * Generates splits based on fixed number of rows.
 *
 * @version $Header: OracleDBRowSplitGenerator.java 09-mar-2018.01:58:11
 * ratiwary Exp $
 * @author ratiwary
 * @since release specific (what release of product did this appear in)
 */
public class OracleDBRowSplitGenerator implements OracleDBSplitGenerator {

  private static final Logger LOG
          = LoggerFactory.getLogger(OracleDBRowSplitGenerator.class);

  @Override
  public OracleDBSplit[] getDBInputSplits(Connection connection, String shardURL,
          Properties prop, String tableName, String inputConditions,
          String[] fields) throws SQLException {
    String temp = prop
            .getProperty(OracleDBSplitGenerator.NUMBER_OF_ROWS_PER_SPLIT);
    long rowsPerSplit = OracleDBSplitGenerator.DEFAULT_ROWS_PER_SPLIT;
    long tempRowsPerSplit = 0;

    if (temp != null) {
      try {
        tempRowsPerSplit = Long.valueOf(temp);
      } catch (NumberFormatException ex) {
        LOG.error("Property " + OracleDBSplitGenerator.NUMBER_OF_ROWS_PER_SPLIT
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

    int maxSplits = OracleDBSplitUtil.getMaxSplitsFromProperties(prop);

    long rowCount = OracleDBConnectionCacheUtil.getRowCount(connection,
            tableName);

    long scn = OracleDBConnectionCacheUtil.getCurrentOracleSCN(connection);
    if (rowCount == -1 || rowCount < rowsPerSplit) {
      return new OracleDBSplit[]{new OracleDBSplit(scn, tableName,
        inputConditions, shardURL)};
    }

    // Number of splits should not exceed maxSplits property for security.
    // If needed, override value of rowsPerSplit to ensure this.    
    int numSplits = (int) Math.ceil(rowCount / (double) rowsPerSplit);
    if (numSplits > maxSplits) {
      numSplits = maxSplits;
    }
    rowsPerSplit = (int) Math.ceil(rowCount / (double) numSplits);
    LOG.debug("numSplits exceeds maxSplits. Setting rowsPerSplit = "
            + rowsPerSplit);
    OracleDBSplit[] returnValue = new OracleDBSplit[numSplits];
    long rownum = 0;
    for (int i = 0; i < returnValue.length; ++i, rownum += rowsPerSplit) {
      returnValue[i] = new OracleDBRowSplit(rownum, rowsPerSplit, scn,
              tableName, inputConditions, shardURL);
    }

    return returnValue;
  }
}
