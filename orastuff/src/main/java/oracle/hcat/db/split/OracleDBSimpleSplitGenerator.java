/* $Header: hadoop/jsrc/oracle/hcat/db/split/OracleDBSimpleSplitGenerator.java ratiwary_bug-27617356/4 2018/03/14 14:10:45 ratiwary Exp $ */

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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Split Generator that generates on split per table.
 *
 * @version $Header: OracleDBSimpleSplitGenerator.java 09-mar-2018.01:58:11
 * ratiwary Exp $
 * @author ratiwary
 * @since release specific (what release of product did this appear in)
 */
public class OracleDBSimpleSplitGenerator implements OracleDBSplitGenerator {
  
  @Override
  public OracleDBSplit[] getDBInputSplits(Connection connection, String shardURL,
          Properties prop, String tableName, String inputConditions,
          String[] fields) throws SQLException {
    // SCN is not required but enforcing it to be common across all splits
    oracle.jdbc.internal.OracleConnection internalConnection
            = connection.unwrap(oracle.jdbc.internal.OracleConnection.class);
    
    long scn = OracleDBConnectionCacheUtil.getCurrentOracleSCN(internalConnection);

    return new OracleDBSplit[]{new OracleDBSplit(scn,
      tableName, inputConditions, shardURL)};
  }

}
