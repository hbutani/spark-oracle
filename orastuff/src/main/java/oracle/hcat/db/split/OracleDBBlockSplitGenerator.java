/* $Header: hadoop/jsrc/oracle/hcat/db/split/OracleDBBlockSplitGenerator.java ratiwary_bug-27617356/4 2018/03/14 14:10:45 ratiwary Exp $ */

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
 * Generates splits based on rowid ranges.
 *
 * @version $Header: OracleDBBlockSplitGenerator.java 09-mar-2018.01:58:11
 * ratiwary Exp $
 * @author ratiwary
 * @since release specific (what release of product did this appear in)
 */
public class OracleDBBlockSplitGenerator implements OracleDBSplitGenerator {

  private static final Logger LOG
          = LoggerFactory.getLogger(OracleDBBlockSplitGenerator.class);
  // NOTE: need access to dba_extents
  static final String ROWID_PARTITION_SQL = "select a.grp, "
          + "dbms_rowid.rowid_create( 1, b.data_object_id, a.lo_fno, a.lo_block, 0) min_rid, "
          + "dbms_rowid.rowid_create( 1, b.data_object_id, a.hi_fno, a.hi_block, 10000) max_rid"
          + " from ("
          + "  select distinct grp,"
          + "  first_value(relative_fno) over"
          + "  (partition by grp order by "
          + "   relative_fno rows between unbounded "
          + "   preceding and unbounded following) lo_fno,"
          + "   first_value(block_id) over "
          + "   (partition by grp order by block_id "
          + "   rows between unbounded "
          + "   preceding and unbounded following) lo_block, "
          + "   last_value(relative_fno) over "
          + "   (partition by grp order by relative_fno "
          + "   rows between unbounded "
          + "   preceding and unbounded following) hi_fno, "
          + "   last_value(block_id+blocks-1) over "
          + "   (partition by grp order by  block_id "
          + "   rows between unbounded "
          + "   preceding and unbounded following) hi_block, "
          + "   sum(blocks) over (partition by grp) sum_blocks "
          + " from ( "
          + " select relative_fno, "
          + "                  block_id, "
          + "                  blocks, "
          + "                  trunc((sum(blocks) over (order by relative_fno, "
          + "                  block_id)-0.01) / (sum(blocks) over ()/?)) grp "
          + "         from dba_extents "
          + "         where segment_name = ? "
          + "           and owner = user "
          + "         order by block_id) "
          + "  )a,"
          + "(select data_object_id "
          + "   from user_objects "
          + "   where object_name = ? and data_object_id is not null "
          + ")b";

  @Override
  public OracleDBSplit[] getDBInputSplits(Connection connection, String shardURL,
          Properties prop, String tableName,
          String inputConditions, String[] fields) throws SQLException {
    
    List<OracleDBSplit> splitList = new ArrayList<OracleDBSplit>();
    int maxSplits = OracleDBSplitUtil.getMaxSplitsFromProperties(prop);

    try (PreparedStatement pstmt = connection
            .prepareStatement(ROWID_PARTITION_SQL)) {
      pstmt.setInt(1, maxSplits);
      pstmt.setString(2, tableName);
      pstmt.setString(3, tableName);

      long scn = OracleDBConnectionCacheUtil.getCurrentOracleSCN(connection);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          splitList.add(new OracleDBRowIDSplit(rs.getString(2), rs.getString(3),
                  scn, tableName, inputConditions, shardURL));
        }
      } catch (SQLException se) {
        // Check if the exception is due to dba_extents table not found (error
        // code 942). If so, show warning and proceed with single splitter.
        if (se.getErrorCode() == 942) {
          LOG.error("The Oracle user specified in table properties cannot "
                  + "access SYS.DBA_EXTENTS. Proceeding with SINGLE_SPLITTER "
                  + "instead of BLOCK_SPLITTER. Rerun the query after granting "
                  + "select privilege to Oracle user to use BLOCK_SPLITTER");
          return new OracleDBSplit[]{new OracleDBSplit(scn,
            tableName, inputConditions, shardURL)};
        } else {
          throw new SQLException(se.getMessage(), se);
        }

      }

    }
    OracleDBSplit[] returnValue = new OracleDBSplit[splitList.size()];
    splitList.toArray(returnValue);
    return returnValue;
  }

}
