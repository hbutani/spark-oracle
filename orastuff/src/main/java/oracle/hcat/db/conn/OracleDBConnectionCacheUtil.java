/* $Header: hadoop/jsrc/oracle/hcat/db/conn/OracleDBConnectionCacheUtil.java /main/1 2018/03/05 08:59:31 ratiwary Exp $ */

/* Copyright (c) 2018, Oracle and/or its affiliates. All rights reserved.*/

 /*
   DESCRIPTION
    <short description of component this file declares/defines>

   PRIVATE CLASSES
    <list of private classes defined - with one-line descriptions>

   NOTES
    <other useful comments, qualifications, etc.>

   MODIFIED    (MM/DD/YY)
    ratiwary    02/28/18 - Creation
 */
/**
 * @version $Header: hadoop/jsrc/oracle/hcat/db/conn/OracleDBConnectionCacheUtil.java /main/1 2018/03/05 08:59:31 ratiwary Exp $
 * @author ratiwary
 * @since release specific (what release of product did this appear in)
 */
package oracle.hcat.db.conn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OracleDBConnectionCacheUtil {

    private static final Logger LOG
            = LoggerFactory.getLogger(OracleDBConnectionCacheUtil.class);

    static final String[] connectionPropertyNames = {
            OracleDBConnectionCache.JAVA_SECURITY_KRB5_CONF,
            OracleDBConnectionCache.KERB_AUTH_CALLBACK,
            OracleDBConnectionCache.ORACLE_NET_TNS_ADMIN,
            OracleDBConnectionCache.SUN_SECURITY_KRB5_PRINCIPAL
    };

    /**
     * Adds all Oracle properties in {@code userProps} to {@code props}
     *
     * @param props
     * @param userProps
     */
    public static void addAllOracleProperties(Properties props,
                                              Map<String, String> userProps) {
        for (Map.Entry<String, String> entry : userProps.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("oracle.jdbc.") || key.startsWith("oracle.net.")) {
                props.put(key, entry.getValue());
                // do stuff with entry
            }
        }
        String fetchSize = userProps.get(OracleDBConnectionCache.ORACLE_FETCH_SIZE);
        if (fetchSize != null) {
            LOG.debug("Fetch size for JDBC connection is " + fetchSize);
            props.put("defaultRowPrefetch", fetchSize);
        }
    }

    /**
     * Add all connection properties in {@code userProps} to {@code props}
     *
     * @param props
     * @param userProps
     */
    public static void addAllConnectionProperties(Properties props,
                                                  Map<String, String> userProps) {
        for (String p : connectionPropertyNames) {
            String v = userProps.get(p);
            if (v != null) {
                props.setProperty(p, v);
            }
        }
    }

    /**
     * Sanitize table and column names to prevent SQL Injection
     *
     * @param connection
     * @param tableName
     * @param columnNames
     * @return
     * @throws SQLException
     */
    public static void sanitizeTableAndColumnNames(String tableName,
                                                   String[] columnNames, Connection conn) throws SQLException {
        StringBuilder query = new StringBuilder();

        // Note that both simple_sql_name and sql_object_name functions allow
        // quoted names as input
        query.append("SELECT /*+ no_parallel */ ");
        // Now add Field names
        if (columnNames != null) {
            for (int i = 0; i < columnNames.length; i++) {
                query.append("sys.dbms_assert.simple_sql_name(?)");
                if (i != columnNames.length) {
                    query.append(", ");
                }
            }
        }
        query.append(" sys.dbms_assert.sql_object_name(?)").append(" from dual");

        LOG.debug("Executing " + query + " to verify input parameters");

        try (PreparedStatement pstmt = conn.prepareStatement(query.toString())) {
            int i = 0;
            if (columnNames != null) {
                for (i = 0; i < columnNames.length; i++) {
                    LOG.debug("Validating column name: " + columnNames[i]);
                    pstmt.setString(i + 1, columnNames[i]);
                }
            }
            pstmt.setString(i + 1, tableName);
            LOG.debug("Validating table name: " + tableName);
            try (ResultSet rs = pstmt.executeQuery()) {
            } catch (SQLException se) {
                LOG.debug("Exception in validating table and column names", se);
                throw new SQLException("Invalid table parameters in query", se);
            }
        } catch (SQLException se) {
            LOG.debug("Exception in validating table and column names", se);
            throw new SQLException("Invalid table parameters in query", se);
        }
    }

    /**
     * Returns current SCN from OracleConnection
     *
     * @param conn
     * @return
     * @throws SQLException
     */
    public static long getCurrentOracleSCN(Connection conn) throws SQLException {
        oracle.jdbc.internal.OracleConnection internalConnection = conn
                .unwrap(oracle.jdbc.internal.OracleConnection.class);
        return internalConnection.getCurrentSCN();
    }

    // Quoted identifiers can contain any characters and punctuation marks as
    // well as spaces. However, neither quoted nor non-quoted identifiers can
    // contain double quotation marks or the null character (\0).
    public static final String QUOTED_ID = "\"[^\"\0]+\"";

    // Non-quoted identifiers can contain only alphanumeric characters from your
    // database character set and the underscore (_), dollar sign ($), and pound
    // sign (#). It cannot contain a double quote.
    public static final String NONQUOTED_ID = "[a-zA-Z][\\w$#&&[^\"\0]]*";

    public static final String DELIMITED_IDENTIFER_PATTERN_STRING
            = "(" + QUOTED_ID
            + "|" + NONQUOTED_ID + ")";
    public static final Pattern DELIMITED_IDENTIFIER_PATTERN = Pattern.
            compile(DELIMITED_IDENTIFER_PATTERN_STRING);

    /**
     * Return the components of the schema qualified table name
     *
     * @param tableName optionally schema qualified table name to be split into
     * schema and table name components
     *
     * @throws IllegalArgumentException if table name is null or is invalid
     * @throws ParseException if table name is invalid
     *
     */
    public static String[] splitTableName(String tableName)
            throws ParseException {
        String[] names = new String[2];
        // parseSQLIdentifiers returns null for null or empty input
        List<String> values = parseSQLIdentifiers(tableName, '.');
        if (values == null) {
            throw new IllegalArgumentException("table name is null or empty");
        }
        final int cnt = values.size();
        if (cnt > 2) {
            throw new ParseException("invalid table name", 0);
        }
        names[1] = values.get(cnt - 1); // table is last and required
        if (cnt == 2) {
            names[0] = values.get(0); // schema is optional
        }
        return names;
    }

    /**
     * Parse a list of SQL identifiers from a delimited list. Identifiers may be
     * quoted or not.
     *
     * <p>
     * Quoted identifiers can contain any characters and punctuations marks as
     * well as spaces. However, neither quoted nor nonquoted identifiers can
     * contain double quotation marks or the null character (\0).
     *
     * <p>
     * Non-quoted identifiers can contain only alphanumeric characters from your
     * database character set and the underscore (_), dollar sign ($), and pound
     * sign (#). It cannot contain a double quote.
     *
     * <p>
     * Null or empty input returns null.
     *
     * @param input a delimited list of SQL identifiers. Identifier
     * @param delimiter The delimiter character, if
     * @return a list of identifiers
     * @throws ParseException for non-conforming input
     */
    public static List<String> parseSQLIdentifiers(String input, char delimiter)
            throws ParseException {
        final String ERRMSG = "Invalid SQL name: ";
        if (input == null) {
            return null;
        }

        final int len = input.length();
        if (len == 0) {
            return null;
        }

        Matcher m = DELIMITED_IDENTIFIER_PATTERN.matcher(input);
        List<String> ids = new ArrayList<String>();

        int start = 0;
        while (m.find(start)) {
            // check that match started at beginning of region
            if (m.start() != start) {
                throw new ParseException(ERRMSG + input, m.start());
            }

            ids.add(m.group(1)); // save the match

            int end = m.end();
            if (end == len) // match ran to end of input
            {
                return ids;
            }

            // stopping char should be delimiter
            if (input.charAt(end) != delimiter) {
                throw new ParseException(ERRMSG + input, end);
            }

            start = end + 1; // start of next region to match
        }
        throw new ParseException(ERRMSG + input, start);
    }

    /**
     * Return true if the first and last characters of the string are the quoting
     * character
     *
     * @param value the string to test
     * @param quote the quoting character
     * @return true if the string is enclosed by the quoting character
     */
    public static boolean isQuoted(String value, char quote) {
        final int len = (value != null) ? value.length() : 0;
        return ((len > 1) && (value.charAt(0) == quote)
                && (value.charAt(len - 1) == quote));
    }

    /**
     * Normalize a SQL name. If the name is enclosed by " then strip the quotes
     * and return the name unchanged. If it is not quoted then convert the name to
     * uppercase.
     *
     * @param name the SQL name to normalize
     * @return the normalized name
     */
    public static String normalizeSQLName(String name) {
        if (isQuoted(name, '"')) {
            return name.substring(1, name.length() - 1);
        }
        return name != null ? name.toUpperCase() : name;
    }

    /**
     * Returns number of rows in {@code tableName}
     *
     * @param connection
     * @param tableName
     * @return
     * @throws SQLException
     */
    public static long getRowCount(Connection connection, String tableName)
            throws SQLException {
        try (PreparedStatement pstmt = connection
                .prepareStatement("SELECT /*+ no_parallel */ COUNT(*) FROM "
                        + tableName)) {
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        }
        return -1;
    }

}
