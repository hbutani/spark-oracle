/* $Header: hadoop/jsrc/oracle/hcat/db/conn/OracleDBConnectionCache.java ratiwary_bug-27617356/3 2018/03/14 14:10:45 ratiwary Exp $ */

/* Copyright (c) 2018, Oracle and/or its affiliates. All rights reserved.*/

/*
   DESCRIPTION
    <short description of component this file declares/defines>

   PRIVATE CLASSES
    <list of private classes defined - with one-line descriptions>

   NOTES
    <other useful comments, qualifications, etc.>

   MODIFIED    (MM/DD/YY)
    ratiwary    02/12/18 - Create Connection Util for Database using methods in
                           OracleStorageHandlerUtil
    ratiwary    02/12/18 - Creation
 */

/**
 * @version $Header: hadoop/jsrc/oracle/hcat/db/conn/OracleDBConnectionCache.java ratiwary_bug-27617356/3 2018/03/14 14:10:45 ratiwary Exp $
 * @author ratiwary
 * @since release specific (what release of product did this appear in)
 */

package oracle.hcat.db.conn;

import com.sun.security.auth.module.Krb5LoginModule;
import oracle.jdbc.OracleConnection;
import oracle.net.ano.AnoServices;
import oracle.ucp.UniversalConnectionPoolException;
import oracle.ucp.admin.UniversalConnectionPoolManager;
import oracle.ucp.admin.UniversalConnectionPoolManagerImpl;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 *
 * @author ratiwary
 */
public class OracleDBConnectionCache {

    private static final Logger LOG = LoggerFactory.getLogger(OracleDBConnectionCache.class);

    private static PoolDataSource pds;
    private static Subject kerbSubject;
    private static UniversalConnectionPoolManager ucpManager;

    /**
     * Path for kerberos configuration
     */
    public static final String JAVA_SECURITY_KRB5_CONF
            = "java.security.krb5.conf";

    /**
     * Kerberos principal
     */
    public static final String SUN_SECURITY_KRB5_PRINCIPAL
            = "sun.security.krb5.principal";

    /**
     * Callback for kerberos authentication
     */
    public static final String KERB_AUTH_CALLBACK
            = "oracle.hcat.osh.kerb.callback";

    /**
     * TNS_ADMIN location required by wallet
     */
    public static final String ORACLE_NET_TNS_ADMIN
            = "oracle.net.tns_admin";

    /**
     * Authentication method used to connect to Oracle Database
     */
    public static final String ORACLE_AUTH_METHOD
            = "oracle.hcat.osh.authentication";

    /**
     * Fetch size for Oracle JDBC connections
     */
    public static final String ORACLE_FETCH_SIZE =
            "oracle.hcat.osh.fetchSize";

    /**
     * Methods for Authenticating a user to Oracle Database
     */
    public static enum OracleAuthenticationMethod {
        SIMPLE,
        ORACLE_WALLET,
        KERBEROS
    }

    static final String[] connectionPropertyNames = {
            JAVA_SECURITY_KRB5_CONF,
            KERB_AUTH_CALLBACK,
            ORACLE_NET_TNS_ADMIN,
            SUN_SECURITY_KRB5_PRINCIPAL
    };

    /**
     * A thread local connection obtained from JDBC Pool Data Source.
     */
    /**
     * A thread local connection obtained from JDBC Pool Data Source.
     */
    private static final ThreadLocal<Connection> connectionHolder
            = new ThreadLocal<Connection>() {
        @Override
        protected Connection initialValue() {
            try {
                LOG.debug("Getting a new connection from pool");
                if (kerbSubject == null) {
                    return pds.getConnection();
                } else {
                    Connection conn = null;
                    try {
                        conn = (Connection) Subject.doAs(kerbSubject, new PrivilegedExceptionAction() {
                            @Override
                            public Object run() throws SQLException {
                                return pds.getConnection();
                            }
                        });
                    } catch (PrivilegedActionException ex) {
                        throw new SQLException("Insufficient priviliges", ex);
                    }
                    return conn;
                }
            } catch (SQLException ex) {
                LOG.error("Exception during connection initialization."
                        + " Null connection returned", ex);
                // Can't throw checked exception in static initializer
                throw new Error("Error in obtaining connection", ex);
            }
        }
    };

    /**
     * Returns a new thread local cached connection from JDBC Pool Data source.
     * @param authMethodString
     * @param props
     * @param url
     * @param createPool Creates a new pooldatasource when true. Tries to return
     * a connection from the existing pool when false
     * @return
     * @throws SQLException
     */
    public static synchronized Connection getCachedConnection(String authMethodString,
                                                              Properties props, String url, boolean createPool)
            throws SQLException {
        LOG.debug("Getting Cached Connection with createPool = " + createPool);
        if (pds == null || createPool == true) {
            LOG.debug("Creating new Pool Data Source");
            pds = getNewPDS(authMethodString, props, url);
        }
        if (ucpManager == null) {
            try {
                ucpManager = UniversalConnectionPoolManagerImpl.getUniversalConnectionPoolManager();
            } catch (UniversalConnectionPoolException ex) {
                throw new SQLException("Can't create UCP Manager", ex);
            }
        }
        Connection conn = connectionHolder.get();
        // Hive closes this connection after fetching the rows from a request. We
        // need to remove the Threadlocal connection here and initialize again by
        // calling get().
        if (conn != null && conn.isClosed()) {
            // If this connection is closed in org.apache.hadoop.hive.ql.exec.FetchOperator#getNextRow()
            // by calling RecordReader.close(). Then remove and initialize again.
            LOG.debug("Cached Connection is closed");
            connectionHolder.remove();
            conn = connectionHolder.get();
        }
        return conn;
    }


    /**
     * Return a new JDBC Pool Data source using properties
     * @param authMethodString
     * @param props
     * @param url
     * @return
     * @throws SQLException
     */
    public static PoolDataSource getNewPDS(String authMethodString, Properties props, String url)
            throws SQLException {
        if (authMethodString == null) {
            authMethodString = OracleDBConnectionCache.OracleAuthenticationMethod.SIMPLE.toString();
        }
        OracleDBConnectionCache.OracleAuthenticationMethod authMethod
                = OracleDBConnectionCache.OracleAuthenticationMethod.
                valueOf(authMethodString);
        if (authMethod != null) switch (authMethod) {
            case SIMPLE:
                return getRegularPDS(props, url);
            case KERBEROS:
                return getKerberosPDS(props, url);
            case ORACLE_WALLET:
                return getWalletPDS(props, url);
            default:
                throw new SQLException("Invalid Authentication Method");
        }
        else {
            return getRegularPDS(props, url);
        }
    }

    /**
     * Returns a JDBC Pool Data Source authenticated using password. Note that
     * this method should not be used in production code as the password is stored
     * in plain text
     * @param props
     * @param url
     * @return
     * @throws SQLException
     */
    private static PoolDataSource getRegularPDS(Properties props, String url)
            throws SQLException {

        PoolDataSource regularPDS = PoolDataSourceFactory.getPoolDataSource();
        regularPDS.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
        regularPDS.setURL(url);
        regularPDS.setMaxPoolSize(1);
        regularPDS.setConnectionProperties(props);
        return regularPDS;
    }

    /**
     * Returns a JDBC Pool Data Source for a Wallet connection
     * @param props
     * @param url
     * @return
     * @throws SQLException
     */
    private static PoolDataSource getWalletPDS(Properties props, String url)
            throws SQLException {

        LOG.debug("Attempting Wallet Authentication");
        if (System.getProperty(OracleDBConnectionCache.ORACLE_NET_TNS_ADMIN) == null) {
            LOG.debug("oracle.net.tns_admin not set in environment. Configuration will"
                    + " be used");
            if (props.get(OracleDBConnectionCache.ORACLE_NET_TNS_ADMIN) != null) {
                System.setProperty(OracleDBConnectionCache.ORACLE_NET_TNS_ADMIN,
                        (String) props.get(OracleDBConnectionCache.ORACLE_NET_TNS_ADMIN));
            } else {
                throw new SQLException("TNS_ADMIN not set");
            }
            LOG.debug("oracle.net.tns_admin set to: "
                    + System.getProperty(OracleDBConnectionCache.ORACLE_NET_TNS_ADMIN));
        }

        PoolDataSource walletPDS = PoolDataSourceFactory.getPoolDataSource();
        walletPDS.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
        LOG.debug("Pool url is " + url);
        walletPDS.setURL(url);
        walletPDS.setInitialPoolSize(1);
        walletPDS.setMaxPoolSize(1);
        walletPDS.setMinPoolSize(1);
        walletPDS.setConnectionProperties(props);
        walletPDS.setPassword(null);

        return walletPDS;
    }

    /**
     * Sets Kerberos configuration for a new connection
     * @param krb5_conf
     */
    private static void setKRBConf(String krb5_conf) {
        // TODO: Document that this requires special permission to set the system
        // property
        LOG.info("** krb5_conf:" + krb5_conf);
        System.setProperty(OracleDBConnectionCache.JAVA_SECURITY_KRB5_CONF,
                krb5_conf);
    }

    /**
     * Returns a JDBC Pool Data source for a Kerberos connection
     * @param props
     * @param url
     * @return
     * @throws SQLException
     */
    private static PoolDataSource getKerberosPDS(final Properties props, final String url)
            throws SQLException {

        LOG.debug("Attempting Kerberos Authentication");

        String krb5_conf = props.getProperty(OracleDBConnectionCache.JAVA_SECURITY_KRB5_CONF);
        setKRBConf(krb5_conf);
        props.setProperty(
                OracleConnection.CONNECTION_PROPERTY_THIN_NET_AUTHENTICATION_SERVICES,
                "(" + AnoServices.AUTHENTICATION_KERBEROS5 + ")");
        props.setProperty(
                OracleConnection.CONNECTION_PROPERTY_THIN_NET_AUTHENTICATION_KRB5_MUTUAL,
                Boolean.TRUE.toString());
        String ccName = props
                .getProperty(OracleConnection.CONNECTION_PROPERTY_THIN_NET_AUTHENTICATION_KRB5_CC_NAME);

        LOG.info("** krb5_cc_name:" + ccName);
        if (ccName != null) {
            props.setProperty(
                    OracleConnection.CONNECTION_PROPERTY_THIN_NET_AUTHENTICATION_KRB5_CC_NAME,
                    ccName);
        }

        LOG.info("** url:" + url);
        // Temporary Subject created just for authenticating kerberos principal
        kerbSubject = new Subject();

        Krb5LoginModule krb5Module = new Krb5LoginModule();
        Map<String, Object> sharedState = new HashMap<>();
        Map<String, Object> options = new HashMap<>();
        options.put("doNotPrompt", "false");
        // Credentials will be checked in the ticket cache first.
        // If not found then call back will be used.
        options.put("useTicketCache", "true");

        String principal = props.getProperty(OracleDBConnectionCache.SUN_SECURITY_KRB5_PRINCIPAL);
        if (principal != null) {
            options.put("principal", principal);
        }
        if (ccName != null) {
            options.put("ticketCache", ccName);
        }

        Class callbackClass = null;
        CallbackHandler callbackHandler = null;
        try {
            String callbackClassName = props.getProperty(OracleDBConnectionCache.KERB_AUTH_CALLBACK);
            if (callbackClassName != null) {
                callbackClass = Class.forName(callbackClassName);
            }
        } catch (ClassNotFoundException ex) {
            throw new SQLException("Callback class not found", ex);
        }
        try {
            if (callbackClass != null) {
                callbackHandler = (CallbackHandler) callbackClass.newInstance();
            }
        } catch (InstantiationException | IllegalAccessException ex) {
            throw new SQLException("Can't create instance of Callback class", ex);
        }

        krb5Module.initialize(kerbSubject, callbackHandler, sharedState, options);
        boolean retLogin = false;
        try {
            retLogin = krb5Module.login();
            krb5Module.commit();
        } catch (LoginException ex) {
            throw new SQLException("LoginException while authenticating with kerberos", ex);
        }

        if (!retLogin) {
            throw new SQLException("Kerberos5 adaptor couldn't retrieve credentials "
                    + "(TGT) from the cache");
        }

        // Now we have a valid Subject with Kerberos credentials.
        // execute driver.connect(...) on behalf of the Subject
        PoolDataSource kerbPDS = null;
        try {
            kerbPDS = (PoolDataSource) Subject.doAs(kerbSubject, new PrivilegedExceptionAction() {
                @Override
                public Object run() throws SQLException {
                    PoolDataSource pds = null;
                    try {
                        pds = PoolDataSourceFactory.getPoolDataSource();
                        pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
                        pds.setURL(url);
                        pds.setInitialPoolSize(1);
                        pds.setMaxPoolSize(1);
                        pds.setMinPoolSize(1);
                        pds.setConnectionProperties(props);
                    } catch (SQLException ex) {
                        throw new SQLException("Exception while establishing kerberos "
                                + "connection:", ex);
                    }
                    return pds;
                }
            });
        } catch (PrivilegedActionException ex) {
            throw new SQLException("Insufficient priviliges", ex);
        }
        return kerbPDS;
    }
}


