package org.trippi.impl.mpt;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;

import org.jrdf.graph.GraphElementFactory;

import org.trippi.RDFUtil;
import org.trippi.TriplestoreConnector;
import org.trippi.TriplestoreReader;
import org.trippi.TriplestoreWriter;
import org.trippi.TrippiException;

import org.trippi.impl.base.AliasManager;
import org.trippi.impl.base.ConcurrentTriplestoreWriter;
import org.trippi.impl.base.ConfigurableSessionPool;
import org.trippi.impl.base.MemUpdateBuffer;
import org.trippi.impl.base.TriplestoreSession;
import org.trippi.impl.base.TriplestoreSessionPool;
import org.trippi.impl.base.UpdateBuffer;

import org.nsdl.mptstore.core.BasicTableManager;
import org.nsdl.mptstore.core.DatabaseAdaptor;
import org.nsdl.mptstore.core.GenericDatabaseAdaptor;
import org.nsdl.mptstore.core.DDLGenerator;
import org.nsdl.mptstore.core.TableManager;

public class MPTConnector extends TriplestoreConnector {

    private GraphElementFactory _elementFactory = new RDFUtil();

    private TriplestoreSession _updateSession;

    private TriplestoreWriter _writer;

    public MPTConnector() {
    }

    // Implements TriplestoreConnector.init(Map)
    public void init(Map config) throws TrippiException {

        // get and validate configuration values
        String ddlGenerator = getRequired(config, "ddlGenerator");
        String jdbcDriver = getRequired(config, "jdbcDriver");
        String jdbcURL = getRequired(config, "jdbcURL");
        String username = getRequired(config, "username");
        String password = getRequired(config, "password");
        int poolInitialSize = getRequiredInt(config, "poolInitialSize");
        int poolMaxSize = getRequiredInt(config, "poolMaxSize");
        if (poolMaxSize < poolInitialSize) {
            throw new TrippiException("poolMaxSize cannot be less than poolInitialSize");
        }
        int fetchSize = getRequiredInt(config, "fetchSize");
        boolean backslashIsEscape = getRequiredBoolean(config, "backslashIsEscape");
        int autoFlushDormantSeconds = getRequiredInt(config, "autoFlushDormantSeconds");
        int autoFlushBufferSize = getRequiredInt(config, "autoFlushBufferSize");
        int bufferSafeCapacity = getRequiredInt(config, "bufferSafeCapacity");
        int bufferFlushBatchSize = getRequiredInt(config, "bufferFlushBatchSize");

        // bring everything together, ultimately constructing our
        // ConcurrentTriplestoreWriter
        try {

            // construct the MPTSessionFactory
            BasicDataSource dbPool = getDBPool(jdbcDriver, jdbcURL, username, 
                                               password, poolMaxSize);
            DDLGenerator dbDDLGenerator = (DDLGenerator)
                   Class.forName(ddlGenerator).newInstance();

            TableManager tableManager = new BasicTableManager(dbPool,
                                                              dbDDLGenerator,
                                                              "tMap",
                                                              "t");
            DatabaseAdaptor dbAdaptor = new GenericDatabaseAdaptor(tableManager,
                                                                   backslashIsEscape);

            MPTSessionFactory sessionFactory = 
                    new MPTSessionFactory(dbPool, dbAdaptor, fetchSize);

            // construct the _updateSession, which is managed outside the pool
            _updateSession = sessionFactory.newSession();

            // construct the TriplestoreSessionPool
            TriplestoreSessionPool sessionPool =
                    new ConfigurableSessionPool(sessionFactory,
                                                poolInitialSize,
                                                poolMaxSize,
                                                0); // no spare sessions

            // construct the UpdateBuffer
            UpdateBuffer updateBuffer = new MemUpdateBuffer(bufferSafeCapacity,
                                                            bufferFlushBatchSize);

            // construct the TriplestoreWriter
            _writer = new ConcurrentTriplestoreWriter(sessionPool,
                                                      new AliasManager(new HashMap()),
                                                      _updateSession,
                                                      updateBuffer,
                                                      autoFlushBufferSize,
                                                      autoFlushDormantSeconds);

                                                     
        } catch (Exception e) {
            throw new TrippiException("Error initializing MPTConnector", e);
        }
    }

    private static BasicDataSource getDBPool(String driver,
                                             String url,
                                             String user,
                                             String pass,
                                             int poolMaxSize) throws Exception {

        Properties dbProperties = new Properties();
        dbProperties.setProperty("url", url);
        dbProperties.setProperty("username", user);
        dbProperties.setProperty("password", pass);
        dbProperties.setProperty("maxActive", "" + poolMaxSize);

        Class.forName(driver);

        BasicDataSource pool = (BasicDataSource)
                BasicDataSourceFactory.createDataSource(dbProperties);

        pool.setDriverClassName(driver);

        return pool;
    }

    /**
     * Get a trimmed non-empty string from the map, or throw an exception.
     */
    private static String getRequired(Map map, String key) throws TrippiException {
        String value = (String) map.get(key);
        if (value == null || value.length() == 0) {
            throw new TrippiException("Missing required configuration value: " + key);
        } else {
            return value.trim();
        }
    }

    /**
     * Get a positive integer from the map, or throw an exception.
     */
    private static int getRequiredInt(Map map, String key) throws TrippiException {
        try {
            int i = Integer.parseInt(getRequired(map, key));
            if (i < 1) {
                throw new TrippiException("Configuration value must be 1 or greater: " + key);
            } else {
                return i;
            }
        } catch (NumberFormatException e) {
            throw new TrippiException("Configuration value must be an integer: " + key);
        }
    }

    private static boolean getRequiredBoolean(Map map, String key) throws TrippiException {
        String val = getRequired(map, key);
        if (val.equalsIgnoreCase("true")) {
            return true;
        } else if (val.equalsIgnoreCase("false")) {
            return false;
        } else {
            throw new TrippiException("Expected boolean for " + key + ", but got " + val);
        }
    }

    // Implements TriplestoreConnector.getReader()
    public TriplestoreReader getReader() {
        return _writer;
    }

    // Implements TriplestoreConnector.getWriter()
    public TriplestoreWriter getWriter() {
        return _writer;
    }

    // Implements TriplestoreConnector.getElementFactory()
    public GraphElementFactory getElementFactory() {
        return _elementFactory;
    }

    // Implements TriplestoreConnector.close()
    public void close() throws TrippiException {
        if (_writer != null) {
            _writer.close(); // flushes and closes update buffer,
                             // then closes session pool,
                             // which closes session factory,
                             // which closes underlying db connections
                             // and db connection pool
            _updateSession.close(); // ensure the update session is also
                                    // closed, as it is not part of the session pool
            _writer = null;
        }
    }

}
