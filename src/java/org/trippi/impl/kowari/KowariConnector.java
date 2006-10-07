package org.trippi.impl.kowari;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jrdf.graph.GraphElementFactory;
import org.trippi.RDFUtil;
import org.trippi.TriplestoreConnector;
import org.trippi.TriplestoreReader;
import org.trippi.TriplestoreWriter;
import org.trippi.TrippiException;
import org.trippi.impl.base.*;

/**
 * A <code>TriplestoreConnector</code> for a local or remote Kowari 
 * triplestore.
 *
 * @author cwilper@cs.cornell.edu
 */
public class KowariConnector extends TriplestoreConnector {

    private static final Logger logger =
        Logger.getLogger(KowariConnector.class.getName());

    private TriplestoreReader m_reader;
    private TriplestoreWriter m_writer;
    private boolean m_closed = false;

    private Map m_config;

    private boolean m_synch = false;

    // 
    private GraphElementFactory m_elementFactory;

    // where writes will occur, if this connector is writable
    private TriplestoreSession m_updateSession = null;
    
    private TriplestoreSessionFactory m_factory = null;

    public KowariConnector() {
    }

    // set reader, writer, elementFactory, and updateSession as needed
    public void init(Map config) throws TrippiException {

        AliasManager aliasManager = new AliasManager(new HashMap());
        // step 1: delcare and initialize variables
        String OPTIONAL = null;
        String REQUIRED = "";
        int NO_MAX = -1;

        boolean remote = false;
        String host = null;
        String port = null;
        String path = null;
        String serverName = null;
        String modelName = null;
        int poolInitialSize = 0;
        int poolMaxGrowth = 0;
        int poolSpareSessions = 0;
        boolean readOnly = false;
        boolean autoCreate = false;
        boolean autoTextIndex = false;
        String textModelName = null;
        boolean memoryBuffer = false;
        String dbDriver = null;
        String dbURL = null;
        String dbUsername = null;
        String dbPassword = null;
        String dbTableName = null;
        int autoFlushDormantSeconds = 0;
        int autoFlushBufferSize = 0;
        int bufferSafeCapacity = 0;
        int bufferFlushBatchSize = 0;

        // step 2: assign values while validating parameters
        m_config = config;
        remote = getBoolean("remote", REQUIRED).booleanValue();
        if (remote) {
            host = getString("host", "remote is true");
            port = getString("port", OPTIONAL);
        } else {
            path = getString("path", "remote is false");
        }
        // default RMI port is 1099
        int portNumber = (port == null) ? 1099 : Integer.valueOf(port).intValue();
        serverName = getString("serverName", REQUIRED);
        modelName = getString("modelName", REQUIRED);
        poolInitialSize = getInt("poolInitialSize", REQUIRED, 0, NO_MAX);
        if (poolInitialSize > 0) {
            String pmg = (String) m_config.get("poolMaxGrowth");
            if (pmg != null && pmg.equals("-1")) {
                poolMaxGrowth = -1;
            } else {
                poolMaxGrowth = getInt("poolMaxGrowth", REQUIRED, 0, NO_MAX);
            }
            poolSpareSessions = getInt("poolSpareSessions", OPTIONAL, 0, NO_MAX);
            if (poolSpareSessions == -1) poolSpareSessions = 0;
        }
        readOnly = getBoolean("readOnly", REQUIRED).booleanValue();
        if (!readOnly) {
            String why = "readOnly is false";
            autoCreate = getBoolean("autoCreate", why).booleanValue();
            autoTextIndex = getBoolean("autoTextIndex", why).booleanValue();
            if (autoTextIndex) {
              textModelName = modelName + "-fullText";
            }
            if (poolInitialSize > 0) {
                memoryBuffer = getBoolean("memoryBuffer", why).booleanValue();
                if (!memoryBuffer) {
                    why += " and memoryBuffer is false";
                    dbDriver = getString("dbDriver", why);
                    dbURL = getString("dbURL", why);
                    dbUsername = getString("dbUsername", why);
                    dbPassword = getString("dbPassword", why);
                    dbTableName = getString("dbTableName", why);
                }
                autoFlushDormantSeconds = getInt("autoFlushDormantSeconds", why, 0, NO_MAX);
                autoFlushBufferSize = getInt("autoFlushBufferSize", why, 1, NO_MAX);
                bufferSafeCapacity = getInt("bufferSafeCapacity", why, autoFlushBufferSize + 1, NO_MAX);
                bufferFlushBatchSize = getInt("bufferFlushBatchSize", why, 1, autoFlushBufferSize);
            }
        }

        // step 3: do the actual initialization
        try {
            m_factory = null;
            if (remote) {
                m_factory =  new KowariSessionFactory(host,
                									portNumber,
													serverName,
													modelName,
													textModelName,
													autoCreate,
													aliasManager);
            } else {
                m_factory =  new KowariSessionFactory(path,
                                                      serverName,
                                                      modelName,
                                                      textModelName,
                                                      autoCreate,
                                                      aliasManager);
            }
            if (poolInitialSize == 0) {
                m_synch = true;
                KowariSession kSession = (KowariSession) m_factory.newSession();
                m_elementFactory = kSession.getElementFactory();
                SynchronizedTriplestoreSession synchSession = new SynchronizedTriplestoreSession(kSession);
                if (readOnly) {
                    m_reader = new SynchronizedTriplestoreReader(synchSession, aliasManager);
                } else {
                    m_writer = new SynchronizedTriplestoreWriter(synchSession, aliasManager, 15000);
                    m_reader = m_writer;
                }
            } else {
                TriplestoreSessionPool pool = 
                        new ConfigurableSessionPool(m_factory,
                                                    poolInitialSize,
                                                    poolMaxGrowth,
                                                    poolSpareSessions);
                if (readOnly) {
                    m_reader = new ConcurrentTriplestoreReader(pool, aliasManager);
                    m_elementFactory = new RDFUtil();
                } else {
                    UpdateBuffer buffer = null;
                    if (memoryBuffer) {
                        buffer = new MemUpdateBuffer(bufferSafeCapacity,
                                                     bufferFlushBatchSize);
                    } else {
                        throw new TrippiException("FIXME: DBBuffer not implemented yet.");
                    }
                    KowariSession updateSession = (KowariSession) m_factory.newSession();
                    m_elementFactory = updateSession.getElementFactory();
                    m_updateSession = updateSession;
                    m_writer = new ConcurrentTriplestoreWriter(pool,
                                                               aliasManager,
                                                               m_updateSession,
                                                               buffer,
                                                               autoFlushBufferSize,
                                                               autoFlushDormantSeconds);
                    m_reader = m_writer;
                }
            }
        } catch (Exception e) {
            if (m_updateSession != null) {
                try { m_updateSession.close(); } catch (Exception ez) { }
            }
            if (e instanceof TrippiException) {
                throw (TrippiException) e;
            } else {
                throw new TrippiException("Could not initialize KowariConnector.", e);
            }
        } finally {
            // This is a workaround to avoid flooding the log with useless warnings
            Logger kLog = Logger.getLogger("org.kowari.store.tuples.UnorderedProjection");
            kLog.setLevel(Level.ERROR);
        }
    }


    private String getString(String paramName, 
                             String requiredWhen) throws TrippiException {
        String value = (String) m_config.get(paramName);
        if ( value != null ) return value;
        if (requiredWhen == null) return null;
        StringBuffer msg = new StringBuffer();
        msg.append("Parameter '" + paramName + "' is required");
        if ( !requiredWhen.equals("") ) {
            msg.append(" when " + requiredWhen);
        }
        msg.append('.');
        throw new TrippiException(msg.toString());
    }

    private Boolean getBoolean(String paramName, 
                               String requiredWhen) throws TrippiException {
        String value = getString(paramName, requiredWhen);
        if (value == null) return null;
        if (value.equalsIgnoreCase("true")) {
            return new Boolean(true);
        } else if (value.equalsIgnoreCase("false")) {
            return new Boolean(false);
        } else {
            throw new TrippiException("Parameter '" + paramName + "' must"
                    + " be true or false.");
        }
    }

    // Get a non-negative int
    private int getInt(String paramName, 
                       String requiredWhen, 
                       int min, 
                       int max) throws TrippiException {
        String value = getString(paramName, requiredWhen);
        if (value == null) return -1;
        int intValue;
        try {
            intValue = Integer.parseInt(value);
        } catch (Exception e) {
            throw new TrippiException("Parameter '" + paramName 
                                         + "' must be an integer.");
        }
        if (intValue < 0) throw new TrippiException("Parameter '" + paramName 
                                                  + "' must be non-negative.");
        if (min >= 0) { // enforce min constraint
            if (intValue < min) throw new TrippiException("Parameter '"
                    + paramName + "' must be at least " + min);
        }
        if (max >= 0) { // enforce max constraint
            if (intValue > max) throw new TrippiException("Parameter '"
                    + paramName + "' must be at most " + max);

        }
        return intValue;
    }



    public TriplestoreReader getReader() {
        return m_reader;
    }

    public TriplestoreWriter getWriter() {
        if (m_writer == null) {
            throw new UnsupportedOperationException(
                    "This KowariConnector is read-only!");
        } else {
            return m_writer;
        }
    }

    public GraphElementFactory getElementFactory() {
        return m_elementFactory;
    }

    public void close() throws TrippiException {
        if (!m_closed) {
            logger.info("Connector closing...");
            try {
                if (m_updateSession != null) {
                    m_updateSession.close();
                }
            } finally {
                m_reader.close();  // ensure this closes even if above dies
                if (m_synch) m_factory.close();
            }
            m_closed = true;
        }
    }

}
