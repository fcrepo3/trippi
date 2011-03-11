package org.trippi.impl.mpt;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;

import org.jrdf.graph.GraphElementFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trippi.RDFUtil;
import org.trippi.TriplestoreConnector;
import org.trippi.TriplestoreReader;
import org.trippi.TriplestoreWriter;
import org.trippi.TrippiException;

import org.trippi.config.ConfigUtils;

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
	private static Logger logger = LoggerFactory.getLogger(MPTConnector.class.getName());
	
	private Map<String,String> m_config;

    private GraphElementFactory m_elementFactory = new RDFUtil();

    private TriplestoreSession m_updateSession;

    private TriplestoreWriter m_writer;

    public MPTConnector() {
    }
    
    public MPTConnector(Map<String,String> config)
        throws TrippiException {
    	setConfiguration(config);
    }

    /**
     * @deprecated
     * @see org.trippi.TriplestoreConnector#init(Map)
     */
    @Deprecated
    @Override
    public void init(Map<String,String> config) throws TrippiException {
    	setConfiguration(config);
    }
    
    /**
     * @see org.trippi.TriplestoreConnector#setConfiguration(Map)
     */
    @Override
	public void setConfiguration(Map<String, String> config) throws TrippiException {

        // validate and store configuration values
    	Map<String,String> validated = new HashMap<String,String>(config);
        validated.put("ddlGenerator", ConfigUtils.getRequired(config, "ddlGenerator"));
        validated.put("jdbcDriver", ConfigUtils.getRequired(config, "jdbcDriver"));
        validated.put("jdbcURL", ConfigUtils.getRequired(config, "jdbcURL"));
        validated.put("username", ConfigUtils.getRequired(config, "username"));
        validated.put("password", ConfigUtils.getRequired(config, "password"));
        
        int poolInitialSize = ConfigUtils.getRequiredInt(config, "poolInitialSize");
        int poolMaxSize = ConfigUtils.getRequiredInt(config, "poolMaxSize");
        if (poolMaxSize < poolInitialSize) {
            throw new TrippiException("poolMaxSize cannot be less than poolInitialSize");
        }
        validated.put("poolInitialSize", Integer.toString(poolInitialSize));
        validated.put("poolMaxSize", Integer.toString(poolMaxSize));
        validated.put("fetchSize", Integer.toString(ConfigUtils.getRequiredInt(config, "fetchSize")));
        validated.put("backslashIsEscape", Boolean.toString(ConfigUtils.getRequiredBoolean(config, "backslashIsEscape")));
        validated.put("autoFlushDormantSeconds", Integer.toString(ConfigUtils.getRequiredNNInt(config, "autoFlushDormantSeconds")));
        validated.put("autoFlushBufferSize", Integer.toString(ConfigUtils.getRequiredPosInt(config, "autoFlushBufferSize")));
        validated.put("bufferSafeCapacity", Integer.toString(ConfigUtils.getRequiredInt(config, "bufferSafeCapacity")));
        validated.put("bufferFlushBatchSize",Integer.toString(ConfigUtils.getRequiredPosInt(config, "bufferFlushBatchSize")));
        
        m_config = validated;
    }
    
    /**
     * @see org.trippi.TriplestoreConnector#getConfiguration()
     */
    @Override
    public Map<String,String> getConfiguration(){
    	return m_config;
    }
    
    /**
     * @see org.trippi.TriplestoreConnector#open()
     */
    @Override
    public void open() throws TrippiException {
    	if (m_config == null){
    		throw new TrippiException("Cannot open " + getClass().getName() + " without valid configuration");
    	}

    	String ddlGenerator = m_config.get("ddlGenerator");
        String jdbcDriver = m_config.get("jdbcDriver");
        String jdbcURL = m_config.get("jdbcURL");
        String username = m_config.get("username");
        String password = m_config.get("password");
        int poolInitialSize = Integer.parseInt(m_config.get("poolInitialSize"));
        int poolMaxSize = Integer.parseInt(m_config.get("poolMaxSize"));
        int fetchSize = Integer.parseInt(m_config.get("fetchSize"));
        boolean backslashIsEscape = Boolean.valueOf(m_config.get("backslashIsEscape"));
        int autoFlushDormantSeconds = Integer.parseInt(m_config.get("autoFlushDormantSeconds"));
        int autoFlushBufferSize = Integer.parseInt(m_config.get("autoFlushBufferSize"));
        int bufferSafeCapacity = Integer.parseInt(m_config.get("bufferSafeCapacity"));
        int bufferFlushBatchSize = Integer.parseInt(m_config.get("bufferFlushBatchSize"));

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
            m_updateSession = sessionFactory.newSession();

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
            m_writer = new ConcurrentTriplestoreWriter(sessionPool,
                                                      new AliasManager(new HashMap<String, String>()),
                                                      m_updateSession,
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
     * @see org.trippi.TriplestoreConnector#getReader()
     */
    @Override
	public TriplestoreReader getReader() {
    	if (m_writer == null){
    		try{
    			open();
    		}
    		catch (TrippiException e){
    			logger.error(e.toString(),e);
    		}
    	}
		return m_writer;
    }

    /**
     * @see org.trippi.TriplestoreConnector#getWriter()
     */
    @Override
	public TriplestoreWriter getWriter() {
    	if (m_writer == null){
    		try{
    			open();
    		}
    		catch (TrippiException e){
    			logger.error(e.toString(),e);
    		}
    	}
        return m_writer;
    }

    /**
     * @see org.trippi.TriplestoreConnector#getElementFactory()
     */
    @Override
	public GraphElementFactory getElementFactory() {
        return m_elementFactory;
    }

    /**
     * @see org.trippi.TriplestoreConnector#close()
     */
    @Override
	public void close() throws TrippiException {
        if (m_writer != null) {
            m_writer.close(); // flushes and closes update buffer,
                             // then closes session pool,
                             // which closes session factory,
                             // which closes underlying db connections
                             // and db connection pool
            m_updateSession.close(); // ensure the update session is also
                                    // closed, as it is not part of the session pool
            m_writer = null;
        }
    }

}
