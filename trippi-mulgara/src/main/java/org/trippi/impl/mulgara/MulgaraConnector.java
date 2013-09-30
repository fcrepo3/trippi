package org.trippi.impl.mulgara;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jrdf.graph.GraphElementFactory;
import org.trippi.AliasManager;
import org.trippi.TriplestoreConnector;
import org.trippi.TriplestoreReader;
import org.trippi.TriplestoreWriter;
import org.trippi.TrippiException;
import org.trippi.config.ConfigUtils;
import org.trippi.impl.base.DefaultAliasManager;
import org.trippi.impl.base.ConcurrentTriplestoreReader;
import org.trippi.impl.base.ConcurrentTriplestoreWriter;
import org.trippi.impl.base.ConfigurableSessionPool;
import org.trippi.impl.base.MemUpdateBuffer;
import org.trippi.impl.base.SynchronizedTriplestoreReader;
import org.trippi.impl.base.SynchronizedTriplestoreSession;
import org.trippi.impl.base.SynchronizedTriplestoreWriter;
import org.trippi.impl.base.TriplestoreSession;
import org.trippi.impl.base.TriplestoreSessionFactory;
import org.trippi.impl.base.TriplestoreSessionPool;
import org.trippi.impl.base.UpdateBuffer;
import org.trippi.io.TripleIteratorFactory;

public class MulgaraConnector extends TriplestoreConnector {
	private static final Logger logger =
        LoggerFactory.getLogger(MulgaraConnector.class.getName());

	private Map<String,String> m_config;
    private TriplestoreReader m_reader;
    private TriplestoreWriter m_writer;
    private GraphElementFactory m_elementFactory;
    
    // where writes will occur, if this connector is writable
    private TriplestoreSession m_updateSession = null;
    
    private TriplestoreSessionFactory m_sessionFactory = null;
    
    private TripleIteratorFactory m_iteratorFactory = null;
    
    private boolean m_isClosed = false;
    private boolean m_synch = false;
    
	/**
	 * @see org.trippi.TriplestoreConnector#close() 
	 */
	@Override
	public void close() throws TrippiException {
		if (!m_isClosed) {
            logger.info("Connector closing...");
            try {
                if (m_updateSession != null) {
                    m_updateSession.close();
                }
            } finally {
                if (m_reader != null) {
                    m_reader.close();  // ensure this closes even if above dies
                }
                if (m_synch) m_sessionFactory.close();
            }
            m_isClosed = true;
        }
	}

	/**
	 * @see org.trippi.TriplestoreConnector#getElementFactory() 
	 */
	@Override
	public GraphElementFactory getElementFactory() {
		return m_elementFactory;
	}

	/**
	 * @see org.trippi.TriplestoreConnector#getReader() 
	 */
	@Override
	public TriplestoreReader getReader() {
		if (m_reader == null){
			try{
				open();
			}
			catch(TrippiException e){
				logger.error(e.toString(),e);
			}
		}
		return m_reader;
	}

	/**
	 * @see org.trippi.TriplestoreConnector#getWriter() 
	 */
	@Override
	public TriplestoreWriter getWriter() {
		if (m_reader == null){
			try{
				open();
			}
			catch(TrippiException e){
				logger.error(e.toString(),e);
			}
		}
		if (m_writer == null) {
            throw new UnsupportedOperationException(
                    "This MulgaraConnector is read-only!");
        } else {
            return m_writer;
        }
	}

	/**
	 * @see org.trippi.TriplestoreConnector#init(Map) 
	 */
	@Deprecated
	@Override
	public void init(Map<String, String> config) throws TrippiException {
		setConfiguration(config);
	}

	/**
	 * @see org.trippi.TriplestoreConnector#setConfiguration(Map) 
	 */
	@Override
	public void setConfiguration(Map<String, String> config) throws TrippiException {
		Map<String,String> validated = new HashMap<String,String>(config);
		
		boolean remote = ConfigUtils.getRequiredBoolean(config, "remote");
		validated.put("remote",Boolean.toString(remote));
		
        if (remote) {
        	validated.put("host",ConfigUtils.getRequired(config, "host"));
            
            String port = config.get("port");
            int portNumber = (port == null) ? 1099 : Integer.valueOf(port).intValue();
            validated.put("port", Integer.toString(portNumber));
        } else {
        	validated.put("path",ConfigUtils.getRequired(config, "path"));
        }
        // default RMI port is 1099
        validated.put("serverName",ConfigUtils.getRequired(config, "serverName"));
        validated.put("autoFlushDormantSeconds", Integer.toString(ConfigUtils.getRequiredNNInt(config, "autoFlushDormantSeconds")));

        String modelName = ConfigUtils.getRequired(config, "modelName");
        validated.put("modelName", modelName);
        
        int autoFlushBufferSize = ConfigUtils.getRequiredPosInt(config, "autoFlushBufferSize");
        validated.put("autoFlushBufferSize", Integer.toString(autoFlushBufferSize));
        
        int bufferSafeCapacity = ConfigUtils.getRequiredInt(config, "bufferSafeCapacity");
        if (bufferSafeCapacity < autoFlushBufferSize + 1) {
            throw new TrippiException("bufferSafeCapacity must be less than or equal to autoFlushBufferSize.");
        }
        validated.put("bufferSafeCapacity",Integer.toString(bufferSafeCapacity));
        

        int bufferFlushBatchSize = ConfigUtils.getRequiredPosInt(config, "bufferFlushBatchSize");
        if (bufferFlushBatchSize > autoFlushBufferSize) {
            throw new TrippiException("bufferFlushBatchSize must be less than or equal to autoFlushBufferSize.");
        }
        validated.put("bufferFlushBatchSize", Integer.toString(bufferFlushBatchSize));

        int poolInitialSize = ConfigUtils.getRequiredInt(config, "poolInitialSize");
        if (poolInitialSize > 0) {
            int poolMaxGrowth = ConfigUtils.getRequiredInt(config, "poolMaxGrowth");
            validated.put("poolMaxGrowth", Integer.toString(poolMaxGrowth));
            String temp = config.get("poolSpareSessions");
            int poolSpareSessions = (temp == null) ? 0 : Integer.parseInt(temp);
            validated.put("poolSpareSessions", Integer.toString(poolSpareSessions));
        }
        validated.put("poolInitialSize", Integer.toString(poolInitialSize));
        
        boolean readOnly = ConfigUtils.getRequiredBoolean(config, "readOnly");
        validated.put("readOnly", Boolean.toString(readOnly));
        
        boolean autoCreate = false, autoTextIndex = false;

        if (!readOnly) {
        	autoCreate = ConfigUtils.getRequiredBoolean(config, "autoCreate");
            
            autoTextIndex = ConfigUtils.getRequiredBoolean(config, "autoTextIndex");
            
            if (autoTextIndex) {
              validated.put("textModelName", modelName + "-fullText");
            }
        }
        validated.put("autoCreate", Boolean.toString(autoCreate));
        validated.put("autoTextIndex", Boolean.toString(autoTextIndex));
        
        m_config = validated;
	}
	
	@Override
	public void setTripleIteratorFactory(TripleIteratorFactory factory) {
	    this.m_iteratorFactory = factory;
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
    	
    	if (m_iteratorFactory == null) {
    	    m_iteratorFactory = TripleIteratorFactory.defaultInstance();
    	}
    	
		AliasManager aliasManager = new DefaultAliasManager(new HashMap<String, String>());

		boolean readOnly = Boolean.valueOf(m_config.get("readOnly"));
        //Mulgara location properties
		boolean remote = Boolean.valueOf(m_config.get("remote"));
        String serverName = m_config.get("serverName");
        String modelName = m_config.get("modelName");
        String textModelName = m_config.get("textModelName"); // will be null when autoTextIndex == false
        
        // connection pool configuration
        int poolInitialSize = Integer.parseInt(m_config.get("poolInitialSize"));
        int poolMaxGrowth = Integer.parseInt(m_config.get("poolMaxGrowth"));
        int poolSpareSessions = Integer.parseInt(m_config.get("poolSpareSessions"));
        boolean autoCreate = Boolean.valueOf(m_config.get("autoCreate"));
        
        // buffer configuration
        int autoFlushBufferSize = Integer.parseInt(m_config.get("autoFlushBufferSize"));
        int bufferFlushBatchSize = Integer.parseInt(m_config.get("bufferFlushBatchSize"));
        int bufferSafeCapacity = Integer.parseInt(m_config.get("bufferSafeCapacity"));
        int autoFlushDormantSeconds = Integer.parseInt(m_config.get("autoFlushDormantSeconds"));
        
        if (remote) {
            String host = m_config.get("host");
            int portNumber = Integer.parseInt(m_config.get("port"));
            m_sessionFactory =  new MulgaraSessionFactory(serverName,
												modelName,
												textModelName,
												aliasManager,
												autoCreate,
												host,
            									portNumber);
        } else {
            String path = m_config.get("path");
            m_sessionFactory =  new MulgaraSessionFactory(serverName,
                                                  modelName,
                                                  textModelName,
                                                  aliasManager,
                                                  autoCreate,
                                                  path);
        }
        
        if (poolInitialSize == 0) {
            m_synch = true;
            MulgaraSession mSession = (MulgaraSession) m_sessionFactory.newSession();
            m_elementFactory = mSession.getElementFactory();
            SynchronizedTriplestoreSession synchSession = new SynchronizedTriplestoreSession(mSession);
            if (readOnly) {
                m_reader = new SynchronizedTriplestoreReader(synchSession, aliasManager);
            } else {
                m_writer = new SynchronizedTriplestoreWriter(synchSession,
                                                             aliasManager,
                                                             m_iteratorFactory,
                                                             15000);
                m_reader = m_writer;
            }
        } else {
            TriplestoreSessionPool pool = 
                    new ConfigurableSessionPool(m_sessionFactory,
                                                poolInitialSize,
                                                poolMaxGrowth,
                                                poolSpareSessions);
            
            MulgaraSession updateSession = (MulgaraSession) m_sessionFactory.newSession();
            m_elementFactory = updateSession.getElementFactory();
            
            if (readOnly) {
                m_reader = new ConcurrentTriplestoreReader(pool, aliasManager);
            } else {
                UpdateBuffer buffer = null;
                buffer = new MemUpdateBuffer(bufferSafeCapacity,
                                             bufferFlushBatchSize);
                m_updateSession = updateSession;
                try {
					m_writer = new ConcurrentTriplestoreWriter(pool,
					                                           aliasManager,
					                                           m_updateSession,
					                                           buffer,
					                                           m_iteratorFactory,
					                                           autoFlushBufferSize,
					                                           autoFlushDormantSeconds);
				} catch (IOException e) {
					throw new TrippiException(e.getMessage(), e);
				}
                m_reader = m_writer;
            }
        }
	}
	
	protected TriplestoreSessionFactory getSessionFactory() {
	    return m_sessionFactory;
	}

}
