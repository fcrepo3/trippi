package org.trippi.impl.mulgara;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jrdf.graph.GraphElementFactory;
import org.trippi.TriplestoreConnector;
import org.trippi.TriplestoreReader;
import org.trippi.TriplestoreWriter;
import org.trippi.TrippiException;
import org.trippi.config.ConfigUtils;
import org.trippi.impl.base.AliasManager;
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

public class MulgaraConnector extends TriplestoreConnector {
	private static final Logger logger =
        Logger.getLogger(MulgaraConnector.class.getName());

    private TriplestoreReader m_reader;
    private TriplestoreWriter m_writer;
    private GraphElementFactory m_elementFactory;
    
    // where writes will occur, if this connector is writable
    private TriplestoreSession m_updateSession = null;
    
    private TriplestoreSessionFactory m_factory = null;
    
    private boolean m_isClosed = false;
    private boolean m_synch = false;
    
	@Override
	public void close() throws TrippiException {
		if (!m_isClosed) {
            logger.info("Connector closing...");
            try {
                if (m_updateSession != null) {
                    m_updateSession.close();
                }
            } finally {
                if (m_reader != null) m_reader.close();  // ensure this closes even if above dies
                //if (m_synch) m_factory.close();
            }
            m_isClosed = true;
        }
	}

	@Override
	public GraphElementFactory getElementFactory() {
		return m_elementFactory;
	}

	@Override
	public TriplestoreReader getReader() {
		return m_reader;
	}

	@Override
	public TriplestoreWriter getWriter() {
		if (m_writer == null) {
            throw new UnsupportedOperationException(
                    "This MulgaraConnector is read-only!");
        } else {
            return m_writer;
        }
	}

	@Override
	public void init(Map<String, String> config) throws TrippiException {
		AliasManager aliasManager = new AliasManager(new HashMap<String, String>());
		
		boolean remote = ConfigUtils.getRequiredBoolean(config, "remote");
		String host = null, port = null, path = null;
        if (remote) {
            host = ConfigUtils.getRequired(config, "host");
            port = (String)config.get("port");
        } else {
            path = ConfigUtils.getRequired(config, "path");
        }
        // default RMI port is 1099
        int portNumber = (port == null) ? 1099 : Integer.valueOf(port).intValue();
        String serverName = ConfigUtils.getRequired(config, "serverName");
        String modelName = ConfigUtils.getRequired(config, "modelName");
        int autoFlushDormantSeconds = ConfigUtils.getRequiredNNInt(config, "autoFlushDormantSeconds");
        int autoFlushBufferSize = ConfigUtils.getRequiredPosInt(config, "autoFlushBufferSize");
        int bufferSafeCapacity = ConfigUtils.getRequiredInt(config, "bufferSafeCapacity");
        int bufferFlushBatchSize = ConfigUtils.getRequiredPosInt(config, "bufferFlushBatchSize");
        int poolInitialSize = ConfigUtils.getRequiredInt(config, "poolInitialSize");
        
        if (bufferSafeCapacity < autoFlushBufferSize + 1) {
            throw new TrippiException("bufferSafeCapacity must be greater than autoFlushBufferSize.");
        }
        if (bufferFlushBatchSize > autoFlushBufferSize) {
            throw new TrippiException("bufferFlushBatchSize must be less than or equal to autoFlushBufferSize.");
        }

        int poolMaxGrowth = 0, poolSpareSessions = 0;
        if (poolInitialSize > 0) {
            poolMaxGrowth = ConfigUtils.getRequiredInt(config, "poolMaxGrowth");
            String temp = (String)config.get("poolSpareSessions");
            poolSpareSessions = (temp == null) ? 0 : Integer.parseInt(temp);
        }
        
        boolean readOnly = ConfigUtils.getRequiredBoolean(config, "readOnly");
        boolean autoCreate = false, autoTextIndex;
        String textModelName = null;
        if (!readOnly) {
        	autoCreate = ConfigUtils.getRequiredBoolean(config, "autoCreate");
            autoTextIndex = ConfigUtils.getRequiredBoolean(config, "autoTextIndex");
            if (autoTextIndex) {
              textModelName = modelName + "-fullText";
            }
        }
        
        m_factory = null;
        if (remote) {
            m_factory =  new MulgaraSessionFactory(serverName,
												modelName,
												textModelName,
												aliasManager,
												autoCreate,
												host,
            									portNumber);
        } else {
            m_factory =  new MulgaraSessionFactory(serverName,
                                                  modelName,
                                                  textModelName,
                                                  aliasManager,
                                                  autoCreate,
                                                  path);
        }
        
        if (poolInitialSize == 0) {
            m_synch = true;
            MulgaraSession mSession = (MulgaraSession) m_factory.newSession();
            m_elementFactory = mSession.getElementFactory();
            SynchronizedTriplestoreSession synchSession = new SynchronizedTriplestoreSession(mSession);
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
            
            MulgaraSession updateSession = (MulgaraSession) m_factory.newSession();
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
	    return m_factory;
	}

}
