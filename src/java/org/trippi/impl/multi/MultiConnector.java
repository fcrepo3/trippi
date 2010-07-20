package org.trippi.impl.multi;

import java.util.HashMap;
import java.util.Map;

import org.jrdf.graph.GraphElementFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trippi.RDFUtil;
import org.trippi.TriplestoreConnector;
import org.trippi.TriplestoreReader;
import org.trippi.TriplestoreWriter;
import org.trippi.TrippiException;

/**
 * A <code>TriplestoreConnector</code> for a local, native Sesame RDF
 * triplestore.
 *
 * @author cwilper@cs.cornell.edu
 */
public class MultiConnector extends TriplestoreConnector {
	private static final Logger logger =
        LoggerFactory.getLogger(MultiConnector.class.getName());

    private TriplestoreConnector[] m_connectors;
    private MultiTriplestoreWriter m_multiWriter;
    private GraphElementFactory m_elementFactory;
    private Map<String,String> m_config = new HashMap<String,String>(0);

    public MultiConnector() {
    }

    public MultiConnector(TriplestoreConnector[] connectors) {
        m_connectors = connectors;

        m_elementFactory = new RDFUtil();
    }

    @Deprecated
    @Override
	public void init(Map<String,String> config) throws TrippiException {
    	setConfiguration(config);
    }
    
    public void setConfiguration(Map<String,String> config) throws TrippiException {
    	m_config = config;
    }
    
    public Map<String,String> getConfiguration(){
    	return m_config;
    }

    @Override
	public TriplestoreReader getReader() {
    	if (m_multiWriter == null){
    		try{
    			open();
    		}
    		catch (TrippiException e){
    			logger.error(e.toString(),e);
    		}
    	}
		return m_connectors[0].getReader();
    }

    @Override
	public TriplestoreWriter getWriter() {
    	if (m_multiWriter == null){
    		try{
    			open();
    		}
    		catch (TrippiException e){
    			logger.error(e.toString(),e);
    		}
    	}
        return m_multiWriter;
    }

    @Override
	public GraphElementFactory getElementFactory() {
        return m_elementFactory;
    }
    
    @Override
    public void open() throws TrippiException {
        TriplestoreWriter[] writers = new TriplestoreWriter[m_connectors.length];
        for (int i = 0; i < m_connectors.length; i++) {
            writers[i] = m_connectors[i].getWriter();
        }
        m_multiWriter = new MultiTriplestoreWriter(m_connectors[0].getReader(), writers);
    }

    @Override
	public void close() throws TrippiException {
        TrippiException m_exception = null;
        for (int i = 0; i < m_connectors.length; i++) {
            try {
                m_connectors[i].close();
            } catch (TrippiException e) {
                m_exception = e;
            }
        }
        if (m_exception != null) throw m_exception;
    }

}
