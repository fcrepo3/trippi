package org.trippi.impl.multi;

import java.util.Map;

import org.apache.log4j.Logger;
import org.jrdf.graph.GraphElementFactory;
import org.trippi.RDFUtil;
import org.trippi.TriplestoreConnector;
import org.trippi.TriplestoreReader;
import org.trippi.TriplestoreWriter;
import org.trippi.TrippiException;
import org.trippi.impl.mulgara.MulgaraConnector;

/**
 * A <code>TriplestoreConnector</code> for multiplexing writes.
 *
 * @author cwilper@cs.cornell.edu
 */
public class MultiConnector extends TriplestoreConnector {
	private static final Logger logger =
        Logger.getLogger(MultiConnector.class.getName());

    private TriplestoreConnector[] m_connectors;
    private TriplestoreConnector m_readConnector;
    private TriplestoreReader m_reader;
    private MultiTriplestoreWriter m_multiWriter;
    private GraphElementFactory m_elementFactory;

    public MultiConnector() {
    	logger.error(MultiConnector.class.getName() + " configured without target connectors!");
    	m_connectors = new TriplestoreConnector[0];
    }

    public MultiConnector(TriplestoreConnector[] connectors) {
    	this(connectors[0],connectors);
    }
    
    public MultiConnector(TriplestoreConnector readConnector, TriplestoreConnector[] writeConnectors) {
        m_connectors = writeConnectors;
        m_readConnector = readConnector;
        m_reader = m_readConnector.getReader();
        TriplestoreWriter[] writers = new TriplestoreWriter[writeConnectors.length];
        for (int i = 0; i < writeConnectors.length; i++) {
            writers[i] = writeConnectors[i].getWriter();
        }
        m_multiWriter = new MultiTriplestoreWriter(m_reader, writers);

        m_elementFactory = new RDFUtil();
    }

    @Override
	public void init(Map<String,String> config) throws TrippiException {
    	logger.warn(MultiConnector.class.getName() + " configured without target connectors!");
    }

    @Override
	public TriplestoreReader getReader() {
        return m_reader;
    }

    @Override
	public TriplestoreWriter getWriter() {
        return m_multiWriter;
    }

    @Override
	public GraphElementFactory getElementFactory() {
        return m_elementFactory;
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
        try{
        	if (m_readConnector != null) m_readConnector.close();
        } catch (TrippiException e) {
            m_exception = e;
        }
        if (m_exception != null) throw m_exception;
    }

}
