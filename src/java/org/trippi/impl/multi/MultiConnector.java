package org.trippi.impl.multi;

import java.util.Map;

import org.jrdf.graph.GraphElementFactory;
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

    private TriplestoreConnector[] m_connectors;
    private MultiTriplestoreWriter m_multiWriter;
    private GraphElementFactory m_elementFactory;

    public MultiConnector() {
    }

    public MultiConnector(TriplestoreConnector[] connectors) {
        m_connectors = connectors;

        TriplestoreWriter[] writers = new TriplestoreWriter[connectors.length];
        for (int i = 0; i < connectors.length; i++) {
            writers[i] = connectors[i].getWriter();
        }
        m_multiWriter = new MultiTriplestoreWriter(m_connectors[0].getReader(), writers);

        m_elementFactory = new RDFUtil();
    }

    public void init(Map config) throws TrippiException {
        throw new TrippiException("This connector cannot be initialized via init()");
    }

    public TriplestoreReader getReader() {
        return m_connectors[0].getReader();
    }

    public TriplestoreWriter getWriter() {
        return m_multiWriter;
    }

    public GraphElementFactory getElementFactory() {
        return m_elementFactory;
    }

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
