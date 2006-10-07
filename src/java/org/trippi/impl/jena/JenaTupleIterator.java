package org.trippi.impl.jena;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.trippi.TrippiException;
import org.trippi.TupleIterator;

import com.hp.hpl.jena.graph.FrontsNode;
import com.hp.hpl.jena.rdql.QueryResults;
import com.hp.hpl.jena.rdql.ResultBinding;

/**
 * An <code>TupleIterator</code> that wraps a Jena QueryResult object.
 * <p>
 * This implementation turns underlying unchecked Jena exceptions into 
 * checked ones.  FIXME: Actually do this.
 * </p>
 * @author cwilper@cs.cornell.edu
 */
public class JenaTupleIterator extends TupleIterator {

    private QueryResults m_results;
    private String[] m_names;
    private boolean m_closed;
    private JenaToJRDF m_jenaToJRDF;

    /**
     * Initialize the iterator given a Jena QueryResult.
     */
    public JenaTupleIterator(QueryResults results) throws TrippiException {
        m_closed = false;
        m_results = results;
        m_jenaToJRDF = new JenaToJRDF();
    }
    
    public boolean hasNext() {
        boolean h = m_results.hasNext();
        if (!h) {
            try {
                close();
            } catch (Exception e) {
                System.err.println("Error aggressively closing Jena tuple iterator: " + e.getMessage());
            }
        }
        return h;
    }

    public Map next() throws TrippiException {
        try {
            Map map = new HashMap();
	        ResultBinding result = (ResultBinding) m_results.next();
	        for (int i = 0; i < names().length; i++) {
	            map.put(m_names[i], 
	                    m_jenaToJRDF.toNode((
	                        (FrontsNode) result.get(m_names[i])).asNode()));
            }
	        return map;
        } catch (Exception e) {
            String msg = e.getClass().getName();
            if (e.getMessage() != null) msg = msg + ": " + e.getMessage();
            throw new TrippiException("Error getting result "
                    + "from Jena QueryResults: " + msg, e);
        }
    }

    public String[] names() {
        if (m_names == null) {
            List names = m_results.getResultVars();
            m_names = new String[names.size()];
            Iterator iter = m_results.getResultVars().iterator();
            int i = 0;
            while (iter.hasNext()) {
                m_names[i++] = (String) iter.next();
            }
        }
        return m_names;
    }

    public void close() throws TrippiException {
        if (!m_closed) {
            try {
                m_results.close();
                m_closed = true;
            } catch (Exception e) {
                String msg = e.getClass().getName();
                if (e.getMessage() != null) msg = msg + ": " + e.getMessage();
                throw new TrippiException("Error closing "
                        + "JenaTupleIterator" + msg, e);
            }
        }
    }

    /**
     * Ensure close() gets called at garbage collection time.
     */
    public void finalize() throws TrippiException {
        close();
    }

}
