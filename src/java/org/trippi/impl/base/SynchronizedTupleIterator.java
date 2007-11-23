package org.trippi.impl.base;

import java.util.List;
import java.util.Map;

import org.jrdf.graph.Node;
import org.jrdf.graph.Triple;
import org.trippi.TriplePattern;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

/**
 * Wraps an <code>TupleIterator</code> and automatically releases the
 * current thread's lock on the associated SynchronizedTriplestoreSession
 * when closed.
 *
 * @author cwilper@cs.cornell.edu
 */
public class SynchronizedTupleIterator extends TupleIterator {

    private TupleIterator m_iter;
    private SynchronizedTriplestoreSession m_session;
    private boolean m_closed = false;

    public SynchronizedTupleIterator(TupleIterator iter,
                                     SynchronizedTriplestoreSession session) {
        m_iter = iter;
        m_session = session;
    }

    public boolean hasNext() throws TrippiException {
        boolean has = m_iter.hasNext();
        if (!has) {
            close();
        }
        return has;
    }
    
    public Map<String, Node> next() throws TrippiException {
        return m_iter.next();
    }

    public List<Triple> nextTriples(TriplePattern[] patterns) throws TrippiException {
        return m_iter.nextTriples(patterns);
    }
    
    public String[] names() throws TrippiException {
        return m_iter.names();
    }
    
    /**
     * Close the wrapped iterator and release the session to the pool.
     */
    public void close() throws TrippiException {
        if (!m_closed) {
            // This ensures that even if the wrapped iter throws an exception,
            // the thread's lock on the session is released.
            try {
                m_iter.close();
            } catch (TrippiException e) {
                throw e;
            } finally {
                m_session.releaseLock();
                m_closed = true;
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
