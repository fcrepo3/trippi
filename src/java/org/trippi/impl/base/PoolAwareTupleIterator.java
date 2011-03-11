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
 * associated session to the pool when closed.
 *
 * @author cwilper@cs.cornell.edu
 */
public class PoolAwareTupleIterator extends TupleIterator {

    private TupleIterator m_iter;
    private TriplestoreSession m_session;
    private TriplestoreSessionPool m_pool;
    private boolean m_closed = false;

    public PoolAwareTupleIterator(TupleIterator iter,
                                   TriplestoreSession session,
                                   TriplestoreSessionPool pool) {
        m_iter = iter;
        m_session = session;
        m_pool = pool;
    }

    @Override
	public boolean hasNext() throws TrippiException {
        boolean has = m_iter.hasNext();
        if (!has) {
            close();
        }
        return has;
    }
    
    @Override
	public Map<String, Node> next() throws TrippiException {
        return m_iter.next();
    }

    @Override
	public List<Triple> nextTriples(TriplePattern[] patterns) throws TrippiException {
        return m_iter.nextTriples(patterns);
    }
    
    @Override
	public String[] names() throws TrippiException {
        return m_iter.names();
    }
    
    /**
     * Close the wrapped iterator and release the session to the pool.
     */
    @Override
	public void close() throws TrippiException {
        if (!m_closed) {
            // This ensures that even if the wrapped iter throws an exception,
            // the session is released to the pool.
            try {
                m_iter.close();
            } catch (TrippiException e) {
                throw e;
            } finally {
                m_pool.release(m_session);
                m_closed = true;
            }
        }
    }

    /**
     * Ensure close() gets called at garbage collection time.
     */
    @Override
	public void finalize() throws TrippiException {
        close();
    }

}
