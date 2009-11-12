package org.trippi.impl.base;

import org.jrdf.graph.Triple;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;

/**
 * Wraps a <code>TripleIterator</code> and automatically releases the
 * current thread's lock on the associated SynchronizedTriplestoreSession
 * when closed.
 *
 * @author cwilper@cs.cornell.edu
 */
public class SynchronizedTripleIterator extends TripleIterator {

    private TripleIterator m_iter;
    private SynchronizedTriplestoreSession m_session;
    private boolean m_closed = false;

    public SynchronizedTripleIterator(TripleIterator iter,
                                      SynchronizedTriplestoreSession session) {
        m_iter = iter;
        m_session = session;
    }

    @Override
	public boolean hasNext() throws TrippiException {
        boolean has = m_iter.hasNext();
        if (!has) close(); // proactively
        return has;
    }
    
    @Override
	public Triple next() throws TrippiException {
        return m_iter.next();
    }

    /**
     * Close the wrapped iterator and release the session to the pool.
     */
    @Override
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
    @Override
	public void finalize() throws TrippiException {
        close();
    }

}
