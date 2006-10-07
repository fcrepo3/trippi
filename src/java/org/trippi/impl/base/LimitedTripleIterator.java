package org.trippi.impl.base;

import org.jrdf.graph.Triple;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;

/**
 * Ensures that no more than a certain number of items are returned 
 * by the wrapped iterator.
 *
 * @author cwilper@cs.cornell.edu
 */
public class LimitedTripleIterator extends TripleIterator {

    private TripleIterator m_wrapped;
    private int m_count;
    private int m_limit;
    private Triple m_next;
    private boolean m_closed = false;

    public LimitedTripleIterator(TripleIterator wrapped,
                                 int limit) throws TrippiException {
        m_wrapped = wrapped;
        m_count = 0;
        m_limit = limit;
        m_next = getNext();
    }

    // return null if there are no more or limit has been reached
    private Triple getNext() throws TrippiException {
        if (m_wrapped.hasNext() && m_count < m_limit) {
            m_count++;
            return m_wrapped.next();
        } else {
            return null;
        }
    }

    public boolean hasNext() {
        return (m_next != null);
    }

    public Triple next() throws TrippiException {
        if (m_next == null) return null;
        Triple last = m_next;
        m_next = getNext();
        return last;
    }

    public void close() throws TrippiException {
        if (!m_closed) {
            m_wrapped.close();
            m_closed = true;
        }
    }

    public void finalize() throws TrippiException {
        close();
    }

}
