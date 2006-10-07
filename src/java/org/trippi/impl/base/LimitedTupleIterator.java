package org.trippi.impl.base;

import java.util.Map;

import org.trippi.TrippiException;
import org.trippi.TupleIterator;

/**
 * Ensures that no more than a certain number of items are returned 
 * by the wrapped iterator.
 *
 * @author cwilper@cs.cornell.edu
 */
public class LimitedTupleIterator extends TupleIterator {

    private TupleIterator m_wrapped;
    private int m_count;
    private int m_limit;
    private Map m_next;
    private boolean m_closed = false;

    public LimitedTupleIterator(TupleIterator wrapped,
                                int limit) throws TrippiException {
        m_wrapped = wrapped;
        m_count = 0;
        m_limit = limit;
        m_next = getNext();
    }

    // return null if there are no more or limit has been reached
    private Map getNext() throws TrippiException {
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

    public Map next() throws TrippiException {
        if (m_next == null) return null;
        Map last = m_next;
        m_next = getNext();
        return last;
    }

    public String[] names() throws TrippiException {
        return m_wrapped.names();
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
