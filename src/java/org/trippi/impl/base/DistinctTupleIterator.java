package org.trippi.impl.base;

import gnu.trove.TIntHashSet;

import java.util.Map;

import org.trippi.TrippiException;
import org.trippi.TupleIterator;

/**
 * Ensures no dupes while iterating through the wrapped iterator.
 *
 * @author cwilper@cs.cornell.edu
 */
public class DistinctTupleIterator extends TupleIterator {

    private TupleIterator m_wrapped;
    private TIntHashSet m_seen;
    private Map m_next;
    private boolean m_closed = false;

    public DistinctTupleIterator(TupleIterator wrapped) throws TrippiException {
        m_wrapped = wrapped;
        m_seen = new TIntHashSet();
        m_next = getNext();
    }

    // return null if there are no more
    private Map getNext() throws TrippiException {
        while (m_wrapped.hasNext()) {
            Map nextMap = m_wrapped.next();
            if (!seen(nextMap.hashCode())) return nextMap;
        }
        return null;
    }

    private boolean seen(int id) {
        if (m_seen.contains(id)) return true;
        m_seen.add(id);
        return false;
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
