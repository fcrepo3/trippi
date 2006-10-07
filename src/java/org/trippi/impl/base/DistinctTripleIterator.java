package org.trippi.impl.base;

import gnu.trove.TIntHashSet;

import org.jrdf.graph.Triple;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;

/**
 * Ensures no dupes while iterating through the wrapped iterator.
 *
 * @author cwilper@cs.cornell.edu
 */
public class DistinctTripleIterator extends TripleIterator {

    private TripleIterator m_wrapped;
    private TIntHashSet m_seen;
    private Triple m_next;
    private boolean m_closed = false;

    public DistinctTripleIterator(TripleIterator wrapped) throws TrippiException {
        m_wrapped = wrapped;
        m_seen = new TIntHashSet();
        m_next = getNext();
    }

    // return null if there are no more
    private Triple getNext() throws TrippiException {
        while (m_wrapped.hasNext()) {
            Triple nextTriple = m_wrapped.next();
            if (!seen(nextTriple.hashCode())) return nextTriple;
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
