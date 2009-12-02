package org.trippi.impl.base;

import java.util.Map;

import org.jrdf.graph.Node;
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
    private Map<String, Node> m_next;
    private boolean m_closed = false;

    public LimitedTupleIterator(TupleIterator wrapped,
                                int limit) throws TrippiException {
        m_wrapped = wrapped;
        m_count = 0;
        m_limit = limit;
        m_next = getNext();
    }

    // return null if there are no more or limit has been reached
    private Map<String, Node> getNext() throws TrippiException {
        if (m_wrapped.hasNext() && m_count < m_limit) {
            m_count++;
            return m_wrapped.next();
        } else {
            return null;
        }
    }

    @Override
	public boolean hasNext() {
        return (m_next != null);
    }

    @Override
	public Map<String, Node> next() throws TrippiException {
        if (m_next == null) return null;
        Map<String, Node> last = m_next;
        m_next = getNext();
        return last;
    }

    @Override
	public String[] names() throws TrippiException {
        return m_wrapped.names();
    }

    @Override
	public void close() throws TrippiException {
        if (!m_closed) {
            m_wrapped.close();
            m_closed = true;
        }
    }

    @Override
	public void finalize() throws TrippiException {
        close();
    }

}
