package org.trippi.impl.base;

import org.trippi.TrippiException;

/**
 * A pool providing access to a single session.
 */
public class SingleSessionPool implements TriplestoreSessionPool {

    private TriplestoreSession m_session;
    private String[] m_tupleLanguages;
    private String[] m_tripleLanguages;

    public SingleSessionPool(TriplestoreSession session,
                             String[] tupleLanguages,
                             String[] tripleLanguages) {
        m_session = session;
        m_tupleLanguages = tupleLanguages;
        m_tripleLanguages = tripleLanguages;
    }

    public TriplestoreSession get() {
        return m_session;
    }

    public void release(TriplestoreSession session) { }

    public String[] listTupleLanguages() {
        return m_tupleLanguages; 
    }

    public String[] listTripleLanguages() {
        return m_tripleLanguages;
    }

    public int getInUseCount() {
        return 1;
    }

    public int getFreeCount() {
        return 0;
    }

    public void close() throws TrippiException {
        m_session.close();
    }

}