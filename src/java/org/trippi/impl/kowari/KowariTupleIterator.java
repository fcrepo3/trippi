package org.trippi.impl.kowari;

import java.util.HashMap;
import java.util.Map;

import org.kowari.query.Answer;
import org.kowari.query.Variable;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

/**
 * A <code>TupleIterator</code> that wraps a Kowari Answer object containing
 * tuples.
 *
 * @author cwilper@cs.cornell.edu
 */
public class KowariTupleIterator extends TupleIterator {

    private Answer m_answer;
    private boolean m_hasNext;
    private String[] m_names;

    private boolean m_closed;

    /**
     * Initialize the iterator given a Kowari Answer.
     *
     * @throws TrippiException if there is an error positioning to the
     *                              first result.
     */
    public KowariTupleIterator(Answer answer) throws TrippiException {
        m_closed = false;
        try {
            m_answer = new CollapsedAnswer(answer);
            m_answer.beforeFirst();
            checkNext();
        } catch (Exception e) {
            String msg = e.getClass().getName();
            if (e.getMessage() != null) msg = msg + ": " + e.getMessage();
            throw new TrippiException("Error getting first result from "
                    + "Kowari Answer" + msg, e);
        }
    }

    private void checkNext() throws TrippiException {
        try {
            m_hasNext = m_answer.next();
        } catch (Exception e) {
            throw new TrippiException("Error getting next result from Kowari Answer.", e);
        }
        if (!m_hasNext) {
            try {
                close();
            } catch (TrippiException e) {
                System.err.println("Error aggressively closing kowari tuple iterator: " + e.getMessage());
            }
        }
    }
    
    public boolean hasNext() {
        return m_hasNext;
    }

    public Map next() throws TrippiException {
        if ( !m_hasNext ) return null;
        try {
            Map map = new HashMap();
            Object obj;
            for (int i = 0; i < names().length; i++) {
                // We're guaranteed that the value will be a JRDF node,
                // since we've wrapped the possibly-answer-containing answer
                // in a CollapsedAnswer
                obj = m_answer.getObject(i);
                map.put(m_names[i], obj);
            }
            checkNext();
	        return map;
        } catch (Exception e) {
            String msg = e.getClass().getName();
            if (e.getMessage() != null) msg = msg + ": " + e.getMessage();
            throw new TrippiException("Error getting subsequent result "
                    + "from Kowari Answer" + msg, e);
        }
    }

    public String[] names() {
        if (m_names == null) {
            Variable[] v = m_answer.getVariables();
            if (v != null) {
                m_names = new String[v.length];
                for (int i = 0; i < v.length; i++) {
                    m_names[i] = v[i].getName();
                }
            }
        }
        return m_names;
    }

    public void close() throws TrippiException {
        if (!m_closed) {
            try {
                m_answer.close();
                m_closed = true;
            } catch (Exception e) {
                String msg = e.getClass().getName();
                if (e.getMessage() != null) msg = msg + ": " + e.getMessage();
                throw new TrippiException("Error closing "
                        + "KowariTupleIterator: " + msg, e);
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
