package org.trippi.impl.mulgara;

import java.util.HashMap;
import java.util.Map;

import org.mulgara.query.Variable;
import org.mulgara.query.Answer;
import org.mulgara.query.TuplesException;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

public class MulgaraTupleIterator extends TupleIterator {
	
	private Answer m_answer;
    private boolean m_hasNext;
    private String[] m_names;

    private boolean m_isClosed;
	
	public MulgaraTupleIterator(Answer answer) throws TrippiException {
		m_isClosed = false;
		try {
			m_answer = new CollapsedAnswer(answer);
			m_answer.beforeFirst();
		} catch (TuplesException e) {
			throw new TrippiException(e.getMessage(), e);
		}
        checkNext();
	}
	
	@Override
	public void close() throws TrippiException {
		if (!m_isClosed) {
            try {
                m_answer.close();
                m_isClosed = true;
            } catch (Exception e) {
            	throw new TrippiException(e.getMessage(), e);
            }
        }
	}

	@Override
	public boolean hasNext() throws TrippiException {
		return m_hasNext;
	}

	@Override
	public String[] names() throws TrippiException {
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

	@Override
	public Map next() throws TrippiException {
		if ( !m_hasNext ) return null;
            Map<String, Object> map = new HashMap<String, Object>();
            Object obj;
            for (int i = 0; i < names().length; i++) {
                // We're guaranteed that the value will be a JRDF node,
                // since we've wrapped the possibly-answer-containing answer
                // in a CollapsedAnswer
                try {
					obj = m_answer.getObject(i);
				} catch (TuplesException e) {
					throw new TrippiException(e.getMessage(), e);
				}
                map.put(m_names[i], obj);
            }
            checkNext();
	        return map;
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
	
	/**
     * Ensure close() gets called at garbage collection time.
     */
    public void finalize() throws TrippiException {
        close();
    }
}
