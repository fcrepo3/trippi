package org.trippi.impl.mulgara;

import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.GraphElementFactoryException;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.mulgara.query.Answer;
import org.mulgara.query.TuplesException;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;

public class MulgaraTripleIterator extends TripleIterator {
	
	private Answer m_answer;
	private GraphElementFactory m_geFactory;
	private boolean m_hasNext;
	private boolean m_isClosed;

    public MulgaraTripleIterator(Answer answer, GraphElementFactory geFactory) throws TrippiException {
    	m_answer = answer;
    	m_geFactory = geFactory;
    	m_isClosed = false;
    	
		try {
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
			} catch (TuplesException e) {
				throw new TrippiException(e.getMessage(), e);
			}
		}
	}

	@Override
	public boolean hasNext() throws TrippiException {
		return m_hasNext;
	}

	@Override
	public Triple next() throws TrippiException {
		if ( !m_hasNext ) return null;
		try {
			SubjectNode s = (SubjectNode) m_answer.getObject(0);
			PredicateNode p = (PredicateNode) m_answer.getObject(1);
			ObjectNode o = (ObjectNode) m_answer.getObject(2);
			checkNext();
			return m_geFactory.createTriple(s, p, o);
		} catch (TuplesException e) {
			throw new TrippiException(e.getMessage(), e);
		} catch (GraphElementFactoryException e) {
			throw new TrippiException(e.getMessage(), e);
		}
	}
	
	private void checkNext() throws TrippiException {
        try {
            m_hasNext = m_answer.next();
        } catch (TuplesException e) {
            throw new TrippiException("Error getting next result from Mulgara Answer.", e);
        }
        if (m_hasNext == false) {
            try {
                close();
            } catch (TrippiException e) {
                System.err.println("Error aggressively closing Mulgara triple iterator: " + e.getMessage());
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
