package org.trippi.impl.count;

import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.trippi.TripleIterator;
import org.trippi.TripleMaker;
import org.trippi.TrippiException;

public class CountTripleIterator extends TripleIterator {

    private boolean m_hasNext;
    private boolean m_isClosed;
    private final TripleIterator m_src;

    public CountTripleIterator(TripleIterator iter){
    	m_isClosed = false;
    	m_hasNext = true;
    	m_src = iter;
    }

	@Override
	public void close() throws TrippiException {
        if (!m_isClosed) {
            m_isClosed = true;
            m_hasNext = false;
            m_src.close();
        }
	}

	@Override
	public boolean hasNext() throws TrippiException {
        return m_hasNext;
	}

	@Override
	public Triple next() throws TrippiException {
        if (!m_hasNext) return null;
        try{
            SubjectNode subject = TripleMaker.createResource();
            PredicateNode predicate = TripleMaker.createResource("http://mulgara.org/mulgara#is");
            return TripleMaker.create(subject,predicate, new CountLiteral(m_src.count()));
        }
        finally{
            m_hasNext = false;
        }
	}
	
    public void finalize() throws TrippiException {
        close();
    }

}
