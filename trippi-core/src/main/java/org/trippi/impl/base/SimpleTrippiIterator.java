package org.trippi.impl.base;

import java.io.OutputStream;
import java.util.Iterator;
import java.util.Set;

import org.trippi.RDFFormat;
import org.trippi.TrippiException;
import org.trippi.TrippiIterator;


public class SimpleTrippiIterator<T> implements TrippiIterator<T> {

    private final Iterator<T> m_src;
    
    private final int m_size;
    
    public SimpleTrippiIterator(Set<T> src) {
        m_src = src.iterator();
        m_size = src.size();
    }
    
    @Override
    public boolean hasNext() throws TrippiException {
        return m_src.hasNext();
    }

    @Override
    public T next() throws TrippiException {
        return m_src.next();
    }

    @Override
    public int count() throws TrippiException {
        return m_size;
    }

    @Override
    public void close() throws TrippiException {
        // no-op
        
    }
    
    @Override
    public int toStream(OutputStream out, RDFFormat format)
        throws TrippiException {
        throw new UnsupportedOperationException("unimplemented");
    }

}
