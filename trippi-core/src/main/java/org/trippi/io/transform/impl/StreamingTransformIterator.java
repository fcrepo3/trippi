package org.trippi.io.transform.impl;

import java.io.OutputStream;

import org.trippi.RDFFormat;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;
import org.trippi.TrippiIterator;
import org.trippi.io.transform.Transformer;


public class StreamingTransformIterator<T>
implements TrippiIterator<T> {

    private final TripleIterator m_src;
    
    private final Transformer<T> m_transform;
    
    public StreamingTransformIterator(
            TripleIterator src,
            Transformer<T> transform) {
        m_src = src;
        m_transform = transform;
    }
    
    public boolean hasNext() throws TrippiException {
        return m_src.hasNext();
    }
    
    public T next() throws TrippiException {
        return m_transform.transform(m_src.next());
    }
    
    public int count() throws TrippiException {
        return m_src.count();
    }
    
    public void close() throws TrippiException {
        m_src.close();
    }
    
    @Override
    public int toStream(OutputStream out, RDFFormat format)
        throws TrippiException {
        throw new UnsupportedOperationException("unimplemented");
    }
}
