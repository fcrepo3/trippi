package org.trippi;

import java.io.OutputStream;


public interface TrippiIterator<T> {
    
    public boolean hasNext() throws TrippiException;
    
    public T next() throws TrippiException;
    
    public int count() throws TrippiException;
    
    public void close() throws TrippiException;
    
    public int toStream(OutputStream out, RDFFormat format) throws TrippiException;

}
