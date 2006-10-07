package org.trippi.io;

import org.trippi.TrippiException;
import org.trippi.TupleIterator;

public abstract class TupleWriter extends RDFWriter {

    /**
     * Write the tuples from the iterator, close the iterator, and return 
     * the number written.  Do not close the outputstream.
     */
    public abstract int write(TupleIterator iter) throws TrippiException;

}
