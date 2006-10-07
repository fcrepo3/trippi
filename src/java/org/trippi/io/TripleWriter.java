package org.trippi.io;

import org.trippi.TripleIterator;
import org.trippi.TrippiException;

public abstract class TripleWriter extends RDFWriter {

    /**
     * Write the triples from the iterator, close the iterator, and return 
     * the number written.  Do not close the outputstream.
     */
    public abstract int write(TripleIterator iter) throws TrippiException;

}
