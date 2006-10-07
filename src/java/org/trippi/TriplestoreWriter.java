package org.trippi;

import java.io.IOException;
import java.util.List;

import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;

/**
 * A TriplestoreReader that also provides buffered write access.
 *
 * @author cwilper@cs.cornell.edu
 */
public interface TriplestoreWriter
        extends TriplestoreReader {

    /**
     * Add a series of triples to the store.
     *
     * @param  triples  a list of <code>Triple</code> objects
     * @param  flush    whether to flush the buffer before returning
     */
    public void add(List triples,
                    boolean flush) throws IOException,
                                          TrippiException;

    /**
     * Add a series of triples to the store.
     *
     * @param  iter     an iterator over the triples
     * @param  flush    whether to flush the buffer before returning
     */
    public void add(TripleIterator iter,
                    boolean flush) throws IOException,
                                          TrippiException;

    /**
     * Add a single triple to the store.
     *
     * @param  triple  the <code>Triple</code> to add
     * @param  flush   whether to flush the buffer before returning
     */
    public void add(Triple triple,
                    boolean flush) throws IOException,
                                          TrippiException;

    /**
     * Remove a series of triples from the store.
     *
     * @param  triples  a list of <code>Triple</code> objects
     * @param  flush    whether to flush the buffer before returning
     */
    public void delete(List triples,
                       boolean flush) throws IOException,
                                             TrippiException;

    /**
     * Remove a series of triples from the store.
     *
     * @param  iter     an iterator over the triples
     * @param  flush    whether to flush the buffer before returning
     */
    public void delete(TripleIterator iter,
                       boolean flush) throws IOException,
                                             TrippiException;

    /**
     * Remove a single triple from the store (convenience method).
     *
     * @param  triple  the <code>Triple</code> to delete
     * @param  flush   whether to flush the buffer before returning
     */
    public void delete(Triple triple,
                       boolean flush) throws IOException,
                                             TrippiException;

    /**
     * Flush the buffer (write the changes to the triplestore).
     */
    public void flushBuffer() throws IOException,
                                     TrippiException;

    /**
     * Set the (optional) handler that will recieve failed flush notification.
     *
     * Applications can use this to ensure that the contents of the buffer
     * are not lost when a flushing error occurs.
     */
    public void setFlushErrorHandler(FlushErrorHandler h);

    /**
     * Get the current size of the buffer.
     */
    public int getBufferSize();
    
    /**
     * Returns an unmodifiable List of TripleUpdates currently in queue.
     * @return unmodifiable List of TripleUpdates currently in queue
     */
    public List findBufferedUpdates(SubjectNode subject, 
    								PredicateNode predicate, 
    								ObjectNode object, 
    								int updateType);

}
