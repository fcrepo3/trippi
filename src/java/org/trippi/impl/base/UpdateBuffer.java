package org.trippi.impl.base;

import java.io.IOException;
import java.util.List;

import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.trippi.FlushErrorHandler;
import org.trippi.TripleUpdate;
import org.trippi.TrippiException;

/**
 * A buffer for triplestore updates.
 *
 * @author cwilper@cs.cornell.edu
 */
public interface UpdateBuffer {
	public final static int ADD_UPDATE_TYPE = 1;
	public final static int DELETE_UPDATE_TYPE = 2;
	public final static int EITHER_UPDATE_TYPE = 3;
	
    /**
     * Buffer the addition of the given triples.
     */
    public void add(List<Triple> triples) throws IOException;

    /**
     * Buffer the addition of the given triple.
     */
    public void add(Triple triple) throws IOException;

    /**
     * Buffer the deletion of the given triples.
     */
    public void delete(List<Triple> triples) throws IOException;

    /**
     * Buffer the deletion of the given triple.
     */
    public void delete(Triple triple) throws IOException;

    /**
     * Get the number of triples currently in the buffer.
     */
    public int size();

    /**
     * Get the number of triples that the buffer can safely contain.
     *
     * This is not an absolute maximum size.
     */
    public int safeCapacity();

    /**
     * Flush the contents of the buffer to the triplestore.
     *
     * Implementations should ensure that adds and deletes to the buffer
     * can still happen during flushes.
     */
    public void flush(TriplestoreSession session) throws IOException, 
                                                         TrippiException;

    /**
     * Set the (optional) handler that will recieve failed flush notification.
     *
     * Applications can use this to ensure that the contents of the buffer
     * are not lost when a flushing error occurs.
     */
    public void setFlushErrorHandler(FlushErrorHandler h);

    /**
     * Close the buffer, releasing any associated system resources.
     */
    public void close() throws IOException;

    /**
     * Returns an unmodifiable List of the TripleUpdates currently in queue.
     * 
     * @return List of TripleUpdates
     */
    public List<TripleUpdate> findBufferedUpdates(SubjectNode subject, 
    		PredicateNode predicate, 
    		ObjectNode object, 
    		int updateType);
}