package org.trippi.impl.base;

import java.util.Set;

import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

/**
 * A read/write session to an RDF database.
 *
 * If a session doesn't support writes, the add and delete methods
 * will throw UnsupportedOperationException (an unchecked exception).
 *
 * @author cwilper@cs.cornell.edu
 */
public interface TriplestoreSession {

    /**
     * Add the given triples to the store.
     *
     * @param  triples  a Set of <code>Triple</code> objects.
     * @throws UnsupportedOperationException if modifications are not supported.
     * @throws TrippiException if adding to the store otherwise failed.
     */
    public void add(Set<Triple> triples) throws UnsupportedOperationException,
                                         TrippiException;

    /**
     * Delete the given triples from the store.
     *
     * @param  triples  a Set of <code>Triple</code> objects.
     * @throws UnsupportedOperationException if modifications are not supported.
     * @throws TrippiException if deleting from the store failed.
     */
    public void delete(Set<Triple> triples) throws UnsupportedOperationException,
                                            TrippiException;

    /**
     * Perform a tuple query against the store.
     *
     * @param  queryText  the text of the query
     * @param   language  the query language
     */
    public TupleIterator query(String queryText,
                                   String language) throws TrippiException;

    public TripleIterator findTriples(String lang,
                                      String queryText) throws TrippiException;

    public TripleIterator findTriples(SubjectNode subject,
                                      PredicateNode predicate,
                                      ObjectNode object) throws TrippiException;

    public String[] listTupleLanguages(); 
    public String[] listTripleLanguages(); 

    /**
     * Close the session, releasing any resources.
     */
    public void close() throws TrippiException;

}
