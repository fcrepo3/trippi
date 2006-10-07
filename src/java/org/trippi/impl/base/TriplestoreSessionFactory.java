package org.trippi.impl.base;

import org.trippi.TrippiException;

/**
 * A provider of triplestore sessions.
 *
 * @author cwilper@cs.cornell.edu
 */
public interface TriplestoreSessionFactory {

    /**
     * Get a new session.
     */
    public TriplestoreSession newSession() throws TrippiException;

    public String[] listTripleLanguages(); 
    public String[] listTupleLanguages(); 

    /**
     * Free up any system resources allocated by the session factory.
     */
    public void close() throws TrippiException;

}