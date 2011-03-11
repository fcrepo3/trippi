package org.trippi.impl.base;

import org.trippi.TrippiException;

/**
 * A pool of triplestore sessions.
 */
public interface TriplestoreSessionPool {

    /**
     * Get a connection from the pool.
     *
     * @return a session, or null if none are available and growth isnt allowed.
     * @throws TrippiException if there were no spare sessions and an 
     *                              attempt to create one on-demand failed.
     */
    public TriplestoreSession get() throws TrippiException;

    /**
     * Release a connection back to the pool.
     */
    public void release(TriplestoreSession session);

    public String[] listTupleLanguages(); 
    public String[] listTripleLanguages(); 

    /**
     * Get the number of sessions currently in use.
     */
    public int getInUseCount();

    /**
     * Get the number of sessions not currently in use.
     */
    public int getFreeCount();

    /**
     * Close all sessions.
     */
    public void close() throws TrippiException;

}