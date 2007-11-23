package org.trippi;

import java.util.List;

/**
 * Handles <code>TripleUpdate</code> buffer flushing errors encountered by
 * a <code>TriplestoreWriter</code>.
 *
 * @author cwilper@cs.cornell.edu
 */
public interface FlushErrorHandler {

    /**
     * Handle the error.
     *
     * The <code>List</code> contains <code>TripleUpdate</code> objects that
     * were being flushed at the time of the failure.
     */
    public void handleFlushError(List<TripleUpdate> failedUpdates, 
                                 TrippiException cause);

}