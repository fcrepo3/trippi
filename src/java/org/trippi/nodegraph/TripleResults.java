package org.trippi.nodegraph;

import org.jrdf.graph.Triple;
import org.jrdf.util.ClosableIterator;

/**
 * A <code>ClosableIterator</code> of <code>Triple</code> objects,
 * with several built-in convenience methods.
 *
 * @author cwilper@cs.cornell.edu
 */
public interface TripleResults extends ClosableIterator {

    /**
     * Get the first triple and automatically close the iterator.
     */
    public Triple first();

    /**
     * Get all triples as an array and automatically close the iterator.
     */
    public Triple[] all();

    /**
     * Get the number of items in the iterator and automatically close it.
     */
    public int count();

}
