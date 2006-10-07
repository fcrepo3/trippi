package org.trippi.nodegraph;

import org.jrdf.graph.Node;
import org.jrdf.util.ClosableIterator;

/**
 * A <code>ClosableIterator</code> of <code>Node</code> objects,
 * with several built-in convenience methods.
 *
 * @author cwilper@cs.cornell.edu
 */
public interface NodeResults extends ClosableIterator {

    /**
     * Get the first node and automatically close the iterator.
     */
    public Node first();

    /**
     * Get all nodes as an array and automatically close the iterator.
     */
    public Node[] all();

    /**
     * Get the number of items in the iterator and automatically close it.
     */
    public int count();

}
