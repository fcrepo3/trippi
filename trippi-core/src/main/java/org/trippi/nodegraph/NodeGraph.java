package org.trippi.nodegraph;

import org.jrdf.graph.Graph;
import org.jrdf.graph.GraphException;
import org.jrdf.graph.Node;
import org.jrdf.graph.mem.GraphImpl;
import org.trippi.nodegraph.impl.NodeGraphImpl;

/**
 * A <code>Graph</code> that provides direct node access.
 *
 * This extension of the Graph interface is intended to simplify the task 
 * of node-by-node graph traversal.
 *
 * @author cwilper@cs.cornell.edu
 */
public abstract class NodeGraph implements Graph {

	private static final long serialVersionUID = 1L;

	/**
     * Get an instance that wraps the given JRDF Graph, or a new memory-backed
     * instance if <code>graph</code> is * given as <code>null</code>.
     *
     * If the given <code>graph</code> is already an instance of 
     * <code>NodeGraph</code>, it will simply be casted and returned.
     */
    public static NodeGraph getInstance(Graph graph) 
            throws GraphException {

        if (graph == null) {
            return new NodeGraphImpl(new GraphImpl());
        } else if (graph instanceof NodeGraph) {
            return (NodeGraph) graph;
        } else {
            return new NodeGraphImpl(graph);
        }
    }

    /**
     * Iterate distinct subjects of triples in the graph with the given 
     * predicate and object.
     *
     * If either parameter is given as <code>null</code> it will
     * be treated as an unconstrained value (all will match).
     */
    public abstract NodeResults findSubjects(Node predicate,
                                             Node object) 
            throws GraphException;

    /**
     * Iterate distinct predicates of triples in the graph with the given 
     * subject and object.
     *
     * If either parameter is given as <code>null</code> it will
     * be treated as an unconstrained value (all will match).
     */
    public abstract NodeResults findPredicates(Node subject,
                                               Node object) 
            throws GraphException;

    /**
     * Iterate distinct objects of triples in the graph with the given 
     * subject and predicate.
     *
     * If either parameter is given as <code>null</code> it will
     * be treated as an unconstrained value (all will match).
     */
    public abstract NodeResults findObjects(Node subject,
                                            Node predicate) 
            throws GraphException;

    /**
     * Iterate triples in the graph that match a given subject, predicate and object.
     * 
     * If any parameter is given as <code>null</code> it will
     * be treated as an unconstrained value (all will match).
     */
    public abstract TripleResults findTriples(Node subject, 
                                              Node predicate, 
                                              Node object)
            throws GraphException;

}
