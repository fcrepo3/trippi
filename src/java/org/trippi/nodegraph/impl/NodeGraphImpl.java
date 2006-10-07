package org.trippi.nodegraph.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.util.*;

import org.jrdf.graph.Graph;
import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.GraphException;
import org.jrdf.graph.Node;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.jrdf.graph.TripleFactory;
import org.jrdf.util.ClosableIterator;

import org.trippi.nodegraph.NodeResults;
import org.trippi.nodegraph.NodeGraph;
import org.trippi.nodegraph.TripleResults;

public class NodeGraphImpl extends NodeGraph {

    private Graph _graph;

    public NodeGraphImpl(Graph graph) {
        _graph = graph;
    }

    ////////////////////////////////////////////////////////////////////////////////

    public NodeResults findSubjects(Node p,
                                         Node o) 
            throws GraphException {

        if ( ( (p == null) || (p instanceof PredicateNode) ) &&
             ( (o == null) || (o instanceof ObjectNode   ) ) ) {
            return new NodeResultsImpl(
                    new NodeIterator(find(null, (PredicateNode) p, (ObjectNode) o),
                                     NodeIterator.SUBJECTS,
                                     ((p != null) && (o != null))));
        } else {
            return new NodeResultsImpl(EmptyClosableIterator.INSTANCE);
        }
    }

    public NodeResults findPredicates(Node s,
                                      Node o) 
            throws GraphException {

        if ( ( (s == null) || (s instanceof SubjectNode) ) &&
             ( (o == null) || (o instanceof ObjectNode ) ) ) {
            return new NodeResultsImpl(
                    new NodeIterator(find((SubjectNode) s, null, (ObjectNode) o),
                                     NodeIterator.PREDICATES,
                                     ((s != null) && (o != null))));
        } else {
            return new NodeResultsImpl(EmptyClosableIterator.INSTANCE);
        }
    }

    public NodeResults findObjects(Node s,
                                   Node p) 
            throws GraphException {

        if ( ( (s == null) || (s instanceof SubjectNode  ) ) &&
             ( (p == null) || (p instanceof PredicateNode) ) ) {
            return new NodeResultsImpl(
                    new NodeIterator(find((SubjectNode) s, (PredicateNode) p, null),
                                     NodeIterator.OBJECTS,
                                     ((s != null) && (p != null))));
        } else {
            return new NodeResultsImpl(EmptyClosableIterator.INSTANCE);
        }
    }

    public TripleResults findTriples(Node s, Node p, Node o)
            throws GraphException {

        if ( ( (s == null) || (s instanceof SubjectNode  ) ) &&
             ( (p == null) || (p instanceof PredicateNode) ) &&
             ( (o == null) || (o instanceof ObjectNode   ) ) ) {
            return new TripleResultsImpl(
                    find((SubjectNode) s, (PredicateNode) p, (ObjectNode) o));
        } else {
            return new TripleResultsImpl(EmptyClosableIterator.INSTANCE);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    // from org.jrdf.graph.Graph
    public boolean contains(SubjectNode subject, 
                            PredicateNode predicate,
                            ObjectNode object) 
            throws GraphException {

        return _graph.contains(subject, predicate, object);
    }

    // from org.jrdf.graph.Graph
    public boolean contains(Triple triple) 
            throws GraphException {

        return _graph.contains(triple);
    }

    // from org.jrdf.graph.Graph
    public ClosableIterator find(SubjectNode subject, 
                                 PredicateNode predicate,
                                 ObjectNode object)
            throws GraphException {

        return _graph.find(subject, predicate, object);
    }

    // from org.jrdf.graph.Graph
    public ClosableIterator find(Triple triple) 
            throws GraphException {

        return _graph.find(triple);
    }

    // from org.jrdf.graph.Graph
    public void add(SubjectNode subject, 
                    PredicateNode predicate, 
                    ObjectNode object)
            throws GraphException {

        _graph.add(subject, predicate, object);
    }

    // from org.jrdf.graph.Graph
    public void add(Triple triple) 
            throws GraphException {

        _graph.add(triple);
    }

    // from org.jrdf.graph.Graph
    public void add(Iterator triples) 
            throws GraphException {

        _graph.add(triples);
    }

    // from org.jrdf.graph.Graph
    public void remove(SubjectNode subject, 
                       PredicateNode predicate, 
                       ObjectNode object)
            throws GraphException {

        _graph.remove(subject, predicate, object);
    }

    // from org.jrdf.graph.Graph
    public void remove(Triple triple) 
            throws GraphException {

        _graph.remove(triple);
    }

    // from org.jrdf.graph.Graph
    public void remove(Iterator triples) 
            throws GraphException {

        _graph.remove(triples);
    }

    // from org.jrdf.graph.Graph
    public GraphElementFactory getElementFactory() {

        return _graph.getElementFactory();
    }

    // from org.jrdf.graph.Graph
    public TripleFactory getTripleFactory() {

        return _graph.getTripleFactory();
    }

    // from org.jrdf.graph.Graph
    public long getNumberOfTriples() 
            throws GraphException {

        return _graph.getNumberOfTriples();
    }

    // from org.jrdf.graph.Graph
    public boolean isEmpty() 
            throws GraphException {

        return _graph.isEmpty();
    }

    // from org.jrdf.graph.Graph
    // defined in JRDF 3.4, but not 3.2 or 3.3
    public void close() {
        try {
            Method closeMethod = _graph.getClass().getMethod("close", 
                                                             new Class[0]);
            closeMethod.invoke(_graph, new Object[0]); 
        } catch (InvocationTargetException e) {
            Throwable u = e.getCause();
            if (u instanceof RuntimeException) {
                throw (RuntimeException) u;
            } else {
                throw new RuntimeException("Unexpected checked exception "
                        + "encountered while reflectively invoking "
                        + "graph.close()", u);
            }
        } catch (SecurityException e) {
            throw e;
        } catch (Throwable th) {
            // must be using JRDF prior to 3.4
        }
    }

}
