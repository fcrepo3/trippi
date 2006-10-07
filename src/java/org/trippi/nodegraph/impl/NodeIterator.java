package org.trippi.nodegraph.impl;

import java.util.*;

import gnu.trove.TIntHashSet;

import org.jrdf.graph.Node;
import org.jrdf.graph.Triple;
import org.jrdf.util.ClosableIterator;

public class NodeIterator implements ClosableIterator {

    public static final int SUBJECTS = 0;
    public static final int PREDICATES = 1;
    public static final int OBJECTS = 2;

    private ClosableIterator _triples;
    private int _type;

    private Node _nextNode;

    private TIntHashSet _seen;
    private boolean _closed;

    public NodeIterator(ClosableIterator triples,
                        int type,
                        boolean alreadyDistinct) {
        _triples = triples;
        _type = type;

        if (!alreadyDistinct) {
            _seen = new TIntHashSet();
        }

        _nextNode = getNextDistinctNode();
    }

    /**
     * Get the next distinct Node from the underlying ClosableIterator 
     * of Triple objects, or null if exhausted.
     */
    private Node getNextDistinctNode() {

        Node node = null;
        while (_triples.hasNext() && node == null) {
            Triple triple = (Triple) _triples.next();
            if (_type == SUBJECTS) {
                node = triple.getSubject();
            } else if (_type == PREDICATES) {
                node = triple.getPredicate();
            } else {
                node = triple.getObject();
            }
            if (!isDistinct(node)) {
                node = null;
            }
        }

        if (node == null) {
            // exhausted
            close();
        }
        return node;
    }

    /**
     * Return false if the given node has already been iterated.
     * Otherwise, add it to the already-iterated list and return true.
     */
    private boolean isDistinct(Node node) {

        if (_seen == null) {
            return true;
        } else {
            int uniqueId = node.hashCode();
            if (_seen.contains(uniqueId)) {
                return false;
            } else {
                _seen.add(uniqueId);
                return true;
            }
        }
    }

    public boolean hasNext() {
        return (_nextNode != null);
    }

    public Object next() {
        if (_nextNode == null) {
            throw new NoSuchElementException();
        } else {
            Node lastNode = _nextNode;
            _nextNode = getNextDistinctNode();
            return lastNode;
        }
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public boolean close() {
        if (!_closed) {
            _closed = _triples.close();
            if (_closed) {
                _triples = null;
            }
            _nextNode = null;
            _seen = null;
        }
        return _closed;
    }

}
