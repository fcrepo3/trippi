package org.trippi.nodegraph.impl;

import java.util.NoSuchElementException;

import org.jrdf.graph.Node;
import org.jrdf.graph.Triple;
import org.jrdf.util.ClosableIterator;

public class EmptyClosableIterator<T> implements ClosableIterator<T> {

    public static final EmptyClosableIterator<Node> NODE_INSTANCE =
            new EmptyClosableIterator<Node>();

    public static final EmptyClosableIterator<Triple> TRIPLE_INSTANCE =
            new EmptyClosableIterator<Triple>();

    private EmptyClosableIterator() {
    }

    public boolean hasNext() {
        return false;
    }

    public T next() throws NoSuchElementException {
        throw new NoSuchElementException();
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public boolean close() {
        return true;
    }

}
