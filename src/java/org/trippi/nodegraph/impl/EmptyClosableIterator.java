package org.trippi.nodegraph.impl;

import java.util.NoSuchElementException;

import org.jrdf.util.ClosableIterator;

public class EmptyClosableIterator implements ClosableIterator {

    public static final EmptyClosableIterator INSTANCE = new EmptyClosableIterator();

    private EmptyClosableIterator() {
    }

    public boolean hasNext() {
        return false;
    }

    public Object next() throws NoSuchElementException {
        throw new NoSuchElementException();
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public boolean close() {
        return true;
    }

}
