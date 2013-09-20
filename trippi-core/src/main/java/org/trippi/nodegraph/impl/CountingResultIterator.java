package org.trippi.nodegraph.impl;

import java.util.NoSuchElementException;

import org.jrdf.util.ClosableIterator;

public class CountingResultIterator<T> implements ClosableIterator<T> {

    private ClosableIterator<T> _iter;
    private boolean _closed;

    public CountingResultIterator(ClosableIterator<T> iter) {
        
        _iter = iter;
    }

    public int count() {
       
        try {
            int count = 0;
            while (hasNext()) {
                next();
                count++;
            }
            return count;
        } finally {
            close();
        }
    }

    // from java.util.Iterator
    public boolean hasNext() {
        return _iter.hasNext();
    }

    // from java.util.Iterator
    public T next() throws NoSuchElementException {
        return _iter.next();
    }

    // from java.util.Iterator
    public void remove() {
        throw new UnsupportedOperationException();
    }

    // from org.jrdf.util.ClosableIterator
    public boolean close() {

        if (!_closed) {
            _closed = _iter.close();
        }
        return _closed;
    }

}
