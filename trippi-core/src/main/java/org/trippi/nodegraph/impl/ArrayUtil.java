package org.trippi.nodegraph.impl;

import java.util.ArrayList;
import java.util.List;

import org.jrdf.graph.Node;
import org.jrdf.graph.Triple;
import org.jrdf.util.ClosableIterator;

public abstract class ArrayUtil {

    private static final Node[]   _EMPTY_NODES   = new Node[0];
    private static final Triple[] _EMPTY_TRIPLES = new Triple[0];

    @Deprecated
    public static Node[] getNodes(ClosableIterator<Node> iter) {
        return getList(iter).toArray(_EMPTY_NODES);
    }

    @Deprecated
    public static Triple[] getTriples(ClosableIterator<Triple> iter) {
        return getList(iter).toArray(_EMPTY_TRIPLES);
    }
    
    @SuppressWarnings("unchecked")
    public static <T> T[] toArray(ClosableIterator<T> iter) {
        return (T[]) getList(iter).toArray();
    }

    private static <T> List<T> getList(ClosableIterator<T> iter) {
        try {
            ArrayList<T> list = new ArrayList<T>();
            while (iter.hasNext()) {
                list.add(iter.next());
            }
            return list;
        } finally {
            iter.close();
        }
    }

}
