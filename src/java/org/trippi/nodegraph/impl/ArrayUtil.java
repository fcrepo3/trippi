package org.trippi.nodegraph.impl;

import java.util.ArrayList;
import java.util.List;

import org.jrdf.graph.Node;
import org.jrdf.graph.Triple;
import org.jrdf.util.ClosableIterator;

public abstract class ArrayUtil {

    private static final Node[]   _EMPTY_NODES   = new Node[0];
    private static final Triple[] _EMPTY_TRIPLES = new Triple[0];

    public static Node[] getNodes(ClosableIterator<?> iter) {
        return getList(iter).toArray(_EMPTY_NODES);
    }

    public static Triple[] getTriples(ClosableIterator<?> iter) {
        return getList(iter).toArray(_EMPTY_TRIPLES);
    }

    private static List<?> getList(ClosableIterator<?> iter) {
        try {
            ArrayList list = new ArrayList();
            while (iter.hasNext()) {
                list.add(iter.next());
            }
            return list;
        } finally {
            iter.close();
        }
    }

}
