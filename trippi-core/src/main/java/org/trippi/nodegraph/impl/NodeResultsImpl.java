package org.trippi.nodegraph.impl;

import org.jrdf.graph.Node;
import org.jrdf.util.ClosableIterator;

import org.trippi.nodegraph.NodeResults;

public class NodeResultsImpl extends CountingResultIterator<Node>
                             implements NodeResults {

    public NodeResultsImpl(ClosableIterator<Node> nodes) {

        super(nodes);
    }

    public Node first() {

        try {
            if (hasNext()) {
                return (Node) next();
            } else {
                return null;
            }
        } finally {
            close();
        }
    }

    @SuppressWarnings("deprecation")
    public Node[] all() {

        return ArrayUtil.getNodes(this);
    }

}
