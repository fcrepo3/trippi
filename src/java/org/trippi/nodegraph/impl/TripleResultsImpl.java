package org.trippi.nodegraph.impl;

import org.jrdf.graph.Triple;
import org.jrdf.util.ClosableIterator;

import org.trippi.nodegraph.TripleResults;

public class TripleResultsImpl extends CountingResultIterator
                               implements TripleResults {

    public TripleResultsImpl(ClosableIterator<?> triples) {

        super(triples);
    }

    public Triple first() {

        try {
            if (hasNext()) {
                return (Triple) next();
            } else {
                return null;
            }
        } finally {
            close();
        }
    }

    public Triple[] all() {

        return ArrayUtil.getTriples(this);
    }

}
