package org.trippi.io.transform.impl;

import org.jrdf.graph.GraphElementFactoryException;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.trippi.RDFUtil;
import org.trippi.TrippiException;
import org.trippi.io.transform.Transformer;


public class TripleReflector implements Transformer<Triple> {
    
    public final static TripleReflector instance = new TripleReflector();
    
    private TripleReflector() {
    }
    
    @Override
    public Triple transform(Triple input) {
        return input;
    }

    @Override
    public Triple transform(SubjectNode s, PredicateNode p, ObjectNode o, RDFUtil u)
    throws TrippiException {
        try {
            return u.createTriple(s, p, o);
        } catch (GraphElementFactoryException e) {
            throw new TrippiException(e.getMessage(), e);
        }
    }

}
