package org.trippi.io.transform;

import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.trippi.RDFUtil;
import org.trippi.TrippiException;


public interface Transformer<T> {
    
    public T transform(Triple input);

    public T transform(SubjectNode s, PredicateNode p, ObjectNode o, RDFUtil u)
    throws TrippiException;
}
