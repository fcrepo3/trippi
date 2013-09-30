package org.trippi.io.transform;

import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.trippi.TrippiException;

/**
 * A way to introduce typed visitors to trippi's RDF parsing.
 * The implementation must be usable both in RDF-XML parsing
 * (org.openrdf.*) and in querying triple stores (org.jrdf.*)
 * @author armintor@gmail.com
 *
 * @param <T>
 */
public interface Transformer<T> {
    
    public T transform(Triple input) throws TrippiException;
    
    public T transform(SubjectNode s, PredicateNode p, ObjectNode o)
    throws TrippiException;

    public T transform(Statement s, GraphElementFactory u)
    throws RDFHandlerException;
}
