package org.trippi.io.transform.impl;

import java.net.URI;
import java.net.URISyntaxException;

import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.GraphElementFactoryException;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.trippi.TrippiException;
import org.trippi.impl.RDFFactories;
import org.trippi.io.transform.Transformer;


public class Identity implements Transformer<Triple> {
    
    public final static Identity instance = new Identity();
    
    private Identity() {
    }
    
    @Override
    public Triple transform(Triple input) {
        return input;
    }
    
    @Override
    public Triple transform(SubjectNode s, PredicateNode p, ObjectNode o)
            throws TrippiException {
        try {
            return RDFFactories.createTriple(s, p, o);
        } catch (GraphElementFactoryException e) {
            throw new TrippiException(e.getMessage(), e);
        }
    }

    @Override
    public Triple transform(Statement o, GraphElementFactory u)
    throws RDFHandlerException {
        try {
            return u.createTriple(
                    u.createResource(URI.create(o.getSubject().stringValue())),
                    u.createResource(URI.create(o.getPredicate().stringValue())),
                    RDFFactories.objectNode(o.getObject()));
        } catch (GraphElementFactoryException e) {
            throw new RDFHandlerException(e.getMessage(), e);
        } catch (URISyntaxException e) {
            throw new RDFHandlerException(e.getMessage(), e);
        }
    }
}
