package org.trippi.impl.mpt;

import java.util.List;

import org.jrdf.graph.GraphElementFactoryException;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.nsdl.mptstore.query.QueryResults;
import org.nsdl.mptstore.rdf.Literal;
import org.nsdl.mptstore.rdf.Node;
import org.nsdl.mptstore.rdf.URIReference;
import org.trippi.RDFUtil;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;

public class MPTTripleIterator extends TripleIterator {

    private QueryResults _results;
    private RDFUtil _util;

    public MPTTripleIterator(QueryResults results) {
        _results = results;
        _util = new RDFUtil();
    }

    public boolean hasNext() {
        return _results.hasNext();
    }

    public Triple next() throws TrippiException {
        try {
            List result = (List) _results.next();
            Node n;
            
            n = (Node) result.get(0);
            SubjectNode subject = (SubjectNode) mptToJRDF(n);

            n = (Node) result.get(1);
            PredicateNode predicate = (PredicateNode) mptToJRDF(n);

            n = (Node) result.get(2);
            ObjectNode object = (ObjectNode) mptToJRDF(n);

            return _util.createTriple(subject, predicate, object);

        } catch (Exception e) {
            throw new TrippiException("Error getting next triple", e);
        }
    }

    public void close() {
        _results.close();
    }

    /**
     * Convert an MPT node to a JRDF node.
     */
    private org.jrdf.graph.Node mptToJRDF(Node mptNode) {

        try {
            if (mptNode instanceof URIReference) {
                URIReference mptURIReference = (URIReference) mptNode;
                return _util.createResource(mptURIReference.getURI());
            } else if (mptNode instanceof Literal) {
                Literal mptLiteral = (Literal) mptNode;
                if (mptLiteral.getLanguage() != null) {
                    return _util.createLiteral(mptLiteral.getValue(),
                                               mptLiteral.getLanguage());
                } else if (mptLiteral.getDatatype() != null) {
                    return _util.createLiteral(mptLiteral.getValue(),
                                               mptLiteral.getDatatype().getURI());
                } else {
                    return _util.createLiteral(mptLiteral.getValue());
                }
            } else {
                throw new RuntimeException("Unrecognized node type: " 
                        + mptNode.getClass().getName());
            }
        } catch (GraphElementFactoryException e) {
            throw new RuntimeException("Unable to create JRDF node", e);
        }
    }

}
