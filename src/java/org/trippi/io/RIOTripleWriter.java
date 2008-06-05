package org.trippi.io;

import java.util.Iterator;
import java.util.Map;

import org.jrdf.graph.BlankNode;
import org.jrdf.graph.Literal;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.jrdf.graph.URIReference;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;

/**
 * Adapter for using Sesame's RIO RdfDocumentWriters for triple serialization.
 */
public class RIOTripleWriter extends TripleWriter {

    private RDFWriter m_writer;
    private org.openrdf.model.impl.ValueFactoryImpl m_rioFactory = 
            new org.openrdf.model.impl.ValueFactoryImpl();

    public RIOTripleWriter(RDFWriter writer, Map<String, String> aliases) throws TrippiException {
        try {
            m_writer = writer;
            Iterator<String> iter = aliases.keySet().iterator();
            while (iter.hasNext()) {
                String prefix = iter.next();
                String name = aliases.get(prefix);
                m_writer.handleNamespace(prefix, name);
            }
        } catch (RDFHandlerException e) {
            throw new TrippiException("Error setting up RIOTripleWriter", e);
        }
    }

    public int write(TripleIterator iter) throws TrippiException {
        try {
            m_writer.startRDF();
            int count = 0;
            while (iter.hasNext()) {
                Triple triple = iter.next();
                // write the triple to the rio writer using rio equivalents
                m_writer.handleStatement(new StatementImpl(rioResource(triple.getSubject()),
                                        rioURI(triple.getPredicate()),
                                        rioValue(triple.getObject())));
                count++;
            }
            m_writer.endRDF();
            iter.close();
            return count;
        } catch (RDFHandlerException e) {
            throw new TrippiException("Error setting up RIOTripleWriter", e);
        }
    }

    private org.openrdf.model.Resource rioResource(SubjectNode s) {
        if (s instanceof BlankNode) {
            return m_rioFactory.createBNode("" + s.hashCode());
        } else {
            return m_rioFactory.createURI(((URIReference) s).getURI().toString());
        }
    }

    private org.openrdf.model.URI rioURI(PredicateNode p) {
        return m_rioFactory.createURI(((URIReference) p).getURI().toString());
    }

    private org.openrdf.model.Value rioValue(ObjectNode o) {
        if (o instanceof BlankNode) {
            return m_rioFactory.createBNode("" + o.hashCode());
        } else if (o instanceof Literal) {
            Literal l = (Literal) o;
            if (l.getDatatypeURI() != null) {
                return m_rioFactory.createLiteral(l.getLexicalForm(), 
                                                  m_rioFactory.createURI(l.getDatatypeURI().toString()));
            } else if (l.getLanguage() != null && !l.getLanguage().equals("")) {
                return m_rioFactory.createLiteral(l.getLexicalForm(), l.getLanguage());
            } else {
                return m_rioFactory.createLiteral(l.getLexicalForm());
            }
        } else {
            return m_rioFactory.createURI(((URIReference) o).getURI().toString());
        }
    }

}
