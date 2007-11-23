package org.trippi.io;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.jrdf.graph.BlankNode;
import org.jrdf.graph.Literal;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.jrdf.graph.URIReference;
import org.openrdf.rio.RdfDocumentWriter;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;

/**
 * Adapter for using Sesame's RIO RdfDocumentWriters for triple serialization.
 */
public class RIOTripleWriter extends TripleWriter {

    private RdfDocumentWriter m_writer;
    private org.openrdf.model.impl.ValueFactoryImpl m_rioFactory = 
            new org.openrdf.model.impl.ValueFactoryImpl();

    public RIOTripleWriter(RdfDocumentWriter writer, Map<String, String> aliases) throws TrippiException {
        try {
            m_writer = writer;
            Iterator<String> iter = aliases.keySet().iterator();
            while (iter.hasNext()) {
                String prefix = iter.next();
                String name = aliases.get(prefix);
                m_writer.setNamespace(prefix, name);
            }
        } catch (IOException e) {
            throw new TrippiException("Error setting up RIOTripleWriter", e);
        }
    }

    public int write(TripleIterator iter) throws TrippiException {
        try {
            m_writer.startDocument();
            int count = 0;
            while (iter.hasNext()) {
                Triple triple = iter.next();
                // write the triple to the rio writer using rio equivalents
                m_writer.writeStatement(rioResource(triple.getSubject()),
                                        rioURI(triple.getPredicate()),
                                        rioValue(triple.getObject()));
                count++;
            }
            m_writer.endDocument();
            iter.close();
            return count;
        } catch (IOException e) {
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
