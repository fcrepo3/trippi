package org.trippi.io;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jrdf.graph.GraphElementFactoryException;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trippi.RDFUtil;
import org.trippi.TrippiException;
import org.trippi.TrippiIterator;
import org.trippi.impl.base.AliasManager;
import org.trippi.io.transform.Transformer;
import org.trippi.io.transform.impl.SimpleTrippiIterator;
import org.trippi.io.transform.impl.TripleReflector;

/**
 * An iterator over triples parsed by a RIO rdf parser.
 *
 * @author armintor@gmail.com
 */
public class SimpleParsingContext<T> implements RDFHandler {

    private static final Logger logger =
        LoggerFactory.getLogger(SimpleParsingContext.class.getName());

    protected RDFUtil m_util;

    protected int m_tripleCount = 0;

    protected AliasManager m_aliases = new AliasManager();

    protected final HashSet<T> m_triples =
            new HashSet<T>();
    
    private final Transformer<T> m_transform;
    
    /**
     * Initialize the iterator by starting the parsing thread.
     * @throws IOException 
     * @throws RDFHandlerException 
     * @throws RDFParseException 
     */
    protected SimpleParsingContext(InputStream in, 
                RDFParser parser, 
                String baseURI,
                Transformer<T> transform)
        throws TrippiException, RDFParseException, RDFHandlerException, IOException {
        parser.setRDFHandler(this);
        parser.setVerifyData(true);
        parser.setStopAtFirstError(false);
        this.m_transform = transform;
        try { m_util = new RDFUtil(); } catch (Exception e) { } // won't happen
        try {
            parser.parse(in, baseURI);
        } finally {
            try {
                in.close();
            } catch (IOException ioe) {
                // just logging any additional exceptions
                logger.warn(ioe.getMessage());
            }
        }
    }
    
    public Set<T> getSet() {
        return m_triples;
    }
    
    public TrippiIterator<T> getIterator() {
        return new SimpleTrippiIterator<T>(m_triples);
    }

    @Override
    public void handleNamespace(String prefix, String uri) {
        if (prefix == null || prefix.equals("")) {

        } else {
            m_aliases.addAlias(prefix, uri);
        }
    }
    
    public Map<String, String> getAliasMap() {
        return m_aliases.getAliasMap();
    }

    /**
     * Handle a statement from the parser.
     * @throws URISyntaxException 
     * @throws GraphElementFactoryException 
     */
    public void handleStatement(org.openrdf.model.Resource subject,
                                org.openrdf.model.URI predicate,
                                org.openrdf.model.Value object) 
            throws GraphElementFactoryException,
            URISyntaxException {
        // first, convert the rio statement to a jrdf triple
        Triple triple = null;
            triple = m_util.createTriple( subjectNode(subject),
                                          predicateNode(predicate),
                                          objectNode(object));
        m_triples.add(m_transform.transform(triple));
    }


    private SubjectNode subjectNode(org.openrdf.model.Resource subject) 
            throws GraphElementFactoryException,
                   URISyntaxException {
        if (subject instanceof org.openrdf.model.URI) {
            return m_util.createResource( new URI(((org.openrdf.model.URI) subject).stringValue()) );
        } else {
            return m_util.createResource(((org.openrdf.model.BNode) subject).getID().hashCode());
        }
    }

    private PredicateNode predicateNode(org.openrdf.model.URI predicate)
            throws GraphElementFactoryException,
                   URISyntaxException {
        return m_util.createResource( new URI((predicate).stringValue()) );
    }

    private ObjectNode objectNode(org.openrdf.model.Value object)
            throws GraphElementFactoryException,
                   URISyntaxException {
        if (object instanceof org.openrdf.model.URI) {
            return m_util.createResource( new URI(((org.openrdf.model.URI) object).stringValue()) );
        } else if (object instanceof  org.openrdf.model.Literal) {
            org.openrdf.model.Literal lit = (org.openrdf.model.Literal) object;
            org.openrdf.model.URI uri = lit.getDatatype();
            String lang = lit.getLanguage();
            if (uri != null) {
                // typed 
                return m_util.createLiteral(lit.getLabel(), new URI(uri.toString()));
            } else if (lang != null && !lang.equals("")) {
                // local
                return m_util.createLiteral(lit.getLabel(), lang);
            } else {
                // plain
                return m_util.createLiteral(lit.getLabel());
            }
        } else {
            return m_util.createResource(((org.openrdf.model.BNode) object).getID().hashCode());
        }
    }

    public void endRDF() throws RDFHandlerException {
        // no-op    
    }

    public void handleComment(String arg0) throws RDFHandlerException {
        // no-op    
    }

    public void handleStatement(Statement st)
            throws RDFHandlerException {
        // first, convert the rio statement to jrdf nodes
        try {
            m_triples.add(m_transform.transform(
                    subjectNode(st.getSubject()),
                            predicateNode(st.getPredicate()),
                                    objectNode(st.getObject()),
                                    m_util
                                    ));
        } catch (GraphElementFactoryException e) {
            throw new RDFHandlerException(e.getMessage(), e);
        } catch (TrippiException e) {
            throw new RDFHandlerException(e.getMessage(), e);
        } catch (URISyntaxException e) {
            throw new RDFHandlerException(e.getMessage(), e);
        }
    }

    public void startRDF() throws RDFHandlerException {
        // no-op    
    }

    public static SimpleParsingContext<Triple>
        parse(InputStream in, 
            RDFParser parser, 
            String baseURI) throws TrippiException, RDFParseException,
            RDFHandlerException, IOException {
        return new SimpleParsingContext<Triple>(in, parser, baseURI, TripleReflector.instance);
    }

    public static <T> SimpleParsingContext<T>
    parse(InputStream in, 
            RDFParser parser, 
            String baseURI,
            Transformer<T> transform) throws TrippiException, RDFParseException,
            RDFHandlerException, IOException {
    return new SimpleParsingContext<T>(in, parser, baseURI, transform);
}
}
