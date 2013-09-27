package org.trippi.io.transform;

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
import org.trippi.impl.base.AliasManager;

/**
 * An iterator over triples parsed by a RIO rdf parser.
 *
 * @author armintor@gmail.com
 */
public class TransformTripleParsingContext<T> implements RDFHandler {

    private static final Logger logger =
        LoggerFactory.getLogger(TransformTripleParsingContext.class.getName());

    private RDFUtil m_util;

    protected int m_tripleCount = 0;

    private AliasManager m_aliases = new AliasManager();
    
    private Transformer<T> m_transform;

    private final HashSet<T> m_triples =
            new HashSet<T>();
    
    /**
     * Initialize the iterator by starting the parsing thread.
     * @throws IOException 
     * @throws RDFHandlerException 
     * @throws RDFParseException 
     */
    private TransformTripleParsingContext(InputStream in, 
                             RDFParser parser, 
                             String baseURI,
                             Transformer<T> transform)
            throws TrippiException, RDFParseException, RDFHandlerException, IOException {
        m_transform = transform;
        parser.setRDFHandler(this);
        parser.setVerifyData(true);
        parser.setStopAtFirstError(false);
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
    
    public Set<T> getTransforms() {
        return m_triples;
    }
    
    @Override
    public void handleNamespace(String prefix, String uri) {
        if (prefix == null || prefix.equals("")) {

        } else {
            m_aliases.addAlias(prefix, uri);
        }
    }
    
    @Deprecated
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
        // first, convert the rio statement to a jrdf triple
        Triple triple = null;
        try {
            triple = m_util.createTriple( subjectNode(st.getSubject()),
                    predicateNode(st.getPredicate()),
                    objectNode(st.getObject()));
            m_triples.add(m_transform.transform(triple));
        } catch (GraphElementFactoryException e) {
            throw new RDFHandlerException(e.getMessage(), e);
        } catch (URISyntaxException e) {
            throw new RDFHandlerException(e.getMessage(), e);
        }
    }

    public void startRDF() throws RDFHandlerException {
        // no-op    
    }

    public static <T> TransformTripleParsingContext<T>
        parse(InputStream in, 
            RDFParser parser, 
            String baseURI,
            Transformer<T> transform )
            throws TrippiException, RDFParseException,
            RDFHandlerException, IOException {
        return new TransformTripleParsingContext<T>(
                in, parser, baseURI, transform);
    }
}
