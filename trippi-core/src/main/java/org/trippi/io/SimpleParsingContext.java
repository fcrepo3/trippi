package org.trippi.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import org.jrdf.graph.Triple;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trippi.TrippiException;
import org.trippi.TrippiIterator;
import org.trippi.impl.RDFFactories;
import org.trippi.io.transform.Transformer;
import org.trippi.io.transform.impl.Identity;
import org.trippi.io.transform.impl.SimpleTrippiIterator;

/**
 * An iterator over triples parsed by a RIO rdf parser.
 *
 * @author armintor@gmail.com
 */
public class SimpleParsingContext<T> implements RDFHandler {

    private static final Logger logger =
        LoggerFactory.getLogger(SimpleParsingContext.class.getName());

    protected int m_tripleCount = 0;

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
    /**
     * Because the statements resolve the prefixes, we do not need
     * to do anything with the prefix data (unlike on serialization)
     */
    public void handleNamespace(String prefix, String uri) {
        // no-op    
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
        m_triples.add(m_transform.transform(st, RDFFactories.FACTORY));
    }

    public void startRDF() throws RDFHandlerException {
        // no-op    
    }

    public static SimpleParsingContext<Triple>
        parse(InputStream in, 
            RDFParser parser, 
            String baseURI) throws TrippiException, RDFParseException,
            RDFHandlerException, IOException {
        return new SimpleParsingContext<Triple>(in, parser, baseURI, Identity.instance);
    }

    public static <T> SimpleParsingContext<T> parse(InputStream in, 
            RDFParser parser, 
            String baseURI,
            Transformer<T> transform)
            throws TrippiException, RDFParseException,
            RDFHandlerException, IOException {
        return new SimpleParsingContext<T>(in, parser, baseURI, transform);
    }
}
