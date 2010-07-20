package org.trippi.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jrdf.graph.GraphElementFactoryException;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.trippi.RDFFormat;
import org.trippi.RDFUtil;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;

/**
 * An iterator over triples parsed by a RIO rdf parser.
 *
 * @author cwilper@cs.cornell.edu
 */
public class RIOTripleIterator extends TripleIterator 
                               implements RDFHandler, Runnable {

    private static final Logger logger =
        LoggerFactory.getLogger(RIOTripleIterator.class.getName());

    private InputStream m_in;
    private RDFParser m_parser;
    private String m_baseURI;

    private Triple m_bucket; // shared between parser/consumer threads
    private Triple m_next;

    private boolean m_closed = false;
    private boolean m_finishedParsing = false;

    private Exception m_parseException = null;

    private RDFUtil m_util;

    private int m_tripleCount = 0;

    private Map<String, String> m_aliases;

    /**
     * Initialize the iterator by starting the parsing thread.
     */
    public RIOTripleIterator(InputStream in, 
                             RDFParser parser, 
                             String baseURI) throws TrippiException {
        m_in = in;
        m_parser = parser;
        m_aliases = new HashMap<String, String>();
        m_baseURI = baseURI;
        m_parser.setRDFHandler(this);
        m_parser.setVerifyData(true);
        m_parser.setStopAtFirstError(false);
        try { m_util = new RDFUtil(); } catch (Exception e) { } // won't happen
        Thread parserThread = new Thread(this);
        if (logger.isDebugEnabled()) {
        	logger.debug("Starting parse thread");
        }
        parserThread.start();
        m_next = getNext();
    }

    public void handleNamespace(String prefix, String uri) {
        if (prefix == null || prefix.equals("")) {

        } else {
            m_aliases.put(prefix, uri);
            setAliasMap(m_aliases);
        }
    }

    /**
     * Get the next triple out of the bucket and return it.
     *
     * If the bucket is empty:
     *   1) If the parser is finished, 
     *      throw an exception if m_parseException != null.
     *      Otherwise, return null.
     *   2) If the parser is not finished, wait for
     *      a) The parser to finish, or
     *      b) Another triple to arrive in the bucket.
     */
    private synchronized Triple getNext() throws TrippiException { 
        // wait until the bucket has a value or 2) finished parsing is true
        while (m_bucket == null && !m_finishedParsing) {
            try {
                // wait for Producer to put value
                wait(5);
            } catch (InterruptedException e) {
            }
        }
        if (m_finishedParsing && m_parseException != null) {
            throw new TrippiException("RDF Parse Error.", m_parseException);
        }
        if (m_bucket == null) {
        	if (logger.isDebugEnabled()) {
        		logger.debug("Finished parsing " + m_tripleCount + " triples.");
        	}
            return null; // parser finished normally, no more triples
        } else {
            Triple triple = m_bucket;
            m_bucket = null;
            m_tripleCount++;
            if (m_tripleCount % 1000 == 0) {
            	if (logger.isDebugEnabled()) {
            		logger.debug("Iterated " + m_tripleCount + ", mem free = " + Runtime.getRuntime().freeMemory() );
            	}
            }
            notifyAll(); // notify parser that the bucket is ready for another
            return triple;
        }
    }
    
    @Override
    public boolean hasNext() throws TrippiException {
        return (m_next != null);
    }
    
    @Override
    public Triple next() throws TrippiException {
        if (m_next == null) return null;
        Triple last = m_next;
        m_next = getNext();
        return last;
    }
    
    @Override
    public void close() throws TrippiException {
        if (!m_closed) {
            m_closed = true;
        }
    }

    ////////////////// Parser Thread Methods ///////////////////////////

    /**
     * The main method of the background thread.
     *
     * This starts the parsing and exits when an error occurs or the parsing 
     * is finished.
     */
    public void run() {
        try {
            m_parser.parse(m_in, m_baseURI);
        } catch (Exception e) {
            m_parseException = e;
        } finally {
            m_finishedParsing = true;
            try { m_in.close(); } catch (IOException e) { }
        }
    }

    /**
     * Handle a statement from the parser.
     * @throws URISyntaxException 
     * @throws GraphElementFactoryException 
     */
    public void handleStatement(org.openrdf.model.Resource subject,
                                org.openrdf.model.URI predicate,
                                org.openrdf.model.Value object) throws GraphElementFactoryException, URISyntaxException {
        // first, convert the rio statement to a jrdf triple
        Triple triple = null;
            triple = m_util.createTriple( subjectNode(subject),
                                          predicateNode(predicate),
                                          objectNode(object));
        put(triple); // locks until m_bucket is free or close() has been called
    }

    private synchronized void put(Triple triple) {
        // wait until the bucket is free or 2) close has been called
        while (m_bucket != null && !m_closed) {
            try {
                // wait for notification, which will happen if
                // 1) the bucket is emptied by the consumer, or
                // 2) close has been called
                wait(5);
            } catch (InterruptedException e) {
            }
        }
        if (m_closed) {
            return;
        }
        m_bucket = triple;
        notifyAll();  // notify the consumer that the bucket has a triple
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

    public static void main(String[] args) throws Exception {
        File f = new File(args[0]);
        String baseURI = "http://localhost/";
        RDFFormat format = RDFFormat.forName(args[1]);
        RDFParser parser;
        if (format == RDFFormat.RDF_XML) {
            parser = new org.openrdf.rio.rdfxml.RDFXMLParser();
        } else if (format == RDFFormat.TURTLE) {
            parser = new org.openrdf.rio.turtle.TurtleParser();
        } else if (format == RDFFormat.N_TRIPLES) {
            parser = new org.openrdf.rio.ntriples.NTriplesParser();
        } else {
            throw new TrippiException("Unsupported input format: " + format.getName());
        }
        TripleIterator iter = new RIOTripleIterator(new FileInputStream(f),
                                                    parser,
                                                    baseURI);
        try {
            iter.toStream(System.out, RDFFormat.forName(args[2]));
        } finally {
            iter.close();
        }
    }

    public void endRDF() throws RDFHandlerException {
        // TODO Auto-generated method stub
        
    }

    public void handleComment(String arg0) throws RDFHandlerException {
        // TODO Auto-generated method stub
        
    }

    public void handleStatement(Statement st) throws RDFHandlerException {
        // first, convert the rio statement to a jrdf triple
        Triple triple = null;
            try {
                triple = m_util.createTriple( subjectNode(st.getSubject()),
                                              predicateNode(st.getPredicate()),
                                              objectNode(st.getObject()));
            } catch (GraphElementFactoryException e) {
                throw new RDFHandlerException(e.getMessage(), e);
            } catch (URISyntaxException e) {
                throw new RDFHandlerException(e.getMessage(), e);
            }
        put(triple); // locks until m_bucket is free or close() has been called
    }

    public void startRDF() throws RDFHandlerException {
        // TODO Auto-generated method stub
        
    }

}
