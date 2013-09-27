package org.trippi.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jrdf.graph.AbstractTriple;
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
    private static final FlagTriple FINISHED = new FlagTriple("FINISHED");
    private static final FlagTriple NEXT = new FlagTriple("NEXT");
    private InputStream m_in;
    private RDFParser m_parser;
    private String m_baseURI;

    // communicate between parser/consumer threads
    private Exchanger<Triple> m_bucket =
            new Exchanger<Triple>();
    
    private Triple m_next;

    private Exception m_parseException = null;

    private RDFUtil m_util;

    protected int m_tripleCount = 0;

    /**
     * Initialize the iterator by starting the parsing thread.
     */
    public RIOTripleIterator(InputStream in, 
                             RDFParser parser, 
                             String baseURI,
                             ExecutorService executor) throws TrippiException {
        m_in = in;
        m_parser = parser;
        m_baseURI = baseURI;
        m_parser.setRDFHandler(this);
        m_parser.setVerifyData(true);
        m_parser.setStopAtFirstError(false);
        try { m_util = new RDFUtil(); } catch (Exception e) { } // won't happen
        if (logger.isDebugEnabled()) {
        	logger.debug("Starting parse thread");
        }
        m_next = NEXT;
        executor.execute(this);
        setNext(NEXT, false);
    }

    @Override
    public void handleNamespace(String prefix, String uri) {
        if (prefix == null || prefix.equals("")) {

        } else {
            addAlias(prefix, uri);
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
    private void setNext(FlagTriple flag, boolean timeout) throws TrippiException { 
        // wait until the bucket has a value or 2) finished parsing is true
        if (m_next == null) return;
        if (m_next == FINISHED) return;
        Triple triple = null;
    	try{
    	    triple = (timeout) ?
    	            m_bucket.exchange(flag, 5, TimeUnit.MILLISECONDS):
    	            m_bucket.exchange(flag, 5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.debug("Interrupted, quitting");
            triple = FINISHED;
        } catch (TimeoutException e) {
            triple = FINISHED;
            logger.debug("Timed out, quitting");
        }
    	// we ignore RDFHandlingInterruptedException, as it is thrown in the
    	// special case that the iterator was closed before parsing was finished
        if (m_parseException != null &&
                ! (m_parseException instanceof RDFHandlingInterruptedException)) {
            throw new TrippiException("RDF Parse Error.", m_parseException);
        }
        if (triple == FINISHED) {
     		logger.debug("Finished parsing {} triples.", m_tripleCount);
            m_next = FINISHED; // parser finished normally, no more triples
        } else {
            m_tripleCount++;
            if (m_tripleCount % 1000 == 0) {
            	if (logger.isDebugEnabled()) {
            		logger.debug("Iterated {}, mem free = ",
            		        m_tripleCount,
            		        Runtime.getRuntime().freeMemory() );
            	}
            }
            m_next = triple;
        }
    }
    
    @Override
    public boolean hasNext() throws TrippiException {
        return (m_next != FINISHED);
    }
    
    @Override
    public Triple next() throws TrippiException {
        if (m_next == FINISHED) return null;
        Triple last = m_next;
        setNext(NEXT, true);
        if (m_next == FINISHED) {
            logger.debug("Got the {} flag from RIOTripleIterator.setNext", m_next);
        } else {
            logger.debug("got a triple: {} from RIOTripleIterator.setNext", m_next);
        }
        return last;
    }
    
    @Override
    public void close() throws TrippiException {
        // if processing was already completed
        if (m_next == FINISHED) {
            return;
        }
        /** signal the end of reading
        /* if the parsing thread is waiting, it will
         * throw a RDFHandlingInterruptedException
         * to exit parsing early. If the consuming thread
         * is somehow waiting it will stop and the producing thread
         * will eventually timeout.
        **/
        try {
            m_next = FINISHED;
            logger.debug("sending {} on RIOTripleIterator.close()", FINISHED);
            m_in.close();
            m_bucket.exchange(FINISHED, 1, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.debug("closing {} offered but interrupted", FINISHED);
        } catch (TimeoutException e) {
            logger.debug("closing {} offered but not accepted", FINISHED);
        } catch (IOException ioe) {
            throw new TrippiException(ioe.getMessage(), ioe);
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
        } catch (RDFHandlingInterruptedException e) {
        } catch (Exception e) {
            m_parseException = e;
        } finally {
            try {
                m_in.close();
                m_parser = null;
            } catch (IOException e) { }
        }
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
            URISyntaxException,
            RDFHandlingInterruptedException {
        // first, convert the rio statement to a jrdf triple
        Triple triple = null;
            triple = m_util.createTriple( subjectNode(subject),
                                          predicateNode(predicate),
                                          objectNode(object));
        put(triple); // locks until m_bucket is free or close() has been called
    }

    private void put(Triple triple) throws RDFHandlingInterruptedException {
        try {
            if (m_next == FINISHED) {
                String msg = "Refusing to put new values when processing is over";
                logger.debug(msg);
                throw new RDFHandlingInterruptedException(msg);
            }
            logger.debug("putting {} on Exchanger in RIOTripleIterator.put",
                    triple);
            triple = m_bucket.exchange(triple, 5, TimeUnit.SECONDS);
            logger.debug("got {} from Exchanger in RIOTripleIterator.put",
                    triple);
            if (triple == FINISHED) {
                String msg = "End of processing has been" +
                        " signalled from the consuming thread.";
                logger.debug(msg);
                throw new RDFHandlingInterruptedException(msg);
            }
        } catch (InterruptedException e) {
            logger.debug("putting {} interrupted", triple);
            throw new RDFHandlingInterruptedException(e);
        } catch (TimeoutException e) {
            logger.debug("putting {} timed out", triple);
            throw new RDFHandlingInterruptedException(e);
        }
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
        TripleIteratorFactory factory = TripleIteratorFactory.defaultInstance();
        if (format != RDFFormat.RDF_XML && format == RDFFormat.TURTLE && format != RDFFormat.N_TRIPLES) {
            throw new TrippiException("Unsupported input format: " + format.getName());
        }
        TripleIterator iter = factory.fromStream(new FileInputStream(f),
                                                 baseURI,
                                                 format);
        try {
            iter.toStream(System.out, RDFFormat.forName(args[2]));
        } finally {
            iter.close();
        }
    }

    public void endRDF() throws RDFHandlerException {
        // signal end of parsing
        put(FINISHED);
    }

    public void handleComment(String arg0) throws RDFHandlerException {
        // TODO Auto-generated method stub
        
    }

    public void handleStatement(Statement st)
            throws RDFHandlerException {
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
    
    static final class FlagTriple extends AbstractTriple {
        /**
         * 
         */
        private static final long serialVersionUID = 1L;
        final String flag;
        FlagTriple(String flag) {
            this.flag = flag;
        }
        public String toString() {
            return "FlagTriple<" + flag + ">";
        }
    }
    
    static final class RDFHandlingInterruptedException
    extends RDFHandlerException {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        public RDFHandlingInterruptedException(String msg) {
            super(msg);
        }
        
        public RDFHandlingInterruptedException(Throwable cause) {
            super(cause);
        }
        
        public RDFHandlingInterruptedException(String msg, Throwable cause) {
            super(msg, cause);
        }
        
    }

}
