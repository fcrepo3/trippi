package org.trippi.impl.sesame;

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.log4j.*;

import org.jrdf.graph.Triple;

import org.openrdf.sesame.constants.QueryLanguage;
import org.openrdf.sesame.query.GraphQueryResultListener;
import org.openrdf.sesame.repository.SesameRepository;

import org.jrdf.graph.GraphElementFactoryException;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;

import org.trippi.*;

public class SesameTripleIterator 
             extends TripleIterator 
             implements GraphQueryResultListener, Runnable {

    private static final Logger logger =
        Logger.getLogger(SesameTripleIterator.class.getName());

    private QueryLanguage m_lang;
    private String m_queryText;
    private SesameRepository m_repository;

    private RDFUtil m_util;

    private Triple m_bucket; // shared between parser/consumer threads
    private Triple m_next;

    private boolean m_closed = false;
    private boolean m_finishedIterating = false;

    /* how many triples have been iterated so far */
    private int m_tripleCount = 0;

    /* holds the exception (if any) that was thrown in the background thread */
    private Exception m_exception;

    public SesameTripleIterator(QueryLanguage lang,
                                String queryText,
                                SesameRepository repository) throws TrippiException {
        m_lang       = lang;
        m_queryText  = queryText;
        m_repository = repository;

        try { m_util = new RDFUtil(); } catch (Exception e) { } // won't happen

        Thread queryThread = new Thread(this);
        queryThread.start();

        m_next = getNext();
    }

    //////////////////////////////////////////////////////////////////////////
    //////////////// TripleIterator //////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////

    /**
     * Return true if there are any more triples.
     */
    public boolean hasNext() throws TrippiException {
        return (m_next != null);
    }
    
    /**
     * Return the next triple.
     */
    public Triple next() throws TrippiException {
        if (m_next == null) return null;
        Triple last = m_next;
        m_next = getNext();
        return last;
    }

    /**
     * Release resources held by this iterator.
     */
    public void close() throws TrippiException {
        if (!m_closed) {
            m_closed = true; // no resources to release, just set the flag
        }
    }

    /**
     * Get the next triple out of the bucket and return it.
     *
     * If the bucket is empty:
     *   1) If the iterator is finished, 
     *      throw an exception if m_parseException != null.
     *      Otherwise, return null.
     *   2) If the iterator is not finished, wait for
     *      a) The iterator to finish, or
     *      b) Another triple to arrive in the bucket.
     */
    private synchronized Triple getNext() throws TrippiException { 
        // wait until the bucket has a value or 2) finished iterating is true
        while (m_bucket == null && !m_finishedIterating) {
            try {
                // wait for Producer to put value
                wait(5);
            } catch (InterruptedException e) {
            }
        }
        if (m_finishedIterating && m_exception != null) {
            String msg = m_exception.getMessage();
            if (msg == null) msg = "Query failed";
            throw new TrippiException(msg, m_exception);
        }
        if (m_bucket == null) {
            logger.info("Finished iterating " + m_tripleCount + " triples from query.");
            return null; // iterator finished normally, no more triples
        } else {
            Triple triple = m_bucket;
            m_bucket = null;
            notifyAll(); // notify parser that the bucket is ready for another
            m_tripleCount++;
            return triple;
        }
    }


    //////////////////////////////////////////////////////////////////////////
    //////////////// Runnable ////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////

    public void run() {
        logger.info("Executing query: " + m_queryText);
        try {
            m_repository.performGraphQuery(m_lang, m_queryText, this);
        } catch (Exception e) {
            m_exception = e;
        } finally {
            m_finishedIterating = true;
        }
    }

    //////////////////////////////////////////////////////////////////////////
    //////////////// GraphQueryResultListener ////////////////////////////////
    //////////////////////////////////////////////////////////////////////////

    public void startGraphQueryResult() throws IOException {
        // nothing to do
    }

    public void endGraphQueryResult() throws IOException {
        // nothing to do
    }

    public void namespace(String prefix, String name) throws IOException {
        // nothing to do
    }

    public void reportError(String msg) throws IOException {
        throw new IOException("Error reported while evaluating query: " + msg);
    }

    public void triple(org.openrdf.model.Resource subject, 
                       org.openrdf.model.URI predicate, 
                       org.openrdf.model.Value object) throws IOException {
        // convert sesame statement to a jrdf triple
        Triple triple = null;
        try {
            triple = m_util.createTriple( subjectNode(subject),
                                          predicateNode(predicate),
                                          objectNode(object));
        } catch (Exception e) {
            String msg = e.getClass().getName();
            if (e.getMessage() != null) msg += ": " + e.getMessage();
            e.printStackTrace();
            throw new IOException("Error converting Sesame (Resource,URI,Value) to JRDF Triple: " + msg);
        }
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
            return m_util.createResource( new URI(((org.openrdf.model.URI) subject).getURI()) );
        } else {
            return m_util.createResource(((org.openrdf.model.BNode) subject).getID().hashCode());
        }
    }

    private PredicateNode predicateNode(org.openrdf.model.URI predicate)
            throws GraphElementFactoryException,
                   URISyntaxException {
        return m_util.createResource( new URI(((org.openrdf.model.URI) predicate).getURI()) );
    }

    private ObjectNode objectNode(org.openrdf.model.Value object)
            throws GraphElementFactoryException,
                   URISyntaxException {
        if (object instanceof org.openrdf.model.URI) {
            return m_util.createResource( new URI(((org.openrdf.model.URI) object).getURI()) );
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

}