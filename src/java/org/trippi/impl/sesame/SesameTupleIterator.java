package org.trippi.impl.sesame;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jrdf.graph.GraphElementFactoryException;
import org.jrdf.graph.Node;
import org.jrdf.graph.ObjectNode;
import org.openrdf.sesame.constants.QueryLanguage;
import org.openrdf.sesame.query.QueryErrorType;
import org.openrdf.sesame.query.TableQueryResultListener;
import org.openrdf.sesame.repository.SesameRepository;
import org.trippi.RDFUtil;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

public class SesameTupleIterator 
             extends TupleIterator 
             implements TableQueryResultListener, Runnable {

    private static final Logger logger =
        Logger.getLogger(SesameTupleIterator.class.getName());

    private QueryLanguage m_lang;
    private String m_queryText;
    private SesameRepository m_repository;

    private RDFUtil m_util;

    private String[] m_names;

    private Map<String, Node> m_bucket; // shared between parser/consumer threads
    private Map<String, Node> m_next;

    private List<Node> m_valueList; // temporarily holds values for current tuple

    private boolean m_closed = false;
    private boolean m_finishedIterating = false;

    /* how many tuples have been iterated so far */
    private int m_tupleCount = 0;

    /* holds the exception (if any) that was thrown in the background thread */
    private Exception m_exception;

    public SesameTupleIterator(QueryLanguage lang,
                               String queryText,
                               SesameRepository repository) throws TrippiException {
        m_lang       = lang;
        m_queryText  = queryText;
        m_repository = repository;

        m_valueList = new ArrayList<Node>();

        try { m_util = new RDFUtil(); } catch (Exception e) { } // won't happen

        Thread queryThread = new Thread(this);
        queryThread.start();

        m_next = getNext();
    }

    //////////////////////////////////////////////////////////////////////////
    //////////////// TupleIterator ///////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////

    /**
     * Get the names of the binding variables.
     *
     * These will be the keys in the map for result.
     */
    public String[] names() throws TrippiException {
        return m_names; 
    }

    /**
     * Return true if there are any more tuples.
     */
    public boolean hasNext() throws TrippiException {
        return (m_next != null);
    }
    
    /**
     * Return the next tuple.
     */
    public Map<String, Node> next() throws TrippiException {
        if (m_next == null || m_closed) return null;
        Map<String, Node> last = m_next;
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
     * Get the next tuple out of the bucket and return it.
     *
     * If the bucket is empty:
     *   1) If the iterator is finished, 
     *      throw an exception if m_exception != null.
     *      Otherwise, return null.
     *   2) If the iterator is not finished, wait for
     *      a) The iterator to finish, or
     *      b) Another tuple to arrive in the bucket.
     */
    private synchronized Map<String, Node> getNext() throws TrippiException { 
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
            logger.info("Finished iterating " + m_tupleCount + " tuples from query.");
            return null; // iterator finished normally, no more tuples
        } else {
            Map<String, Node> tuple = m_bucket;
            m_bucket = null;
            notifyAll(); // notify parser that the bucket is ready for another
            m_tupleCount++;
            return tuple;
        }
    }


    //////////////////////////////////////////////////////////////////////////
    //////////////// Runnable ////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////

    public void run() {
        logger.info("Executing query: " + m_queryText);
        try {
            m_repository.performTableQuery(m_lang, m_queryText, this);
        } catch (Exception e) {
            m_exception = e;
        } finally {
            m_finishedIterating = true;
        }
    }

    //////////////////////////////////////////////////////////////////////////
    //////////////// TableQueryResultListener ////////////////////////////////
    //////////////////////////////////////////////////////////////////////////

    public void startTableQueryResult() throws IOException {
        // nothing to do
    }

    public void startTableQueryResult(String[] names) throws IOException {
        // set the column names
        m_names = names;
    }

    public void endTableQueryResult() throws IOException {
        m_finishedIterating = true;
    }

    public void error(QueryErrorType errType, String msg) throws IOException {
        throw new IOException("Error reported while evaluating query: " + msg);
    }

    public void startTuple() throws IOException {
        m_valueList.clear();
    }

    public void tupleValue(org.openrdf.model.Value value) throws IOException {
        try {
            m_valueList.add(objectNode(value));
        } catch (Exception e) {
            String msg = e.getClass().getName();
            if (e.getMessage() != null) msg += ": " + e.getMessage();
            e.printStackTrace();
            throw new IOException("Error converting Sesame node to JRDF node: " + msg);
        }
    }

    public void endTuple() throws IOException {
        // convert m_valueList (which contains JRDF nodes) into a tuple 
        // (which is a Map keyed by name[]s)
        Map<String, Node> tuple = new HashMap<String, Node>();
        for (int i = 0; i < m_names.length; i ++) {
           tuple.put(m_names[i], m_valueList.get(i));
        }
        put(tuple); // locks until m_bucket is free or close() has been called
    }

    private synchronized void put(Map<String, Node> tuple) {
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
        m_bucket = tuple;
        notifyAll();  // notify the consumer that the bucket has a tuple
    }

    private ObjectNode objectNode(org.openrdf.model.Value object)
            throws GraphElementFactoryException,
                   URISyntaxException {
        if (object == null) return null;
        if (object instanceof org.openrdf.model.URI) {
            return m_util.createResource( new URI(((org.openrdf.model.URI) object).toString()) );
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