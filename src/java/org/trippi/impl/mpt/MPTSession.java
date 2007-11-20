package org.trippi.impl.mpt;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.dbcp.BasicDataSource;
import org.jrdf.graph.Literal;
import org.jrdf.graph.Node;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.jrdf.graph.URIReference;
import org.nsdl.mptstore.core.DatabaseAdaptor;
import org.nsdl.mptstore.query.QueryResults;
import org.nsdl.mptstore.query.lang.QueryLanguage;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;
import org.trippi.impl.base.TriplestoreSession;

public class MPTSession implements TriplestoreSession {

    private static final String _SPO  = "spo";
    private static final String _SPONGE  = "sponge";
    private static final String _UNSUPPORTED = "unsupported";

    public static final String[] TRIPLE_LANGUAGES = new String[] { _SPO, _SPONGE };
    public static final String[] TUPLE_LANGUAGES  = new String[] { _UNSUPPORTED };

    private BasicDataSource _pool;
    private DatabaseAdaptor _adaptor;
    private int _fetchSize;

    public MPTSession(BasicDataSource dbPool,
                      DatabaseAdaptor adaptor,
                      int fetchSize) {
        _pool = dbPool;
        _adaptor = adaptor;
        _fetchSize = fetchSize;
    }

    // Implements TriplestoreSession.listTripleLanguages()
    public String[] listTripleLanguages() { return TRIPLE_LANGUAGES; }
           
    // Implements TriplestoreSession.listTupleLanguages()
    public String[] listTupleLanguages()  { return TUPLE_LANGUAGES; }

    // Implements TriplestoreSession.findTriples(String, String)
    public TripleIterator findTriples(String lang, 
                                      String queryText) throws TrippiException {
        if (lang.equals(_SPONGE)) {
            return findTriples(queryText);
        } else {
            throw new TrippiException("Unsupported triple query language: " + lang);
        }
    }

    private String toString(Node n) {
        if (n == null) return "*";
        return jrdfToMPT(n).toString();
    }

    // Implements TriplestoreSession.findTriples(SubjectNode, PredicateNode, ObjectNode)
    public TripleIterator findTriples(SubjectNode subject, 
                                      PredicateNode predicate, 
                                      ObjectNode object) throws TrippiException {

        // convert to an SPO query
        String spoQuery = toString(subject) + " "
                        + toString(predicate) + " "
                        + toString(object);

        return findTriples(spoQuery);
    }

    private TripleIterator findTriples(String spoQuery) throws TrippiException {

        // get results from adaptor, wrapped in our own TripleIterator
        Connection conn = null;
        try {
            conn = _pool.getConnection();
            conn.setAutoCommit(false);
            QueryResults results = _adaptor.query(conn, 
                                                  QueryLanguage.SPO, 
                                                  _fetchSize,
                                                  true, // autoRelease
                                                  spoQuery);
            return new MPTTripleIterator(results);
        } catch (Exception e) { 
            if (conn != null) {
                try { conn.close(); } catch (Exception e2) { }
            }
            throw new TrippiException("Error querying triples", e);
        }
    }

    // Implements TriplestoreSession.query(String, String)
    public TupleIterator query(String query,
                               String lang) throws TrippiException {
        throw new TrippiException("Unsupported tuple query language: " + lang);
    }

    // Implements TriplestoreSession.add(Set)
    public void add(Set triples) throws TrippiException {
        update(triples, false);
    }

    // Implements TriplestoreSession.delete(Set)
    public void delete(Set triples) throws TrippiException {
        update(triples, true);
    }
    
    private void update(Set triples, boolean delete) 
            throws TrippiException {
        Connection conn = null;
        boolean startedTransaction = false;
        boolean success = false;
        try {
            conn = _pool.getConnection();
            conn.setAutoCommit(false);
            startedTransaction = true;

            Set mptSet = jrdfToMPT(triples);

            if (delete) {
                _adaptor.deleteTriples(conn, mptSet.iterator());
            } else {
                _adaptor.addTriples(conn, mptSet.iterator());
            }
            success = true;

        } catch (Exception e) {
            throw new TrippiException("Error updating triples", e);
        } finally {
            if (conn != null) {
                if (startedTransaction) {
                    if (!success) {
                        try {
                            conn.rollback();
                        } catch (Exception e2) { }
                    }
                    try {
                        conn.setAutoCommit(true);
                    } catch (Exception e2) { }
                }
                try { conn.close(); } catch (Exception e) { }
            }
        }
    }

    /**
     * Convert the given set of JRDF Triple objects to 
     * MPT Triple objects.
     */
    protected static Set jrdfToMPT(Set jrdfTriples) {
        Set mptSet = new HashSet(jrdfTriples.size());
        Iterator iter = jrdfTriples.iterator();
        while (iter.hasNext()) {
            Triple jrdfTriple = (Triple) iter.next();
            org.nsdl.mptstore.rdf.SubjectNode mptSubject =
                    (org.nsdl.mptstore.rdf.SubjectNode) 
                    jrdfToMPT(jrdfTriple.getSubject());
            org.nsdl.mptstore.rdf.PredicateNode mptPredicate =
                    (org.nsdl.mptstore.rdf.PredicateNode) 
                    jrdfToMPT(jrdfTriple.getPredicate());
            org.nsdl.mptstore.rdf.ObjectNode mptObject =
                    (org.nsdl.mptstore.rdf.ObjectNode) 
                    jrdfToMPT(jrdfTriple.getObject());
            mptSet.add(new org.nsdl.mptstore.rdf.Triple(
                    mptSubject, mptPredicate, mptObject));
        }
        return mptSet;
    }

    protected static org.nsdl.mptstore.rdf.Node jrdfToMPT(Node jrdfNode) {
        try {
            if (jrdfNode instanceof URIReference) {
                URIReference jrdfURIReference = (URIReference) jrdfNode;
                return new org.nsdl.mptstore.rdf.URIReference(jrdfURIReference.getURI());
            } else if (jrdfNode instanceof Literal) {
                Literal jrdfLiteral = (Literal) jrdfNode;
                String lang = jrdfLiteral.getLanguage();
                if (lang != null && lang.length() > 0) {
                    return new org.nsdl.mptstore.rdf.Literal(
                            jrdfLiteral.getLexicalForm(), lang);
                } else if (jrdfLiteral.getDatatypeURI() != null) {
                    org.nsdl.mptstore.rdf.URIReference dType =
                            new org.nsdl.mptstore.rdf.URIReference(
                            jrdfLiteral.getDatatypeURI());
                    return new org.nsdl.mptstore.rdf.Literal(
                            jrdfLiteral.getLexicalForm(), dType);
                } else {
                    return new org.nsdl.mptstore.rdf.Literal(
                            jrdfLiteral.getLexicalForm());
                }
            } else {
                throw new RuntimeException("Unrecognized node type; cannot "
                        + "convert to MPT Node: " + jrdfNode.getClass().getName());
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException("Bad URI syntax, cannot convert to "
                    + "MPT Node", e);
        } catch (ParseException e) {
            throw new RuntimeException("Bad language syntax, cannot convert to "
                    + "MPT Node", e);
        }
    }

    // Implements TriplestoreSession.close()
    public void close() throws TrippiException {
        // nothing to release
    }

}
