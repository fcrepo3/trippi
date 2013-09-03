package org.trippi.impl.base;

import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.trippi.Alias;
import org.trippi.TripleIterator;
import org.trippi.TriplePattern;
import org.trippi.TriplestoreReader;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

/**
 * A TriplestoreReader that provides synchronized read access to
 * a triplestore by means of a <code>SynchronizedTriplestoreSession</code>.
 *
 * @author cwilper@cs.cornell.edu
 */
public class SynchronizedTriplestoreReader implements TriplestoreReader {

    private static final Logger logger =
        LoggerFactory.getLogger(SynchronizedTriplestoreReader.class.getName());

    /** The session to use for reading. */
    private SynchronizedTriplestoreSession m_session;

    /** Where aliases are stored. */
    private AliasManager m_aliasManager;

    /**
     * Constructor.
     *
     * @param session the session to use for reading.
     */
    public SynchronizedTriplestoreReader(SynchronizedTriplestoreSession session,
                                         AliasManager aliasManager) {
        m_session = session;
        m_aliasManager = aliasManager;
    }

    public Map<String, String> getAliasMap() {
        return m_aliasManager.getAliasMap();
    }

    public void setAliasMap(Map<String, String> aliasMap) {
        m_aliasManager.setAliasMap(aliasMap);
    }

    public String[] listTupleLanguages() {
        return m_session.listTupleLanguages();
    }

    public String[] listTripleLanguages() {
        return m_session.listTripleLanguages();
    }

    public int countTuples(String queryLang,
                           String tupleQuery,
                           int limit,
                           boolean distinct) throws TrippiException {
        return findTuples(queryLang, tupleQuery, limit, distinct).count();
    }

    public TupleIterator findTuples(String queryLang,
                                    String tupleQuery,
                                    int limit,
                                    boolean distinct) throws TrippiException {
        TupleIterator iter = null;
        iter = m_session.query(tupleQuery, queryLang);
        if (distinct) iter = new DistinctTupleIterator(iter);
        if (limit > 0) iter = new LimitedTupleIterator(iter, limit);
        return iter;
    }

    private String doAliasReplacements(String q) {
        String out = q;
        Map<String, Alias> m = m_aliasManager.getAliases();
        Iterator<String> iter = m.keySet().iterator();
        while (iter.hasNext()) {
            String key = iter.next();
            Alias alias = m.get(key);
            out = alias.replaceSparqlType(alias.replaceSparqlUri(out));
        }
        if (!q.equals(out)) {
            logger.info("Substituted aliases, query is now: " + out);
        }
        return out;
    }

    public int countTriples(String queryLang,
                            String tripleQuery,
                            int limit,
                            boolean distinct) throws TrippiException {
        return findTriples(queryLang, tripleQuery, limit, distinct).count();
    }

    /**
     * Delegates to underlying session for non-basic queries.
     */
    public TripleIterator findTriples(String queryLang,
                                      String tripleQuery,
                                      int limit,
                                      boolean distinct) throws TrippiException {
        if (queryLang.equals("spo")) {
            // parse and call findTriples(subj, pred, obj)
            TriplePattern[] patterns = TriplePattern.parse(doAliasReplacements(tripleQuery));
            if (patterns.length == 1) {
                TriplePattern p = patterns[0];
                SubjectNode subject = null;
                PredicateNode predicate = null;
                ObjectNode object = null;
                if (p.getSubject() instanceof SubjectNode) subject = (SubjectNode) p.getSubject();
                if (p.getPredicate() instanceof PredicateNode) predicate = (PredicateNode) p.getPredicate();
                if (p.getObject() instanceof ObjectNode) object = (ObjectNode) p.getObject();
                return findTriples(subject, predicate, object, limit);
            } else {
                throw new TrippiException("Only one triple pattern may be specified.");
            }
        } else {
            // delegate to the session
            TripleIterator iter = null;
            iter = m_session.findTriples(queryLang, tripleQuery);
            if (distinct) iter = new DistinctTripleIterator(iter);
            if (limit > 0) iter = new LimitedTripleIterator(iter, limit);
            return iter;
        }
    }

    public int countTriples(SubjectNode subject,
                            PredicateNode predicate,
                            ObjectNode object,
                            int limit) throws TrippiException {
        return findTriples(subject, predicate, object, limit).count();
    }

    public TripleIterator findTriples(SubjectNode subject,
                                      PredicateNode predicate,
                                      ObjectNode object,
                                      int limit) throws TrippiException {
        TripleIterator iter = null;
        iter = m_session.findTriples(subject, predicate, object);
        if (limit > 0) iter = new LimitedTripleIterator(iter, limit);
        return iter;
    }

    public int countTriples(String queryLang,
                            String tupleQuery,
                            String tripleTemplate,
                            int limit,
                            boolean distinct) throws TrippiException {
        return findTriples(queryLang, tupleQuery, tripleTemplate, limit, distinct).count();
    }

    public TripleIterator findTriples(String queryLang,
                                      String tupleQuery,
                                      String tripleTemplate,
                                      int limit,
                                      boolean distinct) throws TrippiException {
        TriplePattern[] patterns = TriplePattern.parse(tripleTemplate);
        TripleIterator iter = new TupleBasedTripleIterator(findTuples(queryLang, 
                                                                      tupleQuery,
                                                                      -1,
                                                                      distinct),
                                                           patterns);
        if (distinct) iter = new DistinctTripleIterator(iter);
        if (limit > 0) iter = new LimitedTripleIterator(iter, limit);
        return iter;
    }

    public void close() throws TrippiException {
        logger.info("SynchronizedTripleStoreReader closing TripleStoreSession...");
        m_session.close();
    }

    /**
     * Ensure close() gets called at garbage collection time.
     */
    @Override
	public void finalize() throws TrippiException {
        close();
    }

}
