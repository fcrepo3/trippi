package org.trippi.impl.base;

import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.trippi.TripleIterator;
import org.trippi.TriplePattern;
import org.trippi.TriplestoreReader;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

/**
 * A TriplestoreReader that provides efficient concurrent read access to
 * a triplestore by means of a <code>TriplestoreSessionPool</code>.
 *
 * @author cwilper@cs.cornell.edu
 */
public class ConcurrentTriplestoreReader implements TriplestoreReader {

    private static final Logger logger =
        Logger.getLogger(ConcurrentTriplestoreReader.class.getName());

    /** The session pool to draw from. */
    private TriplestoreSessionPool m_pool;

    /** Where aliases are stored. */
    private AliasManager m_aliasManager;

    /**
     * Constructor.
     *
     * @param  pool  the session pool to draw from.
     */
    public ConcurrentTriplestoreReader(TriplestoreSessionPool pool,
                                       AliasManager aliasManager) {
        m_pool = pool;
        m_aliasManager = aliasManager;
    }

    public Map getAliasMap() {
        return m_aliasManager.getAliasMap();
    }

    public void setAliasMap(Map aliasMap) {
        m_aliasManager.setAliasMap(aliasMap);
    }

    public String[] listTupleLanguages() {
        return m_pool.listTupleLanguages();
    }

    public String[] listTripleLanguages() {
        return m_pool.listTripleLanguages();
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
        TriplestoreSession session = m_pool.get();
        if ( session == null ) {
            throw new TrippiException("Maximum triplestore connections "
                    + "exceeded.");
        }
        TupleIterator iter = null;
        boolean failed = true;
        try {
            iter = session.query(tupleQuery, queryLang);
            if (distinct) iter = new DistinctTupleIterator(iter);
            if (limit > 0) iter = new LimitedTupleIterator(iter, limit);
            iter = new PoolAwareTupleIterator(iter, session, m_pool);
            failed = false;
            return iter;
        } finally {
            if (failed) m_pool.release(session);
        }
    }

    private String doAliasReplacements(String q) {
        String out = q;
        Map m = m_aliasManager.getAliasMap();
        Iterator iter = m.keySet().iterator();
        while (iter.hasNext()) {
            String alias = (String) iter.next();
            String fullForm = (String) m.get(alias);
            out = out.replaceAll("<" + alias + ":", "<" + fullForm)
                     .replaceAll("\\^\\^" + alias + ":(\\S+)", "^^<" + fullForm + "$1>");
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
            TriplestoreSession session = m_pool.get();
            if (session == null ) {
                throw new TrippiException("Maximum triplestore connections "
                    + "exceeded.");
            }
            TripleIterator iter = null;
            boolean failed = true;
            try {
                iter = session.findTriples(queryLang, tripleQuery);
                if (distinct) iter = new DistinctTripleIterator(iter);
                if (limit > 0) iter = new LimitedTripleIterator(iter, limit);
                iter = new PoolAwareTripleIterator( iter, session, m_pool );
                failed = false;
                return iter;
            } finally {
                if (failed) m_pool.release(session);
            }
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
        TriplestoreSession session = m_pool.get();
        if (session == null) {
            throw new TrippiException("Maximum triplestore connections "
                    + "exceeded.");
        }
        TripleIterator iter = null;
        boolean failed = true;
        try {
            iter = session.findTriples(subject, predicate, object);
            if (limit > 0) iter = new LimitedTripleIterator(iter, limit);
            iter = new PoolAwareTripleIterator(iter, session, m_pool);
            failed = false;
            return iter;
        } finally {
            if (failed) m_pool.release(session);
        }
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
        logger.info("ConcurrentTripleStoreReader closing TripleStoreSessionPool...");
        m_pool.close();
    }

    /**
     * Ensure close() gets called at garbage collection time.
     */
    public void finalize() throws TrippiException {
        close();
    }

}
