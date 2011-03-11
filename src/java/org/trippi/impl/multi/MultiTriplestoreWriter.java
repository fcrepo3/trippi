package org.trippi.impl.multi;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.trippi.FlushErrorHandler;
import org.trippi.RDFFormat;
import org.trippi.TripleIterator;
import org.trippi.TripleUpdate;
import org.trippi.TriplestoreReader;
import org.trippi.TriplestoreWriter;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

/**
 * A TriplestoreWriter that dispatches all calls to a set of underlying
 * TriplestoreWriters.
 * <p>
 * @author cwilper@cs.cornell.edu
 */
public class MultiTriplestoreWriter implements TriplestoreWriter {

    private static final Logger logger =
        LoggerFactory.getLogger(MultiTriplestoreWriter.class.getName());

    private TriplestoreReader m_reader;
    private TriplestoreWriter[] m_writers;

    /**
     * Construct.
     */
    public MultiTriplestoreWriter(TriplestoreReader reader, 
                                  TriplestoreWriter[] writers) {
        m_reader = reader;
        m_writers = writers;
    }

    //////

    public String[] listTripleLanguages() {
        return m_reader.listTripleLanguages();
    }

    public String[] listTupleLanguages() {
        return m_reader.listTupleLanguages();
    }

    public Map<String, String> getAliasMap() throws TrippiException {
        return m_reader.getAliasMap();
    }

    public void setAliasMap(Map<String, String> aliasMap) throws TrippiException {
        m_reader.setAliasMap(aliasMap);
        for (int i = 0; i < m_writers.length; i++) {
            m_writers[i].setAliasMap(aliasMap);
        }
    }

    public int countTuples(String queryLang,
                           String tupleQuery,
                           int limit,
                           boolean distinct) throws TrippiException {
        return m_reader.countTuples(queryLang, tupleQuery, limit, distinct);
    }

    public TupleIterator findTuples(String queryLang,
                                    String tupleQuery,
                                    int limit,
                                    boolean distinct) throws TrippiException {
        return m_reader.findTuples(queryLang, tupleQuery, limit, distinct);
    }

    public int countTriples(String queryLang,
                            String tripleQuery,
                            int limit,
                            boolean distinct) throws TrippiException {
        return m_reader.countTriples(queryLang, tripleQuery, limit, distinct);
    }

    public TripleIterator findTriples(String queryLang,
                                      String tripleQuery,
                                      int limit,
                                      boolean distinct) throws TrippiException {
        return m_reader.findTriples(queryLang, tripleQuery, limit, distinct);
    }

    public int countTriples(SubjectNode subject,
                            PredicateNode predicate,
                            ObjectNode object,
                            int limit) throws TrippiException {
        return m_reader.countTriples(subject, predicate, object, limit);
    }

    public TripleIterator findTriples(SubjectNode subject,
                                      PredicateNode predicate,
                                      ObjectNode object,
                                      int limit) throws TrippiException {
        return m_reader.findTriples(subject, predicate, object, limit);
    }

    public int countTriples(String queryLang,
                            String tupleQuery,
                            String tripleTemplate,
                            int limit,
                            boolean distinct) throws TrippiException {
        return m_reader.countTriples(queryLang, tupleQuery, tripleTemplate, limit, distinct);
    }

    public TripleIterator findTriples(String queryLang,
                                      String tupleQuery,
                                      String tripleTemplate,
                                      int limit,
                                      boolean distinct) throws TrippiException {
        return m_reader.findTriples(queryLang, tupleQuery, tripleTemplate, limit, distinct);
    }

    //////

    /**
     * Immediately add all triples in the list to the store, then return.
     */
    public void add(List<Triple> triples, boolean flush) {
        for (int i = 0; i < m_writers.length; i++) {
            try {
                m_writers[i].add(triples, flush);
            } catch (Exception e) {
                logger.warn("Error adding triples from list (" + m_writers[i].getClass().getName() + ")", e); 
            }
        }
    }

    public void add(TripleIterator iter, boolean flush) throws IOException, TrippiException {
        File tempFile = null;
        try {
            tempFile = File.createTempFile("trippi-multiadd", "txt");
            FileOutputStream fout = new FileOutputStream(tempFile);
            try {
                iter.toStream(fout, RDFFormat.TURTLE);
            } finally {
                try { fout.close(); } catch (Exception e) { }
            }
            iter.close();
            for (int i = 0; i < m_writers.length; i++) {
                iter = TripleIterator.fromStream(new FileInputStream(tempFile), 
                                                 RDFFormat.TURTLE);
                try {
                    m_writers[i].add(iter, flush);
                } catch (Exception e) {
                    logger.warn("Error adding triples from iterator (" + m_writers[i].getClass().getName() + ")", e); 
                }
            }
        } finally {
            if (tempFile != null) {
                tempFile.delete();
            }
        }
    }

    public void add(Triple triple, boolean flush) {
        for (int i = 0; i < m_writers.length; i++) {
            try {
                m_writers[i].add(triple, flush);
            } catch (Exception e) {
                logger.warn("Error adding one triple (" + m_writers[i].getClass().getName() + ")", e); 
            }
        }
    }

    public void delete(List<Triple> triples, boolean flush) {
        for (int i = 0; i < m_writers.length; i++) {
            try {
                m_writers[i].delete(triples, flush);
            } catch (Exception e) {
                logger.warn("Error deleting triples from list (" + m_writers[i].getClass().getName() + ")", e); 
            }
        }
    }

    public void delete(TripleIterator iter, boolean flush) throws IOException, TrippiException {
        File tempFile = null;
        try {
            tempFile = File.createTempFile("trippi-multidel", "txt");
            FileOutputStream fout = new FileOutputStream(tempFile);
            try {
                iter.toStream(fout, RDFFormat.TURTLE);
            } finally {
                try { fout.close(); } catch (Exception e) { }
            }
            iter.close();
            for (int i = 0; i < m_writers.length; i++) {
                iter = TripleIterator.fromStream(new FileInputStream(tempFile), 
                                                 RDFFormat.TURTLE);
                try {
                    m_writers[i].delete(iter, flush);
                } catch (Exception e) {
                    logger.warn("Error deleting triples from iterator (" + m_writers[i].getClass().getName() + ")", e); 
                }
            }
        } finally {
            if (tempFile != null) {
                tempFile.delete();
            }
        }
    }

    public void delete(Triple triple, boolean flush) {
        for (int i = 0; i < m_writers.length; i++) {
            try {
                m_writers[i].delete(triple, flush);
            } catch (Exception e) {
                logger.warn("Error deleting one triple (" + m_writers[i].getClass().getName() + ")", e); 
            }
        }
    }

    public void flushBuffer() {
        for (int i = 0; i < m_writers.length; i++) {
            try {
                m_writers[i].flushBuffer();
            } catch (Exception e) {
                logger.warn("Error flushing (" + m_writers[i].getClass().getName() + ")", e); 
            }
        }
    }

    public void setFlushErrorHandler(FlushErrorHandler h) {
        for (int i = 0; i < m_writers.length; i++) {
            try {
                m_writers[i].setFlushErrorHandler(h);
            } catch (Exception e) {
                logger.warn("Error setting error handler (" + m_writers[i].getClass().getName() + ")", e); 
            }
        }
    }

    public int getBufferSize() {
        int biggest = 0;
        for (int i = 0; i < m_writers.length; i++) {
            if (m_writers[i].getBufferSize() > biggest) biggest = m_writers[i].getBufferSize();
        }
        return biggest;
    }

    public void close() {
        for (int i = 0; i < m_writers.length; i++) {
            try {
                m_writers[i].close();
            } catch (Exception e) {
                logger.warn("Error closing writer (" + m_writers[i].getClass().getName() + ")", e); 
            }
        }
    }

	public List<TripleUpdate> findBufferedUpdates(SubjectNode subject, 
			PredicateNode predicate, 
			ObjectNode object, 
			int updateType) {
		List<TripleUpdate> triples = new ArrayList<TripleUpdate>();
		for (int i = 0; i < m_writers.length; i++) {
			triples.addAll(m_writers[i].findBufferedUpdates(subject, 
					predicate, object, updateType));
		}
		return triples;
	}

}
