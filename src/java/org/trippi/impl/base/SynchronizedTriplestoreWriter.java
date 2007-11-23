package org.trippi.impl.base;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.trippi.FlushErrorHandler;
import org.trippi.RDFFormat;
import org.trippi.TripleIterator;
import org.trippi.TripleUpdate;
import org.trippi.TriplestoreWriter;
import org.trippi.TrippiException;

/**
 * A SynchronizedTriplestoreReader that also implements TriplestoreWriter
 * with concurrent buffered write access.
 * <p>
 * @author cwilper@cs.cornell.edu
 */
public class SynchronizedTriplestoreWriter extends SynchronizedTriplestoreReader
                                           implements TriplestoreWriter {

    private static final Logger logger =
        Logger.getLogger(SynchronizedTriplestoreWriter.class.getName());

    private SynchronizedTriplestoreSession m_session;
    private int m_flushSize;

    /**
     * Construct.
     */
    public SynchronizedTriplestoreWriter(SynchronizedTriplestoreSession session,
                                         AliasManager aliasManager,
                                         int flushSize) {
        super(session, aliasManager);
        m_session = session;
        m_flushSize = flushSize;
    }

    /**
     * Immediately add all triples in the list to the store, then return.
     */
    public void add(List<Triple> triples,
                    boolean flush) throws TrippiException {
        HashSet<Triple> set = new HashSet<Triple>();
        Iterator<Triple> iter = triples.iterator();
        while (iter.hasNext()) {
            set.add(iter.next());
        }
        m_session.add(set);
    }

    /**
     * Immediately add all triples in the iterator to the store, then return.
     */
    public void add(TripleIterator iter,
                    boolean flush) throws IOException,
                                          TrippiException {
        try {
            HashSet<Triple> set = new HashSet<Triple>();
            while (iter.hasNext()) {
                set.add(iter.next());
                if (set.size() == m_flushSize) {
                    m_session.add(set);
                    set = null;
                    set = new HashSet<Triple>();
                }
            }
            if (set.size() > 0) {
                m_session.add(set);
            }
        } finally {
            iter.close();
        }
    }

    /**
     * Immediately add the given triple to the store, then return.
     */
    public void add(Triple triple,
                    boolean flush) throws TrippiException {
        HashSet<Triple> set = new HashSet<Triple>(1);
        set.add(triple);
        m_session.add(set);
    }

    /**
     * Remove a series of triples from the store.
     *
     * This implementation actually buffers the triples for asychronous 
     * deletion, which will occur in the updater thread when <b>bufferSize</b> 
     * or <b>dormantSeconds</b> is reached.  However, if <i>flush</i> is 
     * true, the buffer will be flushed in this thread before returning.
     *
     * @param  triples  a list of <code>Triple</code> objects
     * @param    flush  whether to flush the buffer before returning
     */
    public void delete(List<Triple> triples,
                       boolean flush) throws IOException, TrippiException {
        HashSet<Triple> set = new HashSet<Triple>();
        Iterator<Triple> iter = triples.iterator();
        while (iter.hasNext()) {
            set.add(iter.next());
        }
        m_session.delete(set);
    }

    public void delete(TripleIterator iter, boolean flush) throws IOException,
                                                                  TrippiException {
        File tempFile = null;
        try {
            // Send the triples to a temporary file first, because they're 
            // probably coming from an iterator that has the lock,
            // and we can't delete them till the lock is freed.
            tempFile = File.createTempFile("trippi-deltriples", "txt");
            FileOutputStream fout = new FileOutputStream(tempFile);
            try {
                iter.toStream(fout, RDFFormat.TURTLE);
            } finally {
                try { fout.close(); } catch (Exception e) { }
            }
            iter.close();
            iter = TripleIterator.fromStream(new FileInputStream(tempFile), 
                                             RDFFormat.TURTLE);
            try {
                HashSet<Triple> set = new HashSet<Triple>();
                while (iter.hasNext()) {
                    set.add(iter.next());
                    if (set.size() == m_flushSize) {
                        m_session.delete(set);
                        set = null;
                        set = new HashSet<Triple>();
                    }
                }
                if (set.size() > 0) {
                    m_session.delete(set);
                }
            } finally {
                iter.close();
            }
        } finally {
            if (tempFile != null) {
                tempFile.delete();
            }
        }
    }

    /**
     * Immediately delete the given triple from the store, then return.
     */
    public void delete(Triple triple,
                       boolean flush) throws IOException,
                                             TrippiException {
        HashSet<Triple> set = new HashSet<Triple>(1);
        set.add(triple);
        m_session.delete(set);
    }

    public void flushBuffer() {
        // NO-OP (no buffer)
    }

    public void setFlushErrorHandler(FlushErrorHandler h) {
        // NO-OP (no buffer) 
    }

    public int getBufferSize() {
        return 0;
    }

	public List<TripleUpdate> findBufferedUpdates(SubjectNode subject, 
			PredicateNode predicate, 
			ObjectNode object, 
			int updateType) {
		return Collections.emptyList();
	}

}
