package org.trippi.impl.base;

import java.io.*;
import java.util.*;

import org.apache.log4j.Logger;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;

import org.trippi.*;

/**
 * A ConcurrentTriplestoreReader that also implements TriplestoreWriter
 * with concurrent buffered write access.
 * <p>
 * In addition to on-demand flushes, additions and deletions will periodically
 * by flushed by a separate thread when a certain amount of inactivity has 
 * occurred or the buffer reaches a certain size.
 * </p><p>
 * If the buffer ever exceeds its safeCapacity(), all updates will be
 * halted until it is flushed.
 * </p>
 * @author cwilper@cs.cornell.edu
 */
public class ConcurrentTriplestoreWriter extends ConcurrentTriplestoreReader
                                         implements TriplestoreWriter,
                                                    Runnable {

    private static final Logger logger =
        Logger.getLogger(ConcurrentTriplestoreWriter.class.getName());

    // Initialization variables -- see constructor
    private TriplestoreSessionPool m_pool;
    private UpdateBuffer m_buffer;
    private int m_autoFlushBufferSize;
    private int m_autoFlushDormantSeconds;

    // The single session that writes can occur on
    private TriplestoreSession m_updateSession;

    // Signal to the thread to stop running
    private boolean m_needToClose = false;

    private Object m_bufferInputLock = new Object();

    // Last epochMS that the buffer was modified
    private long m_lastBufferInputTime;

    private boolean m_cacheDeletes = false;

    /**
     * Initialize variables, obtain a session for updates, and start the
     * autoFlush thread.
     */
    public ConcurrentTriplestoreWriter(TriplestoreSessionPool pool,
                                       AliasManager aliasManager,
                                       TriplestoreSession updateSession,
                                       UpdateBuffer buffer,
                                       int autoFlushBufferSize,
                                       int autoFlushDormantSeconds)
                                                  throws IOException,
                                                         TrippiException {
        super(pool, aliasManager);
        m_pool = pool;
        m_updateSession = updateSession;
        m_buffer = buffer;
        m_autoFlushBufferSize = autoFlushBufferSize;
        m_autoFlushDormantSeconds = autoFlushDormantSeconds;

        // Flush buffer in case of prior improper shutdown
        flushBuffer();
        m_lastBufferInputTime = System.currentTimeMillis();
        // Start the autoFlush thread
        Thread t = new Thread(this);
        t.start();
    }

    public void setCacheDeletes(boolean cacheDeletes) {
        m_cacheDeletes = cacheDeletes;
    }

    /**
     * Add a series of triples to the store.
     *
     * This implementation actually buffers the triples for asychronous 
     * addition, which will occur in the autoFlush thread when <b>bufferSize</b> 
     * or <b>dormantSeconds</b> is reached.  However, if <i>flush</i> is 
     * true, the buffer will be flushed in this thread before returning.
     *
     * @param  triples  a list of <code>Triple</code> objects
     * @param    flush  whether to flush the buffer before returning.
     */
    public void add(List triples,
                    boolean flush) throws IOException,
                                          TrippiException {
        boolean flushed = false;
        synchronized (m_bufferInputLock) {
            m_buffer.add(triples);
            m_lastBufferInputTime = System.currentTimeMillis();
            if ( m_buffer.size() > m_buffer.safeCapacity() ) {
                logger.info("Forcing flush: Buffer size (" + m_buffer.size() 
                          + ") exceeded safe capacity.");
                flushBuffer();
                flushed = true;
            }
        }
        if (!flushed && flush) flushBuffer();
    }

    public void add(TripleIterator iter,
                    boolean flush) throws IOException,
                                          TrippiException {
        try {
            int maxListSize = m_autoFlushBufferSize;
            List triples = new ArrayList();
            while (iter.hasNext()) {
                triples.add(iter.next());
                if (triples.size() == maxListSize) {
                    add(triples, false);
                    triples.clear();
                }
            }
            if (triples.size() > 0) {
                add(triples, false);
                triples.clear();
            }
            if (flush) flushBuffer();
        } finally {
            iter.close();
        }
    }

    /**
     * Add a single triple to the store (convenience method).
     *
     * This implementation actually buffers the triple for asychronous 
     * addition, which will occur in the autoFlush thread when <b>bufferSize</b> 
     * or <b>dormantSeconds</b> is reached.  However, if <i>flush</i> is 
     * true, the buffer will be flushed in this thread before returning.
     *
     * @param  triple  the <code>Triple</code> to add
     * @param   flush  whether to flush the buffer before returning
     */
    public void add(Triple triple,
                    boolean flush)         throws IOException,
                                                         TrippiException {
        boolean flushed = false;
        synchronized (m_bufferInputLock) {
            m_buffer.add(triple);
            m_lastBufferInputTime = System.currentTimeMillis();
            if ( m_buffer.size() > m_buffer.safeCapacity() ) {
                logger.info("Forcing flush: Buffer size (" + m_buffer.size() 
                          + ") exceeded safe capacity.");
                flushBuffer();
                flushed = true;
            }
        }
        if (!flushed && flush) flushBuffer();
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
    public void delete(List triples,
                       boolean flush)      throws IOException,
                                                         TrippiException {
        boolean flushed = false;
        synchronized (m_bufferInputLock) {
            m_buffer.delete(triples);
            m_lastBufferInputTime = System.currentTimeMillis();
            if ( m_buffer.size() > m_buffer.safeCapacity() ) {
                logger.info("Forcing flush: Buffer size (" + m_buffer.size() 
                          + ") exceeded safe capacity.");
                flushBuffer();
                flushed = true;
            }
        }
        if (!flushed && flush) flushBuffer();
    }

    public void delete(TripleIterator iter,
                       boolean flush) throws IOException,
                                             TrippiException {
        File tempFile = null;
        try {
            if (m_cacheDeletes) {
                tempFile = File.createTempFile("trippi-deltriples", "txt");
                FileOutputStream fout = new FileOutputStream(tempFile);
                try {
                    iter.toStream(fout, RDFFormat.TURTLE);
                } finally {
                    try { fout.close(); } catch (Exception e) { }
                }
                iter.close();
                iter = TripleIterator.fromStream(new FileInputStream(tempFile), RDFFormat.TURTLE);
            }
            try {
                int maxListSize = m_autoFlushBufferSize;
                List triples = new ArrayList();
                while (iter.hasNext()) {
                    triples.add(iter.next());
                    if (triples.size() == maxListSize) {
                        delete(triples, false);
                        triples.clear();
                    }
                }
                if (triples.size() > 0) {
                    delete(triples, false);
                    triples.clear();
                }
                if (flush) flushBuffer();
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
     * Remove a single triple from the store (convenience method).
     *
     * This implementation actually buffers the triple for asychronous 
     * deletion, which will occur in the updater thread when <b>bufferSize</b> 
     * or <b>dormantSeconds</b> is reached.  However, if <i>flush</i> is 
     * true, the buffer will be flushed in this thread before returning.
     *
     * @param  triple  the <code>Triple</code> to delete
     * @param  flush   whether to flush the buffer before returning
     */
    public void delete(Triple triple,
                       boolean flush)      throws IOException,
                                                         TrippiException {
        boolean flushed = false;
        synchronized (m_bufferInputLock) {
            m_buffer.delete(triple);
            m_lastBufferInputTime = System.currentTimeMillis();
            if ( m_buffer.size() > m_buffer.safeCapacity() ) {
                logger.info("Forcing flush: Buffer size (" + m_buffer.size() 
                          + ") exceeded safe capacity.");
                flushBuffer();
                flushed = true;
            }
        }
        if (!flushed && flush) flushBuffer();
    }

    /**
     * Flush the buffer (write the changes to the store).
     *
     * If it's currently being flushed, wait for it to finish, then
     * re-flush it.
     */
    public void flushBuffer() throws IOException,
                                     TrippiException {
        long start = System.currentTimeMillis();
        int size = 0;
        synchronized (m_updateSession) {
            size = m_buffer.size();
            m_buffer.flush(m_updateSession);
        }
        long end = System.currentTimeMillis();
        double sec = ( (double) (end - start) ) / 1000.0;
        logger.info("Flushed " + size + " updates in " + sec + "seconds.");
    }

    public void setFlushErrorHandler(FlushErrorHandler h) {
        m_buffer.setFlushErrorHandler(h);
    }

    public int getBufferSize() {
        return m_buffer.size();
    }

    /**
     * Watch the buffer and automatically flush it if dormantSeconds or
     * bufferSize is reached.
     */
    public void run() {
        while (!m_needToClose) {
            long now = System.currentTimeMillis();
            long secondsSinceLast = ( now - m_lastBufferInputTime ) / 1000;
            if ( (m_buffer.size() > 0) 
                    && ( ( secondsSinceLast >= m_autoFlushDormantSeconds ) 
                        || (m_buffer.size() >= m_autoFlushBufferSize ) ) ) {
                try {
                    logger.info("Auto-flushing update buffer.  (" 
                            + secondsSinceLast + "sec., size = " 
                            + m_buffer.size() + ")");
                    flushBuffer();
                } catch (Exception e) {
                    String msg = e.getClass().getName();
                    if (e.getMessage() != null) msg = msg + ": " + e.getMessage();
                    logger.warn("Error auto-flushing update buffer: " + msg, e);
                }
            }
            if (!m_needToClose) {
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) { }
            }
        }
    }

    /**
     * Close the writer, releasing any resources.
     *
     * This will stop the auto buffer-flushing thread, flush the buffer,
     * close the buffer, return the reserved update session to the pool,
     * and finally, close the pool.
     *
     */
    public synchronized void close() throws TrippiException {
        if (!m_needToClose) {
            logger.info("Closing...");
            m_needToClose = true;
            try {
                flushBuffer();
            } catch (Exception e) {
                String msg = e.getClass().getName();
                if (e.getMessage() != null) msg = msg + ": " + e.getMessage();
                logger.warn("Error flushing update buffer while "
                        + "closing Triplestore: " + msg);
            }
            try {
                m_buffer.close();
            } catch (Exception e) {
                String msg = e.getClass().getName();
                if (e.getMessage() != null) msg = msg + ": " + e.getMessage();
                logger.warn("Error closing update buffer while "
                        + "closing Triplestore: " + msg);
            }
            m_pool.close();
        }
    }

	public List findBufferedUpdates(SubjectNode subject, 
								   PredicateNode predicate, 
								   ObjectNode object, 
								   int updateType) {
		return m_buffer.findBufferedUpdates(subject, predicate, object, updateType);
	}

}
