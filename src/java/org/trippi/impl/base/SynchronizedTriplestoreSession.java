package org.trippi.impl.base;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

/**
 * A synchronized read/write session to an RDF database.
 *
 * If a session doesn't support writes, the add and delete methods
 * will throw UnsupportedOperationException (an unchecked exception).
 *
 * @author cwilper@cs.cornell.edu
 */
public class SynchronizedTriplestoreSession implements TriplestoreSession {

    private static final Logger logger =
            Logger.getLogger(SynchronizedTriplestoreSession.class.getName());

    /** The underlying session. */
    private TriplestoreSession m_session;

    /** The lock queue. The Thread at position 0 has the lock. */
    private List<Thread> m_lockQueue;

    public SynchronizedTriplestoreSession(TriplestoreSession session) {
        m_session = session;
        m_lockQueue = new ArrayList<Thread>();
    }

    public void add(Set<Triple> triples) throws UnsupportedOperationException,
                                         TrippiException {
        waitForLock(false);
        try {
            m_session.add(triples);
        } finally {
            releaseLock();
        }
    }

    public void delete(Set<Triple> triples) throws UnsupportedOperationException,
                                            TrippiException {
        waitForLock(false);
        try {
            m_session.delete(triples);
        } finally {
            releaseLock();
        }
    }

    public TupleIterator query(String queryText,
                               String language) throws TrippiException {
        waitForLock(false);
        boolean success = false;
        try {
            TupleIterator iter = new SynchronizedTupleIterator(
                                         m_session.query(queryText,
                                                         language),
                                         this);
            success = true;
            return iter;
        } finally {
            if (!success) releaseLock();
        }
    }

    public TripleIterator findTriples(String lang,
                                      String queryText) throws TrippiException {
        waitForLock(false);
        boolean success = false;
        try {
            TripleIterator iter = new SynchronizedTripleIterator(
                                         m_session.findTriples(lang,
                                                               queryText),
                                         this);
            success = true;
            return iter;
        } finally {
            if (!success) releaseLock();
        }
    }

    public TripleIterator findTriples(SubjectNode subject,
                                      PredicateNode predicate,
                                      ObjectNode object) throws TrippiException {
        waitForLock(false);
        boolean success = false;
        try {
            TripleIterator iter = new SynchronizedTripleIterator(
                                         m_session.findTriples(subject,
                                                               predicate,
                                                               object),
                                         this);
            success = true;
            return iter;
        } finally {
            if (!success) releaseLock();
        }
    }

    public String[] listTupleLanguages() {
        return m_session.listTupleLanguages();
    }

    public String[] listTripleLanguages() {
        return m_session.listTripleLanguages();
    }

    /**
     * Wait in line
     */
    public synchronized void close() throws TrippiException {
        if (!m_closing) {
            waitForLock(true);
            try {
                m_session.close();
            } finally {
                releaseLock();
            }
        }
    }

    private boolean m_closing = false;

    /**
     * If the current thread is not already in the queue, add it.
     * Then wait until it's at position 0.
     */
    private void waitForLock(boolean closing) throws TrippiException {
        if (closing) {
            m_closing = true;
        } else {
            if (m_closing) throw new TrippiException("Session is closing. Could not get a write lock.");
        }
        Thread ct = Thread.currentThread();
        String id = ct.getName();
        synchronized (m_lockQueue) {
            if (m_lockQueue.contains(ct)) {
                logger.warn("Thread '" + id + "' already in lockQueue, so not re-added.");
            } else {
                m_lockQueue.add(ct);
            }
        }
        logger.info("Thread '" + id + "' waiting for lock.");
        boolean gotLock = false;
        while (!gotLock) {
            synchronized (m_lockQueue) {
                if (m_lockQueue.get(0) == ct) {
                    gotLock = true;
                }
                logLockStatus();
            }
            if (!gotLock) {
                try {
                    Thread.sleep(250); // sleep and try again
                } catch (InterruptedException e) {
                }
            }
        }
        logger.info("Thread '" + id + "' obtained lock.");
    }

    /**
     * If the current thread is at position zero, remove it from the queue.
     */
    public void releaseLock() {
        synchronized (m_lockQueue) {
            String id = Thread.currentThread().getName();
            if (m_lockQueue.size() == 0 || m_lockQueue.get(0) != Thread.currentThread()) {
                logger.warn("Thread '" + id + "' did not have lock, so releaseLock() did nothing.");
                logLockStatus();
            } else {
                m_lockQueue.remove(0);
                logger.info("Thread '" + id + "' released lock.");
                logLockStatus();
            }
        }
    }

    private void logLockStatus() {
        int size = m_lockQueue.size();
        if (size == 0) {
            logger.info("Lock Status: FREE");
        } else {
            int waitCount = size - 1;
            Thread lockingThread = m_lockQueue.get(0);
            logger.info("Lock Status: LOCKER = '" + lockingThread.getName() 
                    + "', WAITING = " + waitCount);
        }
    }

}
