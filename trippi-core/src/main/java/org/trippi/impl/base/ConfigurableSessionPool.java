package org.trippi.impl.base;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trippi.TrippiException;

/**
 * A configurable <code>TriplestoreSessionPool</code> that proactively 
 * increases pool size.
 */
public class ConfigurableSessionPool extends Thread 
                                     implements TriplestoreSessionPool {

    private static final Logger logger =
        LoggerFactory.getLogger(ConfigurableSessionPool.class.getName());

    private TriplestoreSessionFactory m_factory;
    private int m_initialSize;
    private int m_maxGrowth;
    private int m_spareSessions;

    private int m_maxSize;

    private int m_size;

    private List<TriplestoreSession> m_inUseSessions;
    private List<TriplestoreSession> m_freeSessions;

    private boolean m_needToFinish = false;


    /**
     * Initialize the pool and grow it to its initial size.
     *
     * This also starts a background thread that maintains the pool's size.
     *
     * @param    initialSize  number of sessions to start with.
     * @param      maxGrowth  max additional sessions to add.  If -1, no
     *                        limit will be placed on the size.
     * @param  spareSessions  number of unused sessions to keep available.
     *                        Zero means new sessions will only be created
     *                        on demand.  Note if maxGrowth is 0, the
     *                        value of this parameter is inconsequential.
     */
    public ConfigurableSessionPool(TriplestoreSessionFactory factory,
                                   int initialSize,
                                   int maxGrowth,
                                   int spareSessions) 
                                                 throws TrippiException {
        m_factory = factory;
        m_initialSize = initialSize;
        m_maxGrowth = maxGrowth;
        m_spareSessions = spareSessions;

        m_maxSize = m_maxGrowth == -1 ? m_maxGrowth : m_initialSize + m_maxGrowth;
        m_freeSessions = new ArrayList<TriplestoreSession>(initialSize);
        m_inUseSessions = new ArrayList<TriplestoreSession>(initialSize);
        grow(initialSize);
        if (maxGrowth != 0) {
            // only start the thread if it's needed
            this.start();
        }
    }

    public String[] listTripleLanguages() {
        return m_factory.listTripleLanguages();
    }

    public String[] listTupleLanguages() {
        return m_factory.listTupleLanguages();
    }

    /**
     * Get a connection from the pool.
     *
     * @return a session, or null if none are available and growth isn't allowed.
     * @throws TrippiException if there were no spare sessions and an 
     *                              attempt to create one on-demand failed.
     */
    public synchronized TriplestoreSession get() throws TrippiException {
        TriplestoreSession session;
        // do we have any free sessions?
        if ( getFreeCount() == 0 ) {
            // no... are we allowed to grow?
            if ( m_maxSize == -1 || (m_maxGrowth > 0 && m_size < m_maxSize) ) {
                grow(1);
            } else {
                return null;
            }
        }
        synchronized (m_freeSessions) {
            session = m_freeSessions.remove(0);
        }
        synchronized (m_inUseSessions) {
            m_inUseSessions.add(session);
        }
        logger.info("Leased session: " + getStats());
        return session;
    }

    private String getStats() {
        return "free = " + getFreeCount() + ", in-use = " + getInUseCount();
    }

    /**
     * Release a connection back to the pool.
     */
    public synchronized void release(TriplestoreSession session) {
        boolean released = false;
        synchronized (m_inUseSessions) {
            if (m_inUseSessions.remove(session)) {
                synchronized (m_freeSessions) {
                    m_freeSessions.add(session);
                }
                released = true;
            } else {
                System.err.println("Warning: Session not released; it didn't "
                        + "originate with this pool!");
            }
        }
        if (released) logger.info("Got session back: " + getStats());
    }

    /**
     * Get the number of sessions currently in use.
     */
    public int getInUseCount() {
        synchronized (m_inUseSessions) {
            return m_inUseSessions.size();
        }
    }

    /**
     * Get the number of sessions not currently in use.
     */
    public int getFreeCount() {
        synchronized (m_freeSessions) {
            return m_freeSessions.size();
        }
    }

    /**
     * Maintain the pool size.
     *
     * This just ensures that pool growth occurs when necessary,
     * checking every 1/4 of a second.
     */
    @Override
	public void run() {
        while ( !m_needToFinish ) {
            int diff = m_spareSessions - getFreeCount();
            if ( diff > 0 ) {
                // spareSessions is not met.
                int numToAdd = diff;
                if ( m_maxGrowth != -1 ) {
                    // we can assume maxSize is finite (see constructor)
                    // So we will add up to "diff" new sessions,
                    // but the total new size can't exceed m_maxSize
                    while ((numToAdd + m_size) > m_maxSize) {
                        numToAdd--;
                    }
                }
                if ( numToAdd > 0 ) {
                    try {
                        grow(numToAdd);
                    } catch (TrippiException e) {
                        logger.warn("Error proactively growing "
                                + "triplestore session pool (maintenance "
                                + "thread): " + e.getMessage());
                    }
                }
            }
            if (!m_needToFinish) {
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) { }
            }
        }
    }

    private void closeAll(Iterator<TriplestoreSession> iter) {
        while ( iter.hasNext() ) {
            try {
                iter.next().close();
            } catch (Exception e) {
                System.err.println("Warning: attempt to close "
                        + "TriplestoreSession failed, continuing...");
            }
        }
    }

    /** 
     * Add a number of sessions to the pool.  
     */
    private void grow(int numToAdd) throws TrippiException {
        // Rather than synchronizing on the list throughout session creation,
        // we create them first, then add them to the list in a synch block.
        List<TriplestoreSession> newSessions = new ArrayList<TriplestoreSession>();
        for (int i = 0; i < numToAdd; i++) {
            newSessions.add( m_factory.newSession() );
        }
        synchronized (m_freeSessions) {
            m_freeSessions.addAll(newSessions);
            m_size += newSessions.size();
        }
    }

    /**
     * Close all sessions.
     *
     * This also stops the pool maintenance thread.  It should only be called 
     * when finished with the session pool.
     */
    public synchronized void close() throws TrippiException {
        if (!m_needToFinish) {
            logger.info("Closing all sessions...");
            closeAll(m_freeSessions.iterator());
            closeAll(m_inUseSessions.iterator());
            m_factory.close();
            m_needToFinish = true;
        }
    }

    /**
     * Call close() at garbage collection time in case it hasn't been
     * called yet.
     */
    @Override
	public void finalize() throws TrippiException {
        close();
    }

}