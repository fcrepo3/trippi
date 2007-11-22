package org.trippi.impl.base;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;

import org.trippi.FlushErrorHandler;
import org.trippi.RDFUtil;
import org.trippi.TripleUpdate;
import org.trippi.TrippiException;

/**
 * A memory buffer for triplestore updates.
 *
 * @author cwilper@cs.cornell.edu
 */
public class MemUpdateBuffer implements UpdateBuffer {

    private static Logger LOG = Logger.getLogger(MemUpdateBuffer.class.getName());

    private int m_safeCapacity;
    private int m_flushBatchSize;
    private List<TripleUpdate> m_buffer;
    private Object m_bufferLock = new Object();

    private FlushErrorHandler m_flushErrorHandler;

    public MemUpdateBuffer(int safeCapacity,
                           int flushBatchSize) {
        m_safeCapacity = safeCapacity;
        m_flushBatchSize = flushBatchSize;
        m_buffer = Collections.synchronizedList(new ArrayList<TripleUpdate>(safeCapacity));
    }

    public void add(List<Triple> triples) {
        debugUpdate("Adding " + triples.size() + " triple ADDs to buffer", triples);
        synchronized (m_bufferLock) {
            m_buffer.addAll(TripleUpdate.get(TripleUpdate.ADD, triples));
        }
    }

    public void add(Triple triple) {
        debugUpdate("Adding 1 triple ADD to buffer", triple);
        synchronized (m_bufferLock) {
            m_buffer.add(TripleUpdate.get(TripleUpdate.ADD, triple));
        }
    }

    public void delete(List<Triple> triples) {
        debugUpdate("Adding " + triples.size() + " triple DELETEs to buffer", triples);
        synchronized (m_bufferLock) {
            m_buffer.addAll(TripleUpdate.get(TripleUpdate.DELETE, triples));
        }
    }

    public void delete(Triple triple) {
        debugUpdate("Adding 1 triple DELETE to buffer", triple);
        synchronized (m_bufferLock) {
            m_buffer.add(TripleUpdate.get(TripleUpdate.DELETE, triple));
        }
    }

    private static void debugUpdate(String msg, Triple triple) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(msg + "\n" + RDFUtil.toString(triple));
        }
    }

    private static void debugUpdate(String msg, List<Triple> triples) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(msg + "\n" + tripleListToString(triples));
        }
    }

    private static String tripleListToString(List<Triple> triples) {
        StringBuffer out = new StringBuffer();
        for (int i = 0; i < triples.size(); i++) {
            out.append(RDFUtil.toString((Triple) triples.get(i)) + "\n");
        }
        return out.toString();
    }

    public int size() {
        try {
            return m_buffer.size();
        } catch (Exception e) {
            return 0;
        }
    }

    public int safeCapacity() {
        return m_safeCapacity;
    }

    /**
     * Flush the contents of the buffer to the triplestore.
     */
    public synchronized void flush(TriplestoreSession session) throws TrippiException {
        // copy the buffer, then clear it
        List<TripleUpdate> toFlush = null;
        synchronized (m_bufferLock) {
            if (m_buffer.size() > 0) {
                toFlush = m_buffer;
                m_buffer = Collections.synchronizedList(new ArrayList<TripleUpdate>(m_safeCapacity));
            }
        }
        try {
            if (toFlush != null) {
                Set<Triple>[] updates = normalize(toFlush.iterator(), toFlush.size());
                if (updates[0].size() > m_flushBatchSize) {
                    writeBatches(updates[0].iterator(), TripleUpdate.ADD, session);
                } else {
                    writeBatch(TripleUpdate.ADD, updates[0], session);
                }
                if (updates[1].size() > m_flushBatchSize) {
                    writeBatches(updates[1].iterator(), TripleUpdate.DELETE, session);
                } else {
                    writeBatch(TripleUpdate.DELETE, updates[1], session);
                }
            }
        } catch (TrippiException e) {
            // in the event of failure, send toFlush and the exception to the 
            // flushErrorHandler, if set.
            if (m_flushErrorHandler != null) {
                m_flushErrorHandler.handleFlushError(toFlush, e);
            }
            // ... then re-throw the exception
            throw e;
        }
    }

    public void setFlushErrorHandler(FlushErrorHandler h) {
        m_flushErrorHandler = h;
    }

    /**
     * Normalize the content of the buffer for efficiency.
     *
     * This will return an array of two Sets of Triples.
     * The first set consists of the ADDs, and the second
     * set consists of the DELETEs.
     */
    private static Set<Triple>[] normalize(Iterator<TripleUpdate> iter, int size) {
        int initialCapacity = size / 2;
        Set<Triple> adds = new HashSet<Triple>(initialCapacity);
        Set<Triple> deletes = new HashSet<Triple>(initialCapacity);

        while (iter.hasNext()) {
            TripleUpdate update = iter.next();
            if (update.type == TripleUpdate.ADD) {
                if (!deletes.remove(update.triple)) {
                    adds.add(update.triple);
                }
            } else {
                if (!adds.remove(update.triple)) {
                    deletes.add(update.triple);
                }
            }
        }

        return new Set[] { adds, deletes };
    }

    private void writeBatches(Iterator<Triple> iter, int updateType,
            TriplestoreSession session)
            throws TrippiException {
        Set<Triple> triples = new HashSet<Triple>();
        while (iter.hasNext()) {
            triples.add(iter.next());
            if (triples.size() == m_flushBatchSize) {
                writeBatch(updateType, triples, session);
                triples.clear();
            }
        }
        if (triples.size() > 0) { // final write
            writeBatch(updateType, triples, session);
        }
    }

    /**
     * Do the actual writing of a batch to the session.
     */
    private void writeBatch(int type, Set<Triple> triples, TriplestoreSession session)
            throws TrippiException {
        if (type == TripleUpdate.ADD) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Writing batch of " + triples.size() + " ADDs");
            }
            session.add(triples);
        } else if (type == TripleUpdate.DELETE) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Writing batch of " + triples.size() + " DELETEs");
            }
            session.delete(triples);
        }
    }

    /**
     * Close the buffer, releasing any associated system resources.
     */
    public void close() {
        // nothing to release
    }

	public List<TripleUpdate> findBufferedUpdates(SubjectNode subject, 
    								PredicateNode predicate, 
    								ObjectNode object, 
    								int updateType) {
		List<TripleUpdate> updates = new ArrayList<TripleUpdate>();
		synchronized(m_buffer) {
			Iterator<TripleUpdate> it = m_buffer.iterator();
			while (it.hasNext()) {
				TripleUpdate tup = it.next();
				if (updateType == UpdateBuffer.EITHER_UPDATE_TYPE || 
						tup.type == updateType) {
					Triple t = tup.triple;
					if ( (subject == null || t.getSubject().equals(subject)) &&
						 (predicate == null || t.getPredicate().equals(predicate)) &&
						 (object == null || t.getObject().equals(object)) ) {
						updates.add(tup);
					}
				}
			}
		}
		return updates;
	}
}
