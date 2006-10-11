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

    private static Logger logger = Logger.getLogger(MemUpdateBuffer.class.getName());

    private int m_safeCapacity;
    private int m_flushBatchSize;
    private List m_buffer;

    private FlushErrorHandler m_flushErrorHandler;

    public MemUpdateBuffer(int safeCapacity,
                           int flushBatchSize) {
        m_safeCapacity = safeCapacity;
        m_flushBatchSize = flushBatchSize;
        m_buffer = Collections.synchronizedList(new ArrayList(safeCapacity));
    }

    public void add(List triples) {
        debugUpdate("Adding " + triples.size() + " triples to buffer", triples);
        m_buffer.addAll(TripleUpdate.get(TripleUpdate.ADD, triples));
    }

    public void add(Triple triple) {
        debugUpdate("Adding 1 triple to buffer", triple);
        m_buffer.add(TripleUpdate.get(TripleUpdate.ADD, triple));
    }

    public void delete(List triples) {
        debugUpdate("Deleting " + triples.size() + " triples from buffer", triples);
        m_buffer.addAll(TripleUpdate.get(TripleUpdate.DELETE, triples));
    }

    public void delete(Triple triple) {
        debugUpdate("Deleting 1 triple from buffer", triple);
        m_buffer.add(TripleUpdate.get(TripleUpdate.DELETE, triple));
    }

    private static void debugUpdate(String msg, Triple triple) {
        if (logger.isDebugEnabled()) {
            logger.debug(msg + "\n" + RDFUtil.toString(triple));
        }
    }

    private static void debugUpdate(String msg, List triples) {
        if (logger.isDebugEnabled()) {
            logger.debug(msg + "\n" + tripleListToString(triples));
        }
    }

    private static String tripleListToString(List triples) {
        StringBuffer out = new StringBuffer();
        for (int i = 0; i < triples.size(); i++) {
            out.append(RDFUtil.toString((Triple) triples.get(i)) + "\n");
        }
        return out.toString();
    }

    public int size() {
        return m_buffer.size();
    }

    public int safeCapacity() {
        return m_safeCapacity;
    }

    /**
     * Flush the contents of the buffer to the triplestore.
     */
    public void flush(TriplestoreSession session) throws TrippiException {
        // copy the buffer, then clear it
        ArrayList toFlush = new ArrayList(m_buffer.size());
        synchronized (m_buffer) {
            toFlush.addAll(m_buffer);
            m_buffer = Collections.synchronizedList(new ArrayList(m_safeCapacity));
        }
        try {
            if (toFlush.size() > 0) writeBatches(toFlush.iterator(), session);
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
     * Go through iter, writing in batches.
     */
    private void writeBatches(Iterator iter, TriplestoreSession session)
            throws TrippiException {
        int lastType = TripleUpdate.NONE;
        Set triples = new HashSet();
        while (iter.hasNext()) {
            TripleUpdate update = (TripleUpdate) iter.next();
            if (update.type != lastType) {
                // types changed: force write, switch types, and clear list
                writeBatch(lastType, triples, session);
                lastType = update.type;
                triples.clear();
            }
            triples.add(update.triple);
            if (triples.size() >= m_flushBatchSize) {
                // flush batch size reached: force write and clear list
                writeBatch(lastType, triples, session);
                triples.clear();
            }
        }
        if (triples.size() > 0) { // final write
            writeBatch(lastType, triples, session);
        }
    }

    /**
     * Do the actual writing of a batch to the session.
     */
    private void writeBatch(int type, Set triples, TriplestoreSession session)
            throws TrippiException {
        if (type == TripleUpdate.ADD) {
            session.add(triples);
        } else if (type == TripleUpdate.DELETE) {
            session.delete(triples);
        }
    }

    /**
     * Close the buffer, releasing any associated system resources.
     */
    public void close() {
        m_buffer.clear();
        m_buffer = null;
    }

    /**
     * Ensure close() is called at garbage collection time.
     */
    public void finalize() {
        close();
    }

	public List findBufferedUpdates(SubjectNode subject, 
    								PredicateNode predicate, 
    								ObjectNode object, 
    								int updateType) {
		List updates = new ArrayList();
		synchronized(m_buffer) {
			Iterator it = m_buffer.iterator();
			while (it.hasNext()) {
				TripleUpdate tup = (TripleUpdate)it.next();
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
