package org.trippi.impl.base;

import java.net.URI;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;
import junit.swingui.TestRunner;

import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;

import org.trippi.FlushErrorHandler;
import org.trippi.RDFUtil;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

public abstract class UpdateBufferUnitTest extends TestCase {

    private RDFUtil _util;
    private UpdateBuffer _buffer;

    public UpdateBufferUnitTest(String name) throws Exception { 
        super(name); 
        _util = new RDFUtil();
    }

    // subclasses need to provide the appropriate instance when asked
    protected abstract UpdateBuffer getBuffer(int safeCapacity,
                                              int flushBatchSize) throws Exception;
            
    // Test correct reporting of safe capacity

    public void testGetSafeCapacity() throws Exception {
        _buffer = getBuffer(10, 5);
        assertEquals(10, _buffer.safeCapacity());
        _buffer.close();
        _buffer = getBuffer(5, 5);
        assertEquals(5, _buffer.safeCapacity());
    }

    // Test adding adds and deletes to buffer

    public void testAddOneAddToBuffer() throws Exception {
        _buffer = getBuffer(10, 5);
        _buffer.add(getTriple(1, 1, 1));
        assertEquals(1, _buffer.size());
    }

    public void testAddFiveAddsToBuffer() throws Exception {
        _buffer = getBuffer(10, 5);
        _buffer.add(getTriples(1, 1, 5));
        assertEquals(5, _buffer.size());
    }

    public void testAddOneDeleteToBuffer() throws Exception {
        _buffer = getBuffer(10, 5);
        _buffer.delete(getTriple(1, 1, 1));
        assertEquals(1, _buffer.size());
    }

    public void testAddFiveDeletesToBuffer() throws Exception {
        _buffer = getBuffer(10, 5);
        _buffer.delete(getTriples(1, 1, 5));
        assertEquals(5, _buffer.size());
    }

    public void testAddOneAddAndSameDeleteToBuffer() throws Exception {
        _buffer = getBuffer(10, 5);
        Triple triple = getTriple(1, 1, 1);
        _buffer.add(triple);
        _buffer.delete(triple);
        assertEquals(2, _buffer.size());
    }

    public void testAddMixedAddsAndDelsToBuffer() throws Exception {
        _buffer = getBuffer(10, 5);
        Triple triple1 = getTriple(1, 1, 1);
        _buffer.add(triple1);
        _buffer.delete(triple1);
        Triple triple2 = getTriple(1, 1, 2);
        Triple triple3 = getTriple(1, 1, 3);
        Triple triple4 = getTriple(1, 1, 4);
        _buffer.add(triple2);
        _buffer.delete(triple3);
        _buffer.delete(triple4);
        assertEquals(5, _buffer.size());
    }

    // Test that adding to buffer can occur at the same time,
    // on a different thread from the flush

    public void testCanAddDuringFlush() throws Exception {

        // set up our fake session
        FakeTriplestoreSession session = new FakeTriplestoreSession(1000);

        // set up our buffer with an add
        _buffer = getBuffer(10, 5);
        _buffer.add(getTriple(1, 1, 1));

        // prepare a separate thread to perform the flushing
        FlushingThread flusher = new FlushingThread(_buffer, session, 1);
        flusher.stopAfterNextFlush();

        // pause the session so it locks the flusher till we're ready
        session.setPaused(true);

        // start the flusher thread
        flusher.start();

        // wait for the session to pause the thread
        while (session.getPausingThread() == null) {
            try { Thread.sleep(25); } catch (InterruptedException e) { }
        }

        // if the buffer impl blocks this while session.add is being
        // invoked, this will catch it.  the FakeTriplestoreSession
        // will timeout, the flusher will fail and set a flag for us,
        // the buffer's block will be released, and this call will
        // continue.
        _buffer.add(getTriple(1, 1, 2));

        // un-pause the session...this should allow the flusher to complete
        session.setPaused(false);

        // wait a full second if we have to
        long waitedMillis = 0;
        while (flusher.isAlive() && waitedMillis < 1000) {
            try {
                Thread.sleep(100);
                waitedMillis += 100;
            } catch (InterruptedException e) {
            }
        }

        assertFalse("Flusher thread didn't complete even after "
                + "un-pausing the fake session", flusher.isAlive());

        // check whether the flusher detected an error.
        // if it did, it was due to a lock imposed by the buffer impl,
        // and this test should fail.
        Exception flushError = flusher.getError();
        if (flushError != null) {
            fail("Flusher reported error during flush: " 
                    + flushError.getClass().getName() + ": " 
                    + flushError.getMessage());
        }

    }


    // Test that when an error occurs on the underlying session 
    // during a flush, the buffer does both of the following:
    // 1) sends the error to the FlushErrorHandler, and
    // 2) throws the exception

    public void testFailedFlushNotifiesErrorHandlerAndThrows() throws Exception {

        // set up our fake session and error handler
        FakeTriplestoreSession session = new FakeTriplestoreSession();
        session.setExceptionToThrow(new TrippiException("test"));
        FakeFlushErrorHandler errorHandler = new FakeFlushErrorHandler();

        // set up our buffer with an add
        _buffer = getBuffer(10, 5);
        _buffer.add(getTriple(1, 1, 1));
        _buffer.setFlushErrorHandler(errorHandler);

        boolean threwTheException = false;
        try {
            _buffer.flush(session);
        } catch (TrippiException e) {
            threwTheException = true;
        }

        assertTrue("UpdateBuffer impl did not throw exception from session", 
                   threwTheException);

        assertTrue("UpdateBuffer impl did not notify the FlushErrorHandler of session failure",
                   errorHandler.getException() != null);

    }

    // Do several dozen chunks of adds while flushes are intermittenly
    // happening.  Ensure when it's all said and done, we have flushed
    // all the triples we expect to have flushed.

    public void testConcurrentMultiAdds() throws Exception {
        doConcurrentAddTest(true);
    }

    public void testConcurrentSingleAdds() throws Exception {
        doConcurrentAddTest(false);
    }

    public void testConcurrentMultiDeletes() throws Exception {
        doConcurrentDeleteTest(true);
    }

    public void testConcurrentSingleDeletes() throws Exception {
        doConcurrentDeleteTest(false);
    }

    private void doConcurrentAddTest(boolean multi) throws Exception {
        FakeTriplestoreSession session = new FakeTriplestoreSession();

        int numModders       = 10;
        int chunksPerModder  = 20;
        int triplesPerChunk  = 100;
        int flushThreshold   = 2000;
        int expected = numModders * chunksPerModder * triplesPerChunk;

        // the session starts over an empty "triplestore"

        doConcurrentMods(session, numModders, multi, chunksPerModder, 
                         triplesPerChunk, true, flushThreshold);
        assertEquals("Wrong number of triples flushed", expected, session.size());
        
    }

    private void doConcurrentDeleteTest(boolean multi) throws Exception {
        FakeTriplestoreSession session = new FakeTriplestoreSession();

        int numModders       = 10;
        int chunksPerModder  = 20;
        int triplesPerChunk  = 100;
        int flushThreshold   = 2000;
        int expected = numModders * chunksPerModder * triplesPerChunk;

        // populate the "triplestore"
        Set triples = new HashSet();
        for (int i = 0; i < numModders; i++) {
            for (int j = 0; j < chunksPerModder; j++) {
                for (int k = 0; k < triplesPerChunk; k++) {
                    triples.add(getTriple(i, j, k));
                }
            }
        }
        session.add(triples);

        // quick sanity checks to make sure we set up the test correctly
        assertEquals(session.size(), triples.size());
        assertEquals(session.size(), expected);

        // now do the deletes...
        // if dels are flushed properly, we should end up with 0
        doConcurrentMods(session, numModders, multi, chunksPerModder, 
                         triplesPerChunk, false, flushThreshold);
        assertEquals("Triple deletes were not all flushed", 0, session.size());
        
    }

    /**
     * On the given session, do the indicated mods with multiple modder
     * threads and asynchronous flushing.
     *
     * This method returns when all threads are done.
     */
    private void doConcurrentMods(FakeTriplestoreSession session,
                                  int numModders,
                                  boolean useBatchCalls,
                                  int chunksPerModder,
                                  int triplesPerChunk,
                                  boolean adds,
                                  int flushThreshold) throws Exception {

        _buffer = getBuffer(flushThreshold, flushThreshold);

        // init + start the flusher
        FlushingThread flusher = new FlushingThread(_buffer, session, flushThreshold);
        flusher.start();

        // init + start the modders
        ModdingThread[] modders = new ModdingThread[numModders];
        for (int i = 0; i < numModders; i++) {
            modders[i] = new ModdingThread(i,
                                           _buffer,
                                           useBatchCalls,
                                           chunksPerModder,
                                           triplesPerChunk,
                                           adds);
            modders[i].start();
        }

        // wait for modders to finish
        int numModdersFinished = 0;
        while (numModdersFinished < numModders) {
            numModdersFinished = 0;
            for (int i = 0; i < numModders; i++) {
                if (!modders[i].isAlive()) {
                    numModdersFinished++;
                }
            }
        }

        // wait for flusher to deal with overflow
        while (_buffer.size() >= flushThreshold) {
            try { Thread.sleep(10); } catch (InterruptedException e) { }
        }

        // signal to flusher to do one more if needed, then finish
        flusher.stopAfterNextFlush();

        // wait for flusher to finish
        while (flusher.isAlive()) {
            try { Thread.sleep(10); } catch (InterruptedException e) { }
        }

        // check modders for any errors, and throw first found
        for (int i = 0; i < numModders; i++) {
            Exception e = modders[i].getError();
            if (e != null) {
                throw new Exception("Unexpected error in modder thread", e);
            }
        }

    }

    //
    // Helper Methods for the unit tests in this class
    //

    private Triple getTriple(String sURI,
                             String pURI,
                             String oURI) throws Exception {
        return _util.createTriple(_util.createResource(new URI(sURI)), 
                                  _util.createResource(new URI(pURI)),
                                  _util.createResource(new URI(oURI)));
    }

    public void tearDown() throws Exception {
        if (_buffer != null) _buffer.close();
    }

    private Triple getTriple(int sNum, int pNum, int oNum) throws Exception {
        return getTriple("urn:s" + sNum, "urn:p" + pNum, "urn:o" + oNum);
    }

    private List getTriples(int sPerms, int pPerms, int oPerms) throws Exception {
        List list = new ArrayList(sPerms * pPerms * oPerms);
        for (int sNum = sPerms; sNum > 0; sNum--) {
            for (int pNum = pPerms; pNum > 0; pNum--) {
                for (int oNum = oPerms; oNum > 0; oNum--) {
                    list.add(getTriple(sNum, pNum, oNum));
                }
            }
        }
        return list;
    }

    //
    // Mock objects, etc. for the unit tests in this class
    //

    public class FakeTriplestoreSession implements TriplestoreSession {

        private boolean _paused = false;

        private long _maxMillis;

        private Thread _pausingThread;

        private TrippiException _exceptionToThrow;

        private Set _triples;

        /**
         * @param maxMillis maximum milliseconds to pause before
         *        throwing an exception during an add/delete while
         *        "paused".
         */
        public FakeTriplestoreSession(long maxMillis) { 
            _maxMillis = maxMillis;
            _triples = new HashSet();
        }

        public FakeTriplestoreSession() {
            _maxMillis = 0;
            _triples = new HashSet();
        }

        public void setExceptionToThrow(TrippiException e) {
            _exceptionToThrow = e;
        }

        /**
         * Pausing this fake session will cause any calls to add or
         * delete to sleep until un-paused.
         */
        public void setPaused(boolean paused) {
            _paused = paused;
        }

        public Thread getPausingThread() {
            return _pausingThread;
        }

        private synchronized void sleepTillUnpaused() throws TrippiException {
            long waitedMillis = 0;
            _pausingThread = Thread.currentThread();
            while (_paused && waitedMillis <= _maxMillis) {
                try {
                    Thread.sleep(100);
                    waitedMillis += 100;
                } catch (InterruptedException e) {
                }
            }
            _pausingThread = null;
            if (waitedMillis > _maxMillis) {
                throw new TrippiException("FakeTriplestoreSession sleep timeout exceeded");
            }
        }

        private void throwExceptionIfSet() throws TrippiException {
            if (_exceptionToThrow != null) {
                throw _exceptionToThrow;
            }
        }

        // for testing
        public int size() {
            return _triples.size();
        }

        //
        // Test-specific implementations of TriplestoreSession methods.
        //

        public void add(Set triples) throws TrippiException {
            sleepTillUnpaused();
            throwExceptionIfSet();
            _triples.addAll(triples);
        }

        public void delete(Set triples) throws TrippiException {
            sleepTillUnpaused();
            throwExceptionIfSet();
            _triples.removeAll(triples);
        }

        //
        // All other methods from TriplestoreSession interface are no-ops.
        //

        public TupleIterator query(String queryText,
                                   String language) {
            return null;
        }

        public TripleIterator findTriples(String lang,
                                          String queryText) {
            return null;
        }

        public TripleIterator findTriples(SubjectNode subject,
                                          PredicateNode predicate,
                                          ObjectNode object) {
            return null;
        }

        public String[] listTupleLanguages() {
            return null;
        }

        public String[] listTripleLanguages() {
            return null;
        }

        public void close() { }

    }

    public class FakeFlushErrorHandler implements FlushErrorHandler {

        private TrippiException _exception;

        public FakeFlushErrorHandler() { }

        public void handleFlushError(List failedUpdates, 
                                     TrippiException cause) {
            _exception = cause;
        }

        public TrippiException getException() {
            return _exception;
        }

    }

    public class FlushingThread extends Thread {

        private UpdateBuffer _buffer;
        private TriplestoreSession _session;
        private Exception _error;
        private boolean _stopAfterNextFlush;
        private int _flushIfThisSizeOrAbove;

        public FlushingThread(UpdateBuffer buffer,
                              TriplestoreSession session,
                              int flushIfThisSizeOrAbove) {
            _buffer = buffer;
            _session = session;
            _flushIfThisSizeOrAbove = flushIfThisSizeOrAbove;
        }

        public void stopAfterNextFlush() {
            _stopAfterNextFlush = true;
        }

        // flush at least once, and then keep flushing until 
        // stopAfterNextFlush is called
        public void run() {
            try {
                boolean finish = false;
                while (!finish) {
                    if (_stopAfterNextFlush) {
                        finish = true;
                    }
                    if (_buffer.size() >= _flushIfThisSizeOrAbove) {
                        _buffer.flush(_session);
                    }
                    if (!finish) {
                        this.yield();
                        /*
                        try {
                            Thread.sleep(25);
                        } catch (InterruptedException e) {
                        }
                        */
                    }
                }
            } catch (Exception e) {
                _error = e;
            }
        }

        public Exception getError() {
            return _error;
        }
    }

    public class ModdingThread extends Thread {

        private int _id;
        private UpdateBuffer _buffer;
        private boolean _useBatchCalls;
        private int _numChunks;
        private int _triplesPerChunk;
        private boolean _doAdds;

        private Exception _error;

        public ModdingThread(int id, 
                             UpdateBuffer buffer,
                             boolean useBatchCalls, 
                             int numChunks,
                             int triplesPerChunk, 
                             boolean doAdds) {
            _id = id;
            _buffer = buffer;
            _useBatchCalls = useBatchCalls;
            _numChunks = numChunks;
            _triplesPerChunk = triplesPerChunk;
            _doAdds = doAdds;

        }

        public void run() {

            try {
                for (int i = 0; i < _numChunks; i++) {
         
                    // generate triples for this chunk
                    List triples = new ArrayList();
                    for (int j = 0; j < _triplesPerChunk; j++) {
                        triples.add(getTriple(_id, i, j));
                    }

                    // do the adds or deletes for this chunk
                    if (_doAdds) {
                        if (_useBatchCalls) {
                            _buffer.add(triples);
                        } else {
                            Iterator iter = triples.iterator();
                            while (iter.hasNext()) {
                                Triple triple = (Triple) iter.next();
                                _buffer.add(triple);
                            }
                        }
                    } else {
                        if (_useBatchCalls) {
                            _buffer.delete(triples);
                        } else {
                            Iterator iter = triples.iterator();
                            while (iter.hasNext()) {
                                Triple triple = (Triple) iter.next();
                                _buffer.delete(triple);
                            }
                        }
                    }

                    // yield so we're not hogging any impl-specific
                    // buffer locks
                    this.yield();
                }
            } catch (Exception e) {
                _error = e;
            }
        }

        private Exception getError() {
            return _error;
        }

    }

}
