package org.trippi;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import java.net.URI;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;
import junit.swingui.TestRunner;

import org.jrdf.graph.Triple;

import org.trippi.config.TrippiProfile;

public abstract class TriplestoreConnectorIntegrationTest extends TestCase {

    private TriplestoreConnector _connector;
    private TriplestoreReader _reader;
    private TriplestoreWriter _writer;
    private RDFUtil _util;

    public TriplestoreConnectorIntegrationTest(String name) throws Exception { 
        super(name); 
    }

    public void setUp() throws Exception {
        TrippiProfile profile = TestConfig.getTestProfile();
        _connector = profile.getConnector();
        _reader = _connector.getReader();
        _writer = _connector.getWriter();
        _util = new RDFUtil();
    }

    public void tearDown() throws Exception {
        deleteAllTriples();
    }

    private void deleteAllTriples() throws Exception {
        File dump = new File(TestConfig.getTestDir(), "all-triples.txt");
        FileOutputStream out = null;
        try {
            // write all to temp file
            TripleIterator triples = _reader.findTriples(null, null, null, -1);
            out = new FileOutputStream(dump);
            triples.toStream(out, RDFFormat.TURTLE);
            try { out.close(); } catch (Exception e) { }
            out = null;

            // load all from temp file
            triples = TripleIterator.fromStream(new FileInputStream(dump), RDFFormat.TURTLE);
            _writer.delete(triples, true);
        } finally {
            if (out != null) out.close();
//            dump.delete();
        }
    }

    /**
     * Get the connector to use for this test, based on current test 
     * configuration.
     *
     * Subclasses may downcast as necessary for impl-specific tests.
     */
    protected TriplestoreConnector getConnector() throws Exception {
        return _connector;
    }

    /**
     * Test adding some triples.
     *
     * Make sure the count is correct and the set matches.
     */
    public void testAddTriples() throws Exception {
        List testTriples = getTriples(2, 2, 2);
        _writer.add(testTriples, true);
        assertEquals("Wrong number of triples",
                     8,
                     _reader.countTriples(null, null, null, -1));

        Set inputSet = new HashSet();
        inputSet.addAll(testTriples);

        Set outputSet = getSet(_reader.findTriples(null, null, null, -1));

        assertEquals("Count was ok, but got different set of triples!",
                     inputSet,
                     outputSet);
    }

    /**
     * Test deleting some triples.
     *
     * Make sure none exist after adding and deleting the same triples.
     */
    public void testDeleteTriples() throws Exception {
        List testTriples = getTriples(2, 2, 2);
        _writer.add(testTriples, true);
        _writer.delete(testTriples, true);
        assertEquals("Wrong number of triples", 
                     0, 
                     _reader.countTriples(null, null, null, -1));
    }

    /**
     * Test that SPO is a supported triple language.
     */
    public void testSupportsSPO() throws Exception {
        String[] langs = _reader.listTripleLanguages();
        boolean found = false;
        for (int i = 0; i < langs.length; i++) {
            if (langs[i].equalsIgnoreCase("SPO")) {
                found = true;
            }
        }
        assertTrue("None of the triple langs returned were SPO", found);
    }

    // TODO: Add tests for various findTriples(SubjectNode, ...) queries.
    // TODO: Above with unicode chars in literals
    // TODO: Add tests for various SPO queries.
    // TODO: Above with unicode chars in literals
    // TODO: Add tests for adding/deleting triples with unicode chars in literals

    public void testAddConcurrencyForceFlushOnce() throws Exception {
        doAddConcurrency(5, 50, 75, false);
    }

    public void testAddConcurrencyForceFlushOften() throws Exception {
        doAddConcurrency(5, 50, 75, true);
    }

    /**
     * Test that we can add a bunch of triples from multiple threads,
     * and they all get flushed properly.
     */
    private void doAddConcurrency(int numAdders,
                                  int batchesPerAdder,
                                  int triplesPerBatch,
                                  boolean forceFlushOften) throws Exception {

        int expected = numAdders * batchesPerAdder * triplesPerBatch;

        // init and start adder threads
        TripleAdder[] adders = new TripleAdder[numAdders];
        int addersAlive = 0;
        for (int i = 0; i < numAdders; i++) {
            adders[i] = new TripleAdder(i + 1, 
                                        batchesPerAdder, 
                                        triplesPerBatch, 
                                        _writer);
            adders[i].start();
            addersAlive++;
        }

        // wait till all adders are done
        while (addersAlive > 0) {
            if (forceFlushOften) {
                _writer.flushBuffer();
            } else {
                try { Thread.sleep(10); } catch (InterruptedException e) { }
            }
            addersAlive = 0;
            for (int i = 0; i < numAdders; i++) {
                if (adders[i].isAlive()) addersAlive++;
            }
        }

        // check if any had error, if so fail
        for (int i = 0; i < numAdders; i++) {
            Exception e = adders[i].getError();
            if (e != null) {
                int adderId = i + 1;
                throw new RuntimeException("Adder thread #" 
                        + adderId + " encountered error", e);
            }
        }

        // force flush
        _writer.flushBuffer();

        assertEquals("Wrong number of triples after add",
                     expected,
                     _reader.countTriples(null, null, null, -1));
    }

    private Set getSet(TripleIterator iter) throws Exception {
        HashSet set = new HashSet();
        while (iter.hasNext()) {
            set.add(iter.next());
        }
        iter.close();
        return set;
    }

    private List getTriples(int endS, int endP, int endO) throws Exception {
        return getTriples(1, endS, 1, endP, 1, endO);
    }

    private List getTriples(int startS, int endS,
                            int startP, int endP,
                            int startO, int endO) throws Exception {
        List list = new ArrayList();
        for (int s = startS; s <= endS; s++) {
            for (int p = startP; p <= endP; p++) {
                for (int o = startO; o <= endO; o++) {
                    list.add(getTriple(s, p, o));
                }
            }
        }
        return list;
    }

    private Triple getTriple(int s, int p, int o) throws Exception {
        return _util.createTriple(
                _util.createResource(new URI("urn:test:" + s)),
                _util.createResource(new URI("urn:test:" + p)),
                _util.createResource(new URI("urn:test:" + o)));
    }

    public class TripleAdder extends Thread {

        private int _id;
        private int _numBatches;
        private int _triplesPerBatch;
        private TriplestoreWriter _writer;

        private Exception _error;

        public TripleAdder(int id, // first is 1, not 0
                           int numBatches,
                           int triplesPerBatch,
                           TriplestoreWriter writer) {
            _id = id;
            _numBatches = numBatches;
            _triplesPerBatch = triplesPerBatch;
            _writer = writer;
        }

        public void run() {
            try {
                for (int i = 0; i < _numBatches; i++) {
                    List triples = getTriples(_id, _id,
                                              i + 1, i + 1,
                                              1, _triplesPerBatch);
                    _writer.add(triples, false);
                    this.yield();
                }
            } catch (Exception e) {
                _error = e;
            }
        }

        public Exception getError() {
            return _error;
        }

    }
            
}
