package org.trippi;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.Triple;
import org.trippi.config.TrippiProfile;

public abstract class TriplestoreConnectorIntegrationTest extends TestCase {

    private TriplestoreConnector _connector;
    private TriplestoreReader _reader;
    private TriplestoreWriter _writer;
    private GraphElementFactory _geFactory;

    public TriplestoreConnectorIntegrationTest(String name) throws Exception { 
        super(name); 
    }

    @Override
	public void setUp() throws Exception {
        TrippiProfile profile = TestConfig.getTestProfile();
        _connector = profile.getConnector();
        _connector.open();
        _geFactory = _connector.getElementFactory();
        _reader = _connector.getReader();
        _writer = _connector.getWriter();
    }

    @Override
	public void tearDown() throws Exception {
        deleteAllTriples();
        _connector.close();
    }

    private void deleteAllTriples() throws Exception {
        File dump = new File(TestConfig.getTestDir(), "all-triples.txt");
        FileOutputStream out = null;
        try {
            TripleIterator triples = _reader.findTriples(null, null, null, -1);
            _writer.delete(triples, true);
        } finally {
            if (out != null) out.close();
            dump.delete();
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
        List<Triple> testTriples = getTriples(2, 2, 2);
        _writer.add(testTriples, true);
        assertEquals("Wrong number of triples",
                     8,
                     _reader.countTriples(null, null, null, -1));
        Set<Triple> inputSet = new HashSet<Triple>();
        inputSet.addAll(testTriples);
        Set<Triple> outputSet = getSet(_reader.findTriples(null, null, null, -1));
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
        List<Triple> testTriples = getTriples(2, 2, 2);
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

    public void testUnicode() throws Exception {
        Triple t1 = getTriple("一", "二", "吃了吗？");
        Triple t2 = getTriple("一", "二", "这是中文", false);
        List<Triple> triples = new ArrayList<Triple>();
        triples.add(t1);
        triples.add(t2);
        _writer.add(triples, true);
        assertEquals(1, _reader.countTriples(null, null, t1.getObject(), -1));
        assertEquals(1, _reader.countTriples(null, null, t2.getObject(), -1));
        assertEquals(2, _reader.countTriples(t1.getSubject(), null, null, -1));
        assertEquals(2, _reader.countTriples(null, t2.getPredicate(), null, -1));
        
        TripleIterator iter;
        iter = _reader.findTriples(null, null, t1.getObject(), -1);
        assertTrue(iter.hasNext());
        assertEquals(t1, iter.next());
        
        iter = _reader.findTriples(null, null, t2.getObject(), -1);
        assertTrue(iter.hasNext());
        assertEquals(t2, iter.next());
        
        _writer.delete(t1, true);
        assertEquals(0, _reader.countTriples(null, null, t1.getObject(), -1));
        _writer.delete(t2, true);
        assertEquals(0, _reader.countTriples(null, null, t2.getObject(), -1));
    }
    
    public void testAddConcurrencyForceFlushOnce() throws Exception {
        doModifyConcurrency(5, 50, 75, false, false, true);
    }

    public void testAddConcurrencyForceFlushOften() throws Exception {
        doModifyConcurrency(5, 50, 75, false, true, true);
    }

    public void testDeleteConcurrencyForceFlushOnce() throws Exception {
        _writer.add(getTriples(5, 50, 75), true);
        doModifyConcurrency(5, 50, 75, false, false, false);
    }

    public void testDeleteConcurrencyForceFlushOften() throws Exception {
        _writer.add(getTriples(5, 50, 75), true);
        doModifyConcurrency(5, 50, 75, false, true, false);
    }
    
    /**
     * Test that we can add a bunch of triples from multiple threads,
     * and they all get flushed properly.
     */
    private void doModifyConcurrency(int numModifiers,
                                     int batchesPerModifier,
                                     int triplesPerBatch,
                                     boolean oneTripleAtATime,
                                     boolean forceFlushOften,
                                     boolean adds) throws Exception {

        int expected = numModifiers * batchesPerModifier * triplesPerBatch;

        // init and start modifier threads
        TripleModifier[] modifiers = new TripleModifier[numModifiers];
        int modifiersAlive = 0;
        for (int i = 0; i < numModifiers; i++) {
            modifiers[i] = new TripleModifier(i + 1, 
                                        batchesPerModifier, 
                                        triplesPerBatch, 
                                        _writer,
                                        oneTripleAtATime,
                                        adds);
            modifiers[i].start();
            modifiersAlive++;
        }

        // wait till all modifiers are done
        while (modifiersAlive > 0) {
            if (forceFlushOften) {
                _writer.flushBuffer();
            } else {
                try { Thread.sleep(10); } catch (InterruptedException e) { }
            }
            modifiersAlive = 0;
            for (int i = 0; i < numModifiers; i++) {
                if (modifiers[i].isAlive()) modifiersAlive++;
            }
        }

        // check if any had error, if so fail
        for (int i = 0; i < numModifiers; i++) {
            Exception e = modifiers[i].getError();
            if (e != null) {
                int modifierId = i + 1;
                throw new RuntimeException("Modifier thread #" 
                        + modifierId + " encountered error", e);
            }
        }

        // force flush
        _writer.flushBuffer();

        if (adds) {
            assertEquals("Wrong number of triples after add",
                         expected,
                         _reader.countTriples(null, null, null, -1));
        } else {
            assertEquals("Triplestore should have been empty after delete",
                         0,
                         _reader.countTriples(null, null, null, -1));
        }
    }

    private Set<Triple> getSet(TripleIterator iter) throws Exception {
        HashSet<Triple> set = new HashSet<Triple>();
        while (iter.hasNext()) {
            set.add(iter.next());
        }
        iter.close();
        return set;
    }

    private List<Triple> getTriples(int endS, int endP, int endO) throws Exception {
        return getTriples(1, endS, 1, endP, 1, endO);
    }

    private List<Triple> getTriples(int startS, int endS,
                            int startP, int endP,
                            int startO, int endO) throws Exception {
        List<Triple> list = new ArrayList<Triple>();
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
        return _geFactory.createTriple(
                _geFactory.createResource(new URI("urn:test:" + s)),
                _geFactory.createResource(new URI("urn:test:" + p)),
                _geFactory.createResource(new URI("urn:test:" + o)));
    }
    
    private Triple getTriple(String s, String p, String o) throws Exception {
        return getTriple(s, p, o, false);
    }
    
    private Triple getTriple(String s, String p, String o, boolean isLiteral) throws Exception {
        if (isLiteral) {
            return _geFactory.createTriple(
                    _geFactory.createResource(new URI("urn:test:" + s)),
                    _geFactory.createResource(new URI("urn:test:" + p)),
                    _geFactory.createLiteral(o));
        } else {
            return _geFactory.createTriple(
                    _geFactory.createResource(new URI("urn:test:" + s)),
                    _geFactory.createResource(new URI("urn:test:" + p)),
                    _geFactory.createResource(new URI("urn:test:" + o)));
        }
    }

    public class TripleModifier extends Thread {

        private int _id;
        private int _numBatches;
        private int _triplesPerBatch;
        private TriplestoreWriter _writer;
        private boolean _oneAtATime;
        private boolean _adds;

        private Exception _error;

        public TripleModifier(int id, // first is 1, not 0
                           int numBatches,
                           int triplesPerBatch,
                           TriplestoreWriter writer,
                           boolean oneAtATime,
                           boolean adds) {
            _id = id;
            _numBatches = numBatches;
            _triplesPerBatch = triplesPerBatch;
            _writer = writer;
            _oneAtATime = oneAtATime;
            _adds = adds;
        }

        @Override
		public void run() {
            try {
                for (int i = 0; i < _numBatches; i++) {
                    List<Triple> triples = getTriples(_id, _id,
                                              i + 1, i + 1,
                                              1, _triplesPerBatch);
                    if (_oneAtATime) {
                        Iterator<Triple> iter = triples.iterator();
                        while (iter.hasNext()) {
                            if (_adds) {
                                _writer.add(iter.next(), false);
                            } else {
                                _writer.delete(iter.next(), false);
                            }
                        }
                    } else {
                        if (_adds) {
                            _writer.add(triples, false);
                        } else {
                            _writer.delete(triples, false);
                        }
                    }
                    Thread.yield();
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
