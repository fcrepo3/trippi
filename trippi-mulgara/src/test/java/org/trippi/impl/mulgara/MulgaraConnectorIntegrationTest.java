package org.trippi.impl.mulgara;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.Map;

import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.Literal;
import org.jrdf.graph.Node;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mulgara.query.rdf.XSD;
import org.trippi.TripleIterator;
import org.trippi.TriplestoreConnector;
import org.trippi.TriplestoreConnectorIntegrationTest;
import org.trippi.TriplestoreReader;
import org.trippi.TriplestoreWriter;
import org.trippi.TupleIterator;


public class MulgaraConnectorIntegrationTest 
        extends TriplestoreConnectorIntegrationTest {
    private static final String SPARQL = "sparql";
    private static final String ITQL = "itql";
    private final String XSD_DATETIME = "2005-01-19T20:40:17.01Z";
    private TriplestoreConnector _connector;
    private GraphElementFactory _geFactory;
    private SubjectNode _s;
    private PredicateNode _p;
    private ObjectNode _o;
    
    @Before
	public void setUp() throws Exception {
        super.setUp();
        _connector = getConnector();
        _geFactory = _connector.getElementFactory();
        _s = _geFactory.createResource(new URI("urn:test:subject"));
        _p = _geFactory.createResource(new URI("urn:test:predicate"));
        _o = _geFactory.createResource(new URI("urn:test:object"));
    }
    
    private Triple getTestTriple() throws Exception {
        return _geFactory.createTriple(_s, _p, _o);
    }
    
    private String getTupleQuery(URI modelURI, String lang) throws Exception {
    	if (lang.equals(ITQL)) {
            return "select $s $o from <" + modelURI + "> "
            + "where $s <" + _p + "> $o;";
    	}
    	else {
    		return "SELECT * FROM <" + modelURI + "> WHERE { ?s <" + _p + "> ?o }";
    	}
    }
    
    @Test
    public void testXSDdateTime() throws Exception {
        SubjectNode s = _geFactory.createResource(new URI("urn:test:subject"));
        PredicateNode p = _geFactory.createResource(new URI("urn:test:hasXSDdateTime"));
        Literal dateLiteral = _geFactory.createLiteral(XSD_DATETIME, XSD.DATE_TIME_URI);
        Triple triple = _geFactory.createTriple(s, p, dateLiteral);
        TriplestoreWriter writer = _connector.getWriter();
        writer.add(triple, true);
        
        TripleIterator iter = writer.findTriples(s, p, null, -1);
        try {
            assertTrue(iter.hasNext());
            ObjectNode object = iter.next().getObject();
            assertTrue(object instanceof Literal);
            Literal date = (Literal) object;
            assertEquals(XSD.DATE_TIME_URI, date.getDatatypeURI());
            assertEquals(XSD_DATETIME, date.getLexicalForm());
        } finally {
            iter.close();
        }
    }
    
    @Test
    public void testTql() throws Exception {
        if (!(_connector instanceof MulgaraConnector)) {
            fail("expected MulgaraConnector");
        }
        
        TriplestoreReader reader = _connector.getReader();
        TriplestoreWriter writer = _connector.getWriter();
        MulgaraConnector conn = (MulgaraConnector) _connector;
        MulgaraSessionFactory factory = (MulgaraSessionFactory) conn
                .getSessionFactory();
        URI modelURI = factory.getModelURI();

        Triple triple = getTestTriple();
        writer.add(triple, true);

        String query = getTupleQuery(modelURI,ITQL);

        System.out.println("query: " + query);
        
        TupleIterator tuples = reader.findTuples(ITQL, query, 0, false);
        
        assertTrue(tuples.hasNext());
        Map<String, Node> map = tuples.next();
        assertFalse(tuples.hasNext());
        ObjectNode obj = (ObjectNode)map.get("o");
        assertEquals(_o.toString(), obj.toString());
    }
    
    /**
     * This is just a sanity check for SPARQL support.
     * 
     * @throws Exception
     */
    @Test
    public void testSparql() throws Exception {
        if (!(_connector instanceof MulgaraConnector)) {
            fail("expected MulgaraConnector");
        }
        
        TriplestoreReader reader = _connector.getReader();
        TriplestoreWriter writer = _connector.getWriter();
        MulgaraConnector conn = (MulgaraConnector) _connector;
        MulgaraSessionFactory factory = (MulgaraSessionFactory) conn
                .getSessionFactory();
        URI modelURI = factory.getModelURI();

        Triple triple = getTestTriple();
        writer.add(triple, true);

        String query = getTupleQuery(modelURI,SPARQL);

        System.out.println("query: " + query);
        
        TupleIterator tuples = reader.findTuples(SPARQL, query, 0, false);
        
        assertTrue("TupleIterator.hasNext returned false before first row",tuples.hasNext());
        Map<String, Node> map = tuples.next();
        assertFalse("TupleIterator.hasNext returned true past end of result set",tuples.hasNext());
        ObjectNode obj = (ObjectNode)map.get("o");
        assertEquals(_o.toString(), obj.toString());
    }
    
    @Test
    public void testSparqlCount() throws Exception {
        if (!(_connector instanceof MulgaraConnector)) {
            fail("expected MulgaraConnector");
        }
        
        TriplestoreReader reader = _connector.getReader();
        TriplestoreWriter writer = _connector.getWriter();
        MulgaraConnector conn = (MulgaraConnector) _connector;
        MulgaraSessionFactory factory = (MulgaraSessionFactory) conn
                .getSessionFactory();
        URI modelURI = factory.getModelURI();

        Triple triple = getTestTriple();
        writer.add(triple, true);

        String query = getTupleQuery(modelURI,SPARQL);

        System.out.println("query: " + query);
        
        TupleIterator tuples = reader.findTuples(SPARQL, query, 0, false);
        int count = tuples.count();
        assertEquals("Expected 1 row, found " + Integer.toString(count) + " row",1,count);
    }
    
    @Test
    public void testRelativeModelURI() throws Exception {
        if (!(_connector instanceof MulgaraConnector)) {
            fail("expected MulgaraConnector");
        }
        
        TriplestoreReader reader = _connector.getReader();
        TriplestoreWriter writer = _connector.getWriter();
        MulgaraConnector conn = (MulgaraConnector) _connector;
        MulgaraSessionFactory factory = (MulgaraSessionFactory) conn
                .getSessionFactory();
        URI modelURI = factory.getModelURI();

        Triple triple = getTestTriple();
        writer.add(triple, true);
        String relativeURI = modelURI.toString().substring(modelURI.toString().lastIndexOf('#'));
        String query = "SELECT * FROM <" + relativeURI + "> WHERE { ?s <" + _p + "> ?o . ?s ?p ?o }";

        System.out.println("query: " + query);
        
        TupleIterator tuples = reader.findTuples(SPARQL, query, 0, false);
        int count = tuples.count();
        assertEquals("Expected 1 tuple, found " + Integer.toString(count) + " tuple",1,count);
        TripleIterator triples = reader.findTriples(SPARQL, query, 10, false);
        count = triples.count();
        assertEquals("Expected 1 triple, found " + Integer.toString(count) + " triple",1,count);
    }

    /**
     * This is to ensure that Sparql can be used to query triples as well as tuples
     * Test uses a CONSTRUCT query, and depends on a default graph uri
     * @throws Exception
     */
    @Test
    public void testSparqlTriples() throws Exception {
        if (!(_connector instanceof MulgaraConnector)) {
            fail("expected MulgaraConnector");
        }
        
        TriplestoreReader reader = _connector.getReader();
        TriplestoreWriter writer = _connector.getWriter();

        Triple triple = getTestTriple();
        writer.add(triple, true);

        String query = "CONSTRUCT { ?s <dc:title> ?o } WHERE { ?s <" + _p + "> ?o }";

        System.out.println("query: " + query);
        
        TripleIterator triples = reader.findTriples("sparql", query, 0, false);
        assertTrue("TripleIterator.hasNext returned false before first row",triples.hasNext());
        Triple found = triples.next();
        assertFalse("TripleIterator.hasNext returned true past end of result set",triples.hasNext());
        ObjectNode obj = found.getObject();
        assertEquals(_o.toString(), obj.toString());
    }
    
    @Ignore
    public void xtestLuceneModel() throws Exception {
        if (!(_connector instanceof MulgaraConnector)) {
            fail("expected MulgaraConnector");
        }
        
        TriplestoreReader reader = _connector.getReader();
        TriplestoreWriter writer = _connector.getWriter();
        MulgaraConnector conn = (MulgaraConnector) _connector;
        MulgaraSessionFactory factory = (MulgaraSessionFactory) conn
                .getSessionFactory();
        URI modelURI = factory.getModelURI();
        URI textModelURI = factory.getTextModelURI();

        SubjectNode s = _geFactory.createResource(new URI("urn:test:subject"));
        PredicateNode p = _geFactory.createResource(new URI(
                "urn:test:hasFullTextOf"));
        Literal textLiteral = _geFactory.createLiteral("The quick brown fox jumped over the lazy dog.");
        Triple triple = _geFactory.createTriple(s, p, textLiteral);
        writer.add(triple, true);

        String query = "select $s $o from <" + modelURI + "> "
                + "where $s <" + p + "> $o and $s <" + p + "> '+brown' "
                + "in <" + textModelURI + ">;";

        System.out.println("query: " + query);
        
        TupleIterator tuples = reader.findTuples("itql", query, 0, false);
        
        assertTrue(tuples.hasNext());
        Map<String, Node> map = tuples.next();
        assertFalse(tuples.hasNext());
        Literal text = (Literal)map.get("o");
        assertEquals(textLiteral.toString(), text.toString());
    }

}
