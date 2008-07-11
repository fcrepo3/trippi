package org.trippi.impl.mulgara;

import java.net.URI;
import java.util.Map;

import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.Literal;
import org.jrdf.graph.Node;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.mulgara.query.rdf.XSD;
import org.trippi.TripleIterator;
import org.trippi.TriplestoreConnector;
import org.trippi.TriplestoreConnectorIntegrationTest;
import org.trippi.TriplestoreReader;
import org.trippi.TriplestoreWriter;
import org.trippi.TupleIterator;


public class MulgaraConnectorIntegrationTest 
        extends TriplestoreConnectorIntegrationTest {
    
    private final String XSD_DATETIME = "2005-01-19T20:40:17.01Z";
    private TriplestoreConnector _connector;
    private GraphElementFactory _geFactory;
    
    public void setUp() throws Exception {
        super.setUp();
        _connector = getConnector();
        _geFactory = _connector.getElementFactory();
    }
	
    public MulgaraConnectorIntegrationTest(String name) throws Exception { 
        super(name);
    }
    
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
    
    //public void tearDown() throws Exception {}
}
