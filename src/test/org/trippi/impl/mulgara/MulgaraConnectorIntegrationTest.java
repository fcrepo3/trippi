package org.trippi.impl.mulgara;

import java.net.URI;
import java.util.Map;

import org.jrdf.graph.Literal;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.mulgara.query.rdf.XSD;
import org.trippi.RDFUtil;
import org.trippi.TripleIterator;
import org.trippi.TriplestoreConnectorIntegrationTest;
import org.trippi.TriplestoreReader;
import org.trippi.TriplestoreWriter;
import org.trippi.TupleIterator;


public class MulgaraConnectorIntegrationTest 
        extends TriplestoreConnectorIntegrationTest {
    
    private final String XSD_DATETIME = "2005-01-19T20:40:17.01Z";
	
    public MulgaraConnectorIntegrationTest(String name) throws Exception { 
        super(name);
    }
    
    public void testXSDdateTime() throws Exception {
        RDFUtil util = new RDFUtil();
        SubjectNode s = util.createResource(new URI("urn:test:subject"));
        PredicateNode p = util.createResource(new URI("urn:test:hasXSDdateTime"));
        Literal dateLiteral = util.createLiteral(XSD_DATETIME, XSD.DATE_TIME_URI);
        Triple triple = util.createTriple(s, p, dateLiteral);
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
    
    public void testLuceneModel() throws Exception {
        if (!(_connector instanceof MulgaraConnector)) {
            fail("expected MulgaraConnector");
        }
        
        RDFUtil util = new RDFUtil();
        TriplestoreReader reader = _connector.getReader();
        TriplestoreWriter writer = _connector.getWriter();
        MulgaraConnector conn = (MulgaraConnector) _connector;
        MulgaraSessionFactory factory = (MulgaraSessionFactory) conn
                .getSessionFactory();
        URI modelURI = factory.getModelURI();
        URI textModelURI = factory.getTextModelURI();

        SubjectNode s = util.createResource(new URI("urn:test:subject"));
        PredicateNode p = util.createResource(new URI(
                "urn:test:hasFullTextOf"));
        Literal textLiteral = util.createLiteral("The quick brown fox jumped over the lazy dog.");
        Triple triple = util.createTriple(s, p, textLiteral);
        writer.add(triple, true);

        String query = "select $s $o from <" + modelURI + "> "
                + "where $s <" + p + "> $o and $s <" + p + "> '+brown' "
                + "in <" + textModelURI + ">;";

        System.out.println("query: " + query);
        
        TupleIterator tuples = reader.findTuples("itql", query, 0, false);
        
        assertTrue(tuples.hasNext());
        Map map = tuples.next();
        assertFalse(tuples.hasNext());
        Literal text = (Literal)map.get("o");
        assertEquals(textLiteral.toString(), text.toString());
        
        
        

    }
    
    //public void tearDown() throws Exception {}
}
