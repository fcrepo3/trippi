package org.trippi.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jrdf.graph.Triple;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.rdfxml.RDFXMLParser;
import org.trippi.RDFFormat;
import org.trippi.TripleIterator;

public class SimpleTripleParserTest {
    private static String rdf;
    private TripleIteratorFactory m_factory;
    
    @BeforeClass
    public static void bootstrap() {
        StringBuilder sb = new StringBuilder();
        sb.append("<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" ");
        sb.append("xmlns:fedora-model=\"info:fedora/fedora-system:def/model#\" ");
        sb.append("xmlns:rel=\"info:fedora/fedora-system:def/relations-external#\">");
        sb.append("<rdf:Description rdf:about=\"info:fedora/test:pid\">");
        sb.append("<rel:isMemberOf rdf:resource=\"info:fedora/demo:SmileyStuff\"/>");
        sb.append("<fedora-model:hasContentModel rdf:resource=\"info:fedora/demo:CmodelForBMech_DualResImageImpl\"/>");
        sb.append("<fedora-model:testProperty>test</fedora-model:testProperty>");
        sb.append("<fedora-model:testReference rdf:resource=\"info:/fedora/test:otherPid\" />");
        sb.append("</rdf:Description>");
        sb.append("</rdf:RDF>");
        rdf = sb.toString();
    }
    
    @Before
	public void setUp() throws Exception {
        m_factory = new TripleIteratorFactory();
    }

    @After
	public void tearDown() throws Exception {
        m_factory.shutdown();
    }
    
    @Test
    public void testNamespaceMapping() throws Exception {
        StringBuilder sb;
        byte[] rdfxml;
        ByteArrayInputStream in;
        TripleIterator iter;
        Map<String, String> aliasMap;
        
        sb = new StringBuilder();
        sb.append("<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">");
        sb.append("  <rdf:Description rdf:about=\"info:fedora/demo:888\">");
        sb.append("    <foo:p xmlns:foo=\"urn:\" rdf:resource=\"urn:o\"/>");
        sb.append("  </rdf:Description>");
        sb.append("</rdf:RDF>");
        rdfxml = sb.toString().getBytes("UTF-8");
        in = new ByteArrayInputStream(rdfxml);
        
        iter = m_factory.allFromStream(in, "http://localhost/", RDFFormat.RDF_XML);
        aliasMap = iter.getAliasMap();
        for (String key : aliasMap.keySet()) {
            System.out.println(key + " -> " + aliasMap.get(key));
        }
        
        sb = new StringBuilder();
        sb.append("<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">");
        sb.append("  <rdf:Description rdf:about=\"info:fedora/demo:888\">");
        sb.append("    <p xmlns=\"urn:\" rdf:resource=\"urn:o\"/>");
        sb.append("  </rdf:Description>");
        sb.append("</rdf:RDF>");
        rdfxml = sb.toString().getBytes("UTF-8");
        in = new ByteArrayInputStream(rdfxml);
        
        iter = m_factory.allFromStream(in, "http://localhost/", RDFFormat.RDF_XML);
        aliasMap = iter.getAliasMap();
        for (String key : aliasMap.keySet()) {
            System.out.println(key + " -> " + aliasMap.get(key));
        }
        iter.toStream(System.out, RDFFormat.RDF_XML);
    }
    
    @Test
    public void testFromStream() throws Exception {
        InputStream in = new ByteArrayInputStream(rdf.getBytes());
        Set<Triple> parsed = m_factory.allAsSet(in, null, RDFFormat.RDF_XML);
        HashMap<String, Triple> triples = new HashMap<String, Triple>();
        for (Triple next: parsed) {
            triples.put(next.getPredicate().toString(), next);
        }
        String subject =
                "info:fedora/test:pid";
        String contentModel =
                "info:fedora/fedora-system:def/model#hasContentModel";
        String memberOf =
                "info:fedora/fedora-system:def/relations-external#isMemberOf";
        String testProp = "info:fedora/fedora-system:def/model#testProperty";
        Triple actual = triples.remove(contentModel);
        assertTriple(actual, subject,"info:fedora/demo:CmodelForBMech_DualResImageImpl");
        actual = triples.remove(memberOf);
        assertTriple(actual, subject,"info:fedora/demo:SmileyStuff");
        actual = triples.remove(testProp);
        // this property is a literal
        assertTriple(actual, subject,"\"test\"");
        assertEquals("Unexpected remainder in test triples", 1, triples.size());
    }

    private void assertTriple(Triple actual, String subject, String object) {
        assertNotNull("triple was null", actual);
        assertEquals(subject,
                actual.getSubject().toString());
        assertEquals(object,
                actual.getObject().toString());
    }
    
    @Test
    public void testFromStreamToStream() throws Exception {
        InputStream in = new ByteArrayInputStream(rdf.getBytes());
        TripleIterator iter = m_factory.allFromStream(in, RDFFormat.RDF_XML);
        System.out.println("\n\n\n***\n");
        iter.toStream(System.out, RDFFormat.RDF_XML);
    }

    @Test
    public void testFromStreamToJson() throws Exception {
        InputStream in = new ByteArrayInputStream(rdf.getBytes());
        TripleIterator iter = m_factory.allFromStream(in, RDFFormat.RDF_XML);
        
        System.out.println("\n\n\n***\n");
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        iter.toStream(bos, RDFFormat.JSON);
        String jsonString = bos.toString("UTF-8");
        System.out.println("parsing json");
        System.out.println(jsonString);
        JSONObject json = new JSONObject(jsonString);
        JSONArray tripleMaps = json.getJSONArray("results");
        assertEquals(4, tripleMaps.length());
    }
}
