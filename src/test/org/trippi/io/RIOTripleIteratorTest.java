package org.trippi.io;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

import junit.framework.TestCase;

import org.openrdf.rio.RDFParser;
import org.openrdf.rio.rdfxml.RDFXMLParser;
import org.trippi.RDFFormat;
import org.trippi.TripleIterator;

public class RIOTripleIteratorTest extends TestCase {
    private final static String rdf;
    static {
        StringBuilder sb = new StringBuilder();
        sb.append("<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" ");
        sb.append("xmlns:fedora-model=\"info:fedora/fedora-system:def/model#\" ");
        sb.append("xmlns:rel=\"info:fedora/fedora-system:def/relations-external#\">");
        sb.append("<rdf:Description rdf:about=\"info:fedora/test:pid\">");
        sb.append("<rel:isMemberOf rdf:resource=\"info:fedora/demo:SmileyStuff\"/>");
        sb.append("<fedora-model:hasContentModel rdf:resource=\"info:fedora/demo:CmodelForBMech_DualResImageImpl\"/>");
        sb.append("</rdf:Description>");
        sb.append("</rdf:RDF>");
        rdf = sb.toString();
    }
    @Override
	protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
	protected void tearDown() throws Exception {
        super.tearDown();
    }
    
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
        
        RDFParser parser = new RDFXMLParser();
        iter = new RIOTripleIterator(in, parser, "http://localhost/");
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
        
        iter = new RIOTripleIterator(in, parser, "http://localhost/");
        aliasMap = iter.getAliasMap();
        for (String key : aliasMap.keySet()) {
            System.out.println(key + " -> " + aliasMap.get(key));
        }
        iter.toStream(System.out, RDFFormat.RDF_XML);
    }
    
    public void testFromStream() throws Exception {
        InputStream in = new ByteArrayInputStream(rdf.getBytes());
        TripleIterator iter = TripleIterator.fromStream(in, RDFFormat.RDF_XML);
        
        System.out.println("\n\n\n***\n");
        iter.toStream(System.out, RDFFormat.RDF_XML);
    }

}
