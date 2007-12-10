package org.trippi.io;

import java.io.ByteArrayInputStream;
import java.util.Map;

import junit.framework.TestCase;

import org.trippi.TripleIterator;

public class RIOTripleIteratorTest extends TestCase {

    protected void setUp() throws Exception {
        super.setUp();
    }

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
        
        iter = new RIOTripleIterator(in, new org.openrdf.rio.rdfxml.RdfXmlParser(), "http://localhost/");
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
        
        iter = new RIOTripleIterator(in, new org.openrdf.rio.rdfxml.RdfXmlParser(), "http://localhost/");
        aliasMap = iter.getAliasMap();
        for (String key : aliasMap.keySet()) {
            System.out.println(key + " -> " + aliasMap.get(key));
        }
    }

}
