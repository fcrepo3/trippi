package org.trippi.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilderFactory;

import junit.framework.TestCase;

import org.trippi.RDFFormat;
import org.trippi.TupleIterator;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

public class SparqlW3CTupleWriterTest extends TestCase {

	public void testFromStreamSparqlW3CResult() throws Exception {
		TupleIterator iter = TupleIterator.fromStream(this.getClass().getClassLoader().getResourceAsStream("test-result.sparql"), RDFFormat.SPARQL);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		iter.toStream(out, RDFFormat.SPARQL_W3C);
		String xml = out.toString("UTF-8");
		assertTrue(xml.length() > 0);
		Matcher m = Pattern.compile("<variable name=\".*\"/>").matcher(xml);
		int count = 0;
		while (m.find()) {
			count++;
		}
		assertTrue(count == 6);

		m = Pattern.compile("<results>").matcher(xml);
		count = 0;
		while (m.find()) {
			count++;
		}
		assertTrue(count == 1);

		m = Pattern.compile("<result>").matcher(xml);
		count = 0;
		while (m.find()) {
			count++;
		}
		assertTrue(count == 12);
		
		Document doc=DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new ByteArrayInputStream(xml.getBytes()));
		assertTrue(doc.hasChildNodes());
		
		NodeList elements=doc.getElementsByTagName("result");
		assertTrue(elements.getLength() == 12);
		
		assertTrue(doc.getFirstChild().getAttributes().item(0).getNodeValue().equals("http://www.w3.org/2007/SPARQL/results#"));
	}
}
