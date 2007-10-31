package org.trippi.impl.mulgara;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.jrdf.graph.Triple;
import org.jrdf.graph.URIReference;
import org.trippi.RDFUtil;
import org.trippi.TriplestoreReader;
import org.trippi.TriplestoreWriter;

public class MulgaraConnectorTest extends TestCase {
	//private MulgaraConnector connector;

	protected void setUp() throws Exception {
		super.setUp();
		//connector = new MulgaraConnector();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public void testInit() throws Exception {
		MulgaraConnector connector = new MulgaraConnector();
		connector.init(getConfig());
		connector.close();
	}

	public void testGetReader() throws Exception {
		MulgaraConnector connector = new MulgaraConnector();
		connector.init(getConfig());
		TriplestoreReader reader = connector.getReader();
		assertEquals(0, reader.countTriples(null, null, null, -1));
		connector.close();
	}

	public void testGetWriter() throws Exception {
		MulgaraConnector connector = new MulgaraConnector();
		connector.init(getConfig());
		TriplestoreWriter writer = connector.getWriter();
		List triples = new ArrayList();
		triples.add(getTriple("foo", "bar", "baz"));
		writer.add(triples, true);
		
		TriplestoreReader reader = connector.getReader();
		assertEquals(1, reader.countTriples(null, null, null, 10));
		connector.close();
	}
	/*
	public void testGetElementFactory() {
		fail("Not yet implemented");
	}

	public void testClose() {
		fail("Not yet implemented");
	}
	*/
	
	private Map<String, String> getConfig() {
		Map<String, String> config = new HashMap<String, String>();
		config.put("serverName",				"fedora");
		config.put("modelName",					"ri");
		config.put("readOnly",					"false");
		config.put("autoCreate",				"true");
		config.put("autoTextIndex",				"true");
		config.put("autoFlushBufferSize",		"1000");
		config.put("autoFlushDormantSeconds",	"5");
		config.put("bufferFlushBatchSize",		"1000");
		config.put("bufferSafeCapacity",		"2000");
		config.put("poolInitialSize",			"2");
		config.put("poolMaxSize",				"5");
		config.put("poolMaxGrowth",				"-1");
		
		boolean isRemote = false;
		if (isRemote) {
			config.put("remote",				"true");
			config.put("host",					"localhost");
			config.put("port",					"1099");
		} else {
			config.put("remote",				"false");
			config.put("path",					"/tmp/riTest");
		}
		return config;
	}
	
	private Triple getTriple(String s, String p, String o) throws Exception {
		RDFUtil util = new RDFUtil();
		return util.createTriple(getResource(s), getResource(p), getResource(o));
	}
	
	private URIReference getResource(String r) throws Exception {
		RDFUtil util = new RDFUtil();
		return util.createResource(new URI("urn:test/" + r));
	}

}
