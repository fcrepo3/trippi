package org.trippi.impl.mulgara;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.Triple;
import org.jrdf.graph.URIReference;
import org.trippi.TriplestoreReader;
import org.trippi.TriplestoreWriter;

public class MulgaraConnectorTest extends TestCase {
	private MulgaraConnector _connector;
    private GraphElementFactory _geFactory;

	protected void setUp() throws Exception {
		_connector = new MulgaraConnector();
        _connector.init(getConfig());
        _geFactory = _connector.getElementFactory();
	}

	protected void tearDown() throws Exception {
		_connector.close();
	}
	
	public void testGetReader() throws Exception {
        TriplestoreReader reader = _connector.getReader();
        assertEquals(0, reader.countTriples(null, null, null, -1));
    }

	public void testGetWriter() throws Exception {		
	    List<Triple> triples = new ArrayList<Triple>();
        triples.add(getTriple("foo", "bar", "baz"));
		TriplestoreWriter writer = _connector.getWriter();
		writer.add(triples, true);

		assertEquals(1, writer.countTriples(null, null, null, 10));
		
		writer.delete(triples, true);
		assertEquals(0, writer.countTriples(null, null, null, 10));
		writer.close();
	}
	
	private Map<String, String> getConfig() {
		Map<String, String> config = new HashMap<String, String>();
		config.put("serverName",				"server1");
		config.put("modelName",					"ri");
		config.put("readOnly",					"false");
		config.put("autoCreate",				"true");
		config.put("autoTextIndex",				"true");
		config.put("autoFlushBufferSize",		"1000");
		config.put("autoFlushDormantSeconds",	"5");
		config.put("bufferFlushBatchSize",		"1000");
		config.put("bufferSafeCapacity",		"2000");
		config.put("poolInitialSize",			"2");
		config.put("poolMaxGrowth",				"-1");
		
		boolean isRemote = true;
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
		return _geFactory.createTriple(getResource(s), getResource(p), getResource(o));
	}
	
	private URIReference getResource(String r) throws Exception {
	    return _geFactory.createResource(new URI("urn:test/" + r));
	}

}
