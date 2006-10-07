package org.trippi.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import org.trippi.RDFFormat;
import org.trippi.TripleIterator;
import org.trippi.TriplestoreConnector;
import org.trippi.TriplestoreReader;
import org.trippi.TriplestoreWriter;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

/**
 * Implements a Trippi Server without regard to how it's exposed.
 */
public class TrippiServer {

    private TriplestoreReader m_reader;
    private TriplestoreWriter m_writer;

    public TrippiServer(TriplestoreReader reader) {
        m_reader = reader;
    }

    public TrippiServer(TriplestoreConnector conn) {
        m_reader = conn.getReader();
        m_writer = conn.getWriter();
    }
    
    public TrippiServer(TriplestoreWriter writer) {
    	this((TriplestoreReader)writer);
        m_writer = writer;
    }

    public TriplestoreReader getReader() {
        return m_reader;
    }
    
    public TriplestoreWriter getWriter() {
        return m_writer;
    }

    /**
     * Get the RDF response format for a given name,
     * or the default format for the given query type
     * if <code>name</code> is <code>null</code>.
     */
    private static RDFFormat getResponseFormat(String name,
                                               boolean tupleQuery) throws TrippiException {
        if (name != null) {
            return RDFFormat.forName(name);
        } else if (tupleQuery) {
            return RDFFormat.SPARQL;
        } else {
            return RDFFormat.TURTLE;
        }
    }

    public static String getResponseMediaType(String formatName,
                                              boolean isTupleQuery,
                                              boolean useDumbType) throws TrippiException {
        RDFFormat format = getResponseFormat(formatName, isTupleQuery);
        String formatMediaType = format.getMediaType();
        return getMediaType(format.getMediaType(), useDumbType);
    }

    /**
     * Do a query against the triplestore, putting results into the
     * OutputStream.
     */
    public String find(String type,       // default = tuples
                     String template, 
                     String lang,       // required
                     String query,      // required
                     String limit,      // default = 0 (no limit)
                     String distinct,   // default = false
                     String format,     // default = sparql
                     String dumbTypes,  // default = false
					 String flush,      // default = false
                     OutputStream out) throws IOException,
                                              TrippiException {
        // set defaults for unspecified
        if (type == null || type.equals("")) type = "tuples";
        if (template != null && template.equals("")) template = null;
        if (template != null && template.startsWith("http://")) {
            template = loadContentAsString(template);
        }
        if (limit == null || limit.equals("")) limit = "0";
        boolean doDistinct = getBoolean(distinct, false);

        boolean useDumbTypes = getBoolean(dumbTypes, false);

        boolean doFlush = getBoolean(flush, false);
        
        if (doFlush) {
        	if (m_writer != null) {
        		m_writer.flushBuffer();
        	}
        }
        
        RDFFormat fmt = getResponseFormat(format, type.equals("tuples"));

        // validate parameters
        if (lang == null || lang.equals(""))
            throw new TrippiException("Parameter 'lang' must be specified (rdql, itql, spo, etc)");
        if (query == null || query.equals(""))
            throw new TrippiException("Parameter 'query' must be specified (the query text)");
        // if query starts with http://, assume it's by-reference and load it
        if (query.startsWith("http://")) {
            query = loadContentAsString(query);
        }
        //
        if (type.equals("tuples")) {
            // tuple query, returning tuples
            TupleIterator iter = m_reader.findTuples(lang,
                                                     query,
                                                     Integer.parseInt(limit),
                                                     doDistinct);
            try {
                iter.toStream(out, fmt);
                return getMediaType(fmt.getMediaType(), useDumbTypes);
            } finally { iter.close(); }
        } else if (type.equals("triples")) {
            TripleIterator iter = null;
            try {
                if (template == null) {
                    // triple query, returning triples
                    iter = m_reader.findTriples(lang,
                                                query,
                                                Integer.parseInt(limit),
                                                doDistinct);
                } else {
                    // tuple query, returning triples
                    iter = m_reader.findTriples(lang, 
                                                query,
                                                template,
                                                Integer.parseInt(limit),
                                            doDistinct);
                }
                iter.toStream(out, fmt);
                return getMediaType(fmt.getMediaType(), useDumbTypes);
            } finally {
                if (iter != null) iter.close();
            }
        } else {
            throw new TrippiException("Unrecognized response type: " + type);
        }
    }

    private String loadContentAsString(String url) throws IOException {
        InputStream in = new URL(url).openStream();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        sendStream(in, out);
        return out.toString("UTF-8");
    }

    private void sendStream(InputStream in, OutputStream out) throws IOException {
        try {
            byte[] buf = new byte[4096];
            int len;
            while ( ( len = in.read( buf ) ) > 0 ) {
                out.write( buf, 0, len );
            }
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static String getMediaType(String in, boolean dumb) {
        if (!dumb) return in;
        if (in.endsWith("xml")) return "text/xml";
        return "text/plain";
    }

    public static boolean getBoolean(String in, boolean defaultValue) {
        if (in != null && in.length() > 0) {
            if (in.toLowerCase().startsWith("f") || in.toLowerCase().equals("off")) {
                return false;
            } else {
                return true;
            }
        } else {
            return defaultValue;
        }
    }
}
