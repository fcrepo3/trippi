package org.trippi;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jrdf.graph.Node;
import org.jrdf.graph.Triple;
import org.trippi.io.CSVTupleWriter;
import org.trippi.io.FormatCountTupleWriter;
import org.trippi.io.JSONTupleWriter;
import org.trippi.io.SimpleTupleWriter;
import org.trippi.io.SparqlTupleIterator;
import org.trippi.io.SparqlTupleWriter;
import org.trippi.io.SparqlW3CTupleWriter;
import org.trippi.io.TSVTupleWriter;
import org.trippi.io.TupleWriter;
import org.trippi.io.CountTupleWriter;

/**
 * An iterator over a series of tuples.
 *
 * Each tuple is a Map of JRDF Node objects keyed by query binding variable
 * names.
 *
 * @author cwilper@cs.cornell.edu
 */
public abstract class TupleIterator {

    /** 
     * Formats supported for reading.
     *
     * @see #fromStream(InputStream, RDFFormat)
     */
    public static final RDFFormat[] INPUT_FORMATS = 
                                         new RDFFormat[] { RDFFormat.SPARQL };

    /** 
     * Formats supported for writing.
     *
     * @see #toStream(OutputStream, RDFFormat)
     */
    public static final RDFFormat[] OUTPUT_FORMATS = 
                                         new RDFFormat[] { RDFFormat.CSV,
                                                           RDFFormat.SIMPLE,
                                                           RDFFormat.SPARQL,
                                                           RDFFormat.SPARQL_W3C,
                                                           RDFFormat.TSV,
                                                           RDFFormat.JSON,
                                                           RDFFormat.COUNT,
                                                           RDFFormat.COUNT_JSON,
                                                           RDFFormat.COUNT_SPARQL };

    private Map<String, String> m_aliases = new HashMap<String, String>();

    /**
     * Return true if there are more results.
     */
    public abstract boolean hasNext() throws TrippiException;
    
    /**
     * Return the next result.
     */
    public abstract Map<String, Node> next() throws TrippiException;

    /**
     * Get the names of the binding variables.
     *
     * These will be the keys in the map for result.
     */
    public abstract String[] names() throws TrippiException;
    
    /**
     * Release resources held by this TupleIterator.
     */
    public abstract void close() throws TrippiException;

    /**
     * Return the next result as a List of Triple objects.
     */
    public List<Triple> nextTriples(TriplePattern[] patterns) throws TrippiException {
        Map<String, Node> map = next();
        if ( map == null ) return null;
        List<Triple> triples = new ArrayList<Triple>();
        for (int i = 0; i < patterns.length; i++) {
            Triple triple = patterns[i].match(map);
            if (triple != null) {
                triples.add(triple);
            }
        }
        return triples;
    }

    /**
     * Ensure close() gets called at garbage collection time.
     */
    @Override
	public void finalize() throws TrippiException {
        close();
    }

    public void setAliasMap(Map<String, String> aliases) {
        m_aliases = aliases;
    }

    /**
     * Get the number of tuples in the iterator, then close it.
     */
    public int count() throws TrippiException {
        try {
            int n = 0;
            while (hasNext()) {
                next();
                n++;
            }
            return n;
        } finally {
            close();
        }
    }

    /**
     * Serialize to the given stream.
     *
     * After successfully writing, the TupleIterator
     * will be closed, but not the outputstream.
     */
    public int toStream(OutputStream out,
                        RDFFormat format) throws TrippiException {
        TupleWriter writer;
        if (format == RDFFormat.SPARQL) {
            writer = new SparqlTupleWriter(out, m_aliases); 
        }else if(format == RDFFormat.SPARQL_W3C){
        	writer = new SparqlW3CTupleWriter(out,m_aliases);
        } else if (format == RDFFormat.SIMPLE) {
            writer = new SimpleTupleWriter(out, m_aliases); 
        } else if (format == RDFFormat.CSV) {
            writer = new CSVTupleWriter(out, m_aliases); 
        } else if (format == RDFFormat.TSV) {
            writer = new TSVTupleWriter(out, m_aliases); 
        } else if (format == RDFFormat.COUNT) {
            writer = new CountTupleWriter(out); 
        } else if (format == RDFFormat.COUNT_JSON) {
            writer = new FormatCountTupleWriter(new JSONTupleWriter(out, m_aliases)); 
        } else if (format == RDFFormat.COUNT_SPARQL) {
            writer = new FormatCountTupleWriter(new SparqlTupleWriter(out, m_aliases)); 
        } else if (format == RDFFormat.JSON) {
            writer = new JSONTupleWriter(out, m_aliases); 
        } else {
            throw new TrippiException("Unsupported output format: " + format.getName());
        }
        return writer.write(this);
    }

    /**
     * Get an iterator over the tuples in the given stream.
     */
    public static TupleIterator fromStream(InputStream in,
                                           RDFFormat format) throws IOException,
                                                                 TrippiException {
        if (format == RDFFormat.SPARQL) {
            return new SparqlTupleIterator(in);
        } else {
            throw new TrippiException("Unsupported input format: " + format.getName());
        }
    }

    public static void main(String[] args) throws Exception {
        TupleIterator.fromStream(new FileInputStream(new File(args[0])), RDFFormat.SPARQL).toStream(System.out, RDFFormat.SIMPLE);
    }

}
