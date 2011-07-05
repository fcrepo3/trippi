package org.trippi.io;

import java.io.InputStream;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.trippi.RDFFormat;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;

        
public class TripleIteratorFactory {
    private final ExecutorService m_executor;
    
    public TripleIteratorFactory(){
        this(Executors.newCachedThreadPool());
    }
    public TripleIteratorFactory(ExecutorService executor){
        m_executor = executor;
    }
    public void shutdown(){
        m_executor.shutdown();
    }
    /**
     * Get an iterator over the triples in the given stream.
     *
     * The baseURI is used to resolve any relative URI references.
     * If given as null, http://localhost/ will be used.
     */
    public TripleIterator fromStream(InputStream in,
                                            String baseURI,
                                            RDFFormat format) throws TrippiException {
        if (baseURI == null) baseURI = "http://localhost/";
        org.openrdf.rio.RDFParser parser;
        if (format == RDFFormat.RDF_XML) {
            parser = new org.openrdf.rio.rdfxml.RDFXMLParser();
        } else if (format == RDFFormat.TURTLE) {
            parser = new org.openrdf.rio.turtle.TurtleParser();
        } else if (format == RDFFormat.N_TRIPLES) {
            parser = new org.openrdf.rio.ntriples.NTriplesParser();
        } else {
            throw new TrippiException("Unsupported input format: " + format.getName());
        }
        return new RIOTripleIterator(in, parser, baseURI);
    }

    /**
     * Get an iterator over the triples in the given stream.
     */
    public TripleIterator fromStream(InputStream in,
                                            RDFFormat format) throws TrippiException {
        return fromStream(in, null, format);
    }
    
    private static volatile TripleIteratorFactory DEFAULT = null;
    
    /**
     * For use when static methods are the only management context available.
     * The instance returned must be shutdown, or there will be a timeout as
     * the ExecutorService's threadpool expires.
     * @return TripleIteratorFactory
     */
    public static TripleIteratorFactory defaultInstance() {
        if (DEFAULT == null) {
            synchronized(TripleIteratorFactory.class){
                if (DEFAULT == null){
                    DEFAULT = new TripleIteratorFactory();
                }
            }
        }
        return DEFAULT;
    }
    
}

    