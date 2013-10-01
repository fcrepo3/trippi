package org.trippi.io;

import java.io.InputStream;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jrdf.graph.Triple;
import org.trippi.RDFFormat;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;
import org.trippi.TrippiIterator;
import org.trippi.io.transform.Transformer;
import org.trippi.io.transform.impl.Identity;


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
     * This method is a convenience to make sure all the thread
     * creation in Trippi runs through the ExecutorService
            * @param command
     */
    public void execute(Runnable command) {
        m_executor.execute(command);
    }
    
    /**
     * Get an iterator over the triples in the given stream.
     * This iterator will not timeout. 
     *
     * The baseURI is used to resolve any relative URI references.
     * If given as null, http://localhost/ will be used.
     */
    public TripleIterator fromStream(InputStream in,
                                            String baseURI,
                                            RDFFormat format) throws TrippiException {
        return fromStream(in, baseURI, format, RIOTripleIterator.NO_TIMEOUT_MS);
    }
    
    /**
     * Get an iterator over the triples in the given stream.
     *
     * The baseURI is used to resolve any relative URI references.
     * If given as null, http://localhost/ will be used.
     * The timeout value adjusts how long to wait for the parsing
     * thread to return the next parsed triple.
     */
    public TripleIterator fromStream(InputStream in,
            String baseURI,
            RDFFormat format,
            long timeoutMs) throws TrippiException {
        if (baseURI == null) baseURI = "http://localhost/";
        org.openrdf.rio.RDFParser parser =
                getParser(format);
        return new RIOTripleIterator(in, parser, baseURI, m_executor, timeoutMs);
    }
    
    private static <T> SimpleParsingContext<T> getSimpleTriples(
            InputStream in,
            String baseURI,
            RDFFormat format,
            Transformer<T> transform) throws TrippiException {
        if (baseURI == null) baseURI = "http://localhost/";
        org.openrdf.rio.RDFParser parser =
                getParser(format);
        try {
            return SimpleParsingContext.parse(in, parser, baseURI, transform);
        } catch (Exception e) {
            throw new TrippiException(e.getMessage(), e);
        }
    }

    private static SimpleTripleParsingContext getSimpleTriples(
            InputStream in,
            String baseURI,
            RDFFormat format) throws TrippiException {
        if (baseURI == null) baseURI = "http://localhost/";
        org.openrdf.rio.RDFParser parser =
                getParser(format);
        try {
            return SimpleTripleParsingContext.parse(in, parser, baseURI);
        } catch (Exception e) {
            throw new TrippiException(e.getMessage(), e);
        }
    }

    /**
     * Return a set of resolved, unprefixed triples
     * @param in
     * @param baseURI
     * @param format
     * @return
     * @throws TrippiException
     */
    public Set<Triple> allAsSet(InputStream in,
            String baseURI,
            RDFFormat format) throws TrippiException {
        SimpleParsingContext<Triple> src =
                    getSimpleTriples(in, baseURI, format, Identity.instance);
            return src.getSet();
    }

    /**
     * Return a set of trnasformation results from the 
     * resolved, unprefixed triples
     * @param in
     * @param baseURI
     * @param format
     * @return
     * @throws TrippiException
     */
    public <T> Set<T> allAsSet(InputStream in,
            String baseURI,
            RDFFormat format,
            Transformer<T> transform) throws TrippiException {
        SimpleParsingContext<T> src =
                    getSimpleTriples(in, baseURI, format, transform);
            return src.getSet();
    }

    /**
     * Return a iterator of the triples in the stream. This iterator
     * is able to be streamed as prefixed triples.
     * @param in
     * @param baseURI
     * @param format
     * @return
     * @throws TrippiException
     */
    public TripleIterator allFromStream(InputStream in,
            String baseURI,
            RDFFormat format) throws TrippiException {
        SimpleTripleParsingContext src =
                getSimpleTriples(in, baseURI, format);
        return src.getIterator();
    }

    /**
     * Return an iterator over transformations of fully-resolved
     * triples parsed from the input stream.
     * @param in
     * @param baseURI
     * @param format
     * @param transform
     * @return
     * @throws TrippiException
     */
    public <T> TrippiIterator<T> allFromStream(InputStream in,
            String baseURI,
            RDFFormat format,
            Transformer<T> transform) throws TrippiException {
        SimpleParsingContext<T> src =
                getSimpleTriples(in, baseURI, format, transform);
        return src.getIterator();
    }
    /**
     * Get an iterator over the triples in the given stream.
     */
    public TripleIterator fromStream(InputStream in,
                                            RDFFormat format) throws TrippiException {
        return fromStream(in, null, format);
    }
    
    public TrippiIterator<Triple> allFromStream(
            InputStream in,
            RDFFormat format) throws TrippiException {
        return allFromStream(in, null, format);
    }
    
    public <T> TrippiIterator<T> allFromStream(
            InputStream in,
            RDFFormat format,
            Transformer<T> transform) throws TrippiException {
        return allFromStream(in, null, format, transform);
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
    
    private static org.openrdf.rio.RDFParser getParser(RDFFormat format) throws TrippiException {
        if (format == RDFFormat.RDF_XML) {
            return new org.openrdf.rio.rdfxml.RDFXMLParser();
        } else if (format == RDFFormat.TURTLE) {
            return new org.openrdf.rio.turtle.TurtleParser();
        } else if (format == RDFFormat.N_TRIPLES) {
            return new org.openrdf.rio.ntriples.NTriplesParser();
        } else {
            throw new TrippiException("Unsupported input format: " + format.getName());
        }
    }
    
}

    