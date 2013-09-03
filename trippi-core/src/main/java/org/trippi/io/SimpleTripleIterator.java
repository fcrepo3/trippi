package org.trippi.io;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.jrdf.graph.Triple;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;
import org.trippi.impl.base.AliasManager;

/**
 * An iterator over triples parsed by a RIO rdf parser.
 *
 * @author armintor@gmail.com
 */
public class SimpleTripleIterator
    extends TripleIterator {

    protected final int m_tripleCount;

    private final Iterator<Triple> m_iter;
    /**
     * Initialize the iterator by starting the parsing thread.
     * @throws IOException 
     * @throws RDFHandlerException 
     * @throws RDFParseException 
     */
    public SimpleTripleIterator(Set<Triple> triples, Map<String, String> aliases) {
        setAliasMap(aliases);
        m_iter = triples.iterator();
        m_tripleCount = triples.size();
    }
    
    public SimpleTripleIterator(Set<Triple> triples, AliasManager aliases) {
        setAliasManager(aliases);
        m_iter = triples.iterator();
        m_tripleCount = triples.size();
    }

    @Override
    public boolean hasNext() throws TrippiException {
        return m_iter.hasNext();
    }
    
    @Override
    public Triple next() throws TrippiException {
        return m_iter.next();
    }
    
    @Override
    public void close() throws TrippiException {
    }
    
    @Override
    public int count() {
        return m_tripleCount;
    }
    
}
