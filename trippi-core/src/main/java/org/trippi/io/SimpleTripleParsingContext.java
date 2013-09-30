package org.trippi.io;

import java.io.IOException;
import java.io.InputStream;

import org.jrdf.graph.Triple;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.trippi.AliasManager;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;
import org.trippi.impl.base.DefaultAliasManager;
import org.trippi.io.transform.impl.Identity;

/**
 * An iterator over triples parsed by a RIO rdf parser.
 * Unlike the SimpleParser, this implementation will return
 * an iterator capable of serializing its triples to a
 * stream with source-defined prefixes
 *
 * @author armintor@gmail.com
 */
public class SimpleTripleParsingContext extends SimpleParsingContext<Triple> {

    private AliasManager m_aliases;
    
    /**
     * Initialize the iterator by starting the parsing thread.
     * @throws IOException 
     * @throws RDFHandlerException 
     * @throws RDFParseException 
     */
    private SimpleTripleParsingContext(InputStream in, 
                RDFParser parser, 
                String baseURI)
        throws TrippiException, RDFParseException, RDFHandlerException, IOException {
        // though we're shortcutting the Triple creation, this coaches the compiler
        // on the generic types
        super(in, parser, baseURI, Identity.instance);
    }
        
    @Override
    public TripleIterator getIterator() {
        return new SimpleTripleIterator(m_triples, m_aliases);
    }

    @Override
    public void handleNamespace(String prefix, String uri) {
        if (prefix != null && !prefix.isEmpty()) {
            if (m_aliases == null) {
                m_aliases = new DefaultAliasManager();
            }
            m_aliases.addAlias(prefix, uri);
        }
    }

    public static SimpleTripleParsingContext
        parse(InputStream in, 
            RDFParser parser, 
            String baseURI) throws TrippiException, RDFParseException,
            RDFHandlerException, IOException {
        return new SimpleTripleParsingContext(in, parser, baseURI);
    }
}
