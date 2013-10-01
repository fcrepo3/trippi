package org.trippi.io;

import java.net.URI;
import java.util.HashMap;

import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.GraphElementFactoryException;
import org.jrdf.graph.URIReference;


/**
 * A LIFO Cache of UriReferences, on the assumption that local
 * parsing will center on a relatively small number of URIs
 * (ie, a series of triples with the same subject)
 * @author armintor@gmail.com
 *
 */
public class UriReferenceCache extends HashMap<String, URIReference> {
    
    /**
     * 
     */
    private static final long serialVersionUID = -337357880710130885L;
        
    private final GraphElementFactory m_factory;
    
    public UriReferenceCache(GraphElementFactory factory){
        super();
        m_factory =factory;
    }
    
    public UriReferenceCache(int initialCapacity, GraphElementFactory factory){
        super(initialCapacity);
        m_factory = factory;
    }
    
    /**
     * Returns the value mapped to key after replacing to
     * maintain the age-order of entries used.
     * @param key
     * @return
     * @throws GraphElementFactoryException 
     */
    public URIReference get(String key) throws GraphElementFactoryException {
        if (!containsKey(key)){
            put(key, m_factory.createResource(URI.create(key)));
        }
        return super.get(key);
    }
    
}
