package org.trippi.impl.mulgara;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;

import org.jrdf.graph.Graph;
import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.Triple;
import org.mulgara.jrdf.JRDFGraph;
import org.mulgara.jrdf.LocalJRDFSession;
import org.mulgara.query.QueryException;
import org.mulgara.query.rdf.Mulgara;
import org.mulgara.resolver.LocalJRDFDatabaseSession;
import org.mulgara.server.JRDFSession;
import org.mulgara.server.Session;
import org.mulgara.server.SessionFactory;
import org.mulgara.server.driver.JRDFGraphFactory;
import org.mulgara.server.driver.SessionFactoryFinder;
import org.mulgara.server.local.LocalSessionFactory;

public class LuceneModelTest extends TestCase {
    
    private GraphElementFactory geFactory;
    private URI serverURI;
    private URI textModelURI;
    private final String s = "urn:test/s";
    private final String p = "urn:test/p";
    private final String o = "The quick brown fox jumped over the lazy dog.";

    public void xtestLocalLuceneModel() throws Exception {
        serverURI = new URI("local://localhost/server1");
        textModelURI = new URI("local://localhost/server1#text");
        
        LocalSessionFactory factory = 
            (LocalSessionFactory) SessionFactoryFinder.newSessionFactory(serverURI, false);
        LocalJRDFSession session = (LocalJRDFDatabaseSession) factory.newJRDFSession();
        
        Graph graph = new JRDFGraph(session, textModelURI);
        geFactory = graph.getElementFactory();
        
        Set<Triple> triples = getTriples();
        insert(session, triples);
        delete(session, triples);
    }
    
    public void testRemoteLuceneModel() throws Exception {
        serverURI = new URI("rmi://localhost/server1");
        textModelURI = new URI("rmi://localhost/server1#text");
        
        SessionFactory factory = SessionFactoryFinder.newSessionFactory(serverURI, true);
        JRDFSession session = (JRDFSession) factory.newJRDFSession();
        
        Graph graph = JRDFGraphFactory.newClientGraph(session, textModelURI);
        geFactory = graph.getElementFactory();
        
        Set<Triple> triples = getTriples();
        insert(session, triples);
        delete(session, triples);
    }
    
    private void insert(Session session, Set<Triple> triples) throws Exception {
        if (!session.modelExists(textModelURI)) {
            session.createModel(textModelURI, new URI(Mulgara.NAMESPACE + "LuceneModel"));
        }
        session.insert(textModelURI, triples);
    }
    
    private void delete(Session session, Set<Triple> triples) {
        try {
            session.delete(textModelURI, triples);
        } catch (QueryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
    
    private Set<Triple> getTriples() throws Exception {
        Triple triple = geFactory.createTriple(
                geFactory.createResource(new URI(s)),
                geFactory.createResource(new URI(p)),
                geFactory.createLiteral(o));
        Set<Triple> triples = new HashSet<Triple>();
        triples.add(triple);
        return triples;
    }
}
