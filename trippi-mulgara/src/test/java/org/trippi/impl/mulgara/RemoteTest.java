package org.trippi.impl.mulgara;

import static org.junit.Assert.fail;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.jrdf.graph.Graph;
import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.Triple;
import org.junit.Ignore;
import org.mulgara.query.QueryException;
import org.mulgara.query.rdf.Mulgara;
import org.mulgara.server.JRDFSession;
import org.mulgara.server.Session;
import org.mulgara.server.SessionFactory;
import org.mulgara.server.driver.JRDFGraphFactory;
import org.mulgara.server.driver.SessionFactoryFinder;

/**
 * Test of a Remote Mulgara instance. This test assumes Mulgara
 * is already up and running with serverURI of rmi://localhost/server1.
 *
 * @author Edwin Shin
 * @since 1.3
 * @version $Id$
 */
public class RemoteTest {
    
    private GraphElementFactory geFactory;
    private String server = "rmi://localhost/server1";
    private URI m_serverURI = URI.create(server);
    private URI m_modelURI = URI.create(server + "#ri");
    private SessionFactory m_factory;
    
    @Ignore
    public void testRemoteModel() throws Exception {
        System.out.println("" + System.currentTimeMillis());
        createFactory();
        createModels();
        
        JRDFSession session = (JRDFSession) m_factory.newJRDFSession();
        
        Graph graph = JRDFGraphFactory.newClientGraph(session, m_modelURI);
        geFactory = graph.getElementFactory();
        
        Set<Triple> triples = getTriples();
        insert(session, triples);
        delete(session, triples);
        
        session.close();
        m_factory.close();
    }
    
    private void createFactory() throws Exception {
        SessionFactory factory = SessionFactoryFinder.newSessionFactory(m_serverURI, true);
        m_factory = factory;
    }
    
    private void createModels() throws Exception {
        Session session = null;
        try {
            session = m_factory.newSession();
            if (m_modelURI != null && !session.modelExists(m_modelURI))
                session.createModel(m_modelURI, 
                                    URI.create(Mulgara.NAMESPACE + "Model"));
        } finally {
            if (session != null)
                session.close();
        }
    }
    
    private void insert(Session session, Set<Triple> triples) throws Exception {
        try {
            session.insert(m_modelURI, triples);
        } catch (QueryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
    
    private void delete(Session session, Set<Triple> triples) {
        try {
            session.delete(m_modelURI, triples);
        } catch (QueryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
    
    private Set<Triple> getTriples() throws Exception {
        String s = "urn:test/s";
        String p = "urn:test/p";
        String o = "The quick brown fox jumped over the lazy dog.";
        Triple triple = geFactory.createTriple(
                geFactory.createResource(new URI(s)),
                geFactory.createResource(new URI(p)),
                geFactory.createLiteral(o));
        Set<Triple> triples = new HashSet<Triple>();
        triples.add(triple);
        return triples;
    }
}
