package org.trippi.impl.mulgara;

import static org.junit.Assert.fail;

import java.io.File;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.jrdf.graph.Graph;
import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.Triple;
import org.junit.Test;
import org.mulgara.jrdf.JRDFGraph;
import org.mulgara.query.QueryException;
import org.mulgara.query.rdf.Mulgara;
import org.mulgara.resolver.LocalJRDFDatabaseSession;
import org.mulgara.server.Session;
import org.mulgara.server.SessionFactory;
import org.mulgara.server.driver.SessionFactoryFinder;
import org.mulgara.server.local.LocalSessionFactory;

public class LuceneModelTest {
    
    private GraphElementFactory geFactory;
    private String serverPath = System.getProperty("profile.mulgara.config.path");//System.getProperty("java.io.tmpdir") + "/luceneTest";
    private String server = "rmi://localhost/fedora";
    private URI m_serverURI = URI.create(server);
    private URI m_textModelURI = URI.create(server + "#ri-fullText");
    private SessionFactory m_factory;
    
    @Test
    public void testLocalLuceneModel() throws Exception {
        createFactory();
        createModels();
        
        LocalJRDFDatabaseSession session = 
            (LocalJRDFDatabaseSession) m_factory.newJRDFSession();
        
        Graph graph = new JRDFGraph(session, m_textModelURI);
        geFactory = graph.getElementFactory();
        
        Set<Triple> triples = getTriples();
        insert(session, triples);
        delete(session, triples);
        
        session.close();
        m_factory.close();
    }
    
    private void createFactory() throws Exception {
        File serverDir = new File(serverPath);
        serverDir.mkdirs();
        LocalSessionFactory factory = 
            (LocalSessionFactory) SessionFactoryFinder.newSessionFactory(m_serverURI, false);
        if (factory.getDirectory() == null) {
            factory.setDirectory(serverDir);
        }
        m_factory = factory;
    }
    
    private void createModels() throws Exception {
        Session session = null;
        try {
            session = m_factory.newSession();
            if (m_textModelURI != null && !session.modelExists(m_textModelURI))
                session.createModel(m_textModelURI, 
                                    URI.create(Mulgara.NAMESPACE + "LuceneModel"));
        } finally {
            if (session != null)
                session.close();
        }
    }
    
    private void insert(Session session, Set<Triple> triples) throws Exception {
        //session.insert(m_modelURI, triples);
        session.insert(m_textModelURI, triples);
    }
    
    private void delete(Session session, Set<Triple> triples) {
        try {
            //session.delete(m_modelURI, triples);
            session.delete(m_textModelURI, triples);
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
