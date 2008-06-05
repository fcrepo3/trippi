package org.trippi.impl.mulgara;

import java.net.URI;

import junit.framework.TestCase;

import org.mulgara.jrdf.LocalJRDFSession;
import org.mulgara.resolver.LocalJRDFDatabaseSession;
import org.mulgara.server.JRDFSession;
import org.mulgara.server.SessionFactory;
import org.mulgara.server.driver.SessionFactoryFinder;
import org.mulgara.server.local.LocalSessionFactory;

public class ModelExistsTest extends TestCase {
    
    public void xtestLocal() throws Exception {
        URI serverURI = new URI("local://localhost/server1");
        URI modelURI = new URI("local://localhost/server1#ri");
        
        LocalSessionFactory factory = (LocalSessionFactory) SessionFactoryFinder.newSessionFactory(serverURI);
        LocalJRDFSession session = (LocalJRDFDatabaseSession) factory.newJRDFSession();
        
        assertFalse(session.modelExists(modelURI));

        factory.close();

        factory = (LocalSessionFactory) SessionFactoryFinder.newSessionFactory(serverURI);
        session = (LocalJRDFDatabaseSession) factory.newJRDFSession();
        assertFalse(session.modelExists(modelURI));
    }
    
    public void testRemote() throws Exception {
        URI serverURI = new URI("rmi://localhost:1099/server1");
        URI modelURI = new URI("rmi://localhost/server1#ri");
        
        SessionFactory factory = SessionFactoryFinder.newSessionFactory(serverURI, true);
        JRDFSession session = (JRDFSession) factory.newJRDFSession();
        
        assertFalse(session.modelExists(modelURI));
        
        factory.close(); // comment this out to pass
        factory = SessionFactoryFinder.newSessionFactory(serverURI, true);
        session = (JRDFSession) factory.newJRDFSession();
        assertFalse(session.modelExists(modelURI));
    }
}