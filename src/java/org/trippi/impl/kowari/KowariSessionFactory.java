package org.trippi.impl.kowari;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.log4j.Logger;
import org.kowari.query.rdf.Tucana;
import org.kowari.server.Session;
import org.kowari.server.SessionFactory;
import org.kowari.server.driver.SessionFactoryFinder;
import org.kowari.server.driver.SessionFactoryFinderException;
import org.kowari.server.local.LocalSessionFactory;
import org.kowari.store.DatabaseSession;
import org.kowari.store.jena.GraphKowariMaker;
import org.kowari.store.jena.ModelKowariMaker;
import org.trippi.TrippiException;
import org.trippi.impl.base.AliasManager;
import org.trippi.impl.base.TriplestoreSession;
import org.trippi.impl.base.TriplestoreSessionFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.shared.ReificationStyle;

/**
 * A <code>TriplestoreSessionFactory</code> that provides Kowari sessions.
 *
 * This wraps Kowari's built-in <code>SessionFactory</code> object.
 *
 * @author cwilper@cs.cornell.edu
 */
public class KowariSessionFactory implements TriplestoreSessionFactory {

    private static final Logger logger =
        Logger.getLogger(KowariSessionFactory.class.getName());

    /** itql, rdql */
    public static final String[] TUPLE_LANGUAGES = new String[] { "itql", "rdql" };

    /** spo */
    public static final String[] TRIPLE_LANGUAGES = new String[] { "spo" };

    /** http://tucana.org/tucana#XMLSchemaModel */
    public static URI XSD_MODEL_URI; 

    private AliasManager m_aliasManager;
    private String m_modelName;
    private URI m_serverURI;
    private URI m_modelURI;
    private URI m_xsdModelURI;
    private URI m_textModelURI = null;

    private SessionFactory m_factory;
    private boolean m_isLocal;
    
    private boolean m_closed = false;

    /**
     * Constructor for a local Kowari triplestore.
     */
    public KowariSessionFactory(String serverPath,
                                String serverName,
                                String modelName,
                                String textModelName,
                                boolean autoCreate,
                                AliasManager aliasManager)
                                                  throws URISyntaxException,
                                                         TrippiException {
        logger.info("Initializing local interface.");
        XSD_MODEL_URI = new URI("http://tucana.org/tucana#XMLSchemaModel");
        m_aliasManager = aliasManager;
        try {
            m_modelName = modelName;
            m_isLocal = true;
            File serverDir = new File(serverPath);
            if (autoCreate) {
                serverDir.mkdirs();
            } else {
                // ensure it exists before moving on
                if (!serverDir.exists() || !serverDir.isDirectory()) {
                    throw new TrippiException("Kowari server path "
                            + serverDir.toString() + " is not an existing "
                            + "directory (and autoCreate is false)");
                }
            }
            m_serverURI = new URI("rmi", "localhost", "/" + serverName, null);
            // FIXME use scheme, host, path, fragment rather than + "#" + modelName
            m_modelURI = new URI(m_serverURI.toString() + "#" + modelName);
            m_xsdModelURI = new URI(m_serverURI.toString() + "#xsd");
            if (textModelName != null) {
                m_textModelURI = new URI(m_serverURI.toString() + "#" + textModelName);
            }
            LocalSessionFactory factory = 
                                (LocalSessionFactory) 
                                SessionFactoryFinder.newSessionFactory(m_serverURI);
            if (factory.getDirectory() == null) {
                factory.setDirectory(serverDir);
            }       
            m_factory = factory;
            if (autoCreate) autoCreateModels();
        } catch (SessionFactoryFinderException e) {
            String msg = "Error initializing local Kowari session factory: " 
                    + e.getClass().getName();
            if (e.getMessage() != null) msg = msg + ": " + e.getMessage();
            throw new TrippiException(msg, e);
        }
    }

    /**
     * Constructor for a remote Kowari triplestore.
     */
    public KowariSessionFactory(String hostname,
    							int port,
                                String serverName,
                                String modelName,
                                String textModelName,
                                boolean autoCreate,
                                AliasManager aliasManager)
                                                  throws URISyntaxException,
                                                         TrippiException {
        logger.info("Initializing remote interface.");
        XSD_MODEL_URI = new URI("http://tucana.org/tucana#XMLSchemaModel");
        m_aliasManager = aliasManager;
        try {
            m_modelName = modelName;
            m_isLocal = false;

            m_serverURI = new URI("rmi", null, hostname, port,  "/" + serverName, null, null);
            // note modelURIs must not include the port
            m_modelURI = new URI("rmi", hostname, "/" + serverName, modelName);
            m_xsdModelURI = new URI("rmi", hostname, "/" + serverName, "xsd");
            if (textModelName != null) {
                m_textModelURI = new URI("rmi", hostname, "/" + serverName,textModelName);
            }
            SessionFactory factory = 
                                SessionFactoryFinder.newSessionFactory(m_serverURI, true);
            /*
            if (factory.getDirectory() == null) {
                factory.setDirectory(serverDir);
            } */      
            m_factory = factory;
            if (autoCreate) autoCreateModels();
        } catch (SessionFactoryFinderException e) {
            String msg = "Error initializing remote Kowari session factory: " 
                    + e.getClass().getName();
            if (e.getMessage() != null) msg = msg + ": " + e.getMessage();
            throw new TrippiException(msg, e);
        }
    }

    
    
    
    private void autoCreateModels() throws TrippiException {
        Session session = null;
        try {
            session = m_factory.newSession();
            if (!session.modelExists(m_modelURI)) {
                logger.info("Creating " + m_modelURI.toString() + " with type " + Session.KOWARI_MODEL_URI);
                session.createModel(m_modelURI, Session.KOWARI_MODEL_URI);
            }
            if (!session.modelExists(m_xsdModelURI)) {
                logger.info("Creating " + m_xsdModelURI.toString() + " with type " + XSD_MODEL_URI);
                session.createModel(m_xsdModelURI, XSD_MODEL_URI);
            }
            if (m_textModelURI != null && !session.modelExists(m_textModelURI)) {
                URI luceneURI = new URI(Tucana.NAMESPACE + "LuceneModel");
                logger.info("Creating " + m_textModelURI.toString() + " with type " + luceneURI.toString());
                session.createModel(m_textModelURI, luceneURI);
            }
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "Error auto-creating kowari model(s): " 
                    + e.getClass().getName();
            if (e.getMessage() != null) msg = msg + ": " + e.getMessage();
            throw new TrippiException(msg, e);
        } finally {
            if (session != null) {
                try {
                    session.close();
                } catch (Exception e) {
                    logger.warn("Could not close Kowari autoCreate session.");
                }
            }
        }
    }

    public TriplestoreSession newSession() throws TrippiException {
        try {
            if (m_isLocal) {
                DatabaseSession session = (DatabaseSession) m_factory.newSession();
                /**
        			GraphKowariMaker graphMaker = new GraphKowariMaker(session,
					serverURI, ReificationStyle.Minimal);
			ModelKowariMaker modelMaker = new ModelKowariMaker(graphMaker);
			model = modelMaker.openModel(MODEL_NAME);
        */
                GraphKowariMaker gMaker = new GraphKowariMaker(
                                                  session,
                                                  m_serverURI,
                                                  ReificationStyle.Minimal);
                ModelKowariMaker mMaker = new ModelKowariMaker(gMaker); 
                Model jenaModel = mMaker.openModel(m_modelName);                
                return new KowariSession(session, 
                                         m_modelURI, 
                                         m_textModelURI,
                                         jenaModel,
                                         m_aliasManager);
            } else {
            	/* remote session is same as local one... is it really? */
                Session session = (Session) m_factory.newSession();
                /**
        			GraphKowariMaker graphMaker = new GraphKowariMaker(session,
					serverURI, ReificationStyle.Minimal);
			ModelKowariMaker modelMaker = new ModelKowariMaker(graphMaker);
			model = modelMaker.openModel(MODEL_NAME);
        */
                /* FIXME GraphKowariMaker seems only to support DatabaseSession, 
				   so none for remote sessions

                GraphKowariMaker gMaker = new GraphKowariMaker(
                                                  session,
                                                  m_serverURI,
                                                  ReificationStyle.Minimal);
                ModelKowariMaker mMaker = new ModelKowariMaker(gMaker); 
                Model jenaModel = mMaker.openModel(m_modelName);
                */
                return new KowariSession(session, 
                                         m_modelURI, 
                                         m_textModelURI,
                                         null,
                                         m_aliasManager);

            }
        } catch (org.kowari.query.QueryException e) {
            String msg = "Error getting local kowari session: " 
                    + e.getClass().getName();
            if (e.getMessage() != null) msg = msg + ": " + e.getMessage();
            throw new TrippiException(msg, e);
        }
    }

    public String[] listTripleLanguages() {
        return TRIPLE_LANGUAGES;
    }

    public String[] listTupleLanguages() {
        return TUPLE_LANGUAGES;
    }

    public void close() throws TrippiException {
        if (!m_closed) {
            logger.info("Closing underlying SessionFactory...");
            try {
                m_factory.close();
                m_closed = true;
            } catch (org.kowari.query.QueryException e) {
                String msg = "Error closing Kowari session factory: " 
                        + e.getClass().getName();
                if (e.getMessage() != null) msg = msg + ": " + e.getMessage();
                throw new TrippiException(msg, e);
            }
        }
    }

    /**
     * Ensure close() gets called at garbage collection time.
     */
    public void finalize() throws TrippiException {
        close();
    }

}
