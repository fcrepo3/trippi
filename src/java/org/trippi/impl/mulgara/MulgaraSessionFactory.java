package org.trippi.impl.mulgara;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.log4j.Logger;
import org.mulgara.jrdf.LocalJRDFSession;
import org.mulgara.query.QueryException;
import org.mulgara.resolver.LocalJRDFDatabaseSession;
import org.mulgara.server.JRDFSession;
import org.mulgara.server.NonRemoteSessionException;
import org.mulgara.server.Session;
import org.mulgara.server.SessionFactory;
import org.mulgara.server.driver.SessionFactoryFinder;
import org.mulgara.server.driver.SessionFactoryFinderException;
import org.mulgara.server.local.LocalSessionFactory;
import org.trippi.TrippiException;
import org.trippi.impl.base.AliasManager;
import org.trippi.impl.base.TriplestoreSession;
import org.trippi.impl.base.TriplestoreSessionFactory;

public class MulgaraSessionFactory implements TriplestoreSessionFactory {
	private static final Logger logger = Logger
			.getLogger(MulgaraSessionFactory.class.getName());

	/** iTQL */
	public static final String[] TUPLE_LANGUAGES = new String[] { "itql" };

	/** SPO */
	public static final String[] TRIPLE_LANGUAGES = new String[] { "spo" };

	private static final String LOCALHOST = "localhost";
	private static final String RMI = "rmi";

	private AliasManager m_aliasManager;
	private boolean m_isRemote;
	private URI m_serverURI;
	private URI m_modelURI;
	private URI m_xsdModelURI;
	private URI m_textModelURI = null;
	private SessionFactory m_factory;
	private boolean m_isClosed = false;

	/**
	 * Constructor for a local instance.
	 * 
	 * @param serverPath
	 * @param serverName
	 * @param modelName
	 * @param textModelName
	 * @param autoCreate
	 * @throws TrippiException
	 */
	public MulgaraSessionFactory(String serverName,
			String modelName, String textModelName, AliasManager aliasManager, boolean autoCreate, String serverPath)
			throws TrippiException {
		m_isRemote = false;
		m_aliasManager = aliasManager;
		setModelURIs(serverName, modelName, textModelName, LOCALHOST, 0);
		File serverDir = new File(serverPath);
		if (autoCreate) {
			serverDir.mkdirs();
		} else {
			// ensure serverPath exists before moving on
			if (!serverDir.exists() || !serverDir.isDirectory()) {
				throw new TrippiException("Mulgara server path "
						+ serverDir.toString() + " is not an existing "
						+ "directory (and autoCreate is false)");
			}
		}

		try {
			LocalSessionFactory factory = (LocalSessionFactory) SessionFactoryFinder
					.newSessionFactory(m_serverURI, m_isRemote);
			if (factory.getDirectory() == null) {
				factory.setDirectory(serverDir);
			}
			m_factory = factory;
			if (autoCreate) createModels();
		} catch (SessionFactoryFinderException e) {
			throw new TrippiException(e.getMessage(), e);
		} catch (NonRemoteSessionException e) {
			throw new TrippiException(e.getMessage(), e);
		}
	}

	/**
	 * Constructor for a remote instance
	 * 
	 * @param hostname
	 * @param port
	 * @param serverName
	 * @param modelName
	 * @param textModelName
	 * @param autoCreate
	 * @param aliasManager
	 * @throws TrippiException
	 */
	public MulgaraSessionFactory(String serverName,
			String modelName, String textModelName, AliasManager aliasManager, boolean autoCreate, String hostname, int port) throws TrippiException {
		m_isRemote = true;
		m_aliasManager = aliasManager;
		setModelURIs(serverName, modelName, textModelName, hostname, port);
		try {
			SessionFactory factory = SessionFactoryFinder.newSessionFactory(m_serverURI, m_isRemote);
			m_factory = factory;
			if (autoCreate) createModels();
		} catch (SessionFactoryFinderException e) {
			throw new TrippiException(e.getMessage(), e);
		} catch (NonRemoteSessionException e) {
			throw new TrippiException(e.getMessage(), e);
		}
	}

	/**
	 * 
	 * @param serverName
     * @param modelName
     * @param textModelName
	 * @param hostname
	 *            Hostname the Mulgara server is running on. <code>null</code> value indicates Mulgara is running in-JVM.
	 * @param port
	 *            Port the Mulgara server is running on. Ignored if Mulgara is running in-JVM.
	 * 
	 * @throws TrippiException
	 */
	private void setModelURIs(String serverName, 
								  String modelName, 
								  String textModelName,
								  String hostname, 
								  int port)
			throws TrippiException {
		try {
			if (m_isRemote) {
				m_serverURI = new URI(RMI, null, hostname, port, "/"
						+ serverName, null, null);
			} else {
				m_serverURI = new URI(RMI, hostname, "/" + serverName, null);
			}
			m_modelURI = getModelURI(hostname, serverName, modelName);
			m_xsdModelURI = getModelURI(hostname, serverName, "xsd");
			if (textModelName != null) {
				m_textModelURI = getModelURI(hostname, serverName,
						textModelName);
			}
		} catch (URISyntaxException e) {
			throw new TrippiException(e.getMessage(), e);
		}
	}

	public void close() throws TrippiException {
		if (!m_isClosed) {
			logger.info("Closing underlying SessionFactory...");
			try {
				m_factory.close();
				m_isClosed = true;
			} catch (QueryException e) {
				throw new TrippiException(e.getMessage(), e);
			}
		}
	}

	public String[] listTripleLanguages() {
		return TRIPLE_LANGUAGES;
	}

	public String[] listTupleLanguages() {
		return TUPLE_LANGUAGES;
	}

	public TriplestoreSession newSession() throws TrippiException {
		try {
			if (m_isRemote) {
				// Get a Remote JRDF Session (client/server)
				JRDFSession session = (JRDFSession) m_factory.newJRDFSession();
				return new MulgaraSession(session, m_modelURI, m_textModelURI, m_aliasManager);
			} else {
				// Get a local JRDF Session (local)
				LocalJRDFSession session = (LocalJRDFDatabaseSession) m_factory.newJRDFSession();
				return new MulgaraSession(session, m_modelURI, m_textModelURI, m_aliasManager);
			}
		} catch (QueryException e) {
			throw new TrippiException(e.getMessage(), e);
		}
	}

	/**
	 * Create the Mulgara models.
	 * 
	 * @throws TrippiException
	 */
	private void createModels() throws TrippiException {
		Session session = null;
		
		try {
			session = m_factory.newSession();
			
			if (!session.modelExists(m_modelURI)) {
				session.createModel(m_modelURI, MulgaraModelType.MODEL.uri());
			}
			
			if (!session.modelExists(m_xsdModelURI)) {
				session.createModel(m_xsdModelURI, MulgaraModelType.XSD.uri());
			}
			
			if (m_textModelURI != null && !session.modelExists(m_textModelURI)) {
				session.createModel(m_textModelURI, MulgaraModelType.LUCENE
						.uri());
			}
		} catch (QueryException e) {
			throw new TrippiException(e.getMessage(), e);
		} finally {
			if (session != null) {
				try {
					session.close();
				} catch (QueryException e) {
					logger.warn("Error closing Mulgara session.");
				}
			}
		}
	}

	private URI getModelURI(String hostname, String serverName, String modelName)
			throws URISyntaxException {
		return new URI(RMI, hostname, "/" + serverName, modelName);
	}
	
	protected URI getModelURI() {
	    return m_modelURI;
	}
	
	protected URI getTextModelURI() {
	    return m_textModelURI;
	}
	
	/**
     * Ensure close() gets called at garbage collection time.
     */
    public void finalize() throws TrippiException {
        close();
    }
}
