package org.trippi.impl.oracle;

import oracle.jdbc.oci.*;
import oracle.jdbc.pool.*;

import java.io.*;
import java.util.*;

import java.sql.*;

import org.apache.log4j.*;

import org.jrdf.graph.GraphElementFactory;

import org.openrdf.sesame.Sesame;
import org.openrdf.sesame.config.RepositoryConfig;
import org.openrdf.sesame.config.SailConfig;
import org.openrdf.sesame.sailimpl.nativerdf.NativeRdfRepositoryConfig;
import org.openrdf.sesame.sailimpl.rdbms.RdfRepositoryConfig;
import org.openrdf.sesame.repository.SesameRepository;
import org.openrdf.sesame.repository.local.LocalRepository;
import org.openrdf.sesame.repository.local.LocalService;

import org.trippi.*;
import org.trippi.impl.base.*;

/**
 * A <code>TriplestoreConnector</code> for a remote or local, Oracle Spatical
 * triplestore.
 *
 * @author     liberman@case.edu
 * @created    June 26, 2006
 */
public class OracleConnector extends TriplestoreConnector {

	private final static Logger logger =
			Logger.getLogger(OracleConnector.class.getName());

	private ConcurrentTriplestoreWriter m_writer;
	private GraphElementFactory m_elementFactory;


	/**
	 *Constructor for the OracleConnector object
	 */
	public OracleConnector() { }


	/**
	 *  Gets the required attribute of the OracleConnector class
	 *
	 * @param  config               Description of the Parameter
	 * @param  name                 Description of the Parameter
	 * @return                      The required value
	 * @exception  TrippiException  Description of the Exception
	 */
	private static String getRequired(Map config, String name) throws TrippiException {
		String val = (String) config.get(name);
		if (val == null) {
			throw new TrippiException("Must specify " + name);
		}
		return val;
	}


	/**
	 *  Gets the requiredInt attribute of the OracleConnector class
	 *
	 * @param  config               Description of the Parameter
	 * @param  name                 Description of the Parameter
	 * @return                      The requiredInt value
	 * @exception  TrippiException  Description of the Exception
	 */
	private static int getRequiredInt(Map config, String name) throws TrippiException {
		String val = getRequired(config, name);
		try {
			return Integer.parseInt(val);
		} catch (Exception e) {
			throw new TrippiException("Expected an integer for " + name + ", got " + val);
		}
	}


	// set reader, writer, and elementFactory as needed
	/**
	 *  Description of the Method
	 *
	 * @param  config               Description of the Parameter
	 * @exception  TrippiException  Description of the Exception
	 */
	public void init(Map config) throws TrippiException {

		AliasManager aliasManager = new AliasManager(new HashMap());

		// Get and validate configuration
		// Required parameters
		File dir = null;
		String jdbcDriver = null;
		String jdbcUrl = null;
		String user = null;
		String password = null;

		jdbcUrl = getRequired(config, "jdbcUrl");
		user = getRequired(config, "user");
		password = getRequired(config, "password");

		int autoFlushDormantSeconds = getRequiredInt(config, "autoFlushDormantSeconds");
		int autoFlushBufferSize = getRequiredInt(config, "autoFlushBufferSize");
		int bufferSafeCapacity = getRequiredInt(config, "bufferSafeCapacity");
		int bufferFlushBatchSize = getRequiredInt(config, "bufferFlushBatchSize");

		String RDFSchemaName = getRequired(config, "RDFSchemaName");

		Properties poolConfig = new Properties();

		poolConfig.put(OracleOCIConnectionPool.CONNPOOL_MIN_LIMIT,
				getRequired(config, "PoolConnectionMinLimit"));
		poolConfig.put(OracleOCIConnectionPool.CONNPOOL_MAX_LIMIT,
				getRequired(config, "PoolConnectionMaxLimit"));
		poolConfig.put(OracleOCIConnectionPool.CONNPOOL_INCREMENT,
				getRequired(config, "PoolConnectionIncrement"));

		poolConfig.put(OracleOCIConnectionPool.CONNPOOL_NOWAIT, "false");
		//Must wait for next conncetion to free up.
		poolConfig.put(OracleOCIConnectionPool.CONNPOOL_TIMEOUT,
				getRequired(config, "PoolConnectionTimeOut"));

		if (autoFlushDormantSeconds < 0) {
			throw new TrippiException("autoFlushDormantSeconds cannot be negative.");
		}
		if (autoFlushBufferSize < 1) {
			throw new TrippiException("autoFlushBufferSize must be greater than zero.");
		}
		if (bufferSafeCapacity < autoFlushBufferSize + 1) {
			throw new TrippiException("bufferSafeCapacity must be greater than autoFlushBufferSize.");
		}
		if (bufferFlushBatchSize < 1) {
			throw new TrippiException("bufferFlushBatchSize must be greater than zero.");
		}
		if (bufferFlushBatchSize > autoFlushBufferSize) {
			throw new TrippiException("bufferFlushBatchSize must be less than or equal to autoFlushBufferSize.");
		}
		// Initialize appropriate Oracle Repository
		Connection repository;
		try {
			DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());

			repository = DriverManager.getConnection(
					jdbcUrl, user, password);
			//Connect ot oracle
			/*
			 *  repository = new OracleOCIConnectionPool(user, password, jdbcUrl, poolConfig);
			 */
		} catch (Exception e) {
			throw new TrippiException("Failed to initialize Oracle repository", e);
		}
		try {
			OracleSession session = new OracleSession(repository,
					aliasManager,
					RDFSchemaName);

			/*
			 *  **********************************************
			 */
			/*
			 *  *************** HERE *************************
			 */
			/*
			 *  **********************************************
			 */
			m_elementFactory = session.getElementFactory();

			TriplestoreSessionPool sessionPool = new SingleSessionPool(
					session,
					OracleSession.TUPLE_LANGUAGES,
					OracleSession.TRIPLE_LANGUAGES);
			UpdateBuffer updateBuffer = new MemUpdateBuffer(bufferSafeCapacity,
					bufferFlushBatchSize);
			m_writer = new ConcurrentTriplestoreWriter(sessionPool,
					aliasManager,
					session,
					updateBuffer,
					autoFlushBufferSize,
					autoFlushDormantSeconds);
			m_writer.setCacheDeletes(true);
		} catch (Exception e) {
			throw new TrippiException("Failed to initialize Trippi interface to Oracle repository", e);
		}
	}


	/**
	 *  Gets the reader attribute of the OracleConnector object
	 *
	 * @return    The reader value
	 */
	public TriplestoreReader getReader() {
		return m_writer;
	}


	/**
	 *  Gets the writer attribute of the OracleConnector object
	 *
	 * @return    The writer value
	 */
	public TriplestoreWriter getWriter() {
		return m_writer;
	}


	/**
	 *  Gets the elementFactory attribute of the OracleConnector object
	 *
	 * @return    The elementFactory value
	 */
	public GraphElementFactory getElementFactory() {
		return null;
		//m_elementFactory;
	}


	/**
	 *  Description of the Method
	 *
	 * @exception  TrippiException  Description of the Exception
	 */
	public void close() throws TrippiException {
		m_writer.close();
	}

}

