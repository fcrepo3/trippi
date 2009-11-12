package org.trippi.impl.oracle;
import java.sql.Connection;import java.sql.ResultSet;import java.sql.ResultSetMetaData;import java.sql.Statement;import org.jrdf.graph.PredicateNode;import org.jrdf.graph.SubjectNode;import org.jrdf.graph.Triple;import org.trippi.TripleIterator;import org.trippi.TripleMaker;import org.trippi.TrippiException;
/**
 *  Description of the Class
 *
 * @author     liberman@case.edu
 * @created    June 26, 2006
 */
public class OracleTripleIterator extends TripleIterator {
	//private final static Logger logger =
	//		Logger.getLogger(OracleTripleIterator.class.getName());
	private String m_queryText;
	private Connection m_repository;
	private Triple m_next;
	private boolean m_closed = false;	
	/*
	 *  how many triples have been iterated so far
	 */
	//private int m_tripleCount = 0;
	/*
	 *  holds the exception (if any) that was thrown in the background thread
	 */
	//private Exception m_exception;
	private Statement stmt;
	/*
	 *  results of Oracles SQL Qoery
	 *  Format of the Query result:
	 *  SUBJECT | SUBJECT_TYPE | PREDICATE | PREDICATE_TYPE | OBJECT | OBJECT_TYPE
	 */
	private ResultSet m_results;	private boolean firstRead = false;

	/**
	 *Constructor for the OracleTripleIterator object
	 *
	 * @param  queryText            Description of the Parameter
	 * @param  repository           Description of the Parameter
	 * @exception  TrippiException  Description of the Exception
	 */
	public OracleTripleIterator(String queryText, Connection repository) throws TrippiException {
		m_queryText = queryText;
		m_repository = repository;
		try {
			stmt = m_repository.createStatement(); //ResultSet.FETCH_FORWARD,ResultSet.CONCUR_READ_ONLY);
			m_results = stmt.executeQuery(queryText);
			firstRead = true;
		} catch (Exception e) {
			throw new TrippiException("Error with Oracle query.", e);
		}
		try {
			ResultSetMetaData rsmd = m_results.getMetaData();
			if (rsmd.getColumnCount() != 6) {
				throw new TrippiException("Oracle triple itereator got not " +
						"6 columns query was: " + m_queryText);
			}
			if (!(rsmd.getColumnName(1).equalsIgnoreCase("SUBJECT") &&
					rsmd.getColumnName(2).equalsIgnoreCase("SUBJECT_TYPE") &&
					rsmd.getColumnName(3).equalsIgnoreCase("PREDICATE") &&
					rsmd.getColumnName(4).equalsIgnoreCase("PREDICATE_TYPE") &&
					rsmd.getColumnName(5).equalsIgnoreCase("OBJECT") &&
					rsmd.getColumnName(6).equalsIgnoreCase("OBJECT_TYPE"))) {
				throw new TrippiException("Oracle triple itereator table " +
						"order is wrong (order is important " +
						" Proper order is: " +
						"SUBJECT | SUBJECT_TYPE | PREDICATE | PREDICATE_TYPE " +
						"| OBJECT | OBJECT_TYPE" +
						m_queryText);
			}
		} catch (Exception e) {
			e.printStackTrace();
			//TODO REMOVE
			throw new TrippiException("Error with Oracle result set." + e.toString(), e);
		}
		m_next = getNext();
	}
	//////////////////////////////////////////////////////////////////////////
	//////////////// TripleIterator //////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////
	/**
	 * Return true if there are any more triples.
	 *
	 * @return                      Description of the Return Value
	 * @exception  TrippiException  Description of the Exception
	 */
	@Override	public boolean hasNext() throws TrippiException {
		return (m_next != null);
	}
	/**
	 * Return the next triple.
	 *
	 * @return                      Description of the Return Value
	 * @exception  TrippiException  Description of the Exception
	 */
	@Override	public Triple next() throws TrippiException {
		if (m_next == null) {
			return null;
		}
		Triple last = m_next;
		m_next = getNext();
		return last;
	}
	/**
	 * Release resources held by this iterator.
	 *
	 * @exception  TrippiException  Description of the Exception
	 */
	@Override	public void close() throws TrippiException {
		if (!m_closed) {
			try {
				stmt.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			m_closed = true;
			// no resources to release, just set the flag
		}
	}
	/**
	 * Get the next triple out of the bucket and return it.
	 *
	 * If the bucket is empty:
	 *   1) If the iterator is finished,
	 *      throw an exception if m_parseException != null.
	 *      Otherwise, return null.
	 *   2) If the iterator is not finished, wait for
	 *      a) The iterator to finish, or
	 *      b) Another triple to arrive in the bucket.
	 *
	 * @return                      The next value
	 * @exception  TrippiException  Description of the Exception
	 */
	private Triple getNext() throws TrippiException {
		try {
			if (m_results.next()) {
				//more data present
				return (TripleMaker.create((SubjectNode) stringToResource(						m_results.getString("SUBJECT"), m_results								.getString("SUBJECT_TYPE")),						(PredicateNode) stringToResource(m_results								.getString("PREDICATE"), m_results								.getString("PREDICATE_TYPE")),						stringToResource(m_results.getString("OBJECT"),								m_results.getString("OBJECT_TYPE"))));
			} else {
				//no more data
				return (null);
			}
		} catch (Exception e) {
			e.printStackTrace();
			//TODO REMOVE
			throw new TrippiException("Error with Oracle result set.", e);
		}
	}
	/**
	 *  Description of the Method
	 *
	 * @param  val                  Description of the Parameter
	 * @param  type                 Description of the Parameter
	 * @return                      Description of the Return Value
	 * @exception  TrippiException  Description of the Exception
	 */
	private org.jrdf.graph.ObjectNode stringToResource(String val, String type)
			 throws TrippiException {
		if (type.equalsIgnoreCase("UR")) {
			//URI
			try {
				return (TripleMaker.createResource(val));
			} catch (Exception e) {
				return (TripleMaker.createLiteral(val));
			}
		} else if (type.equalsIgnoreCase("BN")) {
			//Blank Node
			return (TripleMaker.createResource());
		} else {
			//Literal
			if (val.trim().compareTo("]") == 0) {
				val = "";
			}
			return (TripleMaker.createLiteral(val));
		}
	}
}