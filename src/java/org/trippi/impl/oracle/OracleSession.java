package org.trippi.impl.oracle;

import java.sql.Connection;import java.sql.SQLException;import java.sql.Statement;import java.util.Iterator;import java.util.Map;import java.util.Set;import org.apache.log4j.Logger;import org.jrdf.graph.BlankNode;import org.jrdf.graph.GraphElementFactory;import org.jrdf.graph.Literal;import org.jrdf.graph.Node;import org.jrdf.graph.ObjectNode;import org.jrdf.graph.PredicateNode;import org.jrdf.graph.SubjectNode;import org.jrdf.graph.Triple;import org.jrdf.graph.URIReference;import org.trippi.RDFUtil;import org.trippi.TripleIterator;import org.trippi.TrippiException;import org.trippi.TupleIterator;import org.trippi.impl.base.AliasManager;import org.trippi.impl.base.TriplestoreSession;

/**
 * A <code>TriplestoreSession</code> that wraps a OracleRepository.
 *
 * @author     liberman@case.edu
 * @created    June 26, 2006
 */
public class OracleSession implements TriplestoreSession, Runnable {

	/**
	 *  Description of the Field
	 */
	public final static String SPO = "spo";

	/**
	 * tuple queries aren't supported yet
	 */
	public final static String[] TUPLE_LANGUAGES = new String[]{"Unsupported"};

	/**
	 * spo
	 */
	public final static String[] TRIPLE_LANGUAGES = new String[]{SPO};

	private final static Logger logger =
			Logger.getLogger(OracleSession.class.getName());

	private Connection m_repository;
	private AliasManager m_aliasManager;

	private GraphElementFactory m_factory;

	private boolean m_closed;
	private String m_oracleSchema;
	private String m_cur_query;


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
	public String[] listTupleLanguages() {
		return TUPLE_LANGUAGES;
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
	public String[] listTripleLanguages() {
		return TRIPLE_LANGUAGES;
	}


	/**
	 * Construct an Oracle session.
	 *
	 * @param  repository     Description of the Parameter
	 * @param  aliasManager   Description of the Parameter
	 * @param  RDFSchemaName  Description of the Parameter
	 */
	public OracleSession(Connection repository,
			AliasManager aliasManager,
			String RDFSchemaName) {
		m_repository = repository;
		m_aliasManager = aliasManager;
		m_closed = false;
		m_oracleSchema = RDFSchemaName;
		logger.info("Created session.");
	}


	/**
	 *  Gets the elementFactory attribute of the OracleSession object
	 *
	 * @return    The elementFactory value
	 */
	public GraphElementFactory getElementFactory() {
		//TODO
		if (m_factory == null) {
			m_factory = new RDFUtil();
		}
		return m_factory;
	}


	/**
	 *  Description of the Method
	 *
	 * @param  triples              Description of the Parameter
	 * @exception  TrippiException  Description of the Exception
	 */
	public void add(Set triples) throws TrippiException {
		doTriples(triples, true);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  triples              Description of the Parameter
	 * @exception  TrippiException  Description of the Exception
	 */
	public void delete(Set triples) throws TrippiException {
		doTriples(triples, false);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  triples              Description of the Parameter
	 * @param  add                  Description of the Parameter
	 * @exception  TrippiException  Description of the Exception
	 */
	private void doTriples(Set triples,
			boolean add) throws TrippiException {
        boolean startedTransaction = false;
        boolean failed = false;
		try {
			if (add) {
				Iterator jrdfTriples = triples.iterator();
				boolean sendTripples = false;
				Statement stmt = m_repository.createStatement();
                m_repository.setAutoCommit(false);
                startedTransaction = true;
				while (jrdfTriples.hasNext()) {
					Triple triple = (Triple) jrdfTriples.next();
					//Statement stmt = m_repository.createStatement();
					/*
					 *  if (sendTripples)
					 *  {
					 *  query = query + ", ";
					 *  }
					 */
					String query = "INSERT INTO " + m_oracleSchema + "_rdf_data VALUES ";
					query = query + " (seq_" + m_oracleSchema + "_rdf_data.nextval," +
							"SDO_RDF_TRIPLE_S('" + m_oracleSchema + "','<" +
							((URIReference) triple.getSubject()).getURI().toString().replaceAll("'", "''") + ">','<" +
					//SUBJECT
							((URIReference) triple.getPredicate()).getURI().toString().replaceAll("'", "''") + ">',";
					//PREDICATE
					if (triple.getObject() instanceof Literal) {
						if (((Literal) triple.getObject()).getLexicalForm().trim().length() == 0) {
							query = query + "']'))";
						} else {
							query = query + "'" + ((Literal) triple.getObject()).getLexicalForm().replaceAll("'", "''") + "'))";
						}
					} else if (triple.getObject() instanceof BlankNode) {
						query = query + "'_:BNSEQN100'))";
					} else {
						query = query + "'<" + ((URIReference) triple.getObject()).getURI().toString().replaceAll("'", "''") + ">'))";
					}
					sendTripples = true;
					////////////////////
					///////////////////////TODO REMOVE
					stmt.addBatch(query);
					//m_cur_query = query;
					//this.run();
				}
				if (sendTripples) {
					stmt.executeBatch();
				}
				stmt.close();
			} else {
				Iterator jrdfTriples = triples.iterator();
				String query = "DELETE FROM " + m_oracleSchema + "_rdf_data WHERE ID IN (SELECT ID FROM " +
						m_oracleSchema + "_rdf_data a " +
						"WHERE (";
				boolean firstTripple = true;
				while (jrdfTriples.hasNext()) {
					Triple triple = (Triple) jrdfTriples.next();
					boolean first = true;
					if (firstTripple) {
						firstTripple = false;
					} else {
						query = query + " OR ( ";
					}
					if ((triple.getSubject()) != null) {
						if (!((triple.getSubject()) instanceof BlankNode)) {
							query = query + " a.triple.get_subject() = '" +
									((URIReference) triple.getSubject()).getURI().toString().replaceAll("'", "''") +
									"' ";
							first = false;
						}
					}
					if ((triple.getPredicate()) != null) {
						if (!((triple.getPredicate()) instanceof BlankNode)) {
							if (!(first)) {
								query = query + " AND ";
							}
							query = query + " a.triple.get_property() = '" +
									((URIReference) triple.getPredicate()).getURI().toString().replaceAll("'", "''") +
									"' ";

							first = false;
						}
					}
					if ((triple.getObject()) != null) {
						if (!((triple.getObject() instanceof BlankNode))) {
							if (!(first)) {
								query = query + " AND ";
							}
							if (triple.getObject() instanceof Literal) {
								if (((Literal) triple.getObject()).getLexicalForm().trim().length() == 0) {
									query = query + " to_char(a.triple.get_object()) = ']'";
								} else {
									query = query + " to_char(a.triple.get_object()) = '" +
											((Literal) triple.getObject()).getLexicalForm().replaceAll("'", "''") +
											"' ";
								}
							} else {
								query = query + " to_char(a.triple.get_object()) = '" +
										((URIReference) triple.getObject()).getURI().toString().replaceAll("'", "''") +
										"' ";
							}
						}
					}
					if (!add) {
						query = query + ")";
					}
					////////////////////
					//m_cur_query = query;
					//run();
				}
				if (firstTripple == false) {
					query = query + " )";
				}
				if (firstTripple == false) {
					Statement stmt = m_repository.createStatement();
					stmt.executeUpdate(query);
					stmt.close();
				}
			}
		} catch (Exception e) {
            failed = true;
			e.printStackTrace();
			String mod = "deleting";
			if (add) {
				mod = "adding";
			}
			String msg = "Error " + mod + " triples: " + e.getClass().getName();
			if (e.getMessage() != null) {
				msg = msg + ": " + e.getMessage();
			}
			throw new TrippiException(msg, e);
		} finally {
            if (startedTransaction) {
                try {
                if (failed) {
                    m_repository.rollback();
                } else {
                    m_repository.commit();
                }
                m_repository.setAutoCommit(true);
                } catch (SQLException e) {
                    throw new TrippiException("Error finishing with transaction", e);
                }
            }
        }
	}


	/**
	 *  Main processing method for the OracleSession object
	 */
	public void run() {
	}


	/**
	 *  Description of the Method
	 *
	 * @param  q           Description of the Parameter
	 * @param  noBrackets  Description of the Parameter
	 * @return             Description of the Return Value
	 */
	private String doAliasReplacements(String q, boolean noBrackets) {
		String out = q;
		Map m = m_aliasManager.getAliasMap();
		Iterator iter = m.keySet().iterator();
		while (iter.hasNext()) {
			String alias = (String) iter.next();
			String fullForm = (String) m.get(alias);
			if (noBrackets) {
				// In serql and rql, aliases are not surrounded by < and >
				// If bob is an alias for http://example.org/robert/,
				// this turns bob:fun into <http://example.org/robert/fun>,
				// {bob:fun} into {<http://example.org/robert/fun>},
				// and "10"^^xsd:int into "10"^^<http://www.w3.org/2001/XMLSchema#int>
				out = out.replaceAll("([\\s{\\^])" + alias + ":([^\\s}]+)",
						"$1<" + fullForm + "$2>");
			} else {
				// In other query languages, aliases are surrounded by < and >
				// If bob is an alias for http://example.org/robert/,
				// this turns <bob:fun> into <http://example.org/robert/fun>
				// and "10"^^xsd:int into "10"^^<http://www.w3.org/2001/XMLSchema#int>
				out = out.replaceAll("<" + alias + ":", "<" + fullForm)
						.replaceAll("\\^\\^" + alias + ":(\\S+)", "^^<" + fullForm + "$1>");
			}
		}
		if (!q.equals(out)) {
			logger.info("Substituted aliases, query is now: " + out);
		}
		return out;
	}


	/**
	 *  Description of the Method
	 *
	 * @param  subject              Description of the Parameter
	 * @param  predicate            Description of the Parameter
	 * @param  object               Description of the Parameter
	 * @return                      Description of the Return Value
	 * @exception  TrippiException  Description of the Exception
	 */
	public TripleIterator findTriples(SubjectNode subject,
			PredicateNode predicate,
			ObjectNode object) throws TrippiException {
		// convert the pattern to a SERQL CONSTRUCT query and run that
		//if (true)
		//	throw new TrippiException("findTriples is not Implementted");

		String query = "SELECT " +
				"    SUBJECT, " +
				"    SUBJECT_TYPE, " +
				"    PREDICATE, " +
				"    PREDICATE_TYPE, " +
				"    OBJECT, " +
				"    OBJECT_TYPE " +
				"FROM " +
				"((SELECT " +
				"    a.triple.get_subject() SUBJECT, " +
				"    vs.value_type AS SUBJECT_TYPE, " +
				"    a.triple.get_property() PREDICATE, " +
				"    vp.value_type AS PREDICATE_TYPE, " +
				"    to_char(a.triple.get_object()) OBJECT, " +
				"    vo.value_type AS OBJECT_TYPE, " +
				"    vs.VALUE_ID, " +
				"    vp.VALUE_ID, " +
				"    vo.VALUE_ID, " +
				"    f.link_id, " +
				"    a.ID " +
				"FROM " +
				"    " + m_oracleSchema + "_rdf_data a, " +
				"    mdsys.rdf_value$ vo, " +
				"    mdsys.rdf_value$ vs, " +
				"    mdsys.rdf_value$ vp, " +
				"    mdsys.RDFM_" + m_oracleSchema + " f " +
				"WHERE " +
				"    (f.link_id IN " +
				"        SDO_RDF.GET_TRIPLE_ID('" + m_oracleSchema + "', a.triple.get_subject(), a.triple.get_property(), '\"'||to_char(a.triple.get_object())||'\"')) " +
				"    AND f.END_NODE_ID = vo.VALUE_ID " +
				"    AND f.START_NODE_ID = vs.VALUE_ID " +
				"    AND f.P_VALUE_ID = vp.VALUE_ID " +
				") UNION ( " +
				"SELECT " +
				"    a.triple.get_subject() SUBJECT, " +
				"    vs.value_type AS SUBJECT_TYPE, " +
				"    a.triple.get_property() PREDICATE, " +
				"    vp.value_type AS PREDICATE_TYPE, " +
				"    to_char(a.triple.get_object()) OBJECT, " +
				"    vo.value_type AS OBJECT_TYPE, " +
				"    vs.VALUE_ID, " +
				"    vp.VALUE_ID, " +
				"    vo.VALUE_ID, " +
				"    f.link_id, " +
				"    a.ID " +
				"FROM " +
				"    " + m_oracleSchema + "_rdf_data a, " +
				"    mdsys.rdf_value$ vo, " +
				"    mdsys.rdf_value$ vs, " +
				"    mdsys.rdf_value$ vp, " +
				"    mdsys.RDFM_" + m_oracleSchema + " f " +
				"WHERE " +
				"    (f.link_id IN " +
				"        SDO_RDF.GET_TRIPLE_ID('" + m_oracleSchema + "', a.triple.get_subject(), a.triple.get_property(), to_char(a.triple.get_object()))) " +
				"    AND f.END_NODE_ID = vo.VALUE_ID " +
				"    AND f.START_NODE_ID = vs.VALUE_ID " +
				"    AND f.P_VALUE_ID = vp.VALUE_ID " +
				")) WHERE 1=1 ";
		//TODO remove 1=1

		if (subject != null) {
			if (!(subject instanceof BlankNode)) {
				query = query + " AND SUBJECT = '" +
						((URIReference) subject).getURI().toString() +
						"' ";
			}
		}
		if (predicate != null) {
			if (!(predicate instanceof BlankNode)) {
				query = query + " AND PREDICATE = '" +
						((URIReference) predicate).getURI().toString() + "' ";
			}
		}
		if (object != null) {
			if (!(object instanceof BlankNode)) {
				if (object instanceof Literal) {
					query = query + " AND OBJECT = '" +
							((Literal) object).getLexicalForm() + "' ";
				} else {
					query = query + " AND OBJECT = '" +
							((URIReference) object).getURI().toString() + "' ";
				}
			}
		}
		return new OracleTripleIterator(query,
				m_repository);
	}


	/**
	 *  Gets the string attribute of the OracleSession object
	 *
	 * @param  ifNull   Description of the Parameter
	 * @param  rdfNode  Description of the Parameter
	 * @return          The string value
	 */
	private String getString(String ifNull, Node rdfNode) {
		if (rdfNode == null) {
			return ifNull;
		}
		return RDFUtil.toString(rdfNode);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  lang                 Description of the Parameter
	 * @param  queryText            Description of the Parameter
	 * @return                      Description of the Return Value
	 * @exception  TrippiException  Description of the Exception
	 */
	public TripleIterator findTriples(String lang,
			String queryText) throws TrippiException {

		return new OracleTripleIterator(queryText,
				m_repository);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  queryText            Description of the Parameter
	 * @param  language             Description of the Parameter
	 * @return                      Description of the Return Value
	 * @exception  TrippiException  Description of the Exception
	 */
	public TupleIterator query(String queryText,
			String language) throws TrippiException {
		return (null);
	}


	/**
	 *  Description of the Method
	 *
	 * @exception  TrippiException  Description of the Exception
	 */
	public void close() throws TrippiException {
		if (!m_closed) {
			try {
				m_repository.close();
			} catch (Exception e) {
				throw new TrippiException("Error shutting down Oracle Connection", e);
			}
			m_closed = true;
			logger.info("Closed Oracle session.");
		}
	}


	/**
	 * Ensure close() gets called at garbage collection time.
	 *
	 * @exception  TrippiException  Description of the Exception
	 */
	public void finalize() throws TrippiException {
		close();
	}
}

