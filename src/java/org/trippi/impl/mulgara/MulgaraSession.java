package org.trippi.impl.mulgara;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jrdf.graph.Graph;
import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.GraphException;
import org.jrdf.graph.Literal;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.mulgara.itql.ItqlInterpreter;
import org.mulgara.itql.lexer.LexerException;
import org.mulgara.itql.parser.ParserException;
import org.mulgara.jrdf.JRDFGraph;
import org.mulgara.query.Answer;
import org.mulgara.query.QueryException;
import org.mulgara.resolver.LocalJRDFDatabaseSession;
import org.mulgara.server.JRDFSession;
import org.mulgara.server.driver.JRDFGraphFactory;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;
import org.trippi.impl.base.AliasManager;
import org.trippi.impl.base.TriplestoreSession;

public class MulgaraSession implements TriplestoreSession {
	private static final Logger logger = Logger.getLogger(MulgaraSession.class.getName());

	private JRDFSession m_session;
	private LocalJRDFDatabaseSession m_dbSession;
	private String m_serverURI;
	private URI m_modelURI;
	private URI m_textModelURI;
	private AliasManager m_aliasManager;
	private GraphElementFactory m_elementFactory;
	private boolean m_isClosed;
	
	public MulgaraSession(JRDFSession session, URI modelURI, URI textModelURI,
			AliasManager aliasManager) {
		
		if (session instanceof LocalJRDFDatabaseSession) {
			m_dbSession = (LocalJRDFDatabaseSession) session;
		} else {
			m_dbSession = null;
		}
		m_session = session;
		m_modelURI = modelURI;
		m_serverURI =  modelURI.toString().substring(0, m_modelURI.toString().lastIndexOf("#") + 1);
		m_textModelURI = textModelURI;
		m_aliasManager = aliasManager;

	}

	public void add(Set triples) throws UnsupportedOperationException,
			TrippiException {
		doTriples(triples, true);
	}

	public void close() throws TrippiException {
		if (!m_isClosed) {
			try {
				m_session.close();
			} catch (QueryException e) {
				throw new TrippiException(e.getMessage(), e);
			}
			m_isClosed = true;
		}
	}

	public void delete(Set triples) throws UnsupportedOperationException,
			TrippiException {
		doTriples(triples, false);
	}

	public TripleIterator findTriples(String lang, String queryText)
			throws TrippiException {
		throw new TrippiException("Unsupported triple query language: " + lang);
	}

	public TripleIterator findTriples(SubjectNode subject,
			PredicateNode predicate, ObjectNode object) throws TrippiException {
		Answer answer = null;
		try {
			answer = m_session.find(m_modelURI, subject, predicate, object);
		} catch (GraphException e) {
			throw new TrippiException(e.getMessage(), e);
		}
		
		return new MulgaraTripleIterator(answer);
	}

	public String[] listTripleLanguages() {
		return MulgaraSessionFactory.TRIPLE_LANGUAGES;
	}

	public String[] listTupleLanguages() {
		return MulgaraSessionFactory.TUPLE_LANGUAGES;
	}

	public TupleIterator query(String queryText, String language)
			throws TrippiException {
		if (language.equalsIgnoreCase("itql")) {
			queryText = doAliasReplacements(queryText);
			Answer ans = null;

	        // expand shortcut "from <#" to "from <" + m_serverURI
	        queryText = queryText.replaceAll("\\s+from\\s+<#", " from <" + m_serverURI); 
	        // expand shortcut "in <#" to "in <" + m_serverURI
	        queryText = queryText.replaceAll("\\s+in\\s+<#", " in <" + m_serverURI);
	        ItqlInterpreter interpreter = new ItqlInterpreter(new HashMap());
	        try {
				ans = m_session.query(interpreter.parseQuery(queryText));
			} catch (QueryException e) {
				throw new TrippiException(e.getMessage(), e);
			} catch (IOException e) {
				throw new TrippiException(e.getMessage(), e);
			} catch (LexerException e) {
				throw new TrippiException(e.getMessage(), e);
			} catch (ParserException e) {
				throw new TrippiException(e.getMessage(), e);
			}
	        return new MulgaraTupleIterator(ans);
		} else {
            throw new TrippiException("Unrecognized query language: " 
                    + language);
        }
	}
	
	public GraphElementFactory getElementFactory() throws GraphException {
        if (m_elementFactory == null) {
        	Graph graph;
        	if (m_dbSession != null) {
        		graph = new JRDFGraph(m_dbSession, m_modelURI);
        	} else {
        		graph = JRDFGraphFactory.newClientGraph(m_session, m_modelURI);
        	}
            m_elementFactory = graph.getElementFactory();
        }
        return m_elementFactory;
    }

	private void doTriples(Set<Triple> triples, boolean add) throws TrippiException {
	    try {
			if (add) {
				m_session.insert(m_modelURI, triples);
			} else {
				m_session.delete(m_modelURI, triples);
			}
			if (m_textModelURI != null)
				doPlainLiteralTriples(triples, add);

		} catch (Exception e) {
			String mod = "deleting";
			if (add)
				mod = "adding";
			String msg = "Error " + mod + " triples: " + e.getClass().getName();
			if (e.getMessage() != null)
				msg = msg + ": " + e.getMessage();
			throw new TrippiException(msg, e);
		}
	}

	private void doPlainLiteralTriples(Set<Triple> triples, boolean add)
			throws Exception {
		Set<Triple> plainLiteralTriples = new HashSet<Triple>();
		Iterator<Triple> iter = triples.iterator();
		while (iter.hasNext()) {
			Triple triple = (Triple) iter.next();
			if (triple.getObject() instanceof Literal) {
				Literal literal = (Literal) triple.getObject();
				if (literal.getDatatypeURI() == null
						&& literal.getLexicalForm().length() > 0) {
					plainLiteralTriples.add(triple);
				}
			}
		}
		if (plainLiteralTriples.size() > 0) {
			if (add) {
				m_session.insert(m_textModelURI, plainLiteralTriples);
			} else {
				m_session.delete(m_textModelURI, plainLiteralTriples);
			}
		}
	}

	private String doAliasReplacements(String q) {
		String out = q;
		Map m = m_aliasManager.getAliasMap();
		Iterator iter = m.keySet().iterator();
		while (iter.hasNext()) {
			String alias = (String) iter.next();
			String fullForm = (String) m.get(alias);
			out = out.replaceAll("<" + alias + ":", "<" + fullForm).replaceAll(
					"\\^\\^" + alias + ":(\\S+)", "^^<" + fullForm + "$1>");
		}
		if (!q.equals(out)) {
			logger.info("Substituted aliases, query is now: " + out);
		}
		return out;
	}
	
	/**
     * Ensure close() gets called at garbage collection time.
     */
    public void finalize() throws TrippiException {
        close();
    }

}
