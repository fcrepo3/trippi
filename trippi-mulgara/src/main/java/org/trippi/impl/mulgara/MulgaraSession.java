package org.trippi.impl.mulgara;

import java.io.IOException;

import java.net.URI;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jrdf.graph.Graph;
import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.GraphException;
import org.jrdf.graph.Literal;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;

import org.mulgara.itql.TqlInterpreter;
import org.mulgara.jrdf.JRDFGraph;
import org.mulgara.parser.MulgaraLexerException;
import org.mulgara.parser.MulgaraParserException;
import org.mulgara.query.Answer;
import org.mulgara.query.QueryException;
import org.mulgara.resolver.LocalJRDFDatabaseSession;
import org.mulgara.server.JRDFSession;
import org.mulgara.server.driver.JRDFGraphFactory;
import org.mulgara.sparql.SparqlInterpreter;

import org.trippi.Alias;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;
import org.trippi.impl.base.AliasManager;
import org.trippi.impl.base.TriplestoreSession;

public class MulgaraSession implements TriplestoreSession {
	private static final Logger logger = LoggerFactory.getLogger(MulgaraSession.class.getName());

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

	public void add(Set<Triple> triples) throws UnsupportedOperationException,
			TrippiException {
		doTriples(triples, true);
	}

	public void close() throws TrippiException {
		if (!m_isClosed) {
			try {
			    synchronized(m_session) {
			        m_session.close();
			    }
			} catch (QueryException e) {
				throw new TrippiException(e.getMessage(), e);
			}
			m_isClosed = true;
		}
	}

	public void delete(Set<Triple> triples) throws UnsupportedOperationException,
			TrippiException {
		doTriples(triples, false);
	}

	public TripleIterator findTriples(String lang, String queryText)
			throws TrippiException {
		if (lang.equalsIgnoreCase("sparql")) {
		    queryText = doAliasReplacements(queryText);
            Answer ans = null;

            SparqlInterpreter interpreter = new SparqlInterpreter();
            interpreter.setDefaultGraphUri(m_modelURI);
            try {
                ans = m_session.query(interpreter.parseQuery(queryText));
            } catch (QueryException e) {
                throw new TrippiException(e.getMessage(), e);
            } catch (IOException e) {
                throw new TrippiException(e.getMessage(), e);
            } catch (MulgaraLexerException e) {
                throw new TrippiException(e.getMessage(), e);
            } catch (MulgaraParserException e) {
                throw new TrippiException(e.getMessage(), e);
            }
            return new MulgaraTripleIterator(ans,getElementFactory());
		} else {
            throw new TrippiException("Unrecognized query language: " 
                    + lang);
        }
	}

	public TripleIterator findTriples(SubjectNode subject,
			PredicateNode predicate, ObjectNode object) throws TrippiException {
		Answer answer = null;
		try {
			answer = m_session.find(m_modelURI, subject, predicate, object);
		} catch (GraphException e) {
			throw new TrippiException(e.getMessage(), e);
		}
		
        return new MulgaraTripleIterator(answer, getElementFactory());
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

	        TqlInterpreter interpreter = new TqlInterpreter(new HashMap<String, URI>());
	        try {
				ans = m_session.query(interpreter.parseQuery(queryText));
			} catch (QueryException e) {
				throw new TrippiException(e.getMessage(), e);
			} catch (IOException e) {
				throw new TrippiException(e.getMessage(), e);
			} catch (MulgaraLexerException e) {
				throw new TrippiException(e.getMessage(), e);
			} catch (MulgaraParserException e) {
				throw new TrippiException(e.getMessage(), e);
			}
	        return new MulgaraTupleIterator(ans);
		} else if (language.equalsIgnoreCase("sparql")) {
		    queryText = doAliasReplacements(queryText);
            Answer ans = null;

            SparqlInterpreter interpreter = new SparqlInterpreter();
            interpreter.setDefaultGraphUri(m_modelURI);
            try {
                ans = m_session.query(interpreter.parseQuery(queryText));
            } catch (QueryException e) {
                throw new TrippiException(e.getMessage(), e);
            } catch (IOException e) {
                throw new TrippiException(e.getMessage(), e);
            } catch (MulgaraLexerException e) {
                throw new TrippiException(e.getMessage(), e);
            } catch (MulgaraParserException e) {
                throw new TrippiException(e.getMessage(), e);
            }
            return new MulgaraTupleIterator(ans);
		} else {
            throw new TrippiException("Unrecognized query language: " 
                    + language);
        }
	}
	
	public GraphElementFactory getElementFactory() throws TrippiException {
        if (m_elementFactory == null) {
        	Graph graph;
        	if (m_dbSession != null) {
        		try {
                    graph = new JRDFGraph(m_dbSession, m_modelURI);
                } catch (GraphException e) {
                    throw new TrippiException(e.getMessage(), e);
                }
        	} else {
        		try {
                    graph = JRDFGraphFactory.newClientGraph(m_session, m_modelURI);
                } catch (GraphException e) {
                    throw new TrippiException(e.getMessage(), e);
                }
        	}
            m_elementFactory = graph.getElementFactory();
        }
        return m_elementFactory;
    }

	private void doTriples(Set<Triple> triples, boolean add) throws TrippiException {
	    if (triples == null || triples.size() == 0) {
	        return;
	    }
	    try {
			if (add) {
			    synchronized(m_session) {
			        m_session.insert(m_modelURI, triples);
			    }
			} else {
			    synchronized(m_session) {
			        m_session.delete(m_modelURI, triples);
			    }
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
			Triple triple = iter.next();
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
			    synchronized(m_session) {
			        m_session.insert(m_textModelURI, plainLiteralTriples);
			    }
			} else {
			    synchronized(m_session) {
			        m_session.delete(m_textModelURI, plainLiteralTriples);
			    }
			}
		}
	}
	
	private String doAliasReplacements(String q) {
		String out = q;
		Map<String, Alias> m = m_aliasManager.getAliases();
		Iterator<String> iter = m.keySet().iterator();
		while (iter.hasNext()) {
			String prefix = iter.next();
			Alias alias = m.get(prefix);
			out = alias.replaceSparqlType(alias.replaceSparqlUri(out));
		}
		// base model URI includes separator
		// relative URIs introduce a library dependency on Jena, so keeping m_serverURI for now 
		out = Alias.replaceRelativeUris(out, m_serverURI); 
		if (!q.equals(out)) {
			logger.info("Substituted aliases, query is now: " + out);
		}
		return out;
	}
	
	/**
     * Ensure close() gets called at garbage collection time.
     */
    @Override
	public void finalize() throws TrippiException {
        close();
    }

}
