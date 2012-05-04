package org.trippi.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;

import org.jrdf.graph.BlankNode;
import org.jrdf.graph.Literal;
import org.jrdf.graph.Node;
import org.jrdf.graph.URIReference;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

public class SparqlW3CTupleWriter extends TupleWriter {

	private PrintWriter writer;
	private Map<String, String> aliases;

	public SparqlW3CTupleWriter(OutputStream out, Map<String, String> aliases) throws TrippiException {
		try {
			this.writer = new PrintWriter(new OutputStreamWriter(out, "UTF-8"));
			this.aliases = aliases;
		} catch (IOException e) {
			throw new TrippiException("Error setting up writer", e);
		}
	}

	@Override
	public int write(final TupleIterator iter) throws TrippiException {
		writer.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
		writer.println("<sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">");
		writeHeader(iter, writer);
		writeResults(iter, writer, aliases);
		writer.println("</sparql>");
		writer.flush();
		return iter.names().length;
	}

	private static void writeResults(final TupleIterator iter, final PrintWriter writer,
			final Map<String, String> aliases) throws TrippiException {
		writer.println("\t<results>");
		while (iter.hasNext()) {
			writeNextTuple(iter, writer, aliases);
		}
		writer.println("\t</results>");
	}

	private static void writeNextTuple(final TupleIterator iter, final PrintWriter writer,
			final Map<String, String> aliases) throws TrippiException {
		int bNodeCount = 0;
		writer.println("\t\t<result>");
		Map<String, Node> tuple = iter.next();
		for (final String variableName : iter.names()) {
			writer.print("\t\t\t<binding name=\"" + variableName + "\">");
			final Node node = tuple.get(variableName);
			if (node == null) {
				// TODO: What to do here?! I have no idea, in the original
				// SparqlTupleWriter
				// "bound = false" is used, but i could not find it in the W3C
				// spec...
			} else if (node instanceof URIReference) {
				writer.print("<uri>" + ((URIReference) node).getURI().toASCIIString() + "</uri>");
			} else if (node instanceof BlankNode) {
				writer.print("<bnode>r" + (++bNodeCount) + "</bnode>");
			} else if (node instanceof Literal) {
				final Literal l = (Literal) node;
				writer.print("<literal");
				if (l.getDatatypeURI() != null) {
					writer.print(" datatype=\"" + getURI(l.getDatatypeURI().toString(), aliases) + "\"");
				}
				if (l.getLanguage() != null) {
					writer.print(" xml:lang=\"" + l.getLanguage() + "\"");
				}
				writer.print(">" + enc(l.getLexicalForm()) + "</literal>");
			} else {
				throw new TrippiException("Unrecognized node type: " + node.getClass().getName());
			}
			writer.println("</binding>");
		}
		writer.println("\t\t</result>");
	}

	private static void writeHeader(final TupleIterator iter, final PrintWriter writer) throws TrippiException {
		writer.println("\t<head>");
		for (String variableName : iter.names()) {
			writer.println("\t\t<variable name=\"" + variableName + "\"/>");
		}
		writer.println("\t</head>");
	}

	private static String getURI(String s, Map<String, String> aliases) {
		if (aliases == null || aliases.keySet().size() == 0)
			return enc(s);
		Iterator<String> iter = aliases.keySet().iterator();
		while (iter.hasNext()) {
			String alias = iter.next();
			String prefix = aliases.get(alias);
			if (s.startsWith(prefix)) {
				return "&" + alias + ";" + enc(s.substring(prefix.length()));
			}
		}
		return enc(s);
	}

}
