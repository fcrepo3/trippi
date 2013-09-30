package org.trippi.impl;

import org.jrdf.graph.AbstractTriple;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;

public class FreeTriple extends AbstractTriple {
	private static final long serialVersionUID = 1L;

	public FreeTriple(SubjectNode subjectNode,
                      PredicateNode predicateNode,
                      ObjectNode objectNode) {
        this.subjectNode = subjectNode;
        this.predicateNode = predicateNode;
        this.objectNode = objectNode;
    }
}