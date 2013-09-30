package org.trippi.impl;

import java.net.URI;

import org.jrdf.graph.AbstractLiteral;

public class FreeLiteral extends AbstractLiteral {
	private static final long serialVersionUID = 1L;
	public FreeLiteral(String lexicalForm) {
        super(lexicalForm);
    }
    public FreeLiteral(String lexicalForm, String language) {
        super(lexicalForm, language);
    }
    public FreeLiteral(String lexicalForm, URI datatypeURI) {
        super(lexicalForm, datatypeURI);
    }
}