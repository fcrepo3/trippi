package org.trippi.impl;

import java.net.URI;

import org.jrdf.graph.AbstractURIReference;

public class FreeURIReference extends AbstractURIReference {
	private static final long serialVersionUID = 1L;
	public FreeURIReference(URI uri) {
        super(uri);
    }
    public FreeURIReference(URI uri, boolean validate) {
        super(uri, validate);
    }
}