package org.trippi.impl;

import org.jrdf.graph.AbstractBlankNode;

public class FreeBlankNode extends AbstractBlankNode {
	private static final long serialVersionUID = 1L;
	private int m_hashCode;
    public FreeBlankNode(int hashCode) { 
        m_hashCode = hashCode;
    }
    public FreeBlankNode(Object object) {
        m_hashCode = object.hashCode();
    }
    @Override
	public int hashCode() { 
        return m_hashCode; 
    }
    public String getID() {
        return "node" + Integer.toString(m_hashCode);
    }
}