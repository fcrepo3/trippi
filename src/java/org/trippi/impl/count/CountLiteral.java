package org.trippi.impl.count;

import org.jrdf.graph.AbstractLiteral;
import org.mulgara.query.rdf.XSD;
import org.openrdf.model.impl.URIImpl;

public class CountLiteral extends AbstractLiteral {

    public static final java.net.URI JAVA_NET_DATATYPE = XSD.NON_NEGATIVE_INTEGER_URI; 
    public static final org.openrdf.model.URI ORG_OPENRDF_DATATYPE = new URIImpl(JAVA_NET_DATATYPE.toString()); 
	public CountLiteral(int value) {
		super(Integer.toString(value),JAVA_NET_DATATYPE);
	}

}
