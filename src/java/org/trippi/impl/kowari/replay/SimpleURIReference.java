package org.trippi.impl.kowari.replay;

import java.net.*;

import org.jrdf.graph.*;

public class SimpleURIReference extends AbstractURIReference {

    public SimpleURIReference(URI uri) {
        super(uri);
    }

    public SimpleURIReference(URI uri, boolean validate) {
        super(uri, validate);
    }
}
