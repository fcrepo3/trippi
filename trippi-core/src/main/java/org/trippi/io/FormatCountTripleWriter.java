package org.trippi.io;

import org.trippi.TripleIterator;
import org.trippi.TrippiException;
import org.trippi.impl.count.CountTripleIterator;

public class FormatCountTripleWriter extends TripleWriter {

	private TripleWriter _out;
	
	public FormatCountTripleWriter(TripleWriter out){
		_out = out;
	}
	@Override
	public int write(TripleIterator iter) throws TrippiException {
        TripleIterator cIter = new CountTripleIterator(iter);
        _out.write( cIter );
            
        /* Documentation states that the TripleWriter should close the 
           Iterator, but not the stream, hence: */
            iter.close();
            return 1; // not sure what the appropriate return value is; using tuples returned	}
	}
}