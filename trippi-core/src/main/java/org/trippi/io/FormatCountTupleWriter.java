package org.trippi.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.trippi.TrippiException;
import org.trippi.TupleIterator;
import org.trippi.impl.count.CountTupleIterator;

/**
 * TupleWriter extension to simply write via proxy the number of
 * tuple rows in the given TupleIterator.
 *
 * @author B J O'Steen
 * @author Benjamin Armintor
 */
public class FormatCountTupleWriter extends TupleWriter {

	private TupleWriter _out;

	public FormatCountTupleWriter(TupleWriter out) {
		_out = out;
	}

    public int write(TupleIterator iter) throws TrippiException {
        /* Use the in-built TupleIterator.count() method, wrap result as a tuple */
        TupleIterator cIter = new CountTupleIterator(iter);
        _out.write( cIter );
            
        /* Documentation states that the TripleWriter should close the 
           Iterator, but not the stream, hence: */
            iter.close();
            return 1; // not sure what the appropriate return value is; using tuples returned
    }
}
