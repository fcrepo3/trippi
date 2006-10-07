package org.trippi.impl.mpt;

import org.apache.commons.dbcp.BasicDataSource;

import org.trippi.TrippiException;
import org.trippi.impl.base.TriplestoreSession;
import org.trippi.impl.base.TriplestoreSessionFactory;

import org.nsdl.mptstore.core.DatabaseAdaptor;

public class MPTSessionFactory implements TriplestoreSessionFactory {

    private BasicDataSource _pool;
    private DatabaseAdaptor _adaptor;
    private int _fetchSize;

    public MPTSessionFactory(BasicDataSource pool,
                             DatabaseAdaptor adaptor,
                             int fetchSize) {
        _pool = pool;
        _adaptor = adaptor;
        _fetchSize = fetchSize;
    }

    // Implements TriplestoreSessionFactory.newSession()
    public TriplestoreSession newSession() throws TrippiException {
        return new MPTSession(_pool, _adaptor, _fetchSize);
    }

    // Implements TriplestoreSessionFactory.listTripleLanguages()
    public String[] listTripleLanguages() {
        return MPTSession.TRIPLE_LANGUAGES;
    }

    // Implements TriplestoreSessionFactory.listTupleLanguages()
    public String[] listTupleLanguages() {
        return MPTSession.TUPLE_LANGUAGES;
    }

    // Implements TriplestoreSessionFactory.close()
    public void close() throws TrippiException {
        if (_pool != null) {
            try {
                // the TriplestoreSessionPool will have closed
                // the TriplestoreSession objects by now, but we
                // need to take care of closing the underlying db 
                // connection pool
                _pool.close();
                _pool = null;
            } catch (Exception e) {
                throw new TrippiException("Error closing db connection pool", e);
            }
        }
    }

}
