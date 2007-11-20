package org.trippi.impl.sesame;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jrdf.graph.GraphElementFactory;
import org.openrdf.sesame.Sesame;
import org.openrdf.sesame.config.RepositoryConfig;
import org.openrdf.sesame.config.SailConfig;
import org.openrdf.sesame.repository.SesameRepository;
import org.openrdf.sesame.repository.local.LocalService;
import org.openrdf.sesame.sailimpl.nativerdf.NativeRdfRepositoryConfig;
import org.openrdf.sesame.sailimpl.rdbms.RdfRepositoryConfig;
import org.trippi.TriplestoreConnector;
import org.trippi.TriplestoreReader;
import org.trippi.TriplestoreWriter;
import org.trippi.TrippiException;
import org.trippi.config.ConfigUtils;
import org.trippi.impl.base.AliasManager;
import org.trippi.impl.base.ConcurrentTriplestoreWriter;
import org.trippi.impl.base.MemUpdateBuffer;
import org.trippi.impl.base.SingleSessionPool;
import org.trippi.impl.base.TriplestoreSessionPool;
import org.trippi.impl.base.UpdateBuffer;

/**
 * A <code>TriplestoreConnector</code> for a local, native Sesame RDF
 * triplestore.
 *
 * @author cwilper@cs.cornell.edu
 */
public class SesameConnector extends TriplestoreConnector {

    private static final Logger logger =
        Logger.getLogger(SesameConnector.class.getName());

    private ConcurrentTriplestoreWriter m_writer;
    private GraphElementFactory m_elementFactory;

    public SesameConnector() {
    }

    // set reader, writer, and elementFactory as needed
    public void init(Map config) throws TrippiException {

        AliasManager aliasManager = new AliasManager(new HashMap());

        // Get and validate configuration
        String storageType            = ConfigUtils.getRequired(config, "storageType");
        File dir          = null;
        String jdbcDriver = null;
        String jdbcUrl    = null;
        String user       = null;
        String password   = null;
        if (storageType.equals("native")) {
            dir                       = new File(ConfigUtils.getRequired(config, "dir"));
        } else if (storageType.equals("rdbms")) {
            jdbcDriver                = ConfigUtils.getRequired(config, "jdbcDriver");
            jdbcUrl                   = ConfigUtils.getRequired(config, "jdbcUrl");
            user                      = ConfigUtils.getRequired(config, "user");
            password                  = ConfigUtils.getRequired(config, "password");
        } else {
            throw new TrippiException("Invalid storageType: " + storageType 
                    + " (must be native or rdbms)");
        }
        int  autoFlushDormantSeconds  = ConfigUtils.getRequiredInt(config, "autoFlushDormantSeconds");
        int  autoFlushBufferSize      = ConfigUtils.getRequiredInt(config, "autoFlushBufferSize");
        int  bufferSafeCapacity       = ConfigUtils.getRequiredInt(config, "bufferSafeCapacity");
        int  bufferFlushBatchSize     = ConfigUtils.getRequiredInt(config, "bufferFlushBatchSize");

        if (autoFlushDormantSeconds < 0) {
            throw new TrippiException("autoFlushDormantSeconds cannot be negative.");
        }
        if (autoFlushBufferSize < 1) {
            throw new TrippiException("autoFlushBufferSize must be greater than zero.");
        }
        if (bufferSafeCapacity < autoFlushBufferSize + 1) {
            throw new TrippiException("bufferSafeCapacity must be greater than autoFlushBufferSize.");
        }
        if (bufferFlushBatchSize < 1) {
            throw new TrippiException("bufferFlushBatchSize must be greater than zero.");
        }
        if (bufferFlushBatchSize > autoFlushBufferSize) {
            throw new TrippiException("bufferFlushBatchSize must be less than or equal to autoFlushBufferSize.");
        }

        // Initialize appropriate Sesame Repository
        SesameRepository repository = null;
        try {
            RepositoryConfig repConfig = new RepositoryConfig("repository");
            SailConfig syncConfig      = new SailConfig("org.openrdf.sesame.sailimpl.sync.SyncRdfRepository");

            SailConfig baseConfig;
            if (storageType.equals("native")) {
                if (!dir.exists()) dir.mkdirs();
                baseConfig = new NativeRdfRepositoryConfig(dir.getPath());
            } else {
                baseConfig = new RdfRepositoryConfig(jdbcDriver, jdbcUrl, user, password);
            }
            baseConfig.setParameter("commitInterval", "" + bufferFlushBatchSize);

            repConfig.addSail(syncConfig);
            repConfig.addSail(baseConfig);
            repConfig.setWorldReadable(true);
            repConfig.setWorldWriteable(true);

            LocalService service = Sesame.getService();
            repository = service.createRepository(repConfig);
        } catch (Exception e) {
            throw new TrippiException("Failed to initialize Sesame repository", e);
        }

        try {
            SesameSession session = new SesameSession(repository,
                                                           aliasManager);
            m_elementFactory = session.getElementFactory();
            TriplestoreSessionPool sessionPool = new SingleSessionPool(
                                                     session,
                                                     SesameSession.TUPLE_LANGUAGES,
                                                     SesameSession.TRIPLE_LANGUAGES);
            UpdateBuffer updateBuffer = new MemUpdateBuffer(bufferSafeCapacity,
                                                            bufferFlushBatchSize);
            m_writer = new ConcurrentTriplestoreWriter(sessionPool,
                                                       aliasManager,
                                                       session,
                                                       updateBuffer,
                                                       autoFlushBufferSize,
                                                       autoFlushDormantSeconds);
            m_writer.setCacheDeletes(true);
        } catch (Exception e) {
            throw new TrippiException("Failed to initialize Trippi interface to Sesame repository", e);
        }
    }


    public TriplestoreReader getReader() {
        return m_writer;
    }

    public TriplestoreWriter getWriter() {
        return m_writer;
    }

    public GraphElementFactory getElementFactory() {
        return m_elementFactory;
    }

    public void close() throws TrippiException {
        m_writer.close();
    }

}
