package org.trippi;

import java.util.ArrayList;
import java.util.List;

import org.jrdf.graph.Triple;

/**
 * A single <code>Triple</code> and a type, indicating whether it is to be
 * added or deleted from the triplestore.
 *
 * This is the internal structure used for the <code>TriplestoreWriter</code>
 * buffer. Applications registering a <code>FlushErrorHandler</code> may
 * will recieve a list of these when a flush error occurs.
 *
 * @author cwilper@cs.cornell.edu
 */
public class TripleUpdate {

        public final static int NONE = -1;

        /** Type value indicating that the triple is to be deleted. */
        public final static int DELETE = 0;

        /** Type value indicating that the triple is to be added. */
        public final static int ADD = 1;

        /** Which type of update (ADD or DELETE) this is. */
        public int type;

        /** The <code>Triple</code> to be added or deleted. */
        public Triple triple;

        // private constructor, see get(...)
        private TripleUpdate(int type,
                            Triple triple) {
            this.type = type;
            this.triple = triple;
        }

        /** 
         * Get a <code>TripleUpdate</code> of a certain type given a 
         * <code>Triple</code>.
         */
        public static TripleUpdate get(int type, Triple triple) {
            return new TripleUpdate(type, triple);
        }

        /** 
         * Get a list of <code>TripleUpdate</code>s of a certain type given
         * a list of <code>Triple</code>s.
         */
        public static List<TripleUpdate> get(int type, List<Triple> triples) {
            List<TripleUpdate> updates = new ArrayList<TripleUpdate>(triples.size());
            for (int i = 0; i < triples.size(); i++) {
                Triple triple = triples.get(i);
                updates.add(new TripleUpdate(type, triple));
            }
            return updates;
        }
    }
