package org.trippi;

import org.trippi.impl.ImplUnitTestSuite;
import org.trippi.io.RIOTripleIteratorTest;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TrippiUnitTestSuite extends TestCase {

    public static Test suite() throws Exception {

        TestSuite suite = new TestSuite(TrippiUnitTestSuite.class.getName());
   
        // classes in this package
        suite.addTestSuite(RDFUtilUnitTest.class);

        // sub-package suites
        suite.addTestSuite(RIOTripleIteratorTest.class);
        suite.addTest(ImplUnitTestSuite.suite());

        return suite;

    }
}
