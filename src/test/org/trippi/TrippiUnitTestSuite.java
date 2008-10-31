package org.trippi;

import org.trippi.impl.ImplUnitTestSuite;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TrippiUnitTestSuite extends TestCase {

    public static Test suite() throws Exception {

        TestSuite suite = new TestSuite(TrippiUnitTestSuite.class.getName());
   
        // classes in this package
        suite.addTestSuite(RDFUtilUnitTest.class);

        // sub-package suites
        suite.addTest(ImplUnitTestSuite.suite());

        return suite;

    }
}
