package org.trippi.impl.mpt;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class MPTIntegrationTestSuite extends TestCase {

    public static Test suite() throws Exception {

        TestSuite suite = new TestSuite(MPTIntegrationTestSuite.class.getName());
   
        // classes in this package
        suite.addTestSuite(MPTConnectorIntegrationTest.class);

        // sub-package suites
        //    suite.addTest(MyClass.suite());

        return suite;

    }
}
