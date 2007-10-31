package org.trippi.impl.mulgara;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class MulgaraIntegrationTestSuite extends TestCase {

    public static Test suite() throws Exception {

        TestSuite suite = new TestSuite(MulgaraIntegrationTestSuite.class.getName());
   
        // classes in this package
        suite.addTestSuite(MulgaraConnectorIntegrationTest.class);

        // sub-package suites
        //    suite.addTest(MyClass.suite());

        return suite;

    }

}
