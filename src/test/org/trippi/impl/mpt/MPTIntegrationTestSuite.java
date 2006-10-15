package org.trippi.impl.mpt;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.swingui.TestRunner;

public class MPTIntegrationTestSuite extends TestCase {

    public static Test suite() throws Exception {

        TestSuite suite = new TestSuite(MPTIntegrationTestSuite.class.getName());
   
        // classes in this package
        suite.addTestSuite(MPTConnectorIntegrationTest.class);

        // sub-package suites
        //    suite.addTest(MyClass.suite());

        return suite;

    }

    public static void main(String[] args) throws Exception {
        if (System.getProperty("text") != null && System.getProperty("text").equals("true")) {
            junit.textui.TestRunner.run(MPTIntegrationTestSuite.suite());
        } else {
            TestRunner.run(MPTIntegrationTestSuite.class);
        }
    }
}
