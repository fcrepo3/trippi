package org.trippi.impl.base;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.swingui.TestRunner;

public class BaseUnitTestSuite extends TestCase {

    public static Test suite() throws Exception {

        TestSuite suite = new TestSuite(BaseUnitTestSuite.class.getName());
   
        // classes in this package
        suite.addTestSuite(MemUpdateBufferUnitTest.class);

        return suite;

    }

    public static void main(String[] args) throws Exception {
        if (System.getProperty("text") != null && System.getProperty("text").equals("true")) {
            junit.textui.TestRunner.run(BaseUnitTestSuite.suite());
        } else {
            TestRunner.run(BaseUnitTestSuite.class);
        }
    }
}
