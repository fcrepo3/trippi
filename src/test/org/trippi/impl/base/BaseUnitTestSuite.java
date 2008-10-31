package org.trippi.impl.base;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class BaseUnitTestSuite extends TestCase {

    public static Test suite() throws Exception {

        TestSuite suite = new TestSuite(BaseUnitTestSuite.class.getName());
   
        // classes in this package
        suite.addTestSuite(MemUpdateBufferUnitTest.class);

        return suite;

    }
}
