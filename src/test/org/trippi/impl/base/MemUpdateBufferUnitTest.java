package org.trippi.impl.base;

import junit.framework.TestCase;
import junit.swingui.TestRunner;

public class MemUpdateBufferUnitTest extends UpdateBufferUnitTest {

    public MemUpdateBufferUnitTest(String name) throws Exception { super (name); }

    public UpdateBuffer getBuffer(int safeCapacity,
                                    int flushBatchSize) {
        return new MemUpdateBuffer(safeCapacity, flushBatchSize);
    }

}
