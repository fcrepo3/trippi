package org.trippi;

import java.net.URI;

import junit.framework.TestCase;

import org.jrdf.graph.Literal;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.jrdf.graph.URIReference;

public class RDFUtilUnitTest extends TestCase {

    private RDFUtil _util;

    public RDFUtilUnitTest(String name) throws Exception { 
        super(name); 
        _util = new RDFUtil();
    }

    public void testURIReferenceEquality() throws Exception {
        URIReference ref1 = _util.createResource(new URI("urn:test:same"));
        URIReference ref2 = _util.createResource(new URI("urn:test:same"));
        URIReference ref3 = _util.createResource(new URI("urn:test:different"));

        doObjectEqualityTest(ref1, ref2, ref3);
    }

    public void testLiteralEquality() throws Exception {

        // empty string plain no lang vs. empty string plain no lang
        Literal ePlainNoLang1 = _util.createLiteral("");
        Literal ePlainNoLang2 = _util.createLiteral("");
        Literal ePlainNoLang3 = _util.createLiteral("not empty");
        doObjectEqualityTest(ePlainNoLang1, ePlainNoLang2, ePlainNoLang3);

        // plain no lang vs. plain no lang
        Literal plainNoLang1 = _util.createLiteral("val");
        Literal plainNoLang2 = _util.createLiteral("val");
        Literal plainNoLang3 = _util.createLiteral("different val");
        doObjectEqualityTest(plainNoLang1, plainNoLang2, plainNoLang3);

        // plain with lang vs. plain with lang
        Literal plainWithLang1 = _util.createLiteral("val", "en");
        Literal plainWithLang2 = _util.createLiteral("val", "en");
        Literal plainWithLang3 = _util.createLiteral("different val", "es");
        doObjectEqualityTest(plainWithLang1, plainWithLang2, plainWithLang3);

        // typed vs. typed
        Literal typed1 = _util.createLiteral("val", new URI("urn:type:1"));
        Literal typed2 = _util.createLiteral("val", new URI("urn:type:1"));
        Literal typed3 = _util.createLiteral("val", new URI("urn:type:3"));
        doObjectEqualityTest(typed1, typed2, typed3);

        // mixed
        ensureDifferent(plainNoLang1, plainWithLang1);
        ensureDifferent(plainNoLang1, typed1);
        ensureDifferent(plainWithLang1, typed1);
    }

    public void testTripleEquality() throws Exception {

        // uriref as object vs. uriref as object
        URIReference ref1 = _util.createResource(new URI("urn:test:same"));
        URIReference ref2 = _util.createResource(new URI("urn:test:same"));
        URIReference ref3 = _util.createResource(new URI("urn:test:different"));
        doTripleEqualityTest(ref1, ref2, ref3);

        // empty plain no lang as object vs. empty plain no lang as object
        Literal ePlainNoLang1 = _util.createLiteral("");
        Literal ePlainNoLang2 = _util.createLiteral("");
        Literal ePlainNoLang3 = _util.createLiteral("not empty");
        doTripleEqualityTest(ePlainNoLang1, ePlainNoLang2, ePlainNoLang3);

        // plain no lang as object vs. plain no lang as object
        Literal plainNoLang1 = _util.createLiteral("val");
        Literal plainNoLang2 = _util.createLiteral("val");
        Literal plainNoLang3 = _util.createLiteral("different val");
        doTripleEqualityTest(plainNoLang1, plainNoLang2, plainNoLang3);

        // plain with lang as object vs. plain with lang as object
        Literal plainWithLang1 = _util.createLiteral("val", "en");
        Literal plainWithLang2 = _util.createLiteral("val", "en");
        Literal plainWithLang3 = _util.createLiteral("different val", "es");
        doTripleEqualityTest(plainWithLang1, plainWithLang2, plainWithLang3);

        // typed as object vs. typed as object
        Literal typed1 = _util.createLiteral("val", new URI("urn:type:1"));
        Literal typed2 = _util.createLiteral("val", new URI("urn:type:1"));
        Literal typed3 = _util.createLiteral("val", new URI("urn:type:3"));
        doTripleEqualityTest(typed1, typed2, typed3);

        // mixed
        ensureDifferent(getTriple(ref1), getTriple(ePlainNoLang1));
        ensureDifferent(getTriple(ref1), getTriple(plainNoLang1));
        ensureDifferent(getTriple(ref1), getTriple(plainWithLang1));
        ensureDifferent(getTriple(ref1), getTriple(typed1));

        ensureDifferent(getTriple(ePlainNoLang1), getTriple(plainNoLang1));
        ensureDifferent(getTriple(ePlainNoLang1), getTriple(plainWithLang1));
        ensureDifferent(getTriple(ePlainNoLang1), getTriple(typed1));

        ensureDifferent(getTriple(plainNoLang1), getTriple(plainWithLang1));
        ensureDifferent(getTriple(plainNoLang1), getTriple(typed1));

        ensureDifferent(getTriple(plainWithLang1), getTriple(typed1));
    }

    private void doTripleEqualityTest(ObjectNode same1, ObjectNode same2,
            ObjectNode different)
            throws Exception {
        doObjectEqualityTest(getTriple(same1), getTriple(same2), getTriple(different));
    }

    private void doObjectEqualityTest(Object same1, Object same2, Object different)
            throws Exception {
        ensureSame(same1, same2);
        ensureDifferent(same1, different);
    }

    private void ensureSame(Object o1, Object o2) throws Exception {
        assertTrue(o1.equals(o2));
        assertTrue(o2.equals(o1));
    }

    private void ensureDifferent(Object o1, Object o2) throws Exception {
        assertFalse(o1.equals(o2));
        assertFalse(o2.equals(o1));
    }

    private Triple getTriple(ObjectNode o) throws Exception {
        return _util.createTriple(_util.createResource(new URI("urn:s")),
                                  _util.createResource(new URI("urn:p")), o);
    }

}
