package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.Test;

import simpledb.systemtest.SimpleDbTestBase;

public class IndexedPageIdTest extends SimpleDbTestBase {

    private IndexedPageId pid;

    @Before public void createPid() {
        pid = new IndexedPageId(1, 1);
    }

    /**
     * Unit test for IndexedPageId.getTableId()
     */
    @Test public void getTableId() {
        assertEquals(1, pid.getTableId());
    }

    /**
     * Unit test for IndexedPageId.pageno()
     */
    @Test public void pageno() {
        assertEquals(1, pid.pageno());
    }

    /**
     * Unit test for IndexedPageId.hashCode()
     */
    @Test public void testHashCode() {
        int code1, code2;

        // NOTE(ghuo): the hashCode could be anything. test determinism,
        // at least.
        pid = new IndexedPageId(1, 1);
        code1 = pid.hashCode();
        assertEquals(code1, pid.hashCode());
        assertEquals(code1, pid.hashCode());

        pid = new IndexedPageId(2, 2);
        code2 = pid.hashCode();
        assertEquals(code2, pid.hashCode());
        assertEquals(code2, pid.hashCode());
    }

    /**
     * Unit test for IndexedPageId.equals()
     */
    @Test public void equals() {
        IndexedPageId pid1 = new IndexedPageId(1, 1);
        IndexedPageId pid1Copy = new IndexedPageId(1, 1);
        IndexedPageId pid2 = new IndexedPageId(2, 2);

        // .equals() with null should return false
        assertFalse(pid1.equals(null));

        // .equals() with the wrong type should return false
        assertFalse(pid1.equals(new Object()));

        assertTrue(pid1.equals(pid1));
        assertTrue(pid1.equals(pid1Copy));
        assertTrue(pid1Copy.equals(pid1));
        assertTrue(pid2.equals(pid2));

        assertFalse(pid1.equals(pid2));
        assertFalse(pid1Copy.equals(pid2));
        assertFalse(pid2.equals(pid1));
        assertFalse(pid2.equals(pid1Copy));
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(IndexedPageIdTest.class);
    }
}

