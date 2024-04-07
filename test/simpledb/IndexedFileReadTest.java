package simpledb;

import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

import java.io.File;
import java.util.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import junit.framework.JUnit4TestAdapter;

public class IndexedFileReadTest extends SimpleDbTestBase {
    private HeapFile hf;
    private TransactionId tid;
    private TupleDesc td;
    private IndexedFile indexedFile;

    /**
     * Set up initial resources for each unit test.
     */
    @Before
    public void setUp() throws Exception {
        hf = SystemTestUtil.createRandomHeapFile(2, 20, null, null);
        td = Utility.getTupleDesc(2);
        tid = new TransactionId();
        File temp = File.createTempFile("table", ".dat");
        indexedFile = new IndexedFile(temp, td.getType(0), hf, 0);
    }

    @After
    public void tearDown() throws Exception {
        Database.getBufferPool().transactionComplete(tid);
    }

    /**
     * Unit test for HeapFile.getId()
     */
    @Test
    public void getId() throws Exception {
        int id = indexedFile.getId();

        // NOTE(ghuo): the value could be anything. test determinism, at least.
        assertEquals(id, indexedFile.getId());
        assertEquals(id, indexedFile.getId());

        HeapFile other = SystemTestUtil.createRandomHeapFile(1, 1, null, null);
        assertTrue(id != other.getId());
    }

    /**
     * Unit test for HeapFile.getTupleDesc()
     */
    @Test
    public void getTupleDesc() throws Exception {
        assertEquals(td, hf.getTupleDesc());
    }
    /**
     * Unit test for HeapFile.numPages()
     */
    @Test
    public void numPages() throws Exception {
        assertEquals(1, hf.numPages());
        // assertEquals(1, empty.numPages());
    }

    @Test
    public void testIteratorBasic() throws Exception {
        HeapFile smallFile = SystemTestUtil.createRandomHeapFile(2, 3, null,
                null);

        IndexedFile smallIndexedFile = Utility.openIndexedFiled(smallFile);
        DbFileIterator it = smallFile.iterator(tid);
        DbFileIterator it2 = smallIndexedFile.iterator(tid);
        // Not open yet
        assertFalse(it.hasNext());
        assertFalse(it2.hasNext());
        try {
            it.next();
            it2.next();
            fail("expected exception");
        } catch (NoSuchElementException e) {
        }

        it.open();
        int count = 0;
        while (it.hasNext()) {
            System.out.println(it.next());
            count += 1;
        }
        assertEquals(3, count);
        it.close();

        it2.open();
        while (it2.hasNext()) {
            System.out.println(it2.next());
        }
        it2.close();
    }

    @Test
    public void testIteratorClose() throws Exception {
        // make more than 1 page. Previous closed iterator would start fetching
        // from page 1.
        HeapFile smallFile = SystemTestUtil.createRandomHeapFile(2, 10000, null,
                null);

        IndexedFile smallIndexedFile = Utility.openIndexedFiled(smallFile);
        DbFileIterator it = smallFile.iterator(tid);
        DbFileIterator it2 = smallIndexedFile.iterator(tid);
        // Not open yet
        assertFalse(it.hasNext());
        assertFalse(it2.hasNext());
        try {
            it.next();
            it2.next();
            fail("expected exception");
        } catch (NoSuchElementException e) {
        }
        System.out.println("Indexed time");
        it2.open();
        Set<Tuple> objectSet = new HashSet<>();
        int count = 0;
        while (it2.hasNext()) {
            Tuple t = it2.next();
            assert !objectSet.contains(t) : "Duplicate object found: " + t.toString();
            objectSet.add(t);
            count++;
        }
        assert count == 10000 : "Skipped tuple " + count;
        it2.close();
    }
    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(HeapFileReadTest.class);
    }
}
