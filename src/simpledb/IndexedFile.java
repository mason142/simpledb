package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see HeapPage#HeapPage
 */
public class IndexedFile implements DbFile {
    private File f;
    //private TupleDesc td;
    private int indexedColumnNumber;
    private int numPages;
    private int levels;
    private ArrayList<Integer> pageNumbers;
    private HeapFile hf;
    private Type type;
    private boolean indicesLoaded;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public IndexedFile(File f, Type type, HeapFile hf, int indexedColumnNumber) {
        this.f = f;
        this.type = type;
        this.indexedColumnNumber = indexedColumnNumber;
        this.pageNumbers = new ArrayList<>();
        this.hf = hf;

    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value
     * for a particular HeapFile. We suggest hashing the absolute file name of
     * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        Type[] t = {type};
        return new TupleDesc(t, new String[]{"Key"});
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        if(pid == null)
            return null;
        int pageNum = pid.pageno();
        int fileOffset = pageNum * BufferPool.PAGE_SIZE;
        byte[] pageData = new byte[BufferPool.PAGE_SIZE];
        try {
            FileInputStream fis = new FileInputStream(f.getAbsoluteFile());
            fis.skip(fileOffset);
            fis.read(pageData);
            fis.close();
            return new IndexedPage((IndexedPageId)pid, pageData, type, false);
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int fileOffset = page.getId().pageno() * BufferPool.PAGE_SIZE;
        try {
            RandomAccessFile raf = new RandomAccessFile(f.getAbsoluteFile(), "rw");
            raf.seek(fileOffset);
            raf.write(page.getPageData());
            raf.close();
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Returns the number of pages in this IndexedFile.
     */
    public int numPages() {
        return pageNumbers.size();
    }

    private RecordId getRecordIdFromPointer(int tupId) {
        int pageNo = tupId / hf.getNumTuplesPerPage(); // Integer division to get the page number
        int tupleNo = tupId % hf.getNumTuplesPerPage();
        return new RecordId(new HeapPageId(hf.getId(), pageNo), tupleNo);
    }
    private int seekTupleSlot(TransactionId tid, Field field) throws TransactionAbortedException, DbException {
        IndexedPageId ipd = (IndexedPageId) searchTuple(tid, field);
        BufferPool bp = Database.getBufferPool();
        IndexedPage indexedPage = (IndexedPage) bp.getPage(tid, ipd, Permissions.READ_ONLY);
        return indexedPage.getTupleLoc(field);
    }

    private IndexedPageId getFirstOpenPage() {
        int[] nums = pageNumbers.stream().mapToInt(Integer::intValue).toArray();
        if (nums.length == 0) {
            return new IndexedPageId(getId(), 0);
        }
        int n=nums.length;
        boolean one = false;
        // 1. Mark the elements which are out of range
        for(int i=0;i<n;i++)
        {
            if(nums[i]==1)
                one = true;
            if(nums[i]<1 || nums[i]>n)
                nums[i]=1;
        }
        if(one==false) {
            return new IndexedPageId(getId(), 1);
        }
        // 2. Map the element with index : arr[element-1]=0-arr[element-1]
        for(int i=0;i<n;i++)
        {
            int idx = Math.abs(nums[i]); // making the value +ve by abs() so that idx will be a +ve one
            nums[idx-1] = -Math.abs(nums[idx-1]); //making the element negative
        }
        // 3. Check which number is not negative i.e. Positive, return its index+1
        for(int i=0;i<n;i++)
        {
            if(nums[i]>0)
                return new IndexedPageId(getId(), i+1);
        }
        return new IndexedPageId(getId(), n+1);
    }

    public PageId findFirstLeaf(TransactionId tid)
            throws TransactionAbortedException, DbException {
        int tableId = getId();
        BufferPool bp = Database.getBufferPool();
        IndexedPageId pId = new IndexedPageId(tableId, 0);
        IndexedPage indexedPage = (IndexedPage) bp.getPage(tid, pId, Permissions.READ_ONLY);
        while(!indexedPage.isLeaf()) {
            int childPageNumber = indexedPage.findLeftMostPageNo();
            pId = new IndexedPageId(tableId, childPageNumber);
            indexedPage = (IndexedPage) bp.getPage(tid, pId, Permissions.READ_ONLY);
        }
        return pId;
    }
    // Return the page that has the tuple and the parent of the page
    public PageId searchTuple(TransactionId tid, Field f)
            throws TransactionAbortedException, DbException {
        int tableId = getId();
        BufferPool bp = Database.getBufferPool();
        IndexedPageId pId = new IndexedPageId(tableId, 0);
        IndexedPage indexedPage = (IndexedPage) bp.getPage(tid, pId, Permissions.READ_ONLY);
        while(!indexedPage.isLeaf()) {
            int childPageNumber = indexedPage.findChildPageNo(f);
            pId = new IndexedPageId(tableId, childPageNumber);
            indexedPage = (IndexedPage) bp.getPage(tid, pId, Permissions.READ_ONLY);
        }
        return pId;
    }

    public IndexedPageId makeNewIndexPage(boolean isRoot) throws IOException {
        // create new page if no space in previous pages. Prevent race condition where 2 new pages with the same
        // page number are created, resulting in the first new page being overwritten by the second page
        synchronized (Database.getCatalog()) {
            IndexedPageId newPageId = getFirstOpenPage();
            IndexedPage newPage = new IndexedPage(newPageId, new byte[BufferPool.PAGE_SIZE], type, isRoot);
            writePage(newPage);
            return newPageId;
        }
    }

    public ArrayList<Page> splitPage(TransactionId tid, Field f, IndexedPage indexedPage, BufferPool bp)
            throws DbException, TransactionAbortedException{
        Pair<Field[], int[]> firstHalf, secondHalf;
        firstHalf = indexedPage.getFirstHalf();
        secondHalf = indexedPage.getSecondHalf();
        Field middleKey = indexedPage.getMiddleKey();
        if (!indexedPage.isRoot()) {
            try {
                // Get parent
                IndexedPage newPage = (IndexedPage) bp.getPage(tid, makeNewIndexPage(false), Permissions.READ_WRITE);
                if (!indexedPage.isLeaf()) {
                    // Update children to point to parent (will be O(1) size since number of children is a fixed size)
                    for (int i = 0; i < secondHalf.getValue().length; i++) {
                        IndexedPageId pageId = new IndexedPageId(getId(), secondHalf.getValue()[i]);
                        IndexedPage childPage = (IndexedPage) bp.getPage(tid, pageId, Permissions.READ_WRITE);
                        childPage.setParentId(indexedPage.getId().pageno());
                    }
                    for (int i = 0; i < firstHalf.getValue().length; i++) {
                        IndexedPageId pageId = new IndexedPageId(getId(), firstHalf.getValue()[i]);
                        IndexedPage childPage = (IndexedPage) bp.getPage(tid, pageId, Permissions.READ_WRITE);
                        childPage.setParentId(newPage.getId().pageno());
                    }
                }

                pageNumbers.add(newPage.pid.pageno());
                System.out.println(pageNumbers);
                newPage.setIsLeaf(indexedPage.isLeaf());
                newPage.setTuplesAndPointers(firstHalf.getKey(), firstHalf.getValue());
                newPage.setNextId(indexedPage.getId().pageno());
                newPage.setParentId(indexedPage.getParentId());
                newPage.setPrevId(indexedPage.getPrevId());

                indexedPage.setTuplesAndPointers(secondHalf.getKey(), secondHalf.getValue());
                indexedPage.setPrevId(newPage.getId().pageno());

                newPage.markDirty(true, tid);
                indexedPage.markDirty(true, tid);
                ArrayList<Page> changes = bTreeInsert(tid, middleKey, new IndexedPageId(getId(), indexedPage.getParentId()), newPage.pid.pageno(), bp);

                if (newPage.getPrevId() != -1) { // For linked list consistency
                    IndexedPage prevPage = (IndexedPage) bp.getPage(tid, newPage.prevId, Permissions.READ_WRITE);
                    prevPage.setNextId(newPage.getId().pageno());
                    changes.add(prevPage);
                }
                // Call b tree insert on parent
                changes.add(indexedPage);
                changes.add(newPage);
                return changes;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            try {
                IndexedPage newPage1 = (IndexedPage) bp.getPage(tid, makeNewIndexPage(false), Permissions.READ_WRITE);
                pageNumbers.add(newPage1.pid.pageno());
                IndexedPage newPage2 = (IndexedPage) bp.getPage(tid, makeNewIndexPage(false), Permissions.READ_WRITE);
                pageNumbers.add(newPage2.pid.pageno());

                if (!indexedPage.isLeaf()) {
                    // Update children to point to parent (will be O(1) size since number of children is a fixed size)
                    for (int i = 0; i < secondHalf.getValue().length; i++) {
                        IndexedPageId pageId = new IndexedPageId(getId(), secondHalf.getValue()[i]);
                        IndexedPage childPage = (IndexedPage) bp.getPage(tid, pageId, Permissions.READ_WRITE);
                        childPage.setParentId(newPage2.getId().pageno());
                    }
                    for (int i = 0; i < firstHalf.getValue().length; i++) {
                        IndexedPageId pageId = new IndexedPageId(getId(), firstHalf.getValue()[i]);
                        IndexedPage childPage = (IndexedPage) bp.getPage(tid, pageId, Permissions.READ_WRITE);
                        childPage.setParentId(newPage1.getId().pageno());
                    }
                }


                newPage1.setIsLeaf(indexedPage.isLeaf());
                newPage1.setTuplesAndPointers(firstHalf.getKey(), firstHalf.getValue());
                newPage1.setParentId(indexedPage.getId().pageno());




                newPage2.setIsLeaf(indexedPage.isLeaf()); // If the root is a leaf, then this page will be a leaf, otherwise it must be internal
                newPage2.setTuplesAndPointers(secondHalf.getKey(), secondHalf.getValue());
                newPage2.setParentId(indexedPage.getId().pageno());


                newPage1.setPrevId(-1);
                newPage1.setNextId(newPage2.getId().pageno());
                newPage2.setNextId(-1);
                newPage2.setPrevId(newPage1.getId().pageno());

                int[] pointers = {newPage1.getId().pageno(), newPage2.getId().pageno()};
                Field[] tuples = {middleKey};
                indexedPage.setIsLeaf(false);
                indexedPage.setTuplesAndPointers(tuples, pointers);


                return new ArrayList<Page>(Arrays.asList(new Page[] {newPage1, newPage2,indexedPage}));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public ArrayList<Page> bTreeInsert(TransactionId tid, Field f, IndexedPageId pageId, int pointer, BufferPool bp)
            throws TransactionAbortedException, DbException {
        IndexedPage indexedPage = (IndexedPage) bp.getPage(tid, pageId, Permissions.READ_WRITE);
        ArrayList<Page> alteredPages = null;
        if (indexedPage.getNumEmptySlots() == 1) {
            try {
                //IndexedPageId parent = new IndexedPageId(indexedPage.pid.getTableId(), indexedPage.parentId.pageno());
                indexedPage.addTuple(f, pointer);
                alteredPages = splitPage(tid, f, indexedPage, bp); // Returns (newPage, oldPage) want new tuple to have left pointer point to new page
            } catch (DbException e) {
                e.printStackTrace();
            }
        }
        else {
            indexedPage.addTuple(f, pointer);
            indexedPage.markDirty(true, tid);
            alteredPages = new ArrayList<>(Arrays.asList(new Page[] {indexedPage}));
        }
        return alteredPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        if (indicesLoaded) {
            hf.addTuple(tid, t);
        }
        BufferPool bp = Database.getBufferPool();
        IndexedPageId pId = (IndexedPageId) searchTuple(tid, t.getField(indexedColumnNumber));
        // Tuple is uniquely defined by tuples per page * page number + slot number.
        int tupleIdentifier = t.getRecordId().getPageId().pageno()*hf.getNumTuplesPerPage() + t.getRecordId().tupleno();
        return bTreeInsert(tid, t.getField(indexedColumnNumber), pId, tupleIdentifier, bp);
    }


    public ArrayList<Page> loadIndices(TransactionId tid) throws TransactionAbortedException, DbException, IOException {
        if (indicesLoaded) {
            return null;
        }
        IndexedPage newPage = (IndexedPage) Database.getBufferPool().getPage(tid, makeNewIndexPage(true), Permissions.READ_WRITE);
        pageNumbers.add(newPage.pid.pageno());
        ArrayList<Page> alteredPages = new ArrayList<>();
        HeapFile.HeapFileIterator heapFileIterator = (HeapFile.HeapFileIterator) hf.iterator(tid);
        heapFileIterator.open();
        while (heapFileIterator.hasNext()) {
            alteredPages.addAll(this.addTuple(tid, heapFileIterator.next()));
        }
        indicesLoaded = true;
        return alteredPages;
    }


    public ArrayList<Page> bTreeDelete(TransactionId tid, Tuple t) throws TransactionAbortedException, DbException {
        IndexedPage indexedPage = (IndexedPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        indexedPage.markDirty(true, tid);

        if (!indexedPage.isHalfFull()) {
            if (indexedPage.getNextId() == -1) {

            }
            else if (indexedPage.getPrevId() == -1) {

            }
            IndexedPageId nextPageId = new IndexedPageId(getId(), indexedPage.getNextId());
            IndexedPageId prevPageId = new IndexedPageId(getId(), indexedPage.getPrevId());

            IndexedPage prevPage = (IndexedPage) Database.getBufferPool().getPage(tid, nextPageId, Permissions.READ_WRITE);
            IndexedPage nextPage = (IndexedPage) Database.getBufferPool().getPage(tid, prevPageId, Permissions.READ_WRITE);


        }
        return null;
    }
    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
            throws DbException, TransactionAbortedException {
        throw new DbException("Not implemented for B+ trees");
        //TODO: Figure out how to garbage collect empty pages and how to defragment data in heap files
        //bTreeDelete(tid, t);
    }

    public class IndexedFileIterator implements DbFileIterator {
        private HeapPage hp;
        private IndexedPage indexedPage;
        private Iterator<Integer> it;
        private TransactionId tid;
        private int tableId;
        private IndexedFile indexedFile;
        private HeapFile hf;

        public IndexedFileIterator(TransactionId tid, IndexedFile indexedFile) {
            this.tableId = indexedFile.getId();
            this.tid = tid;
            this.indexedFile = indexedFile;
            this.hf = indexedFile.hf;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            try {
                indexedFile.loadIndices(tid);
            } catch (IOException e) {
                throw new DbException("Issue with IO in IndexedFile open() load indices");
            }
            IndexedPageId pId = new IndexedPageId(tableId, findFirstLeaf(tid).pageno());
            indexedPage = (IndexedPage) Database.getBufferPool().getPage(tid, pId, null);
            it = indexedPage.iterator();
        }

        public void seek(Field f) throws TransactionAbortedException, DbException {
            IndexedPageId pageId = (IndexedPageId) indexedFile.searchTuple(tid, f);
            int tupleSlot = indexedFile.seekTupleSlot(tid, f);
            indexedPage = (IndexedPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            it = indexedPage.iterator(tupleSlot);
        }
        @Override
        public boolean hasNext() {
            if(indexedPage == null)
                return false;
            if(it.hasNext())
                return true;
            if (indexedPage.isRoot())
                return false;
            while(indexedPage.getNextId() != -1) {
                try {
                    IndexedPageId newpId = new IndexedPageId(tableId, indexedPage.getNextId());
                    IndexedPageId oldpId = indexedPage.pid;
                    indexedPage = (IndexedPage) Database.getBufferPool().getPage(tid, newpId, Permissions.READ_ONLY);
                    it = indexedPage.iterator();
                    if(it.hasNext())
                        return true;
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }

            return false;
        }

        @Override
        public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
            if(!this.hasNext())
                throw new NoSuchElementException();

            if(it.hasNext()) {
                BufferPool bp = Database.getBufferPool();
                int tupleId = it.next();
                RecordId rid = getRecordIdFromPointer(tupleId);
                HeapPageId pid = new HeapPageId(hf.getId(), rid.getPageId().pageno());
                hp = (HeapPage) bp.getPage(tid, pid, Permissions.READ_ONLY);
                return hp.tupleSeek(rid.tupleno());
            }
            else
                return null;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public void close() {
            hp = null;
            it = null;
            tid = null;
            hf = null;
        }

    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new IndexedFileIterator(tid, this);
    }

}
