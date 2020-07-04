package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private final File file;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            RandomAccessFile f = new RandomAccessFile(this.file, "r");
            int offset = BufferPool.getPageSize() * pid.pageNumber();
            byte[] data = new byte[BufferPool.getPageSize()];
            f.seek(offset);
            f.readFully(data);
            f.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        try {
            RandomAccessFile f = new RandomAccessFile(this.file, "rw");
            int offset = BufferPool.getPageSize() * pid.pageNumber();
            f.seek(offset);
            f.write(page.getPageData());
            f.close();
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        long length = file.length();
        int pagesize = BufferPool.getPageSize();
        return (int) (length + pagesize - 1) / pagesize;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapPage page = getPage(tid, Permissions.READ_WRITE);

        if (page != null) {
            page.insertTuple(t);
            return new ArrayList<Page>(Arrays.asList(page));
        }

        page = getEmptyPage(tid);
        page.insertTuple(t);

        return new ArrayList<Page>(Arrays.asList(page));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<Page>(Arrays.asList(page));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    class HeapFileIterator extends AbstractDbFileIterator {
        private final HeapFile heapFile;
        private final TransactionId tid;

        private Iterator<Tuple> tupleIt;
        private int currentPageNumber;

        public HeapFileIterator(HeapFile hf, TransactionId tid) {
            this.heapFile = hf;
            this.tid = tid;
        }

        public void open() throws DbException, TransactionAbortedException {
            currentPageNumber = -1;
        }

        @Override
        protected Tuple readNext() throws TransactionAbortedException, DbException {
            if (tupleIt != null && !tupleIt.hasNext()) {
                tupleIt = null;
            }

            while (tupleIt == null && currentPageNumber < heapFile.numPages() - 1) {
                currentPageNumber++;

                HeapPageId currentPageId = new HeapPageId(heapFile.getId(), currentPageNumber);

                HeapPage currentPage = (HeapPage) Database.getBufferPool().getPage(tid, currentPageId,
                        Permissions.READ_ONLY);
                tupleIt = currentPage.iterator();

                if (!tupleIt.hasNext()) {
                    tupleIt = null;
                }
            }

            if (tupleIt == null) {
                return null;
            }

            return tupleIt.next();
        }

        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        public void close() {
            super.close();
            tupleIt = null;
            currentPageNumber = Integer.MAX_VALUE;
        }

    }

    private HeapPage getPage(TransactionId tid, Permissions perm) throws TransactionAbortedException, DbException {
        for (int i = 0; i < this.numPages(); i++) {
            PageId pid = new HeapPageId(this.getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            if (page.getNumEmptySlots() > 0) {
                return (HeapPage) Database.getBufferPool().getPage(tid, pid, perm);
            }
        }
        return null;
    }

    private HeapPage getEmptyPage(TransactionId tid) throws DbException, IOException, TransactionAbortedException {
        HeapPageId pid = new HeapPageId(this.getId(), this.numPages());
        byte[] data = HeapPage.createEmptyPageData();

        RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
        raf.seek(BufferPool.getPageSize() * this.numPages());
        raf.write(data);
        raf.close();

        Database.getBufferPool().discardPage(pid);

        return getPage(tid, Permissions.READ_WRITE);
    }
}
