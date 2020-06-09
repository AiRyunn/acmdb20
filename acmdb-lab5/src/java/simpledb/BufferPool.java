package simpledb;

import java.io.*;
import java.util.ArrayList;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    private static int timestamp = 0;

    private class PageWithPriority implements Comparable<PageWithPriority> {
        private final Page page;
        private final PageId pid;
        private Integer priority;

        private PageWithPriority(Page page, PageId pid) {
            this.page = page;
            this.pid = pid;
            this.priority = timestamp++;
        }

        private Integer getPriority() {
            return priority;
        }

        private void flush() {
            this.priority = timestamp++;
        }

        @Override
        public int compareTo(PageWithPriority other) {
            return this.priority.compareTo(other.getPriority());
        }

    }

    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    private final ArrayList<PageWithPriority> pages;
    private final int numPages;
    private final TransactionLockManager lockManager;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.pages = new ArrayList<PageWithPriority>();
        this.numPages = numPages;
        this.lockManager = new TransactionLockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        this.lockManager.acquireLock(tid, pid, perm);
        PageWithPriority p = findPage(pid);
        if (p != null) {
            return p.page;
        }
        if (pages.size() >= numPages) {
            evictPage();
        }
        DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = dbfile.readPage(pid);
        this.addPage(page);
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        this.lockManager.releasePage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        this.transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return this.lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            for (PageId pid : this.lockManager.getPagesInTransaction(tid)) {
                PageWithPriority p = findPage(pid);
                if (p != null) {
                    Page page = p.page;
                    if (tid.equals(page.isDirty())) {
                        flushPage(page);
                        page.setBeforeImage();
                    }
                }
            }
        } else {
            for (PageId pid : this.lockManager.getPagesInTransaction(tid)) {
                PageWithPriority p = findPage(pid);
                if (p != null) {
                    Page page = p.page;
                    if (tid.equals(page.isDirty())) {
                        abortPage(page);
                    }
                }
            }
        }

        this.lockManager.releasePages(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pageList = file.insertTuple(tid, t);
        for (Page page : pageList) {
            this.addPage(page);
            page.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> pageList = file.deleteTuple(tid, t);
        for (Page page : pageList) {
            addPage(page);
            page.markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageWithPriority p : this.pages) {
            flushPage(p.page);
        }
    }

    private synchronized void addPage(Page page) {
        PageId pid = page.getId();
        PageWithPriority p = findPage(pid);
        if (p != null) {
            // FIXME
            // p.flush();
        } else {
            this.pages.add(new PageWithPriority(page, pid));
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        this.pages.remove(findPage(pid));
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushDirtyPage(Page page, TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        dbFile.writePage(page);
        page.markDirty(false, tid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        TransactionId tid = page.isDirty();
        if (tid != null) {
            flushDirtyPage(page, tid);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageWithPriority p : pages) {
            Page page = p.page;
            TransactionId dirtyId = page.isDirty();
            if (dirtyId != null && dirtyId.equals(tid)) {
                flushDirtyPage(page, tid);
            }
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void abortPage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        TransactionId tid = page.isDirty();
        if (tid != null) {
            discardPage(pid);
            pages.add(new PageWithPriority(page.getBeforeImage(), pid));
            page.markDirty(false, tid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        PageWithPriority flushPage = null, dirtyPage = null;

        for (PageWithPriority p : this.pages) {
            if (p.page.isDirty() == null) {
                if (flushPage == null || p.compareTo(flushPage) < 0) {
                    flushPage = p;
                }
            } else {
                if (dirtyPage == null || p.compareTo(dirtyPage) < 0) {
                    dirtyPage = p;
                }
            }
        }

        // NOTE: assert for test
        assert flushPage != null;

        if (flushPage == null) {
            throw new DbException("unable to evice page since all pages in the buffer pool are dirty");
            // flushPage = dirtyPage;
            // try {
            //     flushPage(dirtyPage.pid);
            // } catch (Exception e) {
            //     throw new DbException("unable to evice page");
            // }
        }

        assert flushPage != null;

        this.discardPage(flushPage.pid);
    }

    private synchronized PageWithPriority findPage(PageId pid) {
        for (PageWithPriority p : this.pages) {
            if (p.pid.equals(pid)) {
                return p;
            }
        }
        return null;
    }
}
