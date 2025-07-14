package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;


    // Clearly we want to store numpages as a field since we see it in the constructor, and since
    // the getPage method takes in a pageid as a parameter, and requests a page, along with the fact that
    // the concurrent hash map came pre-imported

    private final int numPages;
    private final Map<PageId, Page> pageIdToPage;
    // We declare both final because of the @threadsafe message above

    // For lab 3!!!!!!!!!!!!
    private final LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        pageIdToPage = new ConcurrentHashMap<>();
        lockManager = new LockManager();
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
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here

        try {
            this.lockManager.acquire(tid, pid, perm);
        } catch (InterruptedException e) {
            throw new TransactionAbortedException();
        }
        // "Will acquire a lock and may block if that lock is held by another transaction"


        if (this.pageIdToPage.containsKey(pid)) {
            return this.pageIdToPage.get(pid);
            // Already in bufferpool
        }else {
            if (this.pageIdToPage.size() >= this.numPages) {
                this.evictPage();
            }
            int tableId = pid.getTableId();
            // The table contains a database file, and the explanation for 2.4 says to use DbFile.readPage

            // Remember, Database.java was written so we can access its fields through methods from
            // anywhere
            Catalog c = Database.getCatalog();
            DbFile d = c.getDatabaseFile(tableId);
            Page p = d.readPage(pid);

            this.pageIdToPage.put(pid, p);
            return p;
        }
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
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        this.lockManager.release(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
        // "The version without the additional argument should always commit and so can simply be
        // implemented by calling transactionComplete(tid, true)."
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
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid : pageIdToPage.keySet()) {
            // Get page as well as the table id
            Page p = pageIdToPage.get(pid);

            int tableId = pid.getTableId();

            if (!commit) {
                // We have to re-get the page from the database file
                DbFile dbf = Database.getCatalog().getDatabaseFile(tableId);

                // Get page right back from the disk
                Page newP = dbf.readPage(pid);
                newP.markDirty(false, null);

                pageIdToPage.put(pid, newP);
            } else {
                // Now for each dirty page we need to write into the log, and add a call to setBeforeImage
                if (p.isDirty() != null && p.isDirty().equals(tid)) {
                    // The page is dirty and this txn dirtied it
                    Database.getLogFile().logWrite(tid, p.getBeforeImage(), p);
                    Database.getLogFile().force();

                    // use current page contents as the before-image
                    // for the next transaction that modifies this page.
                    p.setBeforeImage();
                }
            }
        }

        // Get rid of all of our locks no matter if we are committing or aborting
        lockManager.massRelease(tid, null);
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
        // We will ignore anything related to locks, but we will implement the dirty bit logic as its easy and we already
        // made a heappage field for it. (its 100% required too, i just didnt see it on the instructions)
        // Remember what files we coded -> the page and then the file, so we want to insert into a file, and remember
        // we can get from the general database instance to this method that wants a tableid
        HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);

        ArrayList<Page> list = hf.insertTuple(tid, t);
        // Remember that we returned a list of changed tuples

        for (int i = 0; i < list.size(); i++) {
            Page p = list.get(i);

            // In part 2, we must also evict our page too
            if (this.pageIdToPage.size() >= this.numPages) {
                this.evictPage();
                // Call our current method
            }

            //Had to move this to after the eviction above to avoid breaking no steal
            p.markDirty(true, tid);

            // "Adds versions of any pages that have been dirtied to the cache"
            pageIdToPage.put(p.getId(), p);
            // Its very easy to forget that we are actually in the cache itself, and that too put something into
            // the cache means to put it into the data structures that we made.
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
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableid = t.getRecordId().getPageId().getTableId();
        // Similar to insert, we ignore anything related to the lock, Why on earth are we not given the tableid idk, but
        // getting it is pretty easy (just look at all methods). After that, its just almost the exact same as before

        HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);

        ArrayList<Page> list = hf.deleteTuple(tid, t);
        // Remember that we returned a list of changed tuples, delete instead of insert

        for (int i = 0; i < list.size(); i++) {
            Page p = list.get(i);
            p.markDirty(true, tid);

            // No need to change our co`de to evict here like we had to do in insert

            // "Adds versions of any pages that have been dirtied to the cache"
            pageIdToPage.put(p.getId(), p);
            // Its very easy to forget that we are actually in the cache itself, and that too put something into
            // the cache means to put it into the data structures that we made.
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
        for (PageId p : pageIdToPage.keySet()){
            // For each through our internal DS flushing our pages for testing
            this.flushPage(p);
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
        pageIdToPage.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page p = pageIdToPage.get(pid);

        if (pageIdToPage.containsKey(pid)) {
            // Instead of check to see if the page is dirty, we check to see if the txn is still in our map
            // because that means its still running

            TransactionId dirtier = p.isDirty();
            if (dirtier != null) {
                Database.getLogFile().logWrite(dirtier, p.getBeforeImage(), p);
                Database.getLogFile().force();
                // Similar to insert/deleteTuple, we get our databasefile and cast it to the heap version for our method
                HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());


                hf.writePage(p);
                p.markDirty(false, null);
                // Write it with heapfile, and change its dirty status. NOTE THAT WE NEVER EVICT IT HERE
            }
        }

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        ArrayList<PageId> list = new ArrayList<>(this.pageIdToPage.keySet());

        // We switch back to random eviction because lab 4 flips it back
        int r = (int) (Math.random() * this.pageIdToPage.keySet().size());
        // Because this method (Math.random()), returns a num  between 0 and 1, so we multiply to get into a more
        // usable form

        // Craft our page id/page
        PageId p = list.get(r);

        try {
            flushPage(p);
            discardPage(p);
            // Remember since its been a while, discard removes from the map
        } catch (IOException e) {
            throw new DbException("Cannot evict this page" + p);
        }

    }

}
