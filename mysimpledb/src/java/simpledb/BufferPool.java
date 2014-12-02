package simpledb;

import java.io.*;
import java.util.ArrayList;


import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.sun.corba.se.impl.orbutil.closure.Future;
import com.sun.corba.se.impl.orbutil.threadpool.TimeoutException;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p/>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    private int numPages;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    private List<Page> pages;
    private List<Long> timeStamps;
    private LockManager lockManager;
        
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int np) {
    	pages = new ArrayList<Page>();
    	timeStamps = new ArrayList<Long>();
    	numPages = np;
    	lockManager = new LockManager();
    }

    public int getNumberOfPages(){
    	return pages.size();
    }
    
    public static int getPageSize() {
        return pageSize;
    }
    
    public LockManager getLockManager(){
    	return lockManager;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p/>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */

    public Page getPage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException, DbException { 
    	    	    	
    	lockManager.getLock(tid, pid, perm);
 	
    	for (int i = 0; i < pages.size(); i++){
    		if (pages.get(i).getId().equals(pid)){
    			timeStamps.set(i, (long) 0);
    			return pages.get(i);
    		}
    	}
    	
        // if it wasn't in the buffer pool
    	if (pages.size() == numPages){
    		//System.out.println("attempted eviction");
    		evictPage();
    	}
    	
    	// get the page from the catalog
    	Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
    	    	
    	// These better line up...
    	pages.add(page);
    	timeStamps.add( (long) System.currentTimeMillis());
    	
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
    public void releasePage(TransactionId tid, PageId pid) {                                                       // cosc460
    	lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);                                                    // cosc460
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {                                                  // cosc460
        return lockManager.holdsLock(tid, pid, perm);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) throws IOException {
        
    	if (commit){
    		//System.out.println("committing");
    		flushPages(tid);
    		
    	} else {
    		// all pages that are dirtied by this txn will be replaced by their Catalog version
    		Catalog c = Database.getCatalog();
    		
    		for (Page p : pages){
    			if (p.isDirty() == tid){ // dirtied by this txn
    				p = c.getDatabaseFile(p.getId().getTableId()).readPage(p.getId());
    				releasePage(tid, p.getId());
    			}
    		}
    	}
    	
    	lockManager.releaseLocksAndRequests(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed until lab5).                                  // cosc460
     * May block if the lock(s) cannot be acquired.
     * <p/>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        
    	DbFile f = Database.getCatalog().getDatabaseFile(tableId);
    	//HeapFile hf = (HeapFile) f;
    	ArrayList<Page> dirtiedPages = f.insertTuple(tid, t);
    	
    	for (int i = 0; i < dirtiedPages.size(); i++) {
    		dirtiedPages.get(i).markDirty(true, tid); // changed 0 to i
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p/>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        
    	PageId pid = t.getRecordId().getPageId();
    	HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    	p.markDirty(true, tid);
    	p.deleteTuple(t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Page p : pages){
        	if (p.isDirty() != null){
        		flushPage(p.getId());
        	}
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab6                                                                            // cosc460
    }

    /**
     * Flushes a certain page to disk 
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // find it
    	Page toFlush = null;
    	
    	for (Page p : pages){
    		if (p.getId().equals(pid)){
    			//System.out.println("***pgtoflush");
    			toFlush = p;
    		}
    	}

    	DbFile table = Database.getCatalog().getDatabaseFile(pid.getTableId());
    	table.writePage(toFlush);
    	toFlush.markDirty(false, new TransactionId());
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        //
    	for (Page p : pages){
    		if (p.isDirty() != null){
	    		if (p.isDirty().equals(tid)){
	    			flushPage(p.getId());
    			}
    		}
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
    	
    	int lruIndex = 0;
    	long earliest = Long.MAX_VALUE;
    	
    	for (int i = 0; i < pages.size(); i++){
    		if (timeStamps.get(i) < earliest){
    			if (pages.get(i) != null){
    				lruIndex = i;
    			}
    		}
    	}
    	
    	if (lruIndex == 0){
    		throw new DbException("All pages are dirty, cannot evict a page");
    	}
    	
    	// evict page at lruIndex
    	HeapPage hp = (HeapPage) pages.get(lruIndex);
    	
    	try {
    		flushPage(hp.getId());
    		pages.remove(hp);
    		timeStamps.remove(lruIndex);
    	} catch (IOException e) {
    		throw new DbException("Page wasn't flushed or evicted");
    	}	
    } 
}
