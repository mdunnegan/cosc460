package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import java.util.List;

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
    private List<Long> timeSinceUse;
        
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int np) {
    	pages = new ArrayList<Page>();
    	timeSinceUse = new ArrayList<Long>();
    	numPages = np;
    }

    public static int getPageSize() {
        return pageSize;
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
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException { // when page number over limit
    	
    	// go through all the pages, find the matching on
    	for (int i = 0; i < pages.size(); i++){
    		if (pages.get(i).getId().equals(pid)){
    			// if you get a page in the bufferpool, you're using it, so the timeSinceUse
    			// should reset to 0
    			timeSinceUse.set(i, (long) 0);
    			return pages.get(i);
    		}
    	}
    	
        // if it wasn't in the buffer pool
    	if (pages.size() == numPages){
    		evictPage(); // evicts LRU page
    	}
    	
    	DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
    	
    	HeapPage newPage = (HeapPage) dbfile.readPage(pid);
    	
    	// These better line up...
    	pages.add(newPage);
    	timeSinceUse.add(System.currentTimeMillis());
    	    	
        return newPage;
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
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
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
    	
    	// 1 element, but insert returns an array...
    	ArrayList<Page> dirtiedPage = f.insertTuple(tid, t);
    	
    	dirtiedPage.get(0).markDirty(true, tid);
    	
    	// updates the pool
    	int i = 0;
    	for (Page p : pages){
    		if (p.getId() == dirtiedPage.get(0).getId()){
    			pages.set(i, dirtiedPage.get(0));
    		}
    		i++;
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
        
    	int tableId = t.getRecordId().getPageId().getTableId();
    	
    	int i = 0;
    	for (Page p : pages){
    		if (p.getId().getTableId() == tableId){
    			// mark dirty
    			p.markDirty(true, tid);
    			pages.remove(t);
    			timeSinceUse.set(i, (long) 0);
    		}
    		i++;
    	}  	
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Page p : pages){
        	flushPage(p.getId());
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
    			toFlush = p;
    		}
    	}
    	
    	// write it to a specific table
    	DbFile table = Database.getCatalog().getDatabaseFile(pid.getTableId());
    	table.writePage(toFlush);
    	toFlush.markDirty(false, new TransactionId());
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {

    	int lruIndex = 0;
    	
    	for (int i = 0; i < pages.size(); i++){
    		if (timeSinceUse.get(i) > timeSinceUse.get(lruIndex)){
    			lruIndex = i;
    		}
    	}
    	
    	// evict page at lruIndex
    	HeapPage hp = (HeapPage) pages.get(lruIndex);
    	
    	try {
    		this.flushPage(hp.getId());
    		pages.remove(hp);
    		System.out.println("Should have flushed page");
    	} catch (IOException e) {
    		throw new DbException("Page wasn't flushed or evicted");
    	}	
    }
}
