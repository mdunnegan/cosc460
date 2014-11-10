package simpledb;

import java.io.*;
import java.security.Permission;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
//import java.util.HashMap;
//import java.util.Map;
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
    private LockManager lockManager;
        
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int np) {
    	pages = new ArrayList<Page>();
    	timeSinceUse = new ArrayList<Long>();
    	numPages = np;
    	lockManager = new LockManager();
    }

    public int getNumberOfPages(){
    	return pages.size();
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

    public Page getPage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException, DbException { 
    	
    	//System.out.println("in getPage");
    	//System.out.println(lockManager.holdslock(pid, tid));
    	
    	for (int i = 0; i < pages.size(); i++){
    		if (pages.get(i).getId().equals(pid)){
    			
    			synchronized (this){
    				
    				System.out.println(lockManager.holdslock(pid, tid)); // keeps saying nobody has the lock...
    				
    				if (!lockManager.holdslock(pid, tid)){ // if the lock manager doesn't hold our value
    					System.out.println("found the page, isn't locked");
    					lockManager.lock(pid, tid, perm);
    					timeSinceUse.set(i, (long) 0);
    					lockManager.unlock(pid, tid);
    					return pages.get(i);
    	    			
    				} else { // if locking returns false, we must wait our turn!
    					try {
    						System.out.println("lock returns false");
							wait();
							return pages.get(i);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
    				}
    			}
    		}
    		//System.out.println("wasnt found");
    	}
    	//System.out.println("uh");
    	
        // if it wasn't in the buffer pool
    	if (pages.size() == numPages){
    		evictPage(); // evicts LRU page
    		// should probably also remove elements from the the timestamp list, and the page array
    	}
    	
    	// get the page from the catalog
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
    	lockManager.unlock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    // does it mean to be called completeTransaction? ..
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {                                                     // cosc460
        return lockManager.holdslock(p, tid);
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
    	HeapFile hf = (HeapFile) f;
    	
    	// 1 element, but insert returns an array...
    	ArrayList<Page> dirtiedPage = f.insertTuple(tid, t);
    	
    	dirtiedPage.get(0).markDirty(true, tid);
    	
    	// updates the pool
    	// Should this be heapfile insert?
//    	int i = 0;
//    	for (Page p : pages){
//    		if (p.getId() == dirtiedPage.get(0).getId()){
//    			//pages.set(i, dirtiedPage.get(0));
//    		}
//    		i++;
//    	}
    	hf.deleteTuple(tid, t);
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
    	DbFile f = Database.getCatalog().getDatabaseFile(tableId);
    	
    	HeapFile hf = (HeapFile) f;
    	System.out.println("Calling HeapFile delete tuple from BP");
    	hf.deleteTuple(tid, t);
    	
    	//int i = 0;
    	for (Page p : pages){
    		
    		if (p.getId().getTableId() == tableId){   			
    			
    			// Heapfile.deleteTuple
    			
    			// mark dirty
    			//hf.deleteTuple(tid, t);
    			p.markDirty(true, tid);
//    			//HeapPage hp = (HeapPage) p;
//    			//hp.deleteTuple(t);
//    			
//    			//hf.deleteTuple(tid, t);
//    			timeSinceUse.set(i, (long) 0);
    		}
//    		i++;
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
    	} catch (IOException e) {
    		throw new DbException("Page wasn't flushed or evicted");
    	}	
    }
    
    public class TransactionStruct{
    	TransactionId transactionId;
    	Permissions lockModeRequested;
    	boolean lockGranted; // 0 no, 1 yes
    }
    
    public class LockManager {
    	
    	ConcurrentHashMap<PageId, LinkedList<ArrayList<TransactionStruct>>> lockTable = new ConcurrentHashMap<PageId, LinkedList<ArrayList<TransactionStruct>>>();	
    	
    	public synchronized void lock(PageId pid, TransactionId tid, Permissions mode){
    		System.out.println("pid hashcode: " + pid.hashCode());
    		ArrayList<TransactionStruct> page = new ArrayList<TransactionStruct>();
    		if (lockTable.contains(pid.hashCode())){ 
    			
    			// iterator over pages in this bucket
    			Iterator<ArrayList<TransactionStruct>> pageIterator = lockTable.get(pid.hashCode()).iterator();
    			
    			while (pageIterator.hasNext()){
    				
    				page = pageIterator.next();
    				TransactionStruct ts = new TransactionStruct();
    				Iterator<TransactionStruct> tsIterator = page.iterator();
    				
    				while (tsIterator.hasNext()){
    					ts = tsIterator.next();
    					if (ts.lockGranted == true){ // later, determine if this is a shared lock
    						TransactionStruct record = new TransactionStruct();
            				record.transactionId = tid;
            				record.lockModeRequested = mode;
            				record.lockGranted = false;
            				page.add(record);
            				return;
    					}
    				}	
    			}
    			// if it got here, there were no transactions that were locking our page
       			TransactionStruct record = new TransactionStruct();
				record.transactionId = tid;
				record.lockModeRequested = mode;
				record.lockGranted = true;
				page.add(record);
				return;
		
    			
//    				if (page.get(pageIndex).transactionId == tid){
//    					return true;
//    				}
    				
    				// determines if there are threads in line for the lock
//    				if (page.size() > 0){ // easier to check the size of an arraylist than use an iterator on a linked list
//    					
//    					// if there are multiple pages, we need to see if anything has the lock
//    					
//    					System.out.println("pg size > 0");
//    					
//    					TransactionStruct record = new TransactionStruct();
//        				record.transactionId = tid;
//        				record.lockModeRequested = mode;
//        				record.lockGranted = false;
//        				page.add(record);
//        				return;
//    				}
//    				
//    				System.out.println("pg size < 0");
//    					
//    				TransactionStruct record = new TransactionStruct();
//    				record.transactionId = tid;
//    				record.lockModeRequested = mode;
//    				record.lockGranted = true;
//    				page.add(record);
//    				return;
    			
    			

    		} else { // if the hashmap doesn't have this bucket	
    			System.out.println("went to else, pid was not in lockmanager");
    			//System.out.println("pidhashcode: " + pid.hashCode());
    			
    			LinkedList<ArrayList<TransactionStruct>> newData = new LinkedList<ArrayList<TransactionStruct>>();
    			
    			TransactionStruct ts = new TransactionStruct();
    			ts.lockGranted = true;
    			ts.lockModeRequested = mode;
    			ts.transactionId = tid;
    			
    			ArrayList<TransactionStruct> array = new ArrayList<TransactionStruct>();
    			array.add(ts);

    			newData.add(array);    			
    			lockTable.put(pid, newData);
    			
    			//System.out.println(lockTable);
    			
    			return;
    		}
    	}
    	
    	public synchronized void unlock(PageId pid, TransactionId tid){
    		if (lockTable.contains(pid.hashCode())){
    			
	    		ArrayList<TransactionStruct> page;
	    		Iterator<ArrayList<TransactionStruct>> pageIterator = lockTable.get(pid.hashCode()).iterator();
	    		
	    		int pageIndex = 0;
	    		while (pageIterator.hasNext()){
    				page = pageIterator.next();    			
    				
    				if (page.size() > 0){
    					for (TransactionStruct t : page){ // i is a 
    						if (t.transactionId == tid){
    							lockTable.get(pid.hashCode()).get(pageIndex).remove(t);
    							
    							// after removing this element, recheck the size of the array and try to start the next thing
    							if (page.size() > 0){
    								BufferPool b = Database.getBufferPool();
    								b.lockManager.lock(pid, tid, Permissions.READ_ONLY); // this is probably wrong 
    							}
    						}	
    					}
    				}
    				pageIndex++;
    				
    				throw new RuntimeException("You tried to unlock a thread that didn't have anything!");
	    		}
    		}	
    	}
    	
    	public synchronized boolean holdslock(PageId pid, TransactionId tid){
    		if (lockTable.contains(pid.hashCode())){
    		
    			ArrayList<TransactionStruct> page;
	    		Iterator<ArrayList<TransactionStruct>> pageIterator = lockTable.get(pid.hashCode()).iterator();
	    			
	    		while (pageIterator.hasNext()){
    				page = pageIterator.next();
    				TransactionStruct ts;
    				
    				// gotta loop through all the transactions of a page
    				
    				Iterator<TransactionStruct> transactionStructIterator = page.iterator();
    				while (transactionStructIterator.hasNext()){
    					ts = transactionStructIterator.next();
    					if (ts.transactionId == tid){
    						return true;
    					}
    				}
    				//return false;
	    		}
    		}
    		return false;	
    	}
    }
}
