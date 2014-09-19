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
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

	private HeapPage[] pages;
	private File file;
	private TupleDesc tupleDesc;
    /**
     * Constructs a heap file backed by the specified file.
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
    	file = f;
    	tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere TO (doesn't anyone proofread these things) ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    // not passing unit test
    public Page readPage(PageId pid) throws IllegalArgumentException {
    	
    	int pageNumber = pid.pageNumber();
    	int pageSize = BufferPool.PAGE_SIZE;
    	
    	byte[] byteArray = new byte[pageSize];
    	BufferedInputStream data = null;
    	    	
    	try {
    		data = new BufferedInputStream(new FileInputStream(file));
    		data.skip(pageSize * pageNumber);
    		data.read(byteArray);
    		HeapPageId hpid = (HeapPageId) pid;
	    	data.close();
	    	return new HeapPage(hpid, byteArray);
    	} catch (IOException e) {
    		System.err.println("IO Error");
    		return null;
    	}
    }

    // see DbFile.java for javadocs
    
    // Write a page to the disk. Probably some buffer pool stuff
    public void writePage(Page page) throws IOException {
        // find a page that isn't full
    	
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	return (int) Math.floor(file.length() / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    
    // Add a tuple to a page
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        
    	// There are a lot of requirements to this
    	
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	    	
    	ArrayList<Page> returnArray = new ArrayList<Page>();
    	
    	RecordId rid = t.getRecordId();
    	BufferPool b = Database.getBufferPool();
    	
    	Page p = b.getPage(tid, rid.getPageId(), null);
    	
    	// I feel weird casting this
    	HeapPage hp = (HeapPage) p;
    	
    	Iterator<Tuple> pageIterator = hp.iterator();
    	
    	while (pageIterator.hasNext()){
    		if (pageIterator.next() == t){
    			hp.deleteTuple(t);
    		}
    	}
    	    
    	return returnArray; 
    	
    	// for each page in heapfile
//    	for (int i = 0; i < BufferPool.DEFAULT_PAGES; i++){
//    		
//    		// for each tuple in heappage
//    		Iterator<Tuple> tupleIterator = BufferPool.getPage(tid, pages[i].getId()).iterator();
//    		
//    		while (tupleIterator.hasNext()){
//    			if (tupleIterator.next().equals(t)){
//    				
//    				returnArray.add(pages[i]);
//    			}
//    			throw new DbException("The tuple was found, but not deleted. Developer error");
//    		}
//    	}

        // return an ArrayList of Pages that were modified
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	return new HeapFileIterator(this, tid);
    }
    
    // Iterates through tuples
	class HeapFileIterator implements DbFileIterator {

		Iterator<Tuple> tuples;
		private HeapFile hf;
		private int pageNum;
		private int numPages;
		private TransactionId transactionId;
		private boolean iteratorOpen;
		
		public HeapFileIterator(HeapFile hpFile, TransactionId tid){
			hf = hpFile;
			transactionId = tid;
			pageNum = 0;
			numPages = hf.numPages();
			iteratorOpen = false;
		}
		
		@Override
		public boolean hasNext() {
			if (!iteratorOpen){
				return false;
			}
			if (pageNum <= numPages){
				return true;
			}
			return false;
		}

		@Override
		public Tuple next() throws TransactionAbortedException, DbException {
			if (!iteratorOpen) {
        		throw new NoSuchElementException("Iterator is closed");
        	}
        	if (tuples.hasNext()){
        		return this.tuples.next();
        	}
        	throw new NoSuchElementException("No more tuples in file.");
		}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			if (iteratorOpen){
				System.err.println("Iterator is already open");
				throw new TransactionAbortedException();	
			}
			
			HeapPageId firstPageId = new HeapPageId(hf.getId(), 0);
			HeapPage firstPage = (HeapPage) Database.getBufferPool().getPage(transactionId, firstPageId, Permissions.READ_ONLY);
			tuples = firstPage.iterator();
			iteratorOpen = true;
			pageNum = 0;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// go to beginning of the whole heapfile
			HeapPageId firstPageId = new HeapPageId(hf.getId(), 0);
			HeapPage firstPage = (HeapPage) Database.getBufferPool().getPage(transactionId, firstPageId, null);
			pageNum = 0;
			tuples = firstPage.iterator();
		}

		@Override
		public void close() {
			iteratorOpen = false;
		}
	}	
}

