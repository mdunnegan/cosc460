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
    public Page readPage(PageId pid) throws IllegalArgumentException {
    	
     	BufferPool b = Database.getBufferPool();
    	int pageNumber = pid.pageNumber();
    	int pageSize = b.PAGE_SIZE;
    	HeapPage heapPage = null;
    	
    	byte[] byteArray = new byte[pageSize];
    	BufferedInputStream data = null;
    	    	
    	try {
    		data = new BufferedInputStream(new FileInputStream(file));
    		data.skip(pageSize * pageNumber);
    		data.read(byteArray);
    		HeapPageId hpid = (HeapPageId) pid;
	    	heapPage = new HeapPage(hpid, byteArray); 
	    	data.close();
    	} catch (IOException e) {
    		System.err.println("IO Error");
    	}
        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	return pages.length;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	return new HeapFileIterator(tid);
    }
    
    // Iterates through tuples
	class HeapFileIterator implements DbFileIterator {

		Iterator<Tuple> tuples;
		//Iterator<HeapPage> heapPages;
		private int pageNum;
		private int numPages;
		private TransactionId transactionId;
		private boolean iteratorOpen;
		
		public HeapFileIterator(TransactionId tid){
			pageNum = 0; 
			numPages = numPages();
			transactionId = tid;
			iteratorOpen = false;
		}
		
		@Override
		public boolean hasNext() {
			if (!iteratorOpen){
				throw new RuntimeException("Iterator is not open");
			}
			if (pageNum <= numPages){
				return true;
			}
			return false;
		}

		@Override
		public Tuple next() {
			if (!iteratorOpen){
				throw new RuntimeException("Iterator is not open");
			}
			pageNum++;
			if (tuples.hasNext()){
				return tuples.next();
			}
			return null;
		}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			if (iteratorOpen){
				System.err.println("Iterator is already open");
				throw new TransactionAbortedException();	
			}
			iteratorOpen = true;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// go to beginning of iterator and keep it open
			pageNum = 0;
		}

		@Override
		public void close() {
			iteratorOpen = false;
		}
	}
    	
}

