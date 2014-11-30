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
    	
    	int pageNumber = pid.pageNumber();
    	int pageSize = BufferPool.PAGE_SIZE;
    	
    	byte[] byteArray = new byte[pageSize];
    	//BufferedInputStream data;
    	    	
    		try {
    			//System.out.println("0");
				BufferedInputStream data = new BufferedInputStream(new FileInputStream(file));
				//System.out.println("1");
				data.skip(pageSize * pageNumber);
	    		//System.out.println("2");
	    		data.read(byteArray);
	    		//System.out.println("3");
	    		HeapPageId hpid = (HeapPageId) pid;
	    		//System.out.println("4");
		    	data.close();
		    	return new HeapPage(hpid, byteArray);
			} catch (FileNotFoundException e) {
				System.err.println("File not found in HeapFile.readPage()");
				e.printStackTrace();
				return null;
			} catch (IOException e) {
				System.err.println("IO error in HeapFile.readPage()");
				e.printStackTrace();
				return null;
			}
    		
    }

    // see DbFile.java for javadocs
    
    public void writePage(Page page) throws IOException {
    	RandomAccessFile raf = new RandomAccessFile(file, "rw");
        int offset = page.getId().pageNumber() * BufferPool.getPageSize();
        raf.skipBytes(offset);
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	return (int) Math.floor(file.length() / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        
    	ArrayList<Page> returnArray = new ArrayList<Page>();
    	BufferPool b = Database.getBufferPool();
    	HeapPage hp = null;
    	    	
    	for (int i = 0; i < numPages(); i++){
    		
			hp = (HeapPage) b.getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
    		if (hp.getNumEmptySlots() > 0){
    			hp.insertTuple(t);
    			writePage(hp);
    			returnArray.add(hp);
    			return returnArray;
    		}
    	}
    	
    	// pages were all full, doing it here
    	hp = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());    	
    	hp.insertTuple(t);    	
    	    	
    	OutputStream output = new BufferedOutputStream(new FileOutputStream(file, true), BufferPool.getPageSize());	
    	output.write(hp.getPageData(), 0, BufferPool.getPageSize());
    	writePage(hp);
    	
    	output.flush();
    	output.close();
    	
    	returnArray.add(hp);
    	return returnArray;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	    	
    	ArrayList<Page> returnArray = new ArrayList<Page>();
    	
    	RecordId rid = t.getRecordId();
    	BufferPool b = Database.getBufferPool();
    	Page p = b.getPage(tid, rid.getPageId(), Permissions.READ_WRITE);
    	HeapPage hp = (HeapPage) p;
    	
    	//System.out.println("Calling HeapPage delete tuple from HF");
    	hp.deleteTuple(t);
    	return returnArray; 
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	return new HeapFileIterator(this, tid);
    }
    
	class HeapFileIterator implements DbFileIterator {

		Iterator<Tuple> tuples;
		private HeapFile hf;
		private int pageNum;
		private TransactionId transactionId;
		private boolean iteratorOpen;
		private BufferPool bp;
		
		public HeapFileIterator(HeapFile hpFile, TransactionId tid){
			hf = hpFile;
			transactionId = tid;
			pageNum = 0;
			iteratorOpen = false;
			bp = Database.getBufferPool();
		}
		
		@Override
		public boolean hasNext() throws TransactionAbortedException, DbException {
			
			//System.out.println("Checking next (of heapfile iterator)...");
			
			if (iteratorOpen == false){
//				throw new TransactionAbortedException();
				return false;
			}
			
			//System.out.println("pageNum:"+pageNum);
			//System.out.println("numPages:"+numPages());
			
			if (tuples.hasNext()){
				//System.out.println("returns true by tuples.hasNext");
				// happening too much! 
				// could be returning true for empty tuples
				return true;
			}
			
			pageNum++;
			HeapPageId hpid = null;
			while (pageNum < numPages()){
				hpid = new HeapPageId(hf.getId(), pageNum);
				HeapPage hp = (HeapPage) bp.getPage(transactionId, hpid, Permissions.READ_ONLY);
				tuples = hp.iterator();

				if (tuples.hasNext()){
					return true;
				}
				pageNum++;
			}
		
			// not releasing here
			return false;	
		}

		@Override
		public Tuple next() throws TransactionAbortedException, DbException {
			if (!iteratorOpen) {
        		throw new NoSuchElementException("Iterator is closed");
        	}
        	if (!hasNext()){
        		throw new NoSuchElementException();
        		
        	}
        	return tuples.next();
		}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			pageNum = 0;
			//System.out.println("1");
			HeapPageId hpid = new HeapPageId(hf.getId(), pageNum);
			System.out.println("In heapfile iterator open, calling getPage");
			
			// ATTENTION: InsertTest is failing at getPage in the following line. There is an exclusive
			// write lock that isn't giving up the lock
			
			
			HeapPage firstPage = (HeapPage) Database.getBufferPool().getPage(transactionId, hpid, Permissions.READ_ONLY);			
			//System.out.println("3");
			tuples = firstPage.iterator();
			//System.out.println("4");
			iteratorOpen = true;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// go to beginning of the whole heapfile
			HeapPageId hpid = new HeapPageId(hf.getId(), pageNum);
			HeapPage firstPage = (HeapPage) Database.getBufferPool().getPage(transactionId, hpid, Permissions.READ_ONLY);
			pageNum = 0;
			tuples = firstPage.iterator();
		}

		@Override
		public void close() {
			iteratorOpen = false;
		}
	}	
}

