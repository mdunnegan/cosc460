package simpledb;

/**
 * Unique identifier for HeapPage objects.
 */
public class HeapPageId implements PageId {
	
	private int tId;
	private int pNum;
    /**
     * Constructor. Create a page id structure for a specific page of a 
     * specific table. // map of tables
     *
     * @param tableId The table that is being referenced
     * @param pgNo    The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
    	
    	if (tableId < 0) { // or tableId DNE
    		throw new RuntimeException("Table Id is invalid");
    	} if (pgNo < 0) { // or pgNo DNE
    		throw new RuntimeException("Page Number is invalid");
    	}
    	
    	tId = tableId;
    	pNum = pgNo;
    	
    	// make a new DbIterator
    	
    	//private int pageId = new int;
    	// Get the table
    	// Get the page
    	// validate them
    	// find
    	
    }

    /**
     * @return the table associated with this PageId
     */
    public int getTableId() {
        return tId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     * this PageId
     */
    public int pageNumber() {
        return pNum;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     * the table number and the page number (needed if a PageId is used as a
     * key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
    	String code = new String();
    	code = tId + " " + pNum;
        return code.hashCode();
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     * ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here
    	if (o instanceof HeapPageId){
    		HeapPageId obj = (HeapPageId) o; // attempt at typecasting
    		if (obj.pageNumber() == pNum && obj.pNum == tId){
    			return true;
    		}
    	}
        return false;
    }

    /**
     * Return a representation of this object as an array of
     * integers, for writing to disk.  Size of returned array must contain
     * number of integers that corresponds to number of args to one of the
     * constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }

}
