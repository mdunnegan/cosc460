package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    TransactionId trid;
    DbIterator child;
    boolean open;
    int numTimesFetchNextCalled;
    
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
    	this.trid = t;
        this.child = child;
        this.open = false;    
        numTimesFetchNextCalled = 0;
    }

    public TupleDesc getTupleDesc() {
    	Type typ[] = {Type.INT_TYPE};
    	String title[] = {"Number of Tuples Deleted"};
    	TupleDesc td = new TupleDesc(typ, title);
    	return td;
    }

    public void open() throws DbException, TransactionAbortedException {
    	super.open();
        child.open();
        open = true;
    }

    public void close() {
        child.close();
        open = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        
    	if (numTimesFetchNextCalled > 0){
    		return null;
    	}
    	numTimesFetchNextCalled++;
    	
    	BufferPool b = Database.getBufferPool();
    	Tuple t = new Tuple(getTupleDesc());
    	int numTuplesDeleted = 0;
    	
    	while (child.hasNext()){
    		
    		t = child.next();
    		
    		try {
    			//System.out.println("Trying to Delete in Delete!");
    			//System.out.println("before: " + b.getNumberOfPages());
				b.deleteTuple(trid, t);
				//System.out.println("after: " + b.getNumberOfPages());
				numTuplesDeleted++;
			} catch (IOException e) {
				throw new TransactionAbortedException();
			}
    	}
    	
    	// this exact thing gets called in getTupleDesc()
    	Type typ[] = {Type.INT_TYPE};
    	String title[] = {"Number of Tuples Inserted"};
    	TupleDesc td = new TupleDesc(typ, title);
    	// the following is not called in getTupleDesc()
 
    	Tuple tup = new Tuple(td);
    	IntField f = new IntField(numTuplesDeleted);    	
    	tup.setField(0, f);

        return tup;    	
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    }

}
