package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    TransactionId trid;
    DbIterator child;
    int tableid;
    boolean open;
    int numTimesFetchNextCalled = 0;
    
    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        this.trid = t;
        this.child = child;
        this.tableid = tableid;
        this.open = false;
    }

    public TupleDesc getTupleDesc() {
    	// copied from part of fetchNext()
    	Type typ[] = {Type.INT_TYPE};
    	String title[] = {"Number of Tuples Inserted"};
    	TupleDesc td = new TupleDesc(typ, title);
    	return td;
    }

    public void open() throws DbException, TransactionAbortedException {
    	super.open();
        child.open();
        open = true;
    }

    public void close() {
        // some code goes here
    	open = false;
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        
    	if (numTimesFetchNextCalled > 0) {
    		return null;
    	}
    	numTimesFetchNextCalled++;
    	
    	BufferPool b = Database.getBufferPool();
    	Tuple t = new Tuple(getTupleDesc());
    	int numTuplesInserted = 0;
    	
    	while (child.hasNext()){
    		
    		t = child.next();
    		
    		try {
				b.insertTuple(trid, tableid, t);
				numTuplesInserted++;
			} catch (IOException e) {
				throw new TransactionAbortedException();
			}
    	}
    		
    	// this exact block gets called in getTupleDesc()
    	Type typ[] = {Type.INT_TYPE};
    	String title[] = {"Number of Tuples Inserted"};
    	TupleDesc td = new TupleDesc(typ, title);
    	// end
 
    	Tuple tup = new Tuple(td);
    	IntField f = new IntField(numTuplesInserted);    	
    	tup.setField(0, f);

        return tup;
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator[] ret = {child};
        return ret;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }
}
