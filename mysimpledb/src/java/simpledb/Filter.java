package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate p;
    private DbIterator child;
    private boolean open;
    
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.p = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        return p;
    }

    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	open = true;
        child.open();
        super.open();
    }

    public void close() {
    	open = false;
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	if (!open){
    		throw new DbException("Iterator closed");
    	}
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     * more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        
    	if (!open){
    		throw new DbException("Iterator closed");
    	}
    	
        while(child.hasNext()){
        	Tuple t = child.next();
        	if (p.filter(t)){
        		return t;
        	}
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // return an iterator for each child?
    	DbIterator[] childIterator = {child};
        return childIterator;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child = children[0];
    }

}
