package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId trId;
    private int tableId;
    private String tAlias;
    private DbFile dbTable;
    private DbFileIterator iterator;
    
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
    	trId = tid;
    	tableId = tableid;
    	tAlias = tableAlias;
    	dbTable = Database.getCatalog().getDatabaseFile(tableid);
    	iterator = dbTable.iterator(trId);
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }
    
    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        return tAlias;
    }

    public void open() throws DbException, TransactionAbortedException {
        iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     * TupleDesc (type, tableAlias-tableName)
     */
    public TupleDesc getTupleDesc() {
    	
    	TupleDesc td= Database.getCatalog().getTupleDesc(this.tableId);
        
        Iterator<TupleDesc.TDItem> tdiIterator = td.iterator();
        Type[] types = new Type[td.numFields()];
        String[] fields = new String[td.numFields()];
        
        int i=0;
        while (tdiIterator.hasNext()) {
        	TupleDesc.TDItem item = tdiIterator.next();
        	types[i] = item.fieldType;
        	fields[i] = tAlias + "." + item.fieldName;
        	i++;
        }  
        return new TupleDesc(types, fields);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
        return iterator.next();
    }

    public void close() {
        iterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
    	iterator.rewind();
    }
}
