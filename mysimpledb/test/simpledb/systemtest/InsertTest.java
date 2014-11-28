package simpledb.systemtest;

import java.io.IOException;
import java.util.ArrayList;

import simpledb.*;

import static org.junit.Assert.*;

import org.junit.Test;

public class InsertTest extends SimpleDbTestBase {
	
    private void validateInsert(int columns, int sourceRows, int destinationRows)
            throws DbException, IOException, TransactionAbortedException {
    	
        // Create the two tables
        ArrayList<ArrayList<Integer>> sourceTuples = new ArrayList<ArrayList<Integer>>();
        HeapFile source = SystemTestUtil.createRandomHeapFile(columns, sourceRows, null, sourceTuples);
        assert sourceTuples.size() == sourceRows;
        
        ArrayList<ArrayList<Integer>> destinationTuples = new ArrayList<ArrayList<Integer>>();
        HeapFile destination = SystemTestUtil.createRandomHeapFile(columns, destinationRows, null, destinationTuples);
        assert destinationTuples.size() == destinationRows;

        // Insert source into destination
        TransactionId tid = new TransactionId();
        SeqScan ss = new SeqScan(tid, source.getId(), "");
        
        Insert insOp = new Insert(tid, ss, destination.getId());

//        Query q = new Query(insOp, tid);
        insOp.open();
        boolean hasResult = false;
        while (insOp.hasNext()) {
        	//System.out.println("wahoo");
            Tuple tup = insOp.next();
            assertFalse(hasResult);
            hasResult = true;
            //System.out.println("point 1 in loop");
            assertEquals(SystemTestUtil.SINGLE_INT_DESCRIPTOR, tup.getTupleDesc());
            //System.out.println("point 2 in loop");
            assertEquals(sourceRows, ((IntField) tup.getField(0)).getValue());
            //System.out.println("point 3 in loop");
        }
        assertTrue(hasResult);
        insOp.close();

        //System.out.println("checkpoint 1");
        // As part of the same transaction, scan the table
        sourceTuples.addAll(destinationTuples);
        
        //System.out.println("destination:\t" + destination);
        //System.out.println("tid:\t" + tid);
        //System.out.println("sourceTuples:\t" + sourceTuples); 

        //System.out.println("point 2 outside loop");

        // As part of a different transaction, scan the table
        Database.getBufferPool().transactionComplete(tid);
        //System.out.println("point 3 outside loop");
        Database.getBufferPool().flushAllPages();
        //System.out.println("point 4 outside loop");
        
        
        // what is destination? what is sourcetuples?
        // what are their values?
        //System.out.println("destination:\t"+destination.toString());
        //System.out.println("sourceTuples:\t"+sourceTuples.toString());
        
        // a file, and a tuple 
        
        SystemTestUtil.matchTuples(destination, sourceTuples); // fails here...
        //System.out.println("finally finished..");
    }

    @Test
    public void testEmptyToEmpty()
            throws IOException, DbException, TransactionAbortedException {
        validateInsert(3, 0, 0);
    }

    @Test
    public void testEmptyToOne()
            throws IOException, DbException, TransactionAbortedException {
        validateInsert(8, 0, 1);
    }

    @Test
    public void testOneToEmpty()
            throws IOException, DbException, TransactionAbortedException {
        validateInsert(3, 1, 0);
    }

    @Test
    public void testOneToOne()
            throws IOException, DbException, TransactionAbortedException {
        validateInsert(1, 1, 1);
    }

    /**
     * Make test compatible with older version of ant.
     */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(InsertTest.class);
    }
}
