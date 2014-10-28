package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.Predicate.Op;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p/>
 * This class is not needed in implementing lab1|lab2|lab3.                                                   // cosc460
 */
public class TableStats {
	
	private DbFile f;
	private ArrayList<ArrayList<Field>> allFields;
	private ArrayList<Field> minValues;
	private ArrayList<Field> maxValues;
	private ArrayList<Integer> distinctValues;
	private TupleDesc td;
	private int numPages;
	private int numTups;

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(HashMap<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    	
    	f = Database.getCatalog().getDatabaseFile(tableid);
    	TransactionId tid = new TransactionId();
    	DbFileIterator iterator = f.iterator(tid);
    	
    	td = f.getTupleDesc();
    	
    	// Make an arraylist for each column
    	allFields = new ArrayList<ArrayList<Field>>();
    	
    	Tuple t;
    	int tupNum = 0;
    	try {
    		iterator.open();
			while (iterator.hasNext()){ // for each tuple
				t = iterator.next();
				
				for (int i = 0; i < td.getSize(); i++){
					allFields.get(tupNum).set(i, t.getField(i));
					
				}
				tupNum++;
			}
		} catch (DbException | TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	numTups = tupNum;
    	
    	try {
			iterator.rewind();
		} catch (DbException | TransactionAbortedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	
    	minValues = new ArrayList<Field>();
    	maxValues = new ArrayList<Field>();
    	
    	    	
    	// Initialize min and max
    	minValues.add(allFields.get(0).get(0));
    	maxValues.add(allFields.get(0).get(0));
    	
    	// for each column, get the max and min values
    	// Also keep track of the number of distinct values
    	tupNum = 0;
    	try {
    		for (int j = 0; j < td.getSize(); j++){ // for each column
    			while (iterator.hasNext()){ // for each tuple
    				t = iterator.next();
    				if (allFields.get(j).get(tupNum).compare(Op.GREATER_THAN, maxValues.get(j))){
    					maxValues.set(j, allFields.get(j).get(tupNum));
    				}
    				if (allFields.get(j).get(tupNum).compare(Op.LESS_THAN, minValues.get(j))){
    					minValues.set(j, allFields.get(j).get(tupNum));
    				}	
    			    					
    				tupNum++;	
    			}
			}
			
		} catch (DbException | TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	distinctValues = new ArrayList<Integer>();
    	for (int i = 0; i < td.getSize(); i++){
    		distinctValues.set(i, numTups);
    	}
    	
    	try {
    		for (int j = 0; j < td.getSize(); j++){ // for each column
    			while (iterator.hasNext()){ // for each tuple
    				t = iterator.next();
    				for (int i = 0; i < td.getSize(); i++){
    					if (allFields.get(j).get(tupNum) == allFields.get(1).get(tupNum)){
    						distinctValues.set(distinctValues.get(j), distinctValues.get(j)-1);
    					}
    				}
    			}
			}
			
		} catch (DbException | TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p/>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        int numPages = ((HeapFile) f).numPages();
        return numPages * TableStats.IOCOSTPERPAGE;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
    	if(selectivityFactor > 0 && selectivityFactor < (1/numTups)){
    		return 1;    		
    	}
    	return (int) Math.ceil(numTups * selectivityFactor);
    }

    /**
     * This method returns the number of distinct values for a given field.
     * If the field is a primary key of the table, then the number of distinct
     * values is equal to the number of tuples.  If the field is not a primary key
     * then this must be explicitly calculated.  Note: these calculations should
     * be done once in the constructor and not each time this method is called. In
     * addition, it should only require space linear in the number of distinct values
     * which may be much less than the number of values.
     *
     * @param field the index of the field
     * @return The number of distinct values of the field.
     */
    public int numDistinctValues(int field) {
       return distinctValues.get(field);
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
            	
    	if (constant.getType() == Type.INT_TYPE){
    		IntField c = (IntField) constant;
    		IntField max = (IntField) maxValues.get(field);
    		IntField min = (IntField) minValues.get(field);	
    		IntHistogram h = new IntHistogram(NUM_HIST_BINS, min.getValue(), max.getValue());
    		return h.estimateSelectivity(op, c.getValue());
    	} else {
    		StringField c = (StringField) constant;
    		StringHistogram h = new StringHistogram(NUM_HIST_BINS);
    		return h.estimateSelectivity(op, c.getValue());
    	} 
    }
}
