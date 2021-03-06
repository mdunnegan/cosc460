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
	private int[] numDistinct;
	private TupleDesc td;
	private int numTups;
	private Object[] histograms;
	private int IoCostPerPage;
	private Tuple maxes;
	private Tuple mins;
	ArrayList<HashSet<Field>> distinctVals;

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
    	
    	f = Database.getCatalog().getDatabaseFile(tableid);
    	IoCostPerPage = ioCostPerPage;
    	TransactionId tid = new TransactionId();
    	SeqScan iterator = new SeqScan(tid, tableid);
    	td = f.getTupleDesc();
    	this.numDistinct = new int[td.numFields()];
    	
    	//System.out.println("td.numFields: " + td.numFields());
    	    	
    	// http://stackoverflow.com/questions/19013855/java-avoid-inserting-duplicate-in-arraylist
    	distinctVals = new ArrayList<HashSet<Field>>();
    	
    	maxes = new Tuple(td); // tuple for max values
    	mins = new Tuple(td);  // surprisingly a tuple for minimum values
    	
    	int tupleNum = 0;
    	
    	Tuple t;
    	// compute min and max for each field
    	try {
    		iterator.open();	

    		t = iterator.next();
    		// Populate max and min tuples
    		for(int i = 0; i < td.numFields(); i++) {
    			//System.out.println(i);
				mins.setField(i, t.getField(i));
				maxes.setField(i, t.getField(i));
			}
    		    		
    		iterator.rewind();
    		
			while (iterator.hasNext()){ // for each tuple
				t = iterator.next();
				
				for (int i = 0; i < td.numFields(); i++){ // for each column
					
					if (t.getField(i).compare(Op.GREATER_THAN, maxes.getField(i))){
						maxes.setField(i, t.getField(i));
					}
					if (t.getField(i).compare(Op.LESS_THAN, mins.getField(i))){
						mins.setField(i, t.getField(i));
					}
					
				}
				tupleNum++;
			}
		} catch (DbException | TransactionAbortedException e) {
			e.printStackTrace();
		}
    	    	    	
    	// Make empty histograms for each field
    	histograms = new Object[td.numFields()];
    	for(int i=0; i < td.numFields(); i++) {
			if(td.getFieldType(i).equals(Type.INT_TYPE)) {
				int max = ((IntField) maxes.getField(i)).getValue();
    			int min = ((IntField) mins.getField(i)).getValue();
		    	histograms[i] = new IntHistogram(NUM_HIST_BINS, min, max);
			} else {
				histograms[i] = new StringHistogram(NUM_HIST_BINS);
			}
			
			// one set of distincts per histogram
			distinctVals.add(new HashSet<Field>());
			
		}
    	
    	// populate each histogram
    	numTups = tupleNum;
    	//tupleNum = 0;
    	try {
    		iterator.rewind();
			while (iterator.hasNext()){
				t = iterator.next();
				for (int i = 0; i < td.numFields(); i++){
					if (td.getFieldType(i).equals(Type.INT_TYPE)){
						// adding values takes an integer
						int value = ((IntField)t.getField(i)).getValue();
						((IntHistogram) histograms[i]).addValue(value);
					} else {
						String value = ((StringField)t.getField(i)).getValue();
						((StringHistogram) histograms[i]).addValue(value);
					}
					distinctVals.get(i).add(t.getField(i));
				}
				tupleNum++;
			}
	
		} catch (DbException | TransactionAbortedException e) {
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
    	return ((HeapFile)f).numPages()*IoCostPerPage;
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
    	
    	//System.out.println(distinctVals);
        return distinctVals.get(field).size();
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
    		
    		IntHistogram hist = (IntHistogram) histograms[field];
    		return hist.estimateSelectivity(op, c.getValue());
    	} else {
    		StringField c = (StringField) constant;
    		StringHistogram hist = (StringHistogram) histograms[field];
    		return hist.estimateSelectivity(op, c.getValue());
    	} 
    }
}
