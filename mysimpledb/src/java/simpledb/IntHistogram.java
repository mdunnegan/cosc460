package simpledb;

import simpledb.Predicate.Op;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	
	int[] histogram;
	int bucketWidth;
	int totalNumValues; 
	int buckets;
	int min;
	int max;
    /**
     * Create a new IntHistogram.
     * <p/>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p/>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p/>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.histogram = new int[buckets];
        this.bucketWidth = (max - min) / buckets;
        this.totalNumValues = 0;
     }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if (v < min || v > max){
        	throw new RuntimeException("Value too large");
        }
        
        histogram[(v - min)/buckets]++;
        totalNumValues++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p/>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	
    	// the selectivity of the expression is roughly (h / w) / ntups for EQ
    	
    	// h = nTups
    	// w = bucketWidth
    	// n = numTups
    
    	int vBucket = histogram[(v - min)/buckets];
    	
    	double EQ = (histogram[vBucket] / bucketWidth) / totalNumValues;
    	
    	// Greater Than
    	int bRightGT = min + (vBucket*bucketWidth);
    	int remainingBucketSelectivityGT = 0;
    	for (int i = 0; i < histogram.length - vBucket; i++){
    		remainingBucketSelectivityGT += histogram[vBucket+i] / totalNumValues;
    	} 	
    	double GT = (bRightGT - 1 - v) + remainingBucketSelectivityGT;
    	//
    
    	// Less Than
    	int bRightLT = min + (vBucket*bucketWidth);
    	int remainingBucketSelectivityLT = 0;
    	for (int i = vBucket; i < min; i--){
    		remainingBucketSelectivityLT += histogram[vBucket+i] / totalNumValues;
    	} 	
    	double LT = (bRightLT - 1 - v) + remainingBucketSelectivityLT;
    	//	
    		
    	if (op.equals(Op.EQUALS) || op.equals(Op.LIKE)){
    		return EQ;
    	} else if (op.equals(Op.GREATER_THAN)){
    		return GT;
    	} else if (op.equals(Op.LESS_THAN)){
    		return LT;
    	} else if (op.equals(Op.GREATER_THAN_OR_EQ)){
    		return GT+EQ;
    	} else if (op.equals(Op.LESS_THAN_OR_EQ)){
    		return LT+EQ;	
    	} else if (op.equals(Op.NOT_EQUALS)){ 
    		return totalNumValues-EQ;
    	}
    	
        return -1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {  
    	String myStr = "histogram: " + histogram.toString() + "\n bucket width: " + bucketWidth + "\n max: " + max + "\n min: " + min;
        return myStr;
    }
}
