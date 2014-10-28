package simpledb;

import simpledb.Predicate.Op;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	
	int[] histogram;
	int bucketWidth;
	int totalNumValues; 
	int numBuckets;
	int lastBucketWidth;
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
        this.min = min;
        this.max = max;
        this.totalNumValues = 0;
        this.bucketWidth = (int) Math.ceil((double)buckets / (max-min+1));
        
        if ((max - min + 1) < buckets){ 
        	// resize
        	bucketWidth = 1;
        	numBuckets = max - min;
        	lastBucketWidth = 1;
        } else {
        	// no resize necessary
        	numBuckets = buckets;
        	lastBucketWidth = max - (min + (bucketWidth * (numBuckets-1))) + 1;
        }     
        this.histogram = new int[numBuckets+1];
     }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if (v < min || v > max){
        	throw new RuntimeException("Value out of range");
        }
        
        if ((v-min)/bucketWidth < histogram.length) {
        	histogram[(v-min)/bucketWidth]++;
        } else {
        	histogram[histogram.length-1]++;
        }
        
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
    	    	
    	if (v < min && (op.equals(Op.GREATER_THAN_OR_EQ) || op.equals(Op.GREATER_THAN))){
    		return 1;
    	}
    	if (v > max && (op.equals(Op.GREATER_THAN_OR_EQ) || op.equals(Op.GREATER_THAN))){
    		return 0;
    	}
    	if (v < min && (op.equals(Op.LESS_THAN_OR_EQ) || op.equals(Op.LESS_THAN))){
    		return 0;
    	}
    	if (v > max && (op.equals(Op.LESS_THAN_OR_EQ) || op.equals(Op.LESS_THAN))){
    		return 1;
    	}
    	
    	// Negative range test params
    	//(10, -60, -10)
    	// adds -60 thru -10
    	
    	int vBucket = (v - min)/bucketWidth;
    	//System.out.println("vBucket: " + vBucket);
    	double EQ = (histogram[vBucket] / (double) bucketWidth) / totalNumValues;
    	      	
    	// Greater Than
    	int bRightGT;
    	if (vBucket == histogram.length - 1){
    		bRightGT = max + 1;
    	} else {
    		bRightGT = min + ((vBucket)*bucketWidth);
    	}
    	
    	double b_f = histogram[vBucket] / totalNumValues;
    	double b_part = (bRightGT - 1 - v) / bucketWidth;
    	double fractionalSelectivityGT = b_f * b_part;
    	double totalBucketSelectivity = fractionalSelectivityGT;
    	
    	for (int i = vBucket+1; i < histogram.length; i++){
    		// if it's the last bucket
    		if (i == histogram.length - 1){
    			totalBucketSelectivity += (histogram[i] / lastBucketWidth) / (double) totalNumValues;
    		} else {
    			totalBucketSelectivity += (histogram[i] / bucketWidth) / (double) totalNumValues;
    		}
    	}
    	double GT = totalBucketSelectivity;
    	// end Greater Than
    	// Less Than
    	int bLeftLT;
    	if (vBucket == histogram.length - 1){ // last bucket
    		bLeftLT = max;
    	} else {
    		bLeftLT = min + ((vBucket-1)*bucketWidth);
    	}
    	
    	double b_f2 = histogram[vBucket] / totalNumValues;
    	double b_part2 = (bLeftLT - 1 - v) / bucketWidth;
    	double fractionalSelectivityLT = b_f2 * b_part2;
    	double totalBucketSelectivity2 = fractionalSelectivityLT;
    	
    	for (int i = vBucket-1; i >= 0; i--){
    		// if it's the last bucket
    		if (i == histogram.length - 1){
    			totalBucketSelectivity2 += (histogram[i] / lastBucketWidth) / (double) totalNumValues;
    		} else {
    			totalBucketSelectivity2 += (histogram[i] / bucketWidth) / (double) totalNumValues;
    		}
    	}
    	
    	double LT = totalBucketSelectivity2;
       		
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
    		return 1-EQ;
    	}
    	
        return -1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
    	
    	String histStr = "";
    	for (int i = 0; i < histogram.length; i++){
    		histStr+=histogram[i];
    		histStr += ",";
    	}
    	
    	String myStr = "histogram: " + histStr + "\n bucket width: " + bucketWidth + "\n max: " + max + "\n min: " + min;
        return myStr;
    }
}
