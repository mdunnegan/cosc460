package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private Field[] fields;
	private TupleDesc tupleDesc;
	private RecordId recordID;
	
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) throws Exception{
    	if (td == null){
    		throw new Exception("Parameter is null, should be type TupleDesc");
    	}
    	fields = new Field[td.numFields()];
    	tupleDesc = td; // shoots it to the global variable
    	recordID = null;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
    	return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     * be null.
     */
    public RecordId getRecordId() {
        return recordID;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) throws Exception{
    	if (rid == null){
    		throw new Exception("Record ID is null");
    	}
        recordID = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) throws Exception{
    	if (fields.length < i || i < 0){
    		throw new Exception("Index does not exist");
    	}
    	Type fieldType = tupleDesc.getFieldType(i);
    	if (tupleDesc.getFieldType(i) != f.getType()){
    		throw new Exception("Invalid field type");
    	}
        fields[i] = f;
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
    	if (i < fields.length || i < 0) {
    		return null;
    	}
    	return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p/>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p/>
     * where \t is any whitespace, except newline
     */
    public String toString() {
    	String contents = "";
        for (int i = 0; i < fields.length; i++){
        	contents += fields[i];
        	contents += "\t";
        }
        // what
        //throw new UnsupportedOperationException("Implement this");
        return contents;
        
    }

}
