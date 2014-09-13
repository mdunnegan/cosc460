package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        //The type of the field
        public final Type fieldType;

        //The name of the field
        public final String fieldName;

        public TDItem(Type t, String n) {
        	this.fieldType = t;
            this.fieldName = n;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }// end of TDItem class

    private static final long serialVersionUID = 1L;
    private TDItem[] schema;
    
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	if (typeAr.length != fieldAr.length) {
    		throw new RuntimeException("TypeAr and FieldAr must be the same length");
    	}
    	schema = new TDItem[typeAr.length];
    	for (int i = 0; i < typeAr.length; i++){
    		TDItem newItem = new TDItem(typeAr[i], fieldAr[i]);
    		schema[i] = newItem;
    	}
    }
    
    public TDItem[] Schema(){
    	return schema;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	schema = new TDItem[typeAr.length];
    	for (int i = 0; i < typeAr.length; i++){
    		TDItem newItem = new TDItem(typeAr[i], null);
    		schema[i] = newItem;
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return schema.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        try {
        	String fieldName = schema[i].fieldName; // 0 is type, 1 is field name
        	return fieldName;
        }
        catch (NoSuchElementException error){
        	System.out.println("No such field: " + error.getMessage());
        }
        return null;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        try {
        	Type type = schema[i].fieldType;
        	return type;
        } 
        catch (NoSuchElementException error) {
        	System.out.println("No such field: " + error.getMessage());
        }
        return null;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        try {
        	for (int i = 0; i < schema.length; i++){
            	if (schema[i].fieldName == name){
            		return i;
            	}
            }
        }
        catch (NoSuchElementException error){
        	System.out.println("No such field: " + error.getMessage());
        }
        return 0;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int size = 0;
    	for (TDItem tdi : schema){
    		size += tdi.fieldType.getLen();
    	}
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	int td1len = td1.numFields();
    	int td2len = td2.numFields();
    	
    	Type[] types = new Type[td1len + td2len];
    	String[] names = new String[td1len + td2len];
    	
        for (int i = 0; i < td1len; i++){
        	//schema[i] = td1.schema[i];
        	types[i] = td1.getFieldType(i);
        	names[i] = td1.getFieldName(i);
        }
        for (int j = 0; j < td2len; j++){
        	types[j+td1len] = td2.getFieldType(j);
        	types[j+td1len] = td2.getFieldType(j);
        }
        return new TupleDesc(types, names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	if (o == null){
    		return false;
    	}
    	if (o.getClass() != schema.getClass()){
    		return false;
    	}
    	
    	TupleDesc obj = (TupleDesc) o;
    	if (obj.numFields() != schema.length) {
			return false;
		}
    	
        for (int i = 0; i < schema.length; i++){
        	if (obj.schema[i].fieldType != schema[i].fieldType){
        		return false;
        	}
        }  
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
    	// what
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldName[0](fieldType[0]), ..., fieldName[M](fieldType[M])"
     *
     * @return String describing this descriptor.
     */
    public String toString() {
    	String tupDescString = "";
        for (int i = 0; i < schema.length; i++){
        	
        	tupDescString += schema[i].fieldName.toString();
        	tupDescString += "(";
        	tupDescString += schema[i].fieldType.toString();
        	tupDescString += ")";
        	tupDescString += ", ";
        	
        }
        return tupDescString;
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
    	// Arrays as List(pass in array).iterator();
    	List<TDItem> itemList = Arrays.asList(schema);
        return itemList.iterator();
    }
}
