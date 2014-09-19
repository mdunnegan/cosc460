package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	private static final long serialVersionUID = 1L;
    private TDItem[] schema;
    
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
    	if (i > schema.length -1 || i < 0){
    		throw new NoSuchElementException("Field out of bounds");
    	} else {
    		return schema[i].fieldName;
    	}   
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
    	if (i > schema.length){
    		throw new NoSuchElementException("Field out of bounds");
    	}
        return schema[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	for (int i=0;i<schema.length;i++) {
    		// calling .equals on something null is causing the error
     		if (schema[i].fieldName == null){
    			continue;
    		}
        	if (schema[i].fieldName.equals(name)) {
        		return i;
        	}
        }
    	// It's catching an IndexOutOfBoundsException. No idea why. 
        throw new NoSuchElementException("Tuple was not found");
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
    	
    	Type[] types = new Type[td1.numFields()+td2.numFields()];
        String[] names = new String[td1.numFields()+td2.numFields()];
        
        for(int i=0;i<td1.numFields();i++) {
        	types[i] = td1.getFieldType(i);
        	names[i] = td1.getFieldName(i);
        }
        for(int i=0;i<td2.numFields();i++) {
        	types[i+td1.numFields()] = td2.getFieldType(i);
        	names[i+td1.numFields()] = td2.getFieldName(i);
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
    	if (!o.getClass().equals(this.getClass())){
    		return false;
    	}
    	
    	TupleDesc obj = (TupleDesc) o;
    	if (obj.numFields() != this.numFields()) {
			return false;
		}
    	
        for (int i = 0; i < this.numFields(); i++){
        	if (!this.getFieldType(i).equals(obj.getFieldType(i))){
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
    	String str = "";
    	for (int i=0;i<schema.length;i++) {
    		str+=schema[i].fieldName;
    		str+="(" + schema[i].fieldType.toString() + ")";
    		if (!(i == schema.length-1)){
    			str += ", ";
    		}
    	}
    	return str;
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
