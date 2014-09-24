package simpledb;
import java.io.*;
import java.util.Iterator;

import javax.print.DocFlavor.URL;

public class Lab2Main {

    public static void main(String[] argv) {
    	
    	// read from some_dat_file.txt
    	File f = new File("../../../some_data_file.txt");
    	// make TD
    	Type[] types = {Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
		String[] fields = {"field0", "field1", "field2"};
		TupleDesc td = new TupleDesc(types, fields);
		// make HeapFile
		HeapFile hf = new HeapFile(f, td);
		Database.getCatalog().addTable(hf);
		
		// update tuples with heapfile iterator
		
		// heapfile iterator
		DbFileIterator hfIterator = hf.iterator(new TransactionId());

		try {
			hfIterator.open();
			Tuple t = hfIterator.next();
			
			while (hfIterator.hasNext()){
				if ((((IntField) t.getField(1)).getValue() < 3)){ // wow
					// delete this tuple
					hf.deleteTuple(new TransactionId(), t); 
					// create a new tuple, put it at this location
					Tuple newT = new Tuple(td);
					
					hf.insertTuple(new TransactionId(), newT);
					
				}
			}
			
		} catch (DbException | TransactionAbortedException e) {
			
			e.printStackTrace();
			throw new RuntimeException("HeapFile iterator did not open");
		} catch (IOException e) {
			System.err.println("Tuple was not inserted");
			e.printStackTrace();
		}
				
		Tuple ninetup = new Tuple(td);
		Field ninetynine = new IntField(99);
		
		// sets the tuples fields to 99
		for (int i = 0; i < 3; i++){
			ninetup.setField(i, ninetynine);
		}
		
    }

}