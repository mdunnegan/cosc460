package simpledb;
import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;
import javax.print.DocFlavor.URL;

public class Lab2Main {

    public static void main(String[] argv) {
    	
    	File f = new File("some_data_file.dat");
    	Type[] types = {Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
		String[] fields = {"field0", "field1", "field2"};
		TupleDesc td = new TupleDesc(types, fields);
		HeapFile hf = new HeapFile(f, td);
		Database.getCatalog().addTable(hf);
		DbFileIterator hfIterator = hf.iterator(new TransactionId());

		try {
			hfIterator.open();
			int i = 0;
			while (hfIterator.hasNext()){
				
				Tuple t = hfIterator.next();
				
				if ((((IntField) t.getField(1)).getValue() < 3)){
					
					// Sexy new tuple
					Tuple newT = new Tuple(td);
					
					System.out.print("Tuple " + i + " before changing: ");
					System.out.print(t.getField(0) + " " + t.getField(1) + " " + t.getField(2) + "\n");
					
					newT.setField(0, t.getField(0));
					newT.setField(1, new IntField(3));
					newT.setField(2, t.getField(2));
					
					System.out.print("Tuple " + i + " after changing: ");
					System.out.print(t.getField(0) + " " + t.getField(1) + " " + t.getField(2) + "\n");
					
					Database.getBufferPool().deleteTuple(new TransactionId(), t);
					Database.getBufferPool().insertTuple(new TransactionId(), hf.getId(), newT);
				}
				
			}
			
		} catch (TransactionAbortedException e) {
			throw new RuntimeException("Transaction Aborted");
		} catch (IOException e) {
			throw new RuntimeException("IO Exception");
		} catch (DbException e) {
			throw new RuntimeException("Db Exception");
		}
				
		Tuple ninetup = new Tuple(td);
		
		// sets the tuples fields to 99
		for (int i = 0; i < 3; i++){
			ninetup.setField(i, new IntField(99));
		}
		
		try {
			hfIterator.rewind();
			while (hfIterator.hasNext()){
				Tuple next = hfIterator.next();
				System.out.println(next.toString());
			}
			hfIterator.close();
		} catch (NoSuchElementException e) {
			System.err.println("No such element");
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			System.err.println("Transaction Aborted!!!");
			e.printStackTrace();
		} catch (DbException e) {
			System.err.println("Database exception");
			e.printStackTrace();
		} 
    }
}