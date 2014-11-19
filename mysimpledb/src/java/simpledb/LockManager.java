package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
	
	public class TransactionStruct{
		TransactionId transactionId;
		Permissions lockModeRequested;
		boolean lockGranted; // 0 no, 1 yes
		
		public TransactionStruct(TransactionId transactionId, Permissions lockModeRequested, boolean lockGranted){
			this.transactionId = transactionId;
			this.lockGranted = lockGranted;
			this.lockModeRequested = lockModeRequested;
		}
		
		public TransactionStruct(){};
		
	}
	
	HashMap<PageId, LinkedList<TransactionStruct>> lockTable = new HashMap<PageId, LinkedList<TransactionStruct>>();	
	// need to separate the data structures.
	
	
	public void getLock(TransactionId tid, PageId pid, Permissions mode){
				
		boolean lockHeld = requestLock(tid, pid, mode);
			
		if (holdsLock(pid, tid)){
			
			// if we hold the lock then we don't need to get it
			System.out.println("Held lock");
		} else {
			// then we do need to get it.
			// how do we get it?
			System.out.println("Didn't hold lock");
			//System.out.println("Lock was not available");
			while (!lockHeld){
				lockHeld = requestLock(tid, pid, mode);
			}
		}			 
	}

	public synchronized boolean requestLock(TransactionId tid, PageId pid, Permissions mode){

		LinkedList<TransactionStruct> ll = new LinkedList<TransactionStruct>();
		
		if (!lockTable.containsKey(pid)){
			TransactionStruct newTS = new TransactionStruct(tid, mode, false);
			ll.add(newTS);
			lockTable.put(pid, ll);
		}
		
		while (lockTable.get(pid).peekFirst() != null){
			if (lockTable.get(pid).peekFirst().transactionId.equals(tid)){
				return true;
			}
		}
		return false;
	}
	
	public synchronized void releaseLock(PageId pid, TransactionId tid){
		if (lockTable.containsKey(pid)){
			System.out.println("contains key");
			LinkedList<TransactionStruct> LL = lockTable.get(pid);
			TransactionStruct ts = new TransactionStruct();
			Iterator<TransactionStruct> llIterator = LL.iterator();
			while (llIterator.hasNext()){
				System.out.println("iteratin");
				ts = llIterator.next();
				if (ts.transactionId == tid){ // && ts.lockGranted  
					//TODO try to get next transaction to run
					System.out.println(lockTable);
					lockTable.get(pid).remove(ts);
					System.out.println(lockTable);
				}
			}
		}	
	}
	
	public synchronized boolean holdsLock(PageId pid, TransactionId tid){
		if (lockTable.containsKey(pid)){
			//System.out.println("holdsLock - contains pid "+lockTable.contains(pid));
			LinkedList<TransactionStruct> LL = lockTable.get(pid);
			TransactionStruct ts = new TransactionStruct();
			Iterator<TransactionStruct> llIterator = LL.iterator();
			while (llIterator.hasNext()){
				ts = llIterator.next();
				if (ts.lockGranted && ts.transactionId.equals(tid)){
					return true;
				}
			}
		}
		return false;
	}
		
}
