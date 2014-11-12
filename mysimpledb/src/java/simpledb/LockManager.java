package simpledb;

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
	
	ConcurrentHashMap<PageId, LinkedList<TransactionStruct>> lockTable = new ConcurrentHashMap<PageId, LinkedList<TransactionStruct>>();	
	
	public void getLock(TransactionId tid, PageId pid, Permissions mode){
				
		boolean lockHeld = requestLock(tid, pid, mode);
			
		if (holdsLock(pid, tid)){
			
			// if we hold the lock then we don't need to get it
			
		} else {
			// then we do need to get it.
			// how do we get it?
					
			//System.out.println("Lock was not available");
			while (!lockHeld){
				requestLock(tid, pid, mode);
				//wait();
				lockHeld = requestLock(tid, pid, mode);
			}
		}			 
		
	}

	public synchronized boolean requestLock(TransactionId tid, PageId pid, Permissions mode){

		LinkedList<TransactionStruct> ll = new LinkedList<TransactionStruct>();
		
		if (!lockTable.containsKey(pid)){
			
			System.out.println("Didn't find '" + pid + "' in lock table");
			System.out.println(lockTable);
			// make a new thing, tack it on
			TransactionStruct newTS = new TransactionStruct(tid, mode, true);
			
			ll.add(newTS);
			lockTable.put(pid, ll);
			return true;
			
		}  else { // lock might be held
					
			TransactionStruct ts = new TransactionStruct();
			Iterator<TransactionStruct> tsIterator = lockTable.get(pid).iterator();
			while (tsIterator.hasNext()){
				ts = tsIterator.next();
				if (ts.transactionId.equals(tid)){ // found ourself waiting in the queue
					return false;
				}
			}
			
			// add this to queue
			TransactionStruct newTS = new TransactionStruct(tid, mode, false);
			ll.add(newTS);
			lockTable.put(pid, ll);
			return false;	
		}
	}
	
	public synchronized void releaseLock(PageId pid, TransactionId tid){
		if (lockTable.containsKey(pid)){
			LinkedList<TransactionStruct> LL = lockTable.get(pid);
			TransactionStruct ts = new TransactionStruct();
			Iterator<TransactionStruct> llIterator = LL.iterator();
			while (llIterator.hasNext()){
				ts = llIterator.next();
				if (ts.lockGranted && ts.transactionId == tid){
					//TODO try to get next transaction to run
					//lockTable.get(pid).remove(ts);
					lockTable.remove(pid);
					//llIterator.next().getLock(llIterator.next().transactionId, pid, Permissions.READ_ONLY);
				}
			}
		}	
	}
	
	public synchronized boolean holdsLock(PageId pid, TransactionId tid){
		if (lockTable.contains(pid)){
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
