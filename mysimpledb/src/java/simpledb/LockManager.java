package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

public class LockManager {
		
	public class LockEntry {
		ArrayList<TransactionId> tids = new ArrayList<TransactionId>();
		boolean lockType = false; // false = readers only (tids can be any size), true = write lock (tids.size() must be 1)
		// whatever request is
	}
	
	HashMap<PageId, LockEntry> lockTable = new HashMap<PageId, LockEntry>();
	
	HashMap<PageId, LinkedList<TransactionId>> waitingTxns = new HashMap<PageId, LinkedList<TransactionId>>();
	
	// this method gets called by bufferpool
	
	public HashMap<PageId, LockEntry> getlockTable(){
		return lockTable;
	}
	
	public HashMap<PageId, LinkedList<TransactionId>> getWaitingTxns(){
		return waitingTxns;
	}
	
	public void getLock(TransactionId tid, PageId pid, Permissions mode){

		if (holdsLock(tid, pid)){
			// nothing!
		} else {
			boolean lockHeld = requestLock(tid, pid, mode);
			while (!lockHeld){
				lockHeld = requestLock(tid, pid, mode);
			}
		}			 
	}
	
//	public boolean getLock(TransactionId tid, PageId pid, Permissions mode){
//		if (holdsLock(tid, pid)){
//			return true;
//		} else {
//			
//			long tStart = System.currentTimeMillis();
//			boolean lockHeld = requestLock(tid, pid, mode);
//			while (!lockHeld){
//				if (System.currentTimeMillis() - tStart > 2000){
//					System.out.println("Timed out!");
//					return false;
//				}
//				lockHeld = requestLock(tid, pid, mode);
//				tStart = System.currentTimeMillis() - tStart;
//			}
//			return true;
//		}			 
//		
//	}

	public synchronized boolean requestLock(TransactionId tid, PageId pid, Permissions mode){

		if (mode.equals(Permissions.READ_WRITE)){
			
			System.out.println("Trying to get an exclusive lock");

			if (!waitingTxns.containsKey(pid)){ // !lockTable.containsKey(pid)
				LinkedList<TransactionId> ll = new LinkedList<TransactionId>();
				ll.add(tid);
				waitingTxns.put(pid, ll);

			} else {
				if (!waitingTxns.get(pid).contains(tid)){
					waitingTxns.get(pid).add(tid);
				}
			}
			
			System.out.println(lockTable.get(pid));
			
			if (lockTable.containsKey(pid)){
				if (lockTable.get(pid).tids.size() > 0){	
					return false;	
				} else {			
					lockTable.get(pid).tids.add(0, tid);
					lockTable.get(pid).lockType = true; // exclusive
					return true;
				}
			} else {
				LockEntry entry = new LockEntry();
				lockTable.put(pid, entry);
				return false;
			}
		} else {
			
			// acquiring read only lock

			if (!waitingTxns.containsKey(pid)){ // !lockTable.containsKey(pid)
				LinkedList<TransactionId> ll = new LinkedList<TransactionId>();
				ll.add(tid);
				waitingTxns.put(pid, ll);
			} else {
				if (!waitingTxns.get(pid).contains(tid)){
					waitingTxns.get(pid).add(tid);
				}
			}
			if (lockTable.containsKey(pid)){
											
				// someone has an exclusive lock!
				if (lockTable.get(pid).lockType == true){					
					return false;
				}
				lockTable.get(pid).tids.add(tid);
				return true;
			} else {
				LockEntry entry = new LockEntry();
				lockTable.put(pid, entry);
				return false;
			}
		}
	}
	
	public synchronized void releaseLock(PageId pid, TransactionId tid){
		System.out.println("ReleaseLock called");
		if (lockTable.containsKey(pid)){
			lockTable.get(pid).tids.remove(tid);
			
			// This was the bug!!
			lockTable.get(pid).lockType = false;
			
		} else {
			System.out.println("No threads held this lock");
		}
	}
	
	// gets called following a transaction, probably
	public void releaseLocksAndRequests(TransactionId tid){
				
		for (PageId p : lockTable.keySet()){
			if (lockTable.get(p).tids.contains(tid)){
				releaseLock(p, tid);
			}
		}
		
		for (PageId p : waitingTxns.keySet()){
			for (TransactionId t : waitingTxns.get(p)){
				if (t.equals(tid)){
					waitingTxns.get(p).remove(t);
				}
			}
		}
	}
	
	public synchronized boolean holdsLock(TransactionId tid, PageId pid){

		if (lockTable.containsKey(pid) && lockTable.get(pid).tids.size() > 0){	
			if (lockTable.get(pid).tids.contains(tid)){
				return true;
			}
		}
		return false;
	}
		
}