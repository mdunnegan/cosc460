package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

public class LockManager {
		
	public class LockEntry {
		ArrayList<TransactionId> tids = new ArrayList<TransactionId>();
		boolean lockType = false; // false = readers only (tids can be any size), true = write lock (tids.size() must be 1)
	}
	
	HashMap<PageId, LockEntry> lockTable = new HashMap<PageId, LockEntry>();
	
	HashMap<PageId, LinkedList<TransactionId>> waitingTxns = new HashMap<PageId, LinkedList<TransactionId>>();
		
	public HashMap<PageId, LockEntry> getlockTable(){
		return lockTable;
	}
	
	public HashMap<PageId, LinkedList<TransactionId>> getWaitingTxns(){
		return waitingTxns;
	}
	
	public void getLock(TransactionId tid, PageId pid, Permissions mode) throws TransactionAbortedException{
		
		//System.out.println("Transaction " + tid + " wants a " +mode+ "lock on page " +pid);
		if (holdsLock(tid, pid, mode)){
			//System.out.println("held lock");
			//System.out.println("Transaction " + tid + " hld a " +mode+ "lock on page " +pid);
		} else {
			
			long tStart = System.currentTimeMillis();
			boolean lockHeld = requestLock(tid, pid, mode);
			while (!lockHeld){
				if (System.currentTimeMillis() - tStart > 3000){
					//System.out.println("Timed out!");
					//System.out.println("Aborted: Transaction " + tid + " wanted a " +mode+ " lock on page " +pid);
					throw new TransactionAbortedException();
				}
				lockHeld = requestLock(tid, pid, mode);
			}
			//System.out.println("Transaction " + tid + " got a " +mode+ "lock on page " +pid);
		}	
		
	}

	public synchronized boolean requestLock(TransactionId tid, PageId pid, Permissions mode){
		
		if (mode.equals(Permissions.READ_WRITE)){
			
			if (!waitingTxns.containsKey(pid)){
				LinkedList<TransactionId> ll = new LinkedList<TransactionId>();
				ll.add(0, tid);
				waitingTxns.put(pid, ll);
			} else {
				if (!waitingTxns.get(pid).contains(tid)){
					waitingTxns.get(pid).add(0, tid);
				}
			}
            /** Above: insertion into waitingTxns. Below: insertion into lockTable, and deletion from waitingTxns  **/		

			if (lockTable.containsKey(pid)){
				if (lockTable.get(pid).tids.size() == 0){
					// no competition
					lockTable.get(pid).lockType = true;
					lockTable.get(pid).tids.add(tid);
					waitingTxns.get(pid).remove(tid);
					return true;
					
				} else if (lockTable.get(pid).tids.size() == 1 && lockTable.get(pid).tids.get(0).equals(tid)){
					// upgrade
					lockTable.get(pid).lockType = true;
					waitingTxns.get(pid).remove(tid);
					return true;
					
				} else {
					// not today kid
					return false;
				}
	
			} else {
				// added at 1:08, didn't change anything. 
				waitingTxns.get(pid).remove(tid);
			
				LockEntry entry = new LockEntry();
				entry.lockType = true;
				entry.tids.add(tid);
				lockTable.put(pid, entry);
				return true;
			}
		} else {
			// acquiring read only lock
						
			if (!waitingTxns.containsKey(pid)){
				LinkedList<TransactionId> ll = new LinkedList<TransactionId>();
				ll.add(tid);
				waitingTxns.put(pid, ll);
			} else {
				if (!waitingTxns.get(pid).contains(tid)){
					waitingTxns.get(pid).add(tid);
				}
			}
			
			if (lockTable.containsKey(pid)){			
				// someone (who isn't us) has an exclusive lock!
				if (lockTable.get(pid).lockType == true && !lockTable.get(pid).tids.get(0).equals(tid)){
					return false;
				} else {
					
					lockTable.get(pid).tids.add(tid);
					lockTable.get(pid).lockType = false;
					waitingTxns.get(pid).remove(tid);
					return true;
				}
				
			} else {
				LockEntry entry = new LockEntry();
				entry.lockType = false;
				entry.tids.add(tid);
				lockTable.put(pid, entry);
				return true;
			}
		}
	}
	
	public synchronized void releaseLock(PageId pid, TransactionId tid){
		//System.out.println("ReleaseLock called");
		if (lockTable.containsKey(pid)){
			lockTable.get(pid).tids.remove(tid);
			lockTable.get(pid).lockType = false;
			
		} else {
			//System.out.println("No threads held this lock");
		}
	}
	
	public void releaseLocksAndRequests(TransactionId tid){
				
		for (PageId p : lockTable.keySet()){
			if (lockTable.get(p).tids.contains(tid)){
				releaseLock(p, tid);
			}
		} // seems right
		
		for (PageId p : waitingTxns.keySet()){

			while(waitingTxns.get(p).contains(tid)){
				waitingTxns.get(p).remove(tid);
			}
		}
	}
	
	public synchronized boolean holdsLock(TransactionId tid, PageId pid, Permissions perm){
		
		if (perm.equals(Permissions.READ_WRITE)){
			if (lockTable.containsKey(pid)){
				if (lockTable.get(pid).tids.size() == 1){
					if (lockTable.get(pid).tids.get(0).equals(tid)){
						return true;
					}
				}
			}
		} else {
			if (lockTable.containsKey(pid)){
				if (lockTable.get(pid).tids.contains(tid)){
					return true;
				}
			}
		}
		return false;
	}
}