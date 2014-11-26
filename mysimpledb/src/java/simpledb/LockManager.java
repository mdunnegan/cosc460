package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
		
	public class LockEntry {
		ArrayList<TransactionId> tids = new ArrayList<TransactionId>();
		boolean lockType = false; // false = readers only (tids can be any size), true = write lock (tids.size() must be 1)
		// whatever request is
	}
	
	HashMap<PageId, LockEntry> lockTable = new HashMap<PageId, LockEntry>();
	
	HashMap<PageId, LinkedList<TransactionId>> waitingTxns = new HashMap<PageId, LinkedList<TransactionId>>();
	
	// this method gets called by bufferpool
	public void getLock(TransactionId tid, PageId pid, Permissions mode){

		// determines availability of the lock for anyone
		//boolean lockHeld = requestLock(tid, pid, mode);
		//System.out.println("lockHeld:\t" + lockHeld);
			
		// if we hold the lock
		if (holdsLock(pid, tid)){
			
			// might want to upgrade...
			
			// if we hold a read lock
				// if the size of the waiting list is 0...
			
			
			// if some other txn holds the lock, we're waiting!
		} else {
			//System.out.println("Begin the infinite loop of lock searching!");
			boolean lockHeld = requestLock(tid, pid, mode);
			while (!lockHeld){
				lockHeld = requestLock(tid, pid, mode);
			}
		}			 
	}

	public synchronized boolean requestLock(TransactionId tid, PageId pid, Permissions mode){

		//System.out.println(mode);
		if (mode.equals(Permissions.READ_WRITE)){
			//System.out.println("waitingTxns.size():\t" + waitingTxns.size());
			//System.out.println("WaitingTxns:\t" + waitingTxns);
			//System.out.println("WaitingTxns.get(pid):\t" + waitingTxns.get(pid));
			if (!waitingTxns.containsKey(pid)){ // !lockTable.containsKey(pid)
				//System.out.println("***WaitingTxns did NOT contain pid***");
				LinkedList<TransactionId> ll = new LinkedList<TransactionId>();
				ll.add(tid);
				waitingTxns.put(pid, ll);
				//System.out.println("Added pid to waitingTxns");
				//System.out.println("WaitingTxns:\t" + waitingTxns);
				//System.out.println("WaitingTxns.get(pid):\t" + waitingTxns.get(pid));
			} else {
				//System.out.println("WaitingTxns had pid");	
				if (!waitingTxns.get(pid).contains(tid)){
					// TODO add to beginning of queue
					waitingTxns.get(pid).add(tid);
				}
			}
			if (lockTable.containsKey(pid)){
				if (lockTable.get(pid).tids.size() > 0){			
					return false;			
				} else {			
					// nobody is in the array! we can get the lock and return true
					lockTable.get(pid).tids.add(0, tid);
					lockTable.get(pid).lockType = true;
					//System.out.println("%return true");
					return true;
				}
			} else {
				LockEntry entry = new LockEntry();
				lockTable.put(pid, entry);		
				return false;
			}
		} else {
			//System.out.println("waitingTxns.size():\t" + waitingTxns.size());
			//System.out.println("WaitingTxns:\t" + waitingTxns);
			//System.out.println("WaitingTxns.get(pid):\t" + waitingTxns.get(pid));
			if (!waitingTxns.containsKey(pid)){ // !lockTable.containsKey(pid)
				//System.out.println("***WaitingTxns did NOT contain pid***");
				LinkedList<TransactionId> ll = new LinkedList<TransactionId>();
				ll.add(tid);
				waitingTxns.put(pid, ll);
				//System.out.println("Added pid to waitingTxns");
				//System.out.println("WaitingTxns:\t" + waitingTxns);
				//System.out.println("WaitingTxns.get(pid):\t" + waitingTxns.get(pid));
			} else {
				//System.out.println("WaitingTxns had pid");	
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
				//System.out.println("%return true");
				return true;
			} else {
				LockEntry entry = new LockEntry();
				lockTable.put(pid, entry);
				return false;
			}
		}
	}
	
	
	public synchronized void releaseLock(PageId pid, TransactionId tid){		
		if (lockTable.containsKey(pid)){
			lockTable.get(pid).tids.remove(tid);
		} else {
			System.out.println("No threads held this lock");
		}
	}
	
	public synchronized boolean holdsLock(PageId pid, TransactionId tid){

		if (lockTable.containsKey(pid) && lockTable.get(pid).tids.size() > 0){	
			if (lockTable.get(pid).tids.contains(tid)){
				return true;
			}
		}
		return false;
	}
		
}
