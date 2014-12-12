package simpledb;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

/**
 * @author mhay
 */
class LogFileRecovery {

    private final RandomAccessFile readOnlyLog;

    /**
     * Helper class for LogFile during rollback and recovery.
     * This class given a read only view of the actual log file.
     *
     * If this class wants to modify the log, it should do something
     * like this:  Database.getLogFile().logAbort(tid);
     *
     * @param readOnlyLog a read only copy of the log file
     */
    public LogFileRecovery(RandomAccessFile readOnlyLog) {
        this.readOnlyLog = readOnlyLog;
    }

    /**
     * Print out a human readable representation of the log
     */
    public void print() throws IOException {
        // since we don't know when print will be called, we can save our current location in the file
        // and then jump back to it after printing
        Long currentOffset = readOnlyLog.getFilePointer();

        readOnlyLog.seek(0);
        long lastCheckpoint = readOnlyLog.readLong(); // ignore this
        System.out.println("BEGIN LOG FILE");
        while (readOnlyLog.getFilePointer() < readOnlyLog.length()) {
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            switch (type) {
                case LogType.BEGIN_RECORD:
                    System.out.println("<T_" + tid + " BEGIN>");
                    break;
                case LogType.COMMIT_RECORD:
                    System.out.println("<T_" + tid + " COMMIT>");
                    break;
                case LogType.ABORT_RECORD:
                    System.out.println("<T_" + tid + " ABORT>");
                    break;
                case LogType.UPDATE_RECORD:
                    Page beforeImg = LogFile.readPageData(readOnlyLog);
                    Page afterImg = LogFile.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " UPDATE pid=" + beforeImg.getId() +">");
                    break;
                case LogType.CLR_RECORD:
                    afterImg = LogFile.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " CLR pid=" + afterImg.getId() +">");
                    break;
                case LogType.CHECKPOINT_RECORD:
                    int count = readOnlyLog.readInt();
                    Set<Long> tids = new HashSet<Long>();
                    for (int i = 0; i < count; i++) {
                        long nextTid = readOnlyLog.readLong();
                        tids.add(nextTid);
                    }
                    System.out.println("<T_" + tid + " CHECKPOINT " + tids + ">");
                    break;
                default:
                    throw new RuntimeException("Unexpected type!  Type = " + type);
            }
            long startOfRecord = readOnlyLog.readLong();   // ignored, only useful when going backwards thru log
        }
        System.out.println("END LOG FILE");

        // return the file pointer to its original position
        readOnlyLog.seek(currentOffset);

    }

    /**
     * Rollback the specified transaction, setting the state of any
     * of pages it updated to their pre-updated state.  To preserve
     * transaction semantics, this should not be called on
     * transactions that have already committed (though this may not
     * be enforced by this method.)
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     *
     * @param tidToRollback The transaction to rollback
     * @throws java.io.IOException if tidToRollback has already committed
     */
    public void rollback(TransactionId tidToRollback) throws IOException {
        
    	readOnlyLog.seek(readOnlyLog.length()); // undoing so move to end of logfile
    	long pointerToNextRecent = readOnlyLog.length()-LogFile.LONG_SIZE;

        while(true){
        	
        	readOnlyLog.seek(pointerToNextRecent);
        	long beginLogEntry = readOnlyLog.readLong(); 	
        	readOnlyLog.seek(beginLogEntry);
        	
        	long hook = beginLogEntry;
        	int type = readOnlyLog.readInt();
        	long tid = readOnlyLog.readLong();
        	      	        	        	
        	//System.out.println("log type:\t"+type);
        	        	
        	if (type == LogType.BEGIN_RECORD){
        		System.out.println("looking begin");
        		if (tid == tidToRollback.getId()){
        			System.out.println("begin");
        			// it MUST get here at some point
        			return;
        		}
        	} else if (type == LogType.COMMIT_RECORD){
        		System.out.println("looking commit");
        		if (tid == tidToRollback.getId()){
        			System.out.println("commit?");
        			throw new IOException("Tried to roll back a committed transaction");
        		}
        	} else if (type == LogType.UPDATE_RECORD){
        		System.out.println("looking update");
        		if (tid == tidToRollback.getId()){
        			System.out.println("update");
        			Page before = LogFile.readPageData(readOnlyLog);
        			DbFile file =  Database.getCatalog().getDatabaseFile(before.getId().getTableId());
        			file.writePage(before);
        			BufferPool bp = Database.getBufferPool();
        			bp.discardPage(before.getId());
        		}
        	}    
        	pointerToNextRecent = hook - LogFile.LONG_SIZE; // puts us at top of the next thing
        }        
    }

    /**
     * Recover the database system by ensuring that the updates of
     * committed transactions are installed and that the
     * updates of uncommitted transactions are not installed.
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     */
    public void recover() throws IOException {

    	print();
    	
        // read the last checkpoint!
    	readOnlyLog.seek(0);
    	
    	long recentCheckpoint = readOnlyLog.readLong();
    	if (recentCheckpoint == -1){
    		readOnlyLog.seek(0);
    		recentCheckpoint = LogFile.LONG_SIZE;
    	}
    	
    	// goes to most recent checkpoint
    	readOnlyLog.seek(recentCheckpoint);
    	// skips checkpoints information
    	readOnlyLog.readInt();  // skips past ckeckpoints type (which is ckpt) - could verify this
    	readOnlyLog.readLong(); // skips past checkpoints tid, who cares

    	// scan forward and determine loser transactions
    	// scanning checkpoint: read int: read longs for int number of times; this will be end of ckpt
    	int numTxnsInCkpt = readOnlyLog.readInt();
    	
    	System.out.println("num txns in ckpt: " + numTxnsInCkpt);
    	
    	LinkedList<Long> tids = new LinkedList<Long>(); // I hate linked lists!
    	for (int i = 0; i < numTxnsInCkpt; i++){
    		tids.add(readOnlyLog.readLong()); // 0 almost always. weird
    	}
    	
    	// must read another long to ignore the checkpoint pointer
    	
    	readOnlyLog.readLong();
    	
    	System.out.println("num tids:" + tids.size());
    	
    	// redo!
    	while (readOnlyLog.getFilePointer() < readOnlyLog.length()){ // or hasnext or whatever...
    		
    		// get type and tids
    		int type = readOnlyLog.readInt();
    		long tid = readOnlyLog.readLong();
    		
    		if (type == LogType.UPDATE_RECORD){
    			System.out.println("update");
    			// perform update
    			//readOnlyLog.readLong(); // skip the old value 
//    			Page beforeImage = LogFile.readPageData(readOnlyLog); // error? 
//    			Page afterImage = LogFile.readPageData(readOnlyLog);
//    			DbFile file =  Database.getCatalog().getDatabaseFile(beforeImage.getId().getTableId());
//    			BufferPool bp = Database.getBufferPool();
//    			bp.discardPage(beforeImage.getId());
//    			file.writePage(afterImage);
    			
    			//readOnlyLog.readLong(); // why was I doing this?
    			LogFile.readPageData(readOnlyLog);
                Page afterImage = LogFile.readPageData(readOnlyLog);
                int tableId = afterImage.getId().getTableId();
                Database.getCatalog().getDatabaseFile(tableId).writePage(afterImage);
    			
    		} else if (type == LogType.ABORT_RECORD){
    			System.out.println("abort");
    			tids.remove(tid);
    			//tids.remove(readOnlyLog.readInt()); 
    			
    		} else if (type == LogType.COMMIT_RECORD){
    			System.out.println("commit");
    			tids.remove(tid);
    			//tids.remove(readOnlyLog.readInt());
    			
    		} else if (type == LogType.CHECKPOINT_RECORD){
    			System.out.println("checkpoint");
//    			int count = readOnlyLog.readInt();
//                for (int i = 0; i < count; i++) {
//                    long nextTid = readOnlyLog.readLong();
//                    tids.add(nextTid);
//                }
    			throw new RuntimeException("Found another checkpoint?");
    		} else if (type == LogType.CLR_RECORD){ // very similar to update, but ...
    			Page afterImg = LogFile.readPageData(readOnlyLog);
                int tableId = afterImg.getId().getTableId();
                Database.getCatalog().getDatabaseFile(tableId).writePage(afterImg);
    		} else if (type == LogType.BEGIN_RECORD){
    			System.out.println("begin");
    			tids.add(tid);
    		}
    		readOnlyLog.readLong(); // goes past the pointer address space
    	}
    	
    	// undo!
    	// remember to make CLR's for each update to a loser txn, i think
    	
    	System.out.println("Hey made past redo here");
    	
    	long pointerToNextRecent = readOnlyLog.length()-LogFile.LONG_SIZE;
    	
    	for (int i = 0; i < tids.size(); i++){
    		System.out.println("tid at i: "+tids.get(i));
    	}
    	
    	while (!tids.isEmpty()){  	
    		readOnlyLog.seek(pointerToNextRecent);
    		
        	long beginLogEntry = readOnlyLog.readLong();
        	readOnlyLog.seek(beginLogEntry);
        	
        	long hook = beginLogEntry;
        	int type = readOnlyLog.readInt();
        	long tid = readOnlyLog.readLong();
        	
        	if (tids.contains(tid)){ // if a loser?
        		if (type == LogType.UPDATE_RECORD){
        			// set the before image
        			Page beforeImage = LogFile.readPageData(readOnlyLog);
        			DbFile file =  Database.getCatalog().getDatabaseFile(beforeImage.getId().getTableId());
        			
        			// add a clr?
        			//file.writePage(beforeImage);
        			        			
        		} else if (type == LogType.BEGIN_RECORD){
        			tids.remove(readOnlyLog.readInt());
        		}
        		
        	} else {
        		//skip it
        		pointerToNextRecent = hook - LogFile.LONG_SIZE; // puts us at top of the next thing
        	}
    	}
    	    	
    	// redo phase:
    	// loser txns never abort or commit
    	// winner txns commit or abort
    	// loop forward in readOnlyLog, if update, redo it!
    	// if a txn commits or aborts, remove it from numTxnsInCkpt
    	
    	// undo phase:
    	// start from bottom, traverse up
    	// if txn is an update and it's a loser
    	//     undo it's stuff
    	//     write a CLR
        	
    }
}
