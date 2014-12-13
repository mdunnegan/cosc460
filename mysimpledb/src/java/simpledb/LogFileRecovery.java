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
    	LogFile writeLog = Database.getLogFile();
    	
        while(true){
        	
        	readOnlyLog.seek(pointerToNextRecent);
        	long beginLogEntry = readOnlyLog.readLong(); 	
        	readOnlyLog.seek(beginLogEntry);
        	
        	long hook = beginLogEntry;
        	int type = readOnlyLog.readInt();
        	long tid = readOnlyLog.readLong();
        	      	        	        	
        	//System.out.println("log type:\t"+type);
        	        	
        	if (type == LogType.BEGIN_RECORD){
        		if (tid == tidToRollback.getId()){
        			//System.out.println("begin");
        			// it MUST get here at some point
        			writeLog.logAbort(tid);
        			return;
        		}
        	} else if (type == LogType.COMMIT_RECORD){
        		if (tid == tidToRollback.getId()){
        			//System.out.println("commit?");
        			throw new IOException("Tried to roll back a committed transaction");
        		}
        	} else if (type == LogType.UPDATE_RECORD){
        		if (tid == tidToRollback.getId()){
        			//System.out.println("update");
        			Page beforeImage = LogFile.readPageData(readOnlyLog);
        			DbFile file =  Database.getCatalog().getDatabaseFile(beforeImage.getId().getTableId());
        			file.writePage(beforeImage);
        			
        			BufferPool bp = Database.getBufferPool();
        			bp.discardPage(beforeImage.getId());
        			writeLog.logCLR(tidToRollback, beforeImage);
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
    	
    	//System.out.println("num tids:" + tids.size());
    	
    	// redo!
    	while (readOnlyLog.getFilePointer() < readOnlyLog.length()){ // optional - LogFile.LONG_SIZE
    		
    		// get type and tids
    		int type = readOnlyLog.readInt();
    		long tid = readOnlyLog.readLong();
    		
    		if (type == LogType.UPDATE_RECORD){
    			//System.out.println("update " + tid);
    			LogFile.readPageData(readOnlyLog);
                Page afterImage = LogFile.readPageData(readOnlyLog);
                int tableId = afterImage.getId().getTableId();
                Database.getCatalog().getDatabaseFile(tableId).writePage(afterImage);
    			
    		} else if (type == LogType.ABORT_RECORD){
    			//System.out.println("abort "+ tid);
    			tids.remove(tid);
    			
    		} else if (type == LogType.COMMIT_RECORD){
    			//System.out.println("commit "+tid);
    			tids.remove(tid);
    			
    		} else if (type == LogType.CHECKPOINT_RECORD){
    			//System.out.println("checkpoint "+tid);
    			throw new RuntimeException("Found another checkpoint?");
    		} else if (type == LogType.CLR_RECORD){ // very similar to update, but ...
    			//System.out.println("CLR "+tid);
    			Page afterImage = LogFile.readPageData(readOnlyLog);
                int tableId = afterImage.getId().getTableId();
                Database.getCatalog().getDatabaseFile(tableId).writePage(afterImage);
    		} else if (type == LogType.BEGIN_RECORD){
    			//System.out.println("begin "+tid);
    			tids.add(tid);
    		}
    		readOnlyLog.readLong(); // goes past the pointer address space
    	}
    	
    	// undo!
    	// remember to make CLR's for each update to a loser txn, i think
    	
    	//System.out.println("Hey made past redo here");
    	    	
    	//System.out.println("*********");
    	for (int i = 0; i < tids.size(); i++){
    		System.out.println("tid at i: "+tids.get(i));
    	}
    	//System.out.println("*********");
    	
    	//
    	readOnlyLog.seek(readOnlyLog.length()); // undoing so move to end of logfile
    	long pointerToNextRecent = readOnlyLog.length()-LogFile.LONG_SIZE;
        	
        readOnlyLog.seek(pointerToNextRecent);
    	
    	while (!tids.isEmpty()){
    		//System.out.println("all tids "+tids);
    		readOnlyLog.seek(pointerToNextRecent);
    		
        	long beginLogEntry = readOnlyLog.readLong();
        	readOnlyLog.seek(beginLogEntry);
        	
        	long hook = beginLogEntry;
        	int type = readOnlyLog.readInt();
        	long tid = readOnlyLog.readLong();
        	//System.out.println("this tid "+tid);
        	
        	LogFile writeLog = Database.getLogFile();
        	
        	if (tids.contains(tid)){ // if a loser?
        		if (type == LogType.UPDATE_RECORD){
        			//System.out.println("Was an update to a loser tid");
        			// set the before image
        			Page beforeImage = LogFile.readPageData(readOnlyLog);
        			LogFile.readPageData(readOnlyLog); // skip afterimage 
        			DbFile file =  Database.getCatalog().getDatabaseFile(beforeImage.getId().getTableId());
        			
        			file.writePage(beforeImage); // error must be in write
        			writeLog.logCLR(tid, beforeImage);
        			        			
        		} else if (type == LogType.BEGIN_RECORD){
        			//System.out.println("Began a loser tid");
        			tids.remove(tid);
        			writeLog.logAbort(tid);
        		} else {
        			System.out.println("Type: "+type);
        		}
        	}
        	pointerToNextRecent = hook - LogFile.LONG_SIZE;
    	}
        // gives the same exact error...
//        for (long tid: tids){
//        	TransactionId t = new TransactionId(tid);
//        	rollback(t);
//        }
    }
}
