package concurrency_tutorial;

public class ZigZagThreads {
    private static final LockManager lm = new LockManager(); // instantiate a lock manager
    
    public static LockManager getLockManager() {
    	return lm; 
    }

    public static void main(String args[]) throws InterruptedException {
        int numZigZags = 10;
        for (int i = 0; i < numZigZags; i++) {
            new Thread(new Zigger()).start();
        }
        for (int i = 0; i < numZigZags; i++) {
            new Thread(new Zagger()).start();
        }
    }

    static class Zigger implements Runnable {

        protected String myPattern;
        protected boolean isZigger;

        public Zigger() {
            myPattern = "//////////";
            isZigger = true;
        }

        public void run() {
            ZigZagThreads.getLockManager().acquireLock(isZigger);
            System.out.println(myPattern);
            ZigZagThreads.getLockManager().releaseLock();
        }
    }

    static class Zagger extends Zigger { // it's Zigger, but it has an extra method, Zagger
        public Zagger() {
            myPattern = "\\\\\\\\\\\\\\\\\\\\";
            isZigger = false;
        }
    }

    static class LockManager {
        private boolean inUse = false;  // someone has the lock
        private boolean needZig = true; // who's turn? true = zigs turn, false = zags turn

        private synchronized boolean isLockFree(boolean isZigger) {
        	return !inUse && isZigger == needZig;
        }
        
        public void acquireLock(boolean isZigger) { // zigger
            boolean waiting = true;
            while (waiting) {
                synchronized (this) {
                    if (isLockFree(isZigger)) { // if lock is free for zigger                	
                    	if (isZigger == needZig){
                    		waiting = false;
                    		needZig = !needZig;
                    	}
                    }
                }
                if (waiting) {
                    try {
                    	Thread.sleep(1);
                    } catch (InterruptedException ignored) { }
                }
            }
        }

        public synchronized void releaseLock() {
            inUse = false;
        }
    }
}

