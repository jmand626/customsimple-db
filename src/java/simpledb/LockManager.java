package simpledb;

import java.util.*;

public class LockManager {
    private Map<TransactionId, HashSet<PageId>> transactionLocks;
    // This is the specific map from the transaction to pages, for which this specific transaction has a lock
    // on. Remember that the ReadMe details that we can lock either at the page level or the tuple level, so
    // this is clearly the former. We use a hashset because we do not need to store pairs of values but want to
    // not have duplicates

    // Now two maps that point from a page id to the set of TransactionIds that hold the shared read lock, or
    // to the singular transaction that holds an exclusive write lock.
    private Map<PageId, HashSet<TransactionId>> sharedMap;
    private Map<PageId, TransactionId> exclusiveMap;


    // Then add a fourth and final map for deadlock detection
    private Map<TransactionId, HashSet<TransactionId>> waitforGraph;

    public LockManager() {
        transactionLocks = new HashMap<>();
        sharedMap = new HashMap<>();
        exclusiveMap = new HashMap<>();

        // Add wait for graph for deadlock detection
        waitforGraph = new HashMap<>();
    }

    public synchronized void acquire(TransactionId tid, PageId pid, Permissions p) throws
            TransactionAbortedException, InterruptedException {
        // Our classical method to handle the logic of giving an lock out, except for the fact
        // that we seperated most of our locks, so this method just really figures out whether
        // or not we can give a lock, and then decides what kind of lock to give out
        if (!transactionLocks.containsKey(tid)) transactionLocks.put(tid, new HashSet<>());
        // Initialize our objects in our maps if needed
        if (!sharedMap.containsKey(pid)) sharedMap.put(pid, new HashSet<>());

        while (true) {
            // Infinite loop as mentioned by ryan

            if (!lockIsAvailable(tid, pid, p)) {
                // Add a deadlock check before this wait call so we always check this too
                if (hasDeadlock(tid, pid)) {
                    waitforGraph.remove(tid);
                    // Cut it from our map and
                    // throw our exception!!
                    throw new TransactionAbortedException();
                }


                try {
                    wait();
                    // A java object method to wait until the transaction finishes
                } catch (InterruptedException e) {
                    throw new TransactionAbortedException();
                    // The only way for this txn to be aborted is if it is somehow interrupted, so we
                    // group these exceptions together
                }

            } else {
                // Must try catch these exceptions, otherwise get a warning
                if (p == Permissions.READ_ONLY) {
                    giveSharedLock(tid, pid);
                } else {
                    giveExclusiveLock(tid, pid);
                }

                transactionLocks.get(tid).add(pid);


                // Now finally remove any pieces from the wait for graph since that graph is based on
                // waiting to ACQUIRE more locks
                if (waitforGraph.containsKey(tid)) {
                    waitforGraph.remove(tid);
                }

                // givesharedlock and giveexclusive lock will handle all of the issues related to
                // adding entries in their specific map
                return;
                // We return because we want to break out of the infinite loop, and return is just more
                // convenient
            }
        }

    }

    public boolean lockIsAvailable(TransactionId tid, PageId pid, Permissions p) {
        if (p == Permissions.READ_ONLY) {
            if (!holdsLock(tid, pid)) {
                return !exclusiveMap.containsKey(pid);
                // So if we dont already have a lock, then we can get one provided the exclusive map
                // does not have one.
            }

            return true;
        } else {
            if (exclusiveMap.containsKey(pid) && exclusiveMap.get(pid).equals(tid)) {
                // We already have this exclusive lock
                return true;
            }

            if (!exclusiveMap.containsKey(pid) && sharedMap.get(pid).isEmpty()) {
                return true;
            }
            // had to add this additional case because originally i thought !holdsLock(tid, pid) cover it but you can
            // clearly see that im dumb, so sadge

            // Otherwise figure out if we can update these locks from a shared lock to an exclusive lock
            // if its the only txn that has a shared lock
            if (!exclusiveMap.containsKey(pid) && sharedMap.get(pid).size() == 1 && sharedMap.get(pid).contains(tid)) {
                return true;
            } else {
                return false;
            }
        }
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {

        return ((sharedMap.containsKey(pid) && sharedMap.get(pid).contains(tid)) ||
                (exclusiveMap.containsKey(pid) && exclusiveMap.get(pid).equals(tid)));

        //return transactionLocks.containsKey(tid) && transactionLocks.get(tid).contains(pid);
    }

    public synchronized void release(TransactionId tid, PageId pid) {
        // Now we just remove from each and every one of our locks

        if (sharedMap.containsKey(pid)) {
            sharedMap.get(pid).remove(tid);
        }

        if (exclusiveMap.containsKey(pid) && exclusiveMap.get(pid).equals(tid)) {
            exclusiveMap.remove(pid);
        }

        if (transactionLocks.containsKey(tid)) {
            transactionLocks.get(tid).remove(pid);
        }

        // Finally have to also deal with waitForGraph
        if (waitforGraph.containsKey(tid)) {
            waitforGraph.remove(tid);
        }

        // Another method like wait()
        notifyAll();
    }

    public synchronized void massRelease(TransactionId tid, PageId pid) {
        // Now our hashset pays off again
        if (transactionLocks.containsKey(tid)) {
            HashSet<PageId> pages = new HashSet<>(transactionLocks.get(tid));

            // Now get into a for each loop with this set
            for (PageId p : pages) {
                this.release(tid, p);
            }

            transactionLocks.remove(tid);
        }
    }

    public void clear() {
        transactionLocks.clear();
        sharedMap.clear();
        exclusiveMap.clear();
        waitforGraph.clear();
    }

    public void giveSharedLock(TransactionId tid, PageId pid) {
        // This is where actually add to our blocks
        sharedMap.get(pid).add(tid);
    }

    // When we add the exclusive lock, we can force shared locks out
    private void giveExclusiveLock(TransactionId tid, PageId pid) {
        sharedMap.get(pid).clear();
        exclusiveMap.put(pid, tid);
    }


    // Now the two deadlock methods we have -> hasDeadlock and dfsCycleCheck
    private boolean hasDeadlock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        if (!waitforGraph.containsKey(tid)) waitforGraph.put(tid, new HashSet<>());
        // If we dont currently have a entry for this current tid, introduce it

        // Start building up a set of transactions that hold a lock on this page, because thats the only way
        // we have a deadlock on it
        HashSet<TransactionId> curr = new HashSet<>();
        HashSet<TransactionId> visited = new HashSet<>();
        HashSet<TransactionId> stack = new HashSet<>();
        boolean toReturn = false;

        // Check from both shared and exclusive map
        if (sharedMap.containsKey(pid)) curr.addAll(sharedMap.get(pid));
        // Have to add all as shared map has a hashset of tids, not just one tid
        if (exclusiveMap.containsKey(pid)) curr.add(exclusiveMap.get(pid));

        /// Get ourselves out of the way quickly
        curr.remove(tid);
        if (!curr.isEmpty()) waitforGraph.get(tid).addAll(curr);
        // Add the rest. For some reason, this makes the tests pass almost instantly in comparison to
        // just adding them all with removing ours

        if (dfsCycleCheck(tid, visited, stack)) {
            waitforGraph.remove(tid);
            // This should still just be removing our selves despite the changes outside of this if statement

            throw new TransactionAbortedException();
        }

        return false;
    }

    private boolean dfsCycleCheck(TransactionId tid, HashSet<TransactionId> visited, HashSet<TransactionId>
            stack) {
        // Start off with our dfs basic pieces to add in the first pieces
        stack.add(tid);
        visited.add(tid);

        if (waitforGraph.containsKey(tid)) {

            // for each loop over the neighbors
            for (TransactionId next : waitforGraph.get(tid)) {
                if (stack.contains(next)) {
                    // If a neighbor is someone who is already on our list of people to look at, we have looped
                    // back to it.
                    stack.remove(tid);
                    return true;
                } else if (!visited.contains(next)) {
                    // We have not already seen this before so we recurse through it.
                    // Note that we clearly set visited at the top of every recursion of this function.
                    if (dfsCycleCheck(next, visited, stack)) {
                        stack.remove(tid);
                        return true;
                    }
                }
            }

        }
        stack.remove(tid);
        // we just could not find any valid neighbors to recurse on or cycles to check
        return false;
    }

}
