package simpledb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class TransactionLockManager {
    private final ConcurrentMap<PageId, Object> locks;
    private final ConcurrentMap<PageId, Set<TransactionId>> sharedLocks;
    private final ConcurrentMap<PageId, TransactionId> exclusiveLocks;
    private final ConcurrentMap<TransactionId, Set<PageId>> pageIdsLockedByTransaction;
    private final ConcurrentMap<TransactionId, Set<TransactionId>> dependencyGraph;

    public TransactionLockManager() {
        locks = new ConcurrentHashMap<PageId, Object>();
        sharedLocks = new ConcurrentHashMap<PageId, Set<TransactionId>>();
        exclusiveLocks = new ConcurrentHashMap<PageId, TransactionId>();
        pageIdsLockedByTransaction = new ConcurrentHashMap<TransactionId, Set<PageId>>();
        dependencyGraph = new ConcurrentHashMap<TransactionId, Set<TransactionId>>();
    }

    public Set<PageId> getPagesInTransaction(TransactionId tid) {
        Set<PageId> pages = pageIdsLockedByTransaction.get(tid);
        if (pages == null) {
            pages = new HashSet<PageId>();
        }
        return pages;
    }

    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        if (perm == Permissions.READ_ONLY) {
            if (hasReadPermissions(tid, pid)) {
                return;
            }
            acquireReadOnlyLock(tid, pid);
        } else if (perm == Permissions.READ_WRITE) {
            if (hasWritePermissions(tid, pid)) {
                return;
            }
            acquireReadWriteLock(tid, pid);
        } else {
            throw new IllegalArgumentException("Expected either READ_ONLY or READ_WRITE permissions.");
        }
        addPageToTransactionLocks(tid, pid);
    }

    public synchronized void releasePage(TransactionId tid, PageId pid) {
        Set<PageId> pids = pageIdsLockedByTransaction.get(tid);
        if (pids != null) {
            releaseLock(tid, pid);
            pids.remove(pid);
        }
    }

    public synchronized void releasePages(TransactionId tid) {
        Set<PageId> pids = pageIdsLockedByTransaction.get(tid);
        if (pids != null) {
            for (PageId pid : pids) {
                releaseLock(tid, pid);
            }
            pageIdsLockedByTransaction.remove(tid);
        }
    }

    private Object getLock(PageId pageId) {
        locks.putIfAbsent(pageId, new Object());
        return locks.get(pageId);
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        Set<PageId> pids = pageIdsLockedByTransaction.get(tid);
        return pids != null && pids.contains(pid);
    }

    private void releaseLock(TransactionId tid, PageId pid) {
        Object lock = getLock(pid);
        synchronized (lock) {
            exclusiveLocks.remove(pid);
            Set<TransactionId> tids = sharedLocks.get(pid);
            if (tids != null) {
                tids.remove(tid);
            }
        }
    }

    private void addSharedUser(TransactionId tid, PageId pid) {
        Set<TransactionId> tids = sharedLocks.get(pid);
        if (tids == null) {
            tids = new HashSet<TransactionId>();
            sharedLocks.put(pid, tids);
        }
        tids.add(tid);
    }

    private boolean hasReadPermissions(TransactionId tid, PageId pid) {
        if (hasWritePermissions(tid, pid)) {
            return true;
        }
        Set<TransactionId> tids = sharedLocks.get(pid);
        return tids != null && tids.contains(tid);
    }

    public boolean hasWritePermissions(TransactionId tid, PageId pid) {
        TransactionId tid2 = exclusiveLocks.get(pid);
        return tid2 != null && tid2.equals(tid);
    }

    private void acquireReadOnlyLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        Object lock = getLock(pid);
        while (true) {
            synchronized (lock) {
                TransactionId exclusiveLockHolder = exclusiveLocks.get(pid);
                if (exclusiveLockHolder == null || tid.equals(exclusiveLockHolder)) {
                    removeDependency(tid);
                    addSharedUser(tid, pid);
                    return;
                }
                addDependency(tid, exclusiveLockHolder);
            }
        }
    }

    private void acquireReadWriteLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        Object lock = getLock(pid);
        while (true) {
            synchronized (lock) {
                Set<TransactionId> lockHolders = getLockHolders(pid);
                if (!isLockedByOthers(tid, lockHolders)) {
                    removeDependency(tid);
                    addExclusiveUser(tid, pid);
                    return;
                }
                addDependencies(tid, lockHolders);
            }
        }
    }

    private void addPageToTransactionLocks(TransactionId tid, PageId pid) {
        Set<PageId> pids = pageIdsLockedByTransaction.get(tid);
        if (pids == null) {
            pids = new HashSet<PageId>();
            pageIdsLockedByTransaction.put(tid, pids);
        }
        pids.add(pid);
    }

    private boolean isLockedByOthers(TransactionId transactionId, Set<TransactionId> lockHolders) {
        if (lockHolders == null || lockHolders.isEmpty()) {
            return false;
        }
        if (lockHolders.size() == 1 && transactionId.equals(lockHolders.iterator().next())) {
            return false;
        }
        return true;
    }

    private Set<TransactionId> getLockHolders(PageId pid) {
        TransactionId tid = exclusiveLocks.get(pid);
        if (tid != null) {
            return new HashSet<TransactionId>(Arrays.asList(tid));
        }

        Set<TransactionId> lockHolders = sharedLocks.get(pid);
        return lockHolders == null ? new HashSet<TransactionId>() : lockHolders;
    }

    private void addExclusiveUser(TransactionId tid, PageId pid) {
        exclusiveLocks.put(pid, tid);
        sharedLocks.remove(pid);
    }

    enum VisitState {
        Visited, Visiting
    }

    private void removeDependency(TransactionId dependent) {
        dependencyGraph.remove(dependent);
    }

    private void testForDeadlock(TransactionId tid, Map<TransactionId, VisitState> visitedTransactionIds)
            throws TransactionAbortedException {
        visitedTransactionIds.put(tid, VisitState.Visiting);
        if (!dependencyGraph.containsKey(tid)) {
            return;
        }
        for (TransactionId dependee : dependencyGraph.get(tid)) {
            VisitState state = visitedTransactionIds.get(dependee);
            if (state == VisitState.Visiting) {
                throw new TransactionAbortedException();
            } else if (state == null) {
                testForDeadlock(dependee, visitedTransactionIds);
            }
        }
        visitedTransactionIds.put(tid, VisitState.Visited);
    }

    private void abortIfDeadlocked(TransactionId dependent) throws TransactionAbortedException {
        Map<TransactionId, VisitState> visitedTransactionIds = new HashMap<TransactionId, VisitState>();
        testForDeadlock(dependent, visitedTransactionIds);
    }

    private void addDependency(TransactionId dependent, TransactionId dependee) throws TransactionAbortedException {
        addDependencies(dependent, new HashSet<TransactionId>(Arrays.asList(dependee)));
    }

    private void addDependencies(TransactionId dependent, Set<TransactionId> dependees)
            throws TransactionAbortedException {
        dependencyGraph.putIfAbsent(dependent, new HashSet<TransactionId>());
        Set<TransactionId> dependeesCollection = dependencyGraph.get(dependent);
        boolean addedDependee = false;
        for (TransactionId dependee : dependees) {
            if (!dependeesCollection.contains(dependee) && !dependee.equals(dependent)) {
                addedDependee = true;
                dependeesCollection.add(dependee);
            }
        }
        if (addedDependee) {
            abortIfDeadlocked(dependent);
        }
    }

}
