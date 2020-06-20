package simpledb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.HashMap;

public class TransactionLockManager {
    private final ConcurrentMap<PageId, Object> locks;
    private final ConcurrentMap<PageId, List<TransactionId>> sharedLocks;
    private final ConcurrentMap<PageId, TransactionId> exclusiveLocks;
    private final ConcurrentMap<TransactionId, Set<PageId>> pageIdsLockedByTransaction;
    private final ConcurrentMap<TransactionId, Set<TransactionId>> dependencyGraph;

    public TransactionLockManager() {
        this.locks = new ConcurrentHashMap<PageId, Object>();
        this.sharedLocks = new ConcurrentHashMap<PageId, List<TransactionId>>();
        this.exclusiveLocks = new ConcurrentHashMap<PageId, TransactionId>();
        this.pageIdsLockedByTransaction = new ConcurrentHashMap<TransactionId, Set<PageId>>();
        this.dependencyGraph = new ConcurrentHashMap<TransactionId, Set<TransactionId>>();
    }

    public Set<PageId> getPagesInTransaction(TransactionId tid) {
        Set<PageId> pages = this.pageIdsLockedByTransaction.get(tid);
        if (pages == null) {
            pages = new HashSet<PageId>();
        }
        return pages;
    }

    public void acquireLock(TransactionId tid, PageId pid, Permissions permissions) throws TransactionAbortedException {
        if (permissions == Permissions.READ_ONLY) {
            if (hasReadPermissions(tid, pid)) {
                return;
            }
            acquireReadOnlyLock(tid, pid);
        } else if (permissions == Permissions.READ_WRITE) {
            if (hasWritePermissions(tid, pid)) {
                return;
            }
            acquireReadWriteLock(tid, pid);
        } else {
            throw new IllegalArgumentException("Expected either READ_ONLY or READ_WRITE permissions.");
        }
        addPageToTransactionLocks(tid, pid);
    }

    public void releasePage(TransactionId tid, PageId pid) {
        releaseLock(tid, pid);
        if (pageIdsLockedByTransaction.containsKey(tid)) {
            pageIdsLockedByTransaction.get(tid).remove(pid);
        }
    }

    public void releasePages(TransactionId transactionId) {
        if (pageIdsLockedByTransaction.containsKey(transactionId)) {
            Set<PageId> pageIds = pageIdsLockedByTransaction.get(transactionId);
            for (PageId pageId : pageIds) {
                releaseLock(transactionId, pageId);
            }
            pageIdsLockedByTransaction.replace(transactionId, new HashSet<PageId>());
        }
    }

    private Object getLock(PageId pageId) {
        locks.putIfAbsent(pageId, new Object());
        return locks.get(pageId);
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        if (!pageIdsLockedByTransaction.containsKey(tid)) {
            return false;
        }
        return pageIdsLockedByTransaction.get(tid).contains(pid);
    }

    private void releaseLock(TransactionId tid, PageId pid) {
        Object lock = getLock(pid);
        synchronized (lock) {
            exclusiveLocks.remove(pid);
            if (sharedLocks.containsKey(pid)) {
                sharedLocks.get(pid).remove(tid);
            }
        }
    }

    private void addSharedUser(TransactionId tid, PageId pid) {
        if (!sharedLocks.containsKey(pid)) {
            sharedLocks.put(pid, new ArrayList<TransactionId>());
        }
        sharedLocks.get(pid).add(tid);
    }

    private boolean hasReadPermissions(TransactionId tid, PageId pid) {
        if (hasWritePermissions(tid, pid)) {
            return true;
        }
        List<TransactionId> tids = sharedLocks.get(pid);
        return tids != null && tids.contains(tid);
    }

    private boolean hasWritePermissions(TransactionId tid, PageId pid) {
        TransactionId tid2 = exclusiveLocks.get(pid);
        return tid2 != null && tid2.equals(tid);
    }

    private boolean acquireReadOnlyLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        Object lock = getLock(pid);
        while (true) {
            synchronized (lock) {
                TransactionId exclusiveLockHolder = exclusiveLocks.get(pid);
                if (exclusiveLockHolder == null || tid.equals(exclusiveLockHolder)) {
                    removeDependencies(tid);
                    addSharedUser(tid, pid);
                    return true;
                }
                addDependency(tid, exclusiveLockHolder);
            }
        }
    }

    private boolean acquireReadWriteLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        Object lock = getLock(pid);
        while (true) {
            synchronized (lock) {
                List<TransactionId> lockHolders = getLockHolders(pid);
                if (!isLockedByOthers(tid, lockHolders)) {
                    removeDependencies(tid);
                    addExclusiveUser(tid, pid);
                    return true;
                }
                addDependencies(tid, lockHolders);
            }
        }
    }

    private void addPageToTransactionLocks(TransactionId tid, PageId pid) {
        pageIdsLockedByTransaction.putIfAbsent(tid, new HashSet<PageId>());
        pageIdsLockedByTransaction.get(tid).add(pid);
    }

    private boolean isLockedByOthers(TransactionId transactionId, List<TransactionId> lockHolders) {
        if (lockHolders == null || lockHolders.isEmpty()) {
            return false;
        }
        if (lockHolders.size() == 1 && transactionId.equals(lockHolders.iterator().next())) {
            return false;
        }
        return true;
    }

    private List<TransactionId> getLockHolders(PageId pid) {
        List<TransactionId> lockHolders = new ArrayList<TransactionId>();
        if (exclusiveLocks.containsKey(pid)) {
            lockHolders.add(exclusiveLocks.get(pid));
            return lockHolders;
        }
        if (sharedLocks.containsKey(pid)) {
            lockHolders.addAll(sharedLocks.get(pid));
        }
        return lockHolders;
    }

    private void addExclusiveUser(TransactionId tid, PageId pid) {
        exclusiveLocks.put(pid, tid);
    }

    private void removeDependencies(TransactionId dependent) {
        dependencyGraph.remove(dependent);
    }

    private void testForDeadlock(TransactionId tid, Set<TransactionId> visitedTransactionIds,
            Stack<TransactionId> parents) throws TransactionAbortedException {
        visitedTransactionIds.add(tid);
        if (!dependencyGraph.containsKey(tid)) {
            return;
        }
        for (TransactionId dependee : dependencyGraph.get(tid)) {
            if (parents.contains(dependee)) {
                throw new TransactionAbortedException();
            }
            if (!visitedTransactionIds.contains(dependee)) {
                parents.push(tid);
                testForDeadlock(dependee, visitedTransactionIds, parents);
                parents.pop();
            }
        }
    }

    private void abortIfDeadlocked() throws TransactionAbortedException {
        // System.exit(-1);
        Set<TransactionId> visitedTransactionIds = new HashSet<TransactionId>();
        for (TransactionId tid : dependencyGraph.keySet()) {
            if (!visitedTransactionIds.contains(tid)) {
                testForDeadlock(tid, visitedTransactionIds, new Stack<TransactionId>());
            }
        }
    }

    private void addDependency(TransactionId dependent, TransactionId dependee) throws TransactionAbortedException {
        addDependencies(dependent, Arrays.asList(dependee));
    }

    private void addDependencies(TransactionId dependent, List<TransactionId> dependees)
            throws TransactionAbortedException {
        // dependencyGraph.put(dependent, new HashSet<TransactionId>());
        dependencyGraph.putIfAbsent(dependent, new HashSet<TransactionId>());
        Set<TransactionId> dependeesCollection = dependencyGraph.get(dependent);
        boolean addedDependee = false;
        for (TransactionId newDependee : dependees) {
            if (!dependeesCollection.contains(newDependee) && !newDependee.equals(dependent)) {
                addedDependee = true;
                dependeesCollection.add(newDependee);
            }
        }
        if (addedDependee) {
            abortIfDeadlocked();
        }
    }
}