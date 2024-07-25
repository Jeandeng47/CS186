package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of
    // the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly,
    // acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of
    // this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
            boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException          if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     *                                       transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        // 0. Error checking

        // Check if a lock is held by the txn
        if (!getExplicitLockType(transaction).equals(LockType.NL)) {
            throw new DuplicateLockRequestException("Duplicate lock request.");
        }
        // Check if the context is readonly
        if (readonly) {
            throw new UnsupportedOperationException("Read-only context.");
        }

        // Check if the lockType is NL, should use release
        if (lockType.equals(LockType.NL)) {
            throw new InvalidLockException("Should use release.");
        }

        // Check if the request is invalid
        if (parent != null && !LockType.canBeParentLock(parent.getEffectiveLockType(transaction), lockType)) {
            throw new InvalidLockException("Invalid lock request.");
        }

        lockman.acquire(transaction, getResourceName(), lockType);
        updateNumChildLocks(transaction, lockType);
    }

    

    // update the number of child locks in parent context
    public void updateNumChildLocks(TransactionContext transaction, LockType newLockType) {
        LockContext currentContext = this;

        while (currentContext.parentContext() != null) {
            LockContext parenContext = currentContext.parentContext();
            // get the current number of child locks
            int numChildLocks = parenContext.getNumChildren(transaction);
            // update the num of child based on lock type
            if (newLockType.equals(LockType.NL)) {
                numChildLocks--; // release a lock
            } else {
                numChildLocks++; // acquire a lock
            }
            // set the update number in parent context
            parenContext.setNumChildren(transaction, numChildLocks);
            currentContext = parenContext;
        }

    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException           if no lock on `name` is held by
     *                                       `transaction`
     * @throws InvalidLockException          if the lock cannot be released because
     *                                       doing so would violate multigranularity
     *                                       locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        // 0. Error checking

        // check if the resource is read only
        if (readonly) {
            throw new UnsupportedOperationException("Read-only context.");
        }

        // Check if a lock is held by the txn
        if (getExplicitLockType(transaction).equals(LockType.NL)) {
            throw new NoLockHeldException("No lock held on " + name);
        }

        // check if the request is invalid
        if (getNumChildren(transaction) > 0) {
            throw new InvalidLockException("Cannot release lock due to multigranularity constraints."); 
        }

        lockman.release(transaction, name);
        updateNumChildLocks(transaction, LockType.NL);
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     *                                       `newLockType` lock
     * @throws NoLockHeldException           if `transaction` has no lock
     * @throws InvalidLockException          if the requested lock type is not a
     *                                       promotion or promoting would cause the
     *                                       lock manager to enter an invalid
     *                                       state (e.g. IS(parent), X(child)). A
     *                                       promotion from lock type A to lock
     *                                       type B is valid if B is substitutable
     *                                       for A and B is not equal to A, or
     *                                       if B is SIX and A is IS/IX/S, and
     *                                       invalid otherwise. hasSIXAncestor may
     *                                       be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        // 0. Error checking

        // check if the resource is read only
        if (readonly) {
            throw new UnsupportedOperationException("Read-only context.");
        }

        // check if new type lock is already held by the txn
        if (getExplicitLockType(transaction).equals(newLockType)) {
            throw new DuplicateLockRequestException("Duplicate lock request.");
        }

        if (parent != null && !LockType.canBeParentLock(parent.getEffectiveLockType(transaction), newLockType)) {
            throw new InvalidLockException("The lock request is invalid");
        }

        // check if the txn has no lock
        LockType currentLockType = getExplicitLockType(transaction);

        // special case of promotion to SIX (from IS/IX/S)
        if (newLockType == LockType.SIX) {
            if (hasSIXAncestor(transaction)) {
                throw new InvalidLockException("Ancestor has SIX lock. Redudant lock request.");
            }
            if (!LockType.substitutable(newLockType, lockman.getLockType(transaction, name))) {
                throw new InvalidLockException("New lockType could not substitute the old LockType.");
            }
            if (lockman.getLocks(transaction).size() == 0) {
                throw new NoLockHeldException("No lock held on this transaction.");
            }
            
            if (currentLockType.equals(LockType.IS) || currentLockType.equals(LockType.IX) 
            || currentLockType.equals(LockType.S)) {
                List<ResourceName> sisDescendants = sisDescendants(transaction);
                sisDescendants.add(name);
                lockman.acquireAndRelease(transaction, name, newLockType, sisDescendants);
            }
            
        } else {
            lockman.promote(transaction, getResourceName(), newLockType);
        }
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException           if `transaction` has no lock at this
     *                                       level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
      
        // Error checking
        if (readonly) {
            throw new UnsupportedOperationException("Read-only context.");
        }

        LockType currentLockType = getExplicitLockType(transaction);
        if (currentLockType.equals(LockType.NL)) {
            throw new NoLockHeldException("No lock held on " + name);
        }

        // Determine the most restrictive lock needed (S or X)
        List<Lock> locks = lockman.getLocks(transaction);
        LockType newLockType = LockType.S;
        for (Lock lock : locks) {
            if (lock.name.isDescendantOf(name) || lock.name.equals(name)) {
                if (lock.lockType == LockType.IX || lock.lockType == LockType.X || lock.lockType == LockType.SIX) {
                    newLockType = LockType.X;
                    break;
                }
            }
        }

        if (currentLockType == newLockType) {
            return;
        }

        // Collect all descendant resource names to be released
        List<ResourceName> descendants = new ArrayList<>();
        for (Lock lock : locks) {
            if (lock.name.isDescendantOf(name)) {
                descendants.add(lock.name);
            }
        }

        // Add the current level lock to the list of locks to be released
        descendants.add(name);

        // Acquire the new lock at the current context level and release all descendant locks
        lockman.acquireAndRelease(transaction, name, newLockType, descendants);

        // Update numChildLocks for all affected contexts
        for (ResourceName descendant : descendants) {
            LockContext descendantContext = fromResourceName(lockman, descendant);
            descendantContext.setNumChildren(transaction, 0);
        }

        // Set the current context's numChildLocks to 0
        setNumChildren(transaction, 0);
        
    }


    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null)
            return LockType.NL;
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction, getResourceName());
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null)
            return LockType.NL;
        // TODO(proj4_part2): implement
        // if there is explicit type, return it
        LockType lockType = getExplicitLockType(transaction);
        if (!lockType.equals(LockType.NL)) {
            return lockType;
        }
        // if no explict, find implicit type
        LockContext parentContext = parentContext();
        while (parentContext != null) {
            // get explicit lock type of parent
            LockType parentLockType = parentContext.getExplicitLockType(transaction);
            // if parent lock type is not intentional (IS/IX), the current lock type 
            // is equal to parent lock type
            if (parentLockType.equals(LockType.S) || parentLockType.equals(LockType.X) || 
            parentLockType.equals(LockType.SIX)) {
                lockType = parentLockType;
            }
            parentContext = parentContext.parent;
        }
        return lockType;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * 
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        LockContext currentContext = parentContext();
        while (currentContext != null) {
            if(currentContext.getExplicitLockType(transaction).equals(LockType.SIX)) {
                return true;
            }
            currentContext = currentContext.parentContext();
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * 
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     *         holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<ResourceName> descendants = new ArrayList<>();
        for (LockContext childContext : children.values()) {
            LockType childLockType = childContext.getExplicitLockType(transaction);
            if (childLockType.equals(LockType.IS) || childLockType.equals(LockType.S)) {
                descendants.add(childContext.getResourceName());
            }
            // recursively to get all the S/IS descendants
            descendants.addAll(childContext.sisDescendants(transaction));
        }
        return descendants;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null)
            child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    public void setNumChildren(TransactionContext transaction, int numChildLocks) {
        if (numChildLocks <= 0) {
            this.numChildLocks.remove(transaction.getTransNum());
        } else {
            this.numChildLocks.put(transaction.getTransNum(), numChildLocks);
        }
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}
