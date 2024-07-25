package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     * lock type can be, and think about how ancestor looks will need to be
     * acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null)
            return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        // considering both explicit and inherited locks
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        // explicitly holds on the current context
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement

        // case 1: current lock type could substitute the requested type
        // the current lock has more capabilities, no need for new lock
        if (LockType.substitutable(effectiveLockType, requestType)) {
            return;
        }

        // case 2: current lock type is IX and the requested lock is S
        // we could promote the lock to SIX (S+IX)
        if (explicitLockType.equals(LockType.IX) && requestType.equals(LockType.S)) {
            lockContext.promote(transaction, LockType.SIX);
            return;
        }

        // case 3: current lock type is an intent lock (escalate the lock to consolidate
        // any descendant locks into a single lock at the current level)
        // database
        //   ├── table1 (Lock IX)
        //   │     ├── page1 (Lock S)
        //   │     ├── page2 (Lock S)
        //   │     └── page3
        //   └── table2
        // ensureSufficientLockHeld(page1Context, LockType.X) : 
        // - parentContext is table1, effectiveLockType is S, current explicit lock type is IX. 
        // - After calling escalate on page1, the locks on page1 and page2 (both S) might be escalated
        // into a single lock on table1, which is lock type X
        // - If not escalate the lock, it could proceed to acquire or promote the lock as needed


        if (explicitLockType.isIntent()) {
            lockContext.escalate(transaction);
            explicitLockType = lockContext.getExplicitLockType(transaction);
            if (explicitLockType.equals(requestType) || explicitLockType.equals(LockType.X)) {
                return;
            }
        }


        // case 4: none of the above (explicit lock type: S or NL)
        // possible (explicitlock, requestlock) combination: (NL, S), (NL, X), (S, X)

        // ensure the ancestors have correct locks
        if (requestType.equals(LockType.S)) {
            ensureAncestorsLocks(transaction, parentContext, LockType.IS);
        } else {
            ensureAncestorsLocks(transaction, parentContext, LockType.IX);
        }

        if (explicitLockType.equals(LockType.NL)) {
            lockContext.acquire(transaction, requestType);
        } else {
            lockContext.promote(transaction, requestType);
        }
       
        return;
    }

    // TODO(proj4_part2) add any helper methods you want
    // case 4:
    private static void ensureAncestorsLocks(TransactionContext transaction, LockContext lockContext, LockType lockType) {
        if (lockContext == null)
            return;
        ensureAncestorsLocks(transaction, lockContext.parentContext(), lockType);
        LockType currLockType = lockContext.getExplicitLockType(transaction);
        if (!LockType.substitutable(currLockType, lockType)) {
            if (currLockType.equals(LockType.NL)) {
                lockContext.acquire(transaction, lockType);
            } else {
                lockContext.promote(transaction, lockType);
            }
        }
    }

   
}
