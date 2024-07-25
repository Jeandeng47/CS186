package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S, // shared
    X, // exclusive
    IS, // intention shared
    IX, // intention exclusive
    SIX, // shared intention exclusive
    NL; // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        switch (a) {
            case NL:
                return true;
            case IS:
                return !(b == X);
            case IX:
                return b == NL || b == IS || b == IX;
            case S:
                return b == NL || b == IS || b == S;
            case SIX:
                return b == NL || b == IS;
            case X:
                return b == NL;
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
            case S:
                return IS;
            case X:
                return IX;
            case IS:
                return IS;
            case IX:
                return IX;
            case SIX:
                return IX;
            case NL:
                return NL;
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a
     * childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        // Ex: to get an S lock on a table, we must have (at the very least) an IS lock
        // on the
        // parent of table, so canBeParentLock(IS, S) is true
        // canBeParentLock(A , B) returns true if having A on a resource lets a
        // transaction acquire a lock of
        // type B on a child
        switch (parentLockType) {
            case NL:
                return childLockType == NL;
            case IS:
                return childLockType == NL || childLockType == IS || childLockType == S;
            case IX:
                return childLockType == NL || childLockType == IS || childLockType == S || childLockType == X
                        || childLockType == IX || childLockType == SIX;
            case S:
                return childLockType == NL;
            case SIX:
                return childLockType == NL || childLockType == X || childLockType == IX || childLockType == SIX;
            case X:
                return childLockType == NL;
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        // Ex: if a transaction requested an X lock, and we quietly gave it an S lock,
        // there will be problems, so substitutable(S, X) = false

        if (substitute == required || required == NL) {
            return true;
        }

        // the substitute should have more capabilities than required
        switch (substitute) {
            case NL:
                return required == NL;
            case IS:
                return required == NL || required == IS;
            case IX:
                return required == NL || required == IS || required == IX;
            case S:
                return required == NL || required == IS || required == S;
            case SIX:
                return !(required == X);
            case X:
                return true;
            default:
                throw new UnsupportedOperationException("bad lock type");
        }

    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
            case S:
                return "S";
            case X:
                return "X";
            case IS:
                return "IS";
            case IX:
                return "IX";
            case SIX:
                return "SIX";
            case NL:
                return "NL";
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }
}
