package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.RecordId;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * A inner node of a B+ tree. Every inner node in a B+ tree of order d stores
 * between d and 2d keys. An inner node with n keys stores n + 1 "pointers" to
 * children nodes (where a pointer is just a page number). Moreover, every
 * inner node is serialized and persisted on a single page; see toBytes and
 * fromBytes for details on how an inner node is serialized. For example, here
 * is an illustration of an order 2 inner node:
 *
 * +----+----+----+----+
 * | 10 | 20 | 30 | |
 * +----+----+----+----+
 * / | | \
 */
class InnerNode extends BPlusNode {
    // Metadata about the B+ tree that this node belongs to.
    private BPlusTreeMetadata metadata;

    // Buffer manager
    private BufferManager bufferManager;

    // Lock context of the B+ tree
    private LockContext treeContext;

    // The page on which this leaf is serialized.
    private Page page;

    // The keys and child pointers of this inner node. See the comment above
    // LeafNode.keys and LeafNode.rids in LeafNode.java for a warning on the
    // difference between the keys and children here versus the keys and children
    // stored on disk. `keys` is always stored in ascending order.
    private List<DataBox> keys;
    private List<Long> children;

    // Constructors ////////////////////////////////////////////////////////////
    /**
     * Construct a brand new inner node.
     */
    InnerNode(BPlusTreeMetadata metadata, BufferManager bufferManager, List<DataBox> keys,
            List<Long> children, LockContext treeContext) {
        this(metadata, bufferManager, bufferManager.fetchNewPage(treeContext, metadata.getPartNum()),
                keys, children, treeContext);
    }

    /**
     * Construct an inner node that is persisted to page `page`.
     */
    private InnerNode(BPlusTreeMetadata metadata, BufferManager bufferManager, Page page,
            List<DataBox> keys, List<Long> children, LockContext treeContext) {
        try {
            assert (keys.size() <= 2 * metadata.getOrder());
            assert (keys.size() + 1 == children.size());

            this.metadata = metadata;
            this.bufferManager = bufferManager;
            this.treeContext = treeContext;
            this.page = page;
            this.keys = new ArrayList<>(keys);
            this.children = new ArrayList<>(children);
            sync();
        } finally {
            page.unpin();
        }
    }

    // Core API ////////////////////////////////////////////////////////////////
    // See BPlusNode.get.
    @Override
    public LeafNode get(DataBox key) {
        // TODO(proj2): implement
        // recursive case
        int childIndex = numLessThanEqual(key, keys); // could be replaced by binary search
        return getChild(childIndex).get(key);
    }

    // See BPlusNode.getLeftmostLeaf.
    @Override
    public LeafNode getLeftmostLeaf() {
        assert (children.size() > 0);
        // TODO(proj2): implement
        // recursive case
        int leftMostChildIndex = 0;
        return getChild(leftMostChildIndex).getLeftmostLeaf();
    }

    // See BPlusNode.put.
    @Override
    public Optional<Pair<DataBox, Long>> put(DataBox key, RecordId rid) {
        // TODO(proj2): implement

        // 1. Recursively navigate to the target child node
        int childIndex = numLessThanEqual(key, keys);
        BPlusNode child = getChild(childIndex); // inner node or leaf node

        // 2. Recursively insert the new node (down to the leaf)
        Optional<Pair<DataBox, Long>> leafPair = child.put(key, rid);

        // 3. case 1: insertion does not cause a split
        // Note: in LeafNode.put(), if split we return the splitLeafNode otherwise
        // Optional.empty()

        if (!leafPair.isPresent()) {
            return Optional.empty(); // do nothing
        }

        // 4. case 2: insertion causes a split, handle split
        // Note: in LeafNode.put(), it returns the splitLeafNode: Optional.of(new
        // Pair<>(splitKey, rightNodePageNum))
        DataBox newKey = leafPair.get().getFirst();
        Long newPage = leafPair.get().getSecond();

        // Insert the new key into the inner node
        keys.add(childIndex, newKey); // newKey == key here
        children.add(childIndex + 1, newPage);

        // 5. Check if the node (after update) overflows
        int d = metadata.getOrder();
        if (keys.size() > 2 * d) {
            return splitInnerNode();
        }

        sync();
        return Optional.empty();
    }

    // Helper method to split the inner node
    private Optional<Pair<DataBox, Long>> splitInnerNode() {
        // 1. Find split point, left - d items, right - the rest
        int d = metadata.getOrder();
        int splitPoint = d;

        // 2. Get right half of keys and children
        List<DataBox> rightKeys = new ArrayList<>(keys.subList(splitPoint + 1, keys.size()));
        List<Long> rightChildren = new ArrayList<>(children.subList(splitPoint + 1, children.size()));

        // 3. Identify the split key (move up)
        DataBox splitKey = keys.get(splitPoint);

        // 4. Clear orginal node's right half
        keys.subList(splitPoint, keys.size()).clear(); // also remove split point (we move it up)
        children.subList(splitPoint + 1, children.size()).clear();

        // 5. Create new right node
        InnerNode rightInnerNode = new InnerNode(metadata, bufferManager, rightKeys, rightChildren, treeContext);
        long rightNodePageNum = rightInnerNode.getPage().getPageNum();

        // 6. Sync changes
        sync();
        rightInnerNode.sync();

        // 7. Return the split information
        return Optional.of(new Pair<>(splitKey, rightNodePageNum));
    }

    // See BPlusNode.bulkLoad.
    @Override
    public Optional<Pair<DataBox, Long>> bulkLoad(Iterator<Pair<DataBox, RecordId>> data,
            float fillFactor) {
        // TODO(proj2): implement
        if (!data.hasNext()) {
            return Optional.empty();
        }

        // 1. Recursively navigate to the rightmost child
        BPlusNode rightMostChild = getChild(children.size() - 1);

        // 2. Recursively insert data to rightmost
        Optional<Pair<DataBox, Long>> leafPair = rightMostChild.bulkLoad(data, fillFactor);

        // 3. Handle possible spilts returned from leaf
        // case 1: if there is spilt (LeafNode return spilt key and new page)
        while (leafPair.isPresent()) {
            DataBox newkey = leafPair.get().getFirst();
            Long newPage = leafPair.get().getSecond();

            // 4. Insert the new key and new page into current inner node
            int index = numLessThan(newkey, keys);
            keys.add(index, newkey);
            children.add(index + 1, newPage);

            // 5. Check if the current inner node overflows (same as put())
            int d = metadata.getOrder();
            if (keys.size() > 2 * d) {
                return splitInnerNode();
            }

            // 6. Continue loading if there are more entries
            if (data.hasNext()) {
                leafPair = rightMostChild.bulkLoad(data, fillFactor);
            } else {
                break;
            }

            // Return info of the spilt key and the new page
        }

        // case 2: there is no spilt
        sync();
        return Optional.empty();

    }

    // See BPlusNode.remove.
    @Override
    public void remove(DataBox key) {
        // TODO(proj2): implement

        BPlusNode removeLeaf = get(key);
        removeLeaf.remove(key);
        sync();
        return;
    }

    // Helpers /////////////////////////////////////////////////////////////////
    @Override
    public Page getPage() {
        return page;
    }

    private BPlusNode getChild(int i) {
        long pageNum = children.get(i);
        return BPlusNode.fromBytes(metadata, bufferManager, treeContext, pageNum);
    }

    private void sync() {
        page.pin();
        try {
            Buffer b = page.getBuffer();
            byte[] newBytes = toBytes();
            byte[] bytes = new byte[newBytes.length];
            b.get(bytes);
            if (!Arrays.equals(bytes, newBytes)) {
                page.getBuffer().put(toBytes());
            }
        } finally {
            page.unpin();
        }
    }

    // Just for testing.
    List<DataBox> getKeys() {
        return keys;
    }

    // Just for testing.
    List<Long> getChildren() {
        return children;
    }

    /**
     * Returns the largest number d such that the serialization of an InnerNode
     * with 2d keys will fit on a single page.
     */
    static int maxOrder(short pageSize, Type keySchema) {
        // A leaf node with n entries takes up the following number of bytes:
        //
        // 1 + 4 + (n * keySize) + ((n + 1) * 8)
        //
        // where
        //
        // - 1 is the number of bytes used to store isLeaf,
        // - 4 is the number of bytes used to store n,
        // - keySize is the number of bytes used to store a DataBox of type
        // keySchema, and
        // - 8 is the number of bytes used to store a child pointer.
        //
        // Solving the following equation
        //
        // 5 + (n * keySize) + ((n + 1) * 8) <= pageSizeInBytes
        //
        // we get
        //
        // n = (pageSizeInBytes - 13) / (keySize + 8)
        //
        // The order d is half of n.
        int keySize = keySchema.getSizeInBytes();
        int n = (pageSize - 13) / (keySize + 8);
        return n / 2;
    }

    /**
     * Given a list ys sorted in ascending order, numLessThanEqual(x, ys) returns
     * the number of elements in ys that are less than or equal to x. For
     * example,
     *
     * numLessThanEqual(0, Arrays.asList(1, 2, 3, 4, 5)) == 0
     * numLessThanEqual(1, Arrays.asList(1, 2, 3, 4, 5)) == 1
     * numLessThanEqual(2, Arrays.asList(1, 2, 3, 4, 5)) == 2
     * numLessThanEqual(3, Arrays.asList(1, 2, 3, 4, 5)) == 3
     * numLessThanEqual(4, Arrays.asList(1, 2, 3, 4, 5)) == 4
     * numLessThanEqual(5, Arrays.asList(1, 2, 3, 4, 5)) == 5
     * numLessThanEqual(6, Arrays.asList(1, 2, 3, 4, 5)) == 5
     *
     * This helper function is useful when we're navigating down a B+ tree and
     * need to decide which child to visit. For example, imagine an index node
     * with the following 4 keys and 5 children pointers:
     *
     * +---+---+---+---+
     * | a | b | c | d |
     * +---+---+---+---+
     * / | | | \
     * 0 1 2 3 4
     *
     * If we're searching the tree for value c, then we need to visit child 3.
     * Not coincidentally, there are also 3 values less than or equal to c (i.e.
     * a, b, c).
     */
    static <T extends Comparable<T>> int numLessThanEqual(T x, List<T> ys) {
        int n = 0;
        for (T y : ys) {
            if (y.compareTo(x) <= 0) {
                ++n;
            } else {
                break;
            }
        }
        return n;
    }

    static <T extends Comparable<T>> int numLessThan(T x, List<T> ys) {
        int n = 0;
        for (T y : ys) {
            if (y.compareTo(x) < 0) {
                ++n;
            } else {
                break;
            }
        }
        return n;
    }

    // Pretty Printing /////////////////////////////////////////////////////////
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < keys.size(); ++i) {
            sb.append(children.get(i)).append(" ").append(keys.get(i)).append(" ");
        }
        sb.append(children.get(children.size() - 1)).append(")");
        return sb.toString();
    }

    @Override
    public String toSexp() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < keys.size(); ++i) {
            sb.append(getChild(i).toSexp()).append(" ").append(keys.get(i)).append(" ");
        }
        sb.append(getChild(children.size() - 1).toSexp()).append(")");
        return sb.toString();
    }

    /**
     * An inner node on page 0 with a single key k and two children on page 1 and
     * 2 is turned into the following DOT fragment:
     *
     * node0[label = "<f0>|k|<f1>"];
     * ... // children
     * "node0":f0 -> "node1";
     * "node0":f1 -> "node2";
     */
    @Override
    public String toDot() {
        List<String> ss = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            ss.add(String.format("<f%d>", i));
            ss.add(keys.get(i).toString());
        }
        ss.add(String.format("<f%d>", keys.size()));

        long pageNum = getPage().getPageNum();
        String s = String.join("|", ss);
        String node = String.format("  node%d[label = \"%s\"];", pageNum, s);

        List<String> lines = new ArrayList<>();
        lines.add(node);
        for (int i = 0; i < children.size(); ++i) {
            BPlusNode child = getChild(i);
            long childPageNum = child.getPage().getPageNum();
            lines.add(child.toDot());
            lines.add(String.format("  \"node%d\":f%d -> \"node%d\";",
                    pageNum, i, childPageNum));
        }

        return String.join("\n", lines);
    }

    // Serialization ///////////////////////////////////////////////////////////
    @Override
    public byte[] toBytes() {
        // When we serialize an inner node, we write:
        //
        // a. the literal value 0 (1 byte) which indicates that this node is not
        // a leaf node,
        // b. the number n (4 bytes) of keys this inner node contains (which is
        // one fewer than the number of children pointers),
        // c. the n keys, and
        // d. the n+1 children pointers.
        //
        // For example, the following bytes:
        //
        // +----+-------------+----+-------------------------+-------------------------+
        // | 00 | 00 00 00 01 | 01 | 00 00 00 00 00 00 00 03 | 00 00 00 00 00 00 00 07 |
        // +----+-------------+----+-------------------------+-------------------------+
        // \__/ \___________/ \__/ \_________________________________________________/
        // a b c d
        //
        // represent an inner node with one key (i.e. 1) and two children pointers
        // (i.e. page 3 and page 7).

        // All sizes are in bytes.
        assert (keys.size() <= 2 * metadata.getOrder());
        assert (keys.size() + 1 == children.size());
        int isLeafSize = 1;
        int numKeysSize = Integer.BYTES;
        int keysSize = metadata.getKeySchema().getSizeInBytes() * keys.size();
        int childrenSize = Long.BYTES * children.size();
        int size = isLeafSize + numKeysSize + keysSize + childrenSize;

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put((byte) 0);
        buf.putInt(keys.size());
        for (DataBox key : keys) {
            buf.put(key.toBytes());
        }
        for (Long child : children) {
            buf.putLong(child);
        }
        return buf.array();
    }

    /**
     * Loads an inner node from page `pageNum`.
     */
    public static InnerNode fromBytes(BPlusTreeMetadata metadata,
            BufferManager bufferManager, LockContext treeContext, long pageNum) {
        Page page = bufferManager.fetchPage(treeContext, pageNum);
        Buffer buf = page.getBuffer();

        byte nodeType = buf.get();
        assert (nodeType == (byte) 0);

        List<DataBox> keys = new ArrayList<>();
        List<Long> children = new ArrayList<>();
        int n = buf.getInt();
        for (int i = 0; i < n; ++i) {
            keys.add(DataBox.fromBytes(buf, metadata.getKeySchema()));
        }
        for (int i = 0; i < n + 1; ++i) {
            children.add(buf.getLong());
        }
        return new InnerNode(metadata, bufferManager, page, keys, children, treeContext);
    }

    // Builtins ////////////////////////////////////////////////////////////////
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof InnerNode)) {
            return false;
        }
        InnerNode n = (InnerNode) o;
        return page.getPageNum() == n.page.getPageNum() &&
                keys.equals(n.keys) &&
                children.equals(n.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(page.getPageNum(), keys, children);
    }
}
