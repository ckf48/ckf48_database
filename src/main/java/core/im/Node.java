package core.im;

import core.common.SubArray;
import core.dm.dataItem.DataItem;
import core.tm.TransactionManagerImp;
import core.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Node结构如下：
 * [LeafFlag][KeyNumber][SiblingUid]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 */
public class Node {
    private static final int IS_LEAF_OFFSET = 0;
    private static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET + 1;
    private static final int SIBLING_OFFSET = NO_KEYS_OFFSET + 2;
    private static final int NODE_HEADER_SIZE = SIBLING_OFFSET + 8;

    private static final int BALANCE_NUMBER = 32;
    private static final int NODE_SIZE = NODE_HEADER_SIZE + (2 * 8) * (BALANCE_NUMBER * 2 + 2);

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if (isLeaf) {
            raw.data[raw.start + IS_LEAF_OFFSET] = (byte) 1;
        } else {
            raw.data[raw.start + IS_LEAF_OFFSET] = (byte) 0;
        }

    }

    static boolean getRawIsLeaf(SubArray raw) {
        return raw.data[raw.start + IS_LEAF_OFFSET] == (byte) 1;
    }

    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short) noKeys), 0, raw.data, raw.start + NO_KEYS_OFFSET, 2);
    }

    static int getRawNoKeys(SubArray raw) {
        return (int) Parser.parserShort(Arrays.copyOfRange(raw.data, raw.start + NO_KEYS_OFFSET, raw.start + NO_KEYS_OFFSET + 2));
    }

    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.data, raw.start + SIBLING_OFFSET, 8);
    }

    static long getRawSibling(SubArray raw) {
        return Parser.parserLong(Arrays.copyOfRange(raw.data, raw.start + SIBLING_OFFSET, raw.start + SIBLING_OFFSET + 8));
    }

    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.data, offset, 8);
    }

    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        return Parser.parserLong(Arrays.copyOfRange(raw.data, offset, offset + 8));
    }

    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.data, offset, 8);
    }

    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        return Parser.parserLong(Arrays.copyOfRange(raw.data, offset, offset + 8));
    }

    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start + NODE_HEADER_SIZE + kth * (8 * 2);
        System.arraycopy(from.data, offset, to.data, to.start + NODE_HEADER_SIZE, from.end - offset);
    }

    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start + NODE_HEADER_SIZE + (kth + 1) * (8 * 2);
        int end = raw.start + NODE_SIZE - 1;
        for (int i = end; i >= begin; i--) {
            raw.data[i] = raw.data[i - (8 * 2)];
        }
    }

    static byte[] newRootRaw(long left, long right, long key) {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, false);
        setRawNoKeys(raw, 2);
        setRawSibling(raw, 0);
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.data;
    }

    static byte[] newNilRootRaw() {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);

        return raw.data;
    }

    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    public void release() {
        dataItem.release();
    }

    public boolean isLeaf() {
        dataItem.rlock();
        try {
            return getRawIsLeaf(raw);
        } finally {
            dataItem.rUnlock();
        }
    }

    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    public SearchNextRes searchNext(long key) {
        dataItem.rlock();
        try {
            SearchNextRes res = new SearchNextRes();
            int noKeys = getRawNoKeys(raw);
            for (int i = 0; i < noKeys; i++) {
                long ik = getRawKthKey(raw, i);
                if (key < ik) {
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }

            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;
        } finally {
            dataItem.rUnlock();
        }
    }

    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rlock();
        try {
            int noKeys = getRawNoKeys(raw);
            int kth = 0;
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if (ik >= leftKey) {
                    break;
                }
                kth++;
            }
            List<Long> uids = new ArrayList<>();
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if (ik <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth++;
                } else {
                    break;
                }
            }

            long siblingUid = 0;
            if (kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.rUnlock();
        }
    }

    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }

    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();
        dataItem.before();
        try {
            success = insert(uid, key);
            if (!success) {
                res.siblingUid = getRawSibling(raw);
                return res;
            }

            if (needSplit()) {
                try {
                    SplitRes r = split();
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch (Exception e) {
                    err = e;
                    throw e;
                }
            } else {
                return res;
            }
        } finally {
            if (err == null && success) {
                dataItem.after(TransactionManagerImp.SUPER_XID);
            } else {
                dataItem.unBefore();
            }
        }

    }

    private boolean insert(long uid, long key) {
        int noKeys = getRawNoKeys(raw);
        int kth = 0;
        while (kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if (ik < key) {
                kth++;
            } else {
                break;
            }
        }

        if (kth == noKeys && getRawSibling(raw) != 0) return false;
        if (getRawIsLeaf(raw)) {
            shiftRawKth(raw, kth);
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            setRawNoKeys(raw, noKeys + 1);
        } else {
            long kk = getRawKthKey(raw, kth);
            setRawKthKey(raw, key, kth);
            shiftRawKth(raw, kth + 1);
            setRawKthKey(raw, kk, kth + 1);
            setRawKthSon(raw, uid, kth + 1);
            setRawNoKeys(raw, noKeys + 1);
        }
        return true;
    }

    private boolean needSplit() {
        return BALANCE_NUMBER * 2 == getRawNoKeys(raw);
    }

    class SplitRes {
        long newSon, newKey;
    }

    private SplitRes split() throws Exception {
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(nodeRaw, getRawIsLeaf(raw));
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        setRawSibling(nodeRaw, getRawSibling(raw));
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        long son = tree.dm.insert(TransactionManagerImp.SUPER_XID, nodeRaw.data);
        setRawNoKeys(raw, BALANCE_NUMBER);
        setRawSibling(raw, son);

        SplitRes res = new SplitRes();
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw, 0);
        return res;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIsLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for (int i = 0; i < KeyNumber; i++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }
}
