package test.java.core.im;

import core.dm.DataManager;
import core.dm.pageCache.PageCache;
import core.im.BPlusTree;
import core.tm.TransactionManager;
import org.junit.Test;
import test.java.core.tm.MockTransactionManager;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class BPlusTreeTest {
    @Test
    public void testTreeSingle() throws Exception {
        TransactionManager tm = new MockTransactionManager();
        DataManager dm = DataManager.create("/tmp/TestTreeSingle", PageCache.PAGE_SIZE * 10, tm);

        long root = BPlusTree.create(dm);
        BPlusTree tree = BPlusTree.load(root, dm);

        int lim = 10000;
        for (int i = lim - 1; i >= 0; i--) {
            tree.insert(i, i);
        }

        for (int i = 0; i < lim; i++) {
            List<Long> uids = tree.search(i);
            assert uids.size() == 1;
            assert uids.get(0) == i;
        }

        assertTrue(new File("/tmp/TestTreeSingle.db").delete());
        assertTrue(new File("/tmp/TestTreeSingle.log").delete());
    }
}
