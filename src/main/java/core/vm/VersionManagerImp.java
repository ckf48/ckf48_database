package core.vm;

import core.common.AbstractCache;
import core.dm.DataManager;
import core.tm.TransactionManager;
import core.vm.Entry;
import core.vm.VersionManager;
import exceptionUtil.Error;
import exceptionUtil.ExceptionDealer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class VersionManagerImp extends AbstractCache<Entry> implements VersionManager {
    DataManager dm;
    TransactionManager tm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lt;

    public VersionManagerImp(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        return Entry.loadEntry(this, uid);
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }

    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        if (t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch (Exception e) {

            ExceptionDealer.shutDown(e);
        }

        try {
            if (Visibility.isVisible(tm, t, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        if (t.err != null) {
            throw t.err;
        }
        byte[] raw = Entry.wrapEntryRaw(xid, data);

        return dm.insert(xid, raw);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        if (t.err != null) {
            throw t.err;
        }
        Entry entry = super.get(uid);
        if (entry == null) {
            return false;
        }
        try {
            if (!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            CountDownLatch l = null;
            try {
                l = lt.add(xid, uid);
            } catch (Exception e) {
                t.err = e;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }

            if (l != null) {
                l.await();
            }

            if (entry.getMax() == xid) {
                return false;
            }

            if (Visibility.isVersionSkip(tm, t, entry)) {
                t.err = new RuntimeException();
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;

            }

            entry.setMax(xid);
            return true;

        } finally {
            entry.release();
        }

    }

    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();


        if (t == null) {
            throw new NullPointerException("transaction not activate");
        }
        if (t.err != null) {
            throw t.err;
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.lock();
        lt.remove(xid);
        tm.commit(xid);
    }

    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if (!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();
        if (t.autoAborted) {
            return;
        }
        lt.remove(xid);
        tm.abort(xid);
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }
}
