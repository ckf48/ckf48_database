package core.dm.dataItem;

import core.common.SubArray;
import core.dm.DataManagerImp;
import core.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 */
public class DataItemImp implements DataItem {
    public static final int OFFSET_VALID = 0;
    public static final int OFFSET_SIZE = OFFSET_VALID + 1;
    public static final int OFFSET_DATA = OFFSET_SIZE + 2;

    private final SubArray data;
    private final byte[] oldData;
    private final Lock rLock;
    private final Lock wLock;
    private final DataManagerImp dm;
    private final long uid;
    private final Page page;

    public DataItemImp(SubArray data, byte[] oldData, Page page, long uid, DataManagerImp dm) {
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.dm = dm;
        this.data = data;
        this.oldData = oldData;
        this.page = page;
        this.uid = uid;
    }

    public boolean isValid() {
        return data.data[data.start + OFFSET_VALID] == (byte) 0;
    }

    @Override
    public SubArray data() {
        return new SubArray(data.data, data.start + OFFSET_DATA, data.end);
    }

    @Override
    public void before() {
        wLock.lock();
        page.setDirty(true);
        System.arraycopy(data.data, data.start, oldData, 0, oldData.length);
    }

    @Override
    public void unBefore() {
        System.arraycopy(oldData, 0, data.data, data.start, oldData.length);
        wLock.unlock();

    }

    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this);
        wLock.unlock();
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void wlock() {
        wLock.lock();
    }

    @Override
    public void wUnlock() {
        wLock.unlock();
    }

    @Override
    public void rlock() {
        rLock.lock();
    }

    @Override
    public void rUnlock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return page;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldData;
    }

    @Override
    public SubArray getRaw() {
        return data;
    }
}
