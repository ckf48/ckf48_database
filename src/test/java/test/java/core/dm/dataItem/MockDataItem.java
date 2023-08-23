package test.java.core.dm.dataItem;

import core.common.SubArray;
import core.dm.dataItem.DataItem;
import core.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MockDataItem implements DataItem {
    private SubArray data;
    private byte[] oldData;
    private long uid;
    private Lock rLock;
    private Lock wLock;
    public static MockDataItem newMockDataItem(long uid, SubArray data){
        MockDataItem dataItem = new MockDataItem();
        dataItem.data = data;
        dataItem.uid = uid;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        dataItem.rLock = lock.readLock();
        dataItem.wLock = lock.writeLock();
        dataItem.oldData = new byte[data.end - data.start];

        return dataItem;
    }
    @Override
    public SubArray data() {
        return data;
    }

    @Override
    public void before() {
        wLock.lock();
        System.arraycopy(data.data,data.start,oldData,0,oldData.length);
    }

    @Override
    public void unBefore() {
        System.arraycopy(oldData,0,data.data,data.start,oldData.length);
        wLock.unlock();
    }

    @Override
    public void after(long xid) {
        wLock.unlock();
    }

    @Override
    public void release() {

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
        return null;
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
