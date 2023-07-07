package core.dm.page;

import core.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MockPage implements Page {
    private int pgno;
    private byte[] data;

    private Lock lock = new ReentrantLock();

    public static MockPage newMockPage(int pgno,byte[] data){
        MockPage page = new MockPage();
        page.data = data;
        page.pgno = pgno;
        return page;
    }
    public MockPage() {
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {

    }

    @Override
    public void setDirty(boolean dirty) {

    }

    @Override
    public boolean isDirty() {
        return false;
    }

    @Override
    public int getPageNumber() {
        return pgno;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
