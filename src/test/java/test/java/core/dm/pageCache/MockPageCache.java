package test.java.core.dm.pageCache;


import test.java.core.dm.page.MockPage;
import core.dm.page.Page;
import core.dm.pageCache.PageCache;
import test.java.core.dm.page.MockPage;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MockPageCache implements PageCache {
    private Map<Integer, MockPage> cache = new HashMap<>();
    private Lock lock = new ReentrantLock();
    private AtomicInteger noPages = new AtomicInteger(0);

    public MockPageCache(){

    }
    @Override
    public int newPage(byte[] initData) {
        lock.lock();
        int pgno = noPages.incrementAndGet();
        MockPage pg = MockPage.newMockPage(pgno,initData);
        cache.put(pgno,pg);
        lock.unlock();
        return pgno;
    }

    @Override
    public Page getPage(int pgno) throws Exception {
        lock.lock();
        Page ret = cache.get(pgno);
        lock.unlock();
        return ret;
    }

    @Override
    public void close() {

    }

    @Override
    public void release(Page page) {

    }

    @Override
    public void truncateByBgno(int maxPgno) {

    }

    @Override
    public int getPageNumber() {
        return noPages.intValue();
    }

    @Override
    public void flushPage(Page pg) {

    }
}
