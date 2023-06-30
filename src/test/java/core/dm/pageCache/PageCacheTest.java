package test.java.core.dm.pageCache;

import core.dm.page.Page;
import core.dm.pageCache.MockPageCache;
import core.dm.pageCache.PageCache;
import exceptionUtil.ExceptionDealer;
import org.junit.Test;


import java.io.File;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.*;

public class PageCacheTest {
    static Random random = new SecureRandom();

    @Test
    public void testPageCache() throws Exception{
        PageCache pageCache = PageCache.create("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/db/test/test0",50 * PageCache.PAGE_SIZE);
        for(int i = 0; i < 100; i++){
            byte[] data = new byte[PageCache.PAGE_SIZE];
            data[0] = (byte) i;
            int pgno = pageCache.newPage(data);
            Page page = pageCache.getPage(pgno);
            page.setDirty(true);
            page.release();
        }

        pageCache.close();

        pageCache = PageCache.open("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/db/test/test0",50 * PageCache.PAGE_SIZE);
        for(int i = 1; i <= 100; i++){
            Page page = pageCache.getPage(i);
            byte a = page.getData()[0];
            byte b = (byte)(i - 1);
            assertEquals(a,b);
            page.release();
        }

        pageCache.close();

        assertTrue(new File("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/db/test/test0.db").delete());

    }

    private PageCache pc1;
    private CountDownLatch cdl1;
    private AtomicInteger noPages1;

    @Test
    public void testPageCacheMultiSimple() throws Exception{
        pc1 = PageCache.create("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/db/test/test1",PageCache.PAGE_SIZE * 180);
        cdl1 = new CountDownLatch(200);
        noPages1 = new AtomicInteger(0);
        for(int i = 0; i < 200; i++){
            Runnable r = this::worker1;
            new Thread(r).start();
        }

        cdl1.await();
        assertTrue(new File("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/db/test/test1.db").delete());
    }

    private void worker1(){
        for(int i = 0; i < 100; i++){
            int operation = Math.abs(random.nextInt() % 20);
            if(operation == 0){
                byte[] data = new byte[PageCache.PAGE_SIZE];
                random.nextBytes(data);
                int pgno = pc1.newPage(data);
                Page page = null;
                try{
                    page = pc1.getPage(pgno);
                } catch (Exception e) {
                    ExceptionDealer.shutDown(e);
                }
                noPages1.incrementAndGet();
                if (page != null) {
                    page.release();
                }
            } else {
                int mod = noPages1.intValue();
                if(mod == 0){
                    continue;
                }

                int pgno = Math.abs(random.nextInt()) % mod + 1;
                Page page = null;

                try{
                    page = pc1.getPage(pgno);
                } catch (Exception e) {
                    ExceptionDealer.shutDown(e);
                }
                if (page != null) {
                    page.release();
                }
            }
        }

        cdl1.countDown();
    }

    private PageCache pc2, mpc;
    private CountDownLatch cdl2;
    private AtomicInteger noPages2;
    private Lock lock;

    @Test
    public void testPageCacheMulti() throws InterruptedException {
        pc2 = PageCache.create("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/db/test/test2",PageCache.PAGE_SIZE * 100);
        mpc = new MockPageCache();
        lock = new ReentrantLock();
        noPages2 = new AtomicInteger(0);
        cdl2 = new CountDownLatch(100);

        for(int i = 0; i < 100; i++){
            Runnable r = this::worker2;
            new Thread(r).start();
        }
        cdl2.await();
        assertTrue(new File("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/db/test/test2.db").delete());
    }

    private void worker2(){
        for(int i = 0; i < 1000; i++){
            int operation = Math.abs(random.nextInt() % 20);
            if(operation == 0){
                byte[] data = new byte[PageCache.PAGE_SIZE];
                random.nextBytes(data);
                lock.lock();
                int pgno = pc2.newPage(data);
                int mpgno = mpc.newPage(data);
                assertEquals(pgno, mpgno);
                lock.unlock();
                noPages2.incrementAndGet();
            } else if (operation < 10) {
                int mod = noPages2.intValue();
                if(mod == 0){
                    continue;
                }
                int pgno = Math.abs(random.nextInt()) % mod + 1;
                Page pg = null;
                Page mpg = null;

                try {
                    pg = pc2.getPage(pgno);
                } catch (Exception e) {
                    ExceptionDealer.shutDown(e);
                }

                try {
                    mpg = mpc.getPage(pgno);
                } catch (Exception e) {
                    ExceptionDealer.shutDown(e);
                }

                if (pg != null && mpg != null) {
                    pg.lock();
                    mpg.lock();
                    assertArrayEquals(pg.getData(), mpg.getData());
                    pg.unlock();
                    mpg.unlock();
                    pg.release();
                }

            }else{
                int mod = noPages2.intValue();
                if(mod == 0){
                    continue;
                }
                int pgno = Math.abs(random.nextInt()) % mod + 1;
                Page pg = null;
                Page mpg = null;

                try {
                    pg = pc2.getPage(pgno);
                } catch (Exception e) {
                    ExceptionDealer.shutDown(e);
                }

                try {
                    mpg = mpc.getPage(pgno);
                } catch (Exception e) {
                    ExceptionDealer.shutDown(e);
                }

                byte[] data = new byte[PageCache.PAGE_SIZE];
                random.nextBytes(data);

                assert pg != null;
                assert mpg != null;
                pg.lock();
                pg.setDirty(true);
                for(int j = 0; j < PageCache.PAGE_SIZE;j ++){
                    pg.getData()[j] = data[j];
                }
                for(int j = 0; j < PageCache.PAGE_SIZE;j ++){
                    mpg.getData()[j] = data[j];
                }
                pg.release();
                pg.unlock();


            }
        cdl2.countDown();
        }
    }

}
