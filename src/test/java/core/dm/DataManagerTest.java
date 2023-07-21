package test.java.core.dm;

import core.common.SubArray;
import core.dm.DataManager;
import core.dm.dataItem.DataItem;
import core.dm.pageCache.PageCache;
import test.java.core.tm.MockTransactionManager;
import core.tm.TransactionManager;
import core.utils.RandomUtil;
import exceptionUtil.ExceptionDealer;
import org.junit.Test;
import test.java.core.dm.MockDataManager;

import java.io.File;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.*;

public class DataManagerTest {
    static List<Long> uids0,uids1;
    static Lock lock;
    static Random random = new SecureRandom();

    private void initUids(){
        uids0 = new ArrayList<>();
        uids1 = new ArrayList<>();
        lock = new ReentrantLock();

    }

    private void worker(DataManager dm0, DataManager dm1, int taskNum, int insertRation, CountDownLatch cdl) throws Exception {
        int dataLen = 60;
        try {
            for(int i = 0; i < taskNum; i++){
                int op = Math.abs(random.nextInt()) % 100;
                if(op < insertRation){
                    byte[] data = RandomUtil.randomBytes(dataLen);
                    long u0 = 0;
                    long u1 = 0;
                    try {
                        u0 = dm0.insert(0,data);
                    } catch (Exception e) {
                        continue;
                    }

                    try{
                        u1 = dm1.insert(0,data);
                    } catch (Exception e) {
                        ExceptionDealer.shutDown(e);
                    }

                    lock.lock();
                    uids0.add(u0);
                    uids1.add(u1);
                    lock.unlock();

                }else {
                    lock.lock();
                    if(uids0.size() == 0){
                        lock.unlock();
                        continue;
                    }
                    int tmp = Math.abs(random.nextInt()) % uids0.size();
                    long u0 = uids0.get(tmp);
                    long u1 = uids1.get(tmp);
                    lock.unlock();
                    DataItem data0 = null;
                    DataItem data1 = null;

                    data0 = dm0.read(u0);
                    data1 = dm1.read(u1);
                    data0.rlock();
                    data1.rlock();
                    SubArray s0 = data0.data();
                    SubArray s1 = data1.data();
                    assertArrayEquals(Arrays.copyOfRange(s0.data,s0.start,s0.end),Arrays.copyOfRange(s1.data,s1.start,s1.end));
                    data0.rUnlock();
                    data1.rUnlock();

                    byte[] newData = RandomUtil.randomBytes(dataLen);
                    data0.before();
                    data1.before();
                    System.arraycopy(newData,0,s0.data,s0.start,dataLen);
                    System.arraycopy(newData,0,s1.data,s1.start,dataLen);
                    data0.after(0);
                    data1.after(0);
                    data0.release();
                    data1.release();


                }
            }
        }finally {
            cdl.countDown();
        }
    }
    @Test
    public void testSingle() throws Exception{
        TransactionManager tm = new MockTransactionManager();
        DataManager dm0 = DataManager.create("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/testDMSingle", PageCache.PAGE_SIZE * 10,tm);
        DataManager dm1 = MockDataManager.newMockDataManager();
        int taskNum = 10000;
        CountDownLatch cdl = new CountDownLatch(1);
        initUids();
        Runnable r = ()-> {
            try {
                worker(dm0,dm1,taskNum,50,cdl);
            } catch (Exception e) {
                ExceptionDealer.shutDown(e);
            }
        };

        new Thread(r).start();
        cdl.await();
        dm0.close();
        dm1.close();
        assertTrue(new File("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/testDMSingle.db").delete());
        assertTrue(new File("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/testDMSingle.log").delete());

    }

    @Test
    public void testMulti() throws InterruptedException {
        TransactionManager tm = new MockTransactionManager();
        DataManager dm0 = DataManager.create("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/testDMMulti", PageCache.PAGE_SIZE * 100,tm);
        DataManager dm1 = MockDataManager.newMockDataManager();
        int taskNum = 500;
        CountDownLatch cdl = new CountDownLatch(10);
        initUids();
        for(int i = 0; i < 10; i++){
            Runnable r = ()-> {
                try {
                    worker(dm0,dm1,taskNum,50,cdl);

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };

            new Thread(r).start();
        }

        cdl.await();
        dm0.close();
        dm1.close();
        assertTrue(new File("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/testDMMulti.db").delete());
        assertTrue(new File("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/testDMMulti.log").delete());
    }

    @Test
    public void testRecoverSimple() throws InterruptedException {
        TransactionManager tm = TransactionManager.create("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/testRecover");
        DataManager dm0 = DataManager.create("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/testRecover",PageCache.PAGE_SIZE * 30,tm);
        DataManager dm1 = MockDataManager.newMockDataManager();
        dm0.close();

        initUids();
        int workNums = 10;
        for(int i = 0; i < 8; i++){
            dm0 = DataManager.open("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/testRecover",PageCache.PAGE_SIZE * 30,tm);
            CountDownLatch cdl = new CountDownLatch(workNums);
            for(int j = 0; j < workNums; j++){
                final DataManager dm = dm0;
                Runnable r = ()-> {
                    try {
                        worker(dm,dm1,100,50,cdl);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };
                new Thread(r).start();
            }
            cdl.await();
        }
        dm0.close();
        dm1.close();
        assertTrue(new File("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/testRecover.db").delete());
        assertTrue(new File("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/testRecover.log").delete());
        assertTrue(new File("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/testRecover.xid").delete());
    }
}
