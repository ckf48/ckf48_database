package core.tm;

import org.junit.Test;

import java.io.File;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.assertTrue;

public class TransactionManagerTest {
    static Random random = new SecureRandom();
    private int transCount = 0;

    private final Lock lock = new ReentrantLock();

    private Map<Long,Byte> transMap;

    private TransactionManager tm;

    private CountDownLatch latch;

    @Test
    public void testThread(){
        transMap = new HashMap<>();
        tm = TransactionManager.create("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/xid/test/test");
        int noWorkers = 50;
        latch = new CountDownLatch(noWorkers);
        for (int i = 0; i < noWorkers; i++){
            Runnable r = this::worker;
            new Thread(r).start();
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertTrue(new File("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/xid/test/test.xid").delete());
    }

    private void worker(){
        boolean inTrans = false;
        long transXID = 0;
        int noWorks = 2000;
        for(int i = 0; i < noWorks; i++){
            int op = Math.abs(random.nextInt(6));
            if(op == 0){
                lock.lock();
                if(!inTrans){
                    long xid = tm.begin();
                    transXID = xid;
                    transMap.put(xid,(byte)0);
                    transCount++;
                    inTrans = true;
                }else{
                    int status = random.nextInt(Integer.MAX_VALUE) % 2 + 1;
                    switch (status) {
                        case 1 -> tm.commit(transXID);
                        case 2 -> tm.abort(transXID);
                    }
                    transMap.put(transXID,(byte)status);
                    inTrans = false;
                }
                lock.unlock();
            }else{
                lock.lock();
                if(transCount > 0){
                    long xid = (long)(random.nextInt(Integer.MAX_VALUE) % transCount + 1);
                    byte status = transMap.get(xid);
                    boolean correct = false;
                    switch (status){
                        case 0 -> correct = tm.isActive(xid);
                        case 1 -> correct = tm.isCommitted(xid);
                        case 2 -> correct = tm.isAborted(xid);
                    }

                    assertTrue(correct);
                }
                lock.unlock();
            }
        }

        latch.countDown();
    }
}
