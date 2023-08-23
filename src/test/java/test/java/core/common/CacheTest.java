package test.java.core.common;

import exceptionUtil.Error;
import exceptionUtil.ExceptionDealer;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

public class CacheTest {
    static Random random = new SecureRandom();
    private CountDownLatch latch;
    private MockCache cache;

    private static final int nWorkers = 200;

    private static final int nWork = 1000;

    @Test
    public void testCache(){
        latch = new CountDownLatch(nWorkers);
        cache = new MockCache();

        for (int i = 0; i < nWorkers; i++){
            Runnable r = this::worker;
            new Thread(r).start();
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        cache.close();
    }

    public void worker(){
        for(int i = 0; i < nWork; i++){
            long id = random.nextLong();
            long resource = 0;

            try {
                resource = cache.get(id);
            } catch (Exception e) {
                if(e == Error.CacheFullException){
                    continue;
                }
                ExceptionDealer.shutDown(e);
            }

            assertEquals(resource,id);
            cache.release(id);
        }

        latch.countDown();
    }
}
