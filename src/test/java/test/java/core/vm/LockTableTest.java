package test.java.core.vm;

import core.vm.LockTable;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class LockTableTest {
    @Test
    public void testLockTable() {
        LockTable lt = new LockTable();
        try {
            lt.add(1, 1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            lt.add(2, 2);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            lt.add(2, 1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertThrows(RuntimeException.class, () -> lt.add(1, 2));
    }

    @Test
    public void testLockTable2() throws InterruptedException {
        LockTable lt = new LockTable();
        for (long i = 1; i <= 100; i++) {
            try {
                CountDownLatch o = lt.add(i, i);
                if (o != null) {
                    Runnable r = () -> {
                        try {
                            o.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    };
                    new Thread(r).start();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        CountDownLatch finish = new CountDownLatch(99);
        for (long i = 1; i <= 99; i++) {
            try {
                CountDownLatch o = lt.add(i, i + 1);
                long index = i;
                if (o != null) {
                    Runnable r = () -> {
                        try {
                            o.await();
                            lt.remove(index);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        finish.countDown();
                    };
                    new Thread(r).start();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        assertThrows(RuntimeException.class, () -> lt.add(100, 1));
        lt.remove(100);

        finish.await();
        try {
            lt.add(100, 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
