package core.vm;

import exceptionUtil.Error;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 维护了一个依赖等待图，以进行死锁检测
 */
public class LockTable {
    private Map<Long, List<Long>> x2u; //XID已经获得的资源的UID列表
    private Map<Long, Long> u2x; //UID被某个XID持有
    private Map<Long, List<Long>> wait;//正在等待UID的XID列表

    private Map<Long, CountDownLatch> waitLock; //正在等待资源的XID的锁
    private Map<Long, Long> waitU;//XID正在等待的UID
    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    // 不需要等待则返回null，否则返回锁对象
    // 会造成死锁则抛出异常
    public CountDownLatch add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            if (isInlist(x2u, xid, uid)) {
                return null;
            }

            if (!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }
            waitU.put(xid, uid);
            putIntoList(wait, uid, xid);
            if (hasDeadLock()) {
                waitU.remove(xid, uid);
                removeFromList(wait, uid, xid);
                throw Error.DeadLockException;
            }

            CountDownLatch res = new CountDownLatch(1);
            waitLock.put(xid, res);
            return res;


        } finally {
            lock.unlock();
        }

    }

    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> l = x2u.get(xid);
            if (l != null) {
                while (!l.isEmpty()) {
                    long uid = l.remove(0);
                    selectNewXid(uid);
                }
            }
            waitU.remove(xid);
            CountDownLatch cdl = waitLock.remove(xid);
            if (cdl != null) {
                cdl.countDown();
            }
            x2u.remove(xid);
        } finally {
            lock.unlock();
        }
    }

    private void selectNewXid(long uid) {
        u2x.remove(uid);
        List<Long> l = wait.get(uid);
        if (l == null) {
            return;
        }

        while (!l.isEmpty()) {
            long xid = l.remove(0);
            if (waitLock.containsKey(xid)) {
                u2x.put(uid, xid);
                CountDownLatch lo = waitLock.remove(xid);
                waitU.remove(xid);
                lo.countDown();
                break;
            }
        }
        if (l.isEmpty()) {
            wait.remove(uid);
        }
    }

    private Map<Long, Integer> xidStamp;
    private int stamp;

    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        for (long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            if (s != null && s > 0) {
                continue;
            }
            stamp++;
            if (dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        if (stp != null && stp == stamp) {
            return true;
        }
        if (stp != null && stp < stamp) {
            return false;
        }
        xidStamp.put(xid, stamp);

        Long uid = waitU.get(xid);
        if (uid == null) return false;
        Long x = u2x.get(uid);
        assert x != null;
        return dfs(x);
    }

    private void removeFromList(Map<Long, List<Long>> map, long uid0, long uid1) {
        List<Long> l = map.get(uid0);
        if (l == null) {
            return;
        }
        Iterator<Long> i = l.iterator();
        while (i.hasNext()) {
            long e = i.next();
            if (e == uid1) {
                i.remove();
                break;
            }
        }
        if (l.isEmpty()) {
            map.remove(uid0);
        }

    }

    private void putIntoList(Map<Long, List<Long>> map, long uid0, long uid1) {
        if (!map.containsKey(uid0)) {
            map.put(uid0, new ArrayList<>());
        }

        map.get(uid0).add(uid1);
    }

    private boolean isInlist(Map<Long, List<Long>> map, long uid0, long uid1) {
        List<Long> list = map.get(uid0);
        if (list == null) {
            return false;
        }

        for (long e : list) {
            if (e == uid1) {
                return true;
            }
        }

        return false;
    }
}
