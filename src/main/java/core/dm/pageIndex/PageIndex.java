package core.dm.pageIndex;

import core.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageIndex {
    private static final int INTERVALS_NO = 40;
    private static final int THERSHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;
    private final Lock lock;

    private final List<PageInfo>[] lists;


    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        for (int i = 0; i <= INTERVALS_NO; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THERSHOLD;
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }

    }

    public PageInfo select(int needSpace) {
        lock.lock();
        try {
            int number = needSpace / THERSHOLD;
            if (number < INTERVALS_NO) {
                number++;
            }
            while (number <= INTERVALS_NO) {
                if (lists[number].isEmpty()) {
                    number++;
                    continue;
                }

                return lists[number].remove(0);
            }

            return null;
        } finally {
            lock.unlock();
        }
    }


}
