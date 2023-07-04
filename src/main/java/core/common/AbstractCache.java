package core.common;

import exceptionUtil.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractCache<T> {
    Map<Long, T> cache;// 实际缓存的数据
    Map<Long, Integer> references;// 元素的引用个数
    Map<Long, Boolean> getting;// 正在获取某资源的线程

    private int maxResource;// 缓存的最大缓存资源数
    private int count;// 缓存中元素的个数

    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        count = 0;
        lock = new ReentrantLock();
    }

    public T get(long key) throws Exception {
        while (true) {
            lock.lock();
            if (getting.containsKey(key)) {
                // 请求的资源正在被其他线程获取
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            if (cache.containsKey(key)) {
                // 资源在缓存中，直接返回
                T t = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return t;
            }

            // 尝试获取该资源
            if (maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }

            count++;
            getting.put(key, true);
            lock.unlock();
            break;
        }

        T t = null;
        try {
            t = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        getting.remove(key);
        cache.put(key, t);
        references.put(key, 1);
        lock.unlock();

        return t;
    }

    /**
     * 强行释放一个缓存
     */
    public void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            if (ref == 0) {
                count--;
                releaseForCache(cache.get(key));
                cache.remove(key);
                references.remove(key);
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    public void close(){
        lock.lock();
        try{
            Set<Long> set = cache.keySet();
            count = 0;
            for(Long key : set){
                T t = cache.get(key);
                releaseForCache(t);
                references.remove(key);
                cache.remove(key);
            }

        }finally {
            lock.unlock();
        }
    }

    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T t);
}
