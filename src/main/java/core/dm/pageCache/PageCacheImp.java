package core.dm.pageCache;

import core.common.AbstractCache;
import core.dm.page.Page;
import core.dm.page.PageImp;
import exceptionUtil.Error;
import exceptionUtil.ExceptionDealer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImp extends AbstractCache<Page> implements PageCache {

    private static final int MEM_MIN_LIM = 10;
    private RandomAccessFile file;

    private FileChannel fileChannel;

    private Lock lock;

    private AtomicInteger pageNumbers;

    public PageCacheImp(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if (maxResource < MEM_MIN_LIM) {
            ExceptionDealer.shutDown(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            ExceptionDealer.shutDown(e);
        }
        this.file = file;
        this.fileChannel = fileChannel;
        lock = new ReentrantLock();
        pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);
    }

    /**
     * 根据pageNumber从数据库文件中读取页数据，并包裹成Page
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        long offset  = pageOffset(pgno);

        ByteBuffer byteBuffer = ByteBuffer.allocate(PAGE_SIZE);
        lock.lock();
        try {
            fileChannel.position(offset);
            fileChannel.read(byteBuffer);
        }catch (IOException e){
            ExceptionDealer.shutDown(e);
        }finally {
          lock.unlock();
        }

        return new PageImp(pgno, byteBuffer.array(), this);
    }

    @Override
    protected void releaseForCache(Page page) {
        if(page.isDirty()){
            flush(page);
            page.setDirty(false);
        }
    }

    @Override
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        Page page = new PageImp(pgno, initData, null);
        flush(page);
        return pgno;
    }

    @Override
    public Page getPage(int pgno) throws Exception {
        return get((long) pgno);
    }

    @Override
    public void close() {
        super.close();
        try {
            file.close();
            fileChannel.close();
        } catch (IOException e) {
            ExceptionDealer.shutDown(e);
        }

    }

    @Override
    public void release(Page page) {
        release((long) page.getPageNumber());
    }

    @Override
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            ExceptionDealer.shutDown(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }

    private void flush(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);

        lock.lock();
        try {
            ByteBuffer byteBuffer = ByteBuffer.wrap(pg.getData());
            fileChannel.position(offset);
            fileChannel.write(byteBuffer);
            fileChannel.force(false);
        } catch (IOException e) {
            ExceptionDealer.shutDown(e);
        } finally {
            lock.unlock();
        }
    }


    private long pageOffset(int pgno) {
        return (long) (pgno - 1) * PAGE_SIZE;
    }
}
