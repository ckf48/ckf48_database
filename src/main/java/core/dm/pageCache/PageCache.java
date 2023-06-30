package core.dm.pageCache;

import core.dm.page.Page;
import exceptionUtil.Error;
import exceptionUtil.ExceptionDealer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public interface PageCache {
    public static final int PAGE_SIZE = 1 << 13;

    public static final String DB_SUFFIX = ".db";

    int newPage(byte[] initData);

    Page getPage(int pgno) throws Exception;

    void close();

    void release(Page page);

    void truncateByBgno(int maxPgno);

    int getPageNumber();

    void flushPage(Page pg);

    public static PageCacheImp create(String path, long memory) {
        File file = new File(path + DB_SUFFIX);
        try {
            if (!file.createNewFile()) {
                ExceptionDealer.shutDown(Error.FileExistsException);
            }
        } catch (IOException e) {
            ExceptionDealer.shutDown(e);
        }

        if (!file.canRead() || !file.canWrite()) {
            ExceptionDealer.shutDown(Error.FileCannotRWException);
        }

        FileChannel fileChannel = null;
        RandomAccessFile accessFile = null;

        try {
            accessFile = new RandomAccessFile(file, "rw");
            fileChannel = accessFile.getChannel();
        } catch (FileNotFoundException e) {
            ExceptionDealer.shutDown(e);
        }

        return new PageCacheImp(accessFile, fileChannel, (int) memory / PageCache.PAGE_SIZE);
    }

    public static PageCacheImp open(String path, long memory) {
        File file = new File(path + DB_SUFFIX);
        if (!file.exists()) {
            ExceptionDealer.shutDown(Error.FileNotExistsException);
        }

        if (!file.canRead() || !file.canWrite()) {
            ExceptionDealer.shutDown(Error.FileCannotRWException);
        }

        FileChannel fileChannel = null;
        RandomAccessFile accessFile = null;

        try {
            accessFile = new RandomAccessFile(file, "rw");
            fileChannel = accessFile.getChannel();
        } catch (FileNotFoundException e) {
            ExceptionDealer.shutDown(e);
        }

        return new PageCacheImp(accessFile, fileChannel, (int) memory / PageCache.PAGE_SIZE);
    }
}
