package core.dm.logger;

import core.utils.Parser;
import exceptionUtil.Error;
import exceptionUtil.ExceptionDealer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface Logger {
    public static final String LOG_SUFFIX = ".log";

    void log(byte[] data);

    byte[] next();

    void truncate(long x) throws Exception;

    void rewind();

    void close();

    public static Logger create(String path) {
        File file = new File(path + LOG_SUFFIX);
        try {
            if (!file.createNewFile()) {
                ExceptionDealer.shutDown(Error.FileExistsException);
            }
        } catch (IOException e) {
            ExceptionDealer.shutDown(e);
        }

        if (!file.canWrite() || !file.canWrite()) {
            ExceptionDealer.shutDown(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            ExceptionDealer.shutDown(e);
        }

        ByteBuffer byteBuffer = ByteBuffer.wrap(Parser.int2Byte(0));

        try {
            fc.position(0);
            fc.write(byteBuffer);
            fc.force(false);
        } catch (IOException e) {
            ExceptionDealer.shutDown(e);
        }

        return new LoggerImp(raf, fc, 0);
    }

    public static Logger open(String path) {
        File file = new File(path + LOG_SUFFIX);
        if (!file.exists()) {
            ExceptionDealer.shutDown(Error.FileNotExistsException);
        }

        if (!file.canWrite() || !file.canWrite()) {
            ExceptionDealer.shutDown(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            ExceptionDealer.shutDown(e);
        }

        LoggerImp loggerImp = new LoggerImp(raf,fc);
        loggerImp.init();

        return loggerImp;


    }

}
