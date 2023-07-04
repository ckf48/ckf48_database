package core.dm.logger;


import com.google.common.primitives.Bytes;
import core.utils.Parser;
import exceptionUtil.Error;
import exceptionUtil.ExceptionDealer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 日志文件读写
 * <p>
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 为后续所有日志计算的Checksum，int类型
 * <p>
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size 4字节int 标识Data长度
 * Checksum 4字节int
 */
public class LoggerImp implements Logger {
    private static final int SEED = 114514;
    private static final int OFFSET_SIZE = 0;
    private static final int OFFSET_CHECKSUM = OFFSET_SIZE + 4;
    private static final int OFFSET_DATA = OFFSET_CHECKSUM + 4;

    private RandomAccessFile file;
    private FileChannel fileChannel;
    private Lock lock;

    private Long position;//指针位置
    private Long fileSize;

    private int xChecksum;

    LoggerImp(RandomAccessFile file, FileChannel fileChannel) {
        this.file = file;
        this.fileChannel = fileChannel;
        lock = new ReentrantLock();
    }

    LoggerImp(RandomAccessFile file, FileChannel fileChannel, int xChecksum) {
        this.file = file;
        this.fileChannel = fileChannel;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            ExceptionDealer.shutDown(e);
        }

        if (size < 4) {
            ExceptionDealer.shutDown(Error.BadLogFileException);
        }

        ByteBuffer data = ByteBuffer.allocate(4);

        try {
            fileChannel.position(0);
            fileChannel.read(data);
        } catch (IOException e) {
            ExceptionDealer.shutDown(e);
        }

        this.xChecksum = Parser.parserInt(data.array());
        this.fileSize = size;

        checkAndRemoveTail();
    }

    //检查并移除bad tail
    private void checkAndRemoveTail() {
        rewind();
        int xCheck = 0;
        while (true) {
            byte[] log = internNext();
            if (log == null) {
                break;
            }
            xCheck = calChecksum(xCheck, log);
        }

        if (xCheck != xChecksum) {
            ExceptionDealer.shutDown(Error.BadLogFileException);
        }

        try {
            truncate(position);
        } catch (Exception e) {
            ExceptionDealer.shutDown(e);
        }

        rewind();
    }

    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }

        return xCheck;
    }

    private byte[] internNext() {
        if (position + OFFSET_DATA >= fileSize) {
            return null;
        }

        ByteBuffer buffer = ByteBuffer.allocate(4);
        try {
            fileChannel.position(position);
            fileChannel.read(buffer);
        } catch (IOException e) {
            ExceptionDealer.shutDown(e);
        }

        int size = Parser.parserInt(buffer.array());
        if (position + size + OFFSET_DATA > fileSize) {
            return null;
        }

        buffer = ByteBuffer.allocate(OFFSET_DATA + size);
        try {
            fileChannel.position(position);
            fileChannel.read(buffer);
        } catch (IOException e) {
            ExceptionDealer.shutDown(e);
        }

        byte[] log = buffer.array();
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OFFSET_DATA, log.length));
        int checkSum2 = Parser.parserInt(Arrays.copyOfRange(log, OFFSET_CHECKSUM, OFFSET_DATA));
        if (checkSum1 != checkSum2) {
            return null;
        }

        position += log.length;
        return log;

    }

    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buffer = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fileChannel.position(fileChannel.size());
            fileChannel.write(buffer);
        } catch (IOException e) {
            ExceptionDealer.shutDown(e);
        } finally {
            lock.unlock();
        }

        updateChecksum(log);
    }

    private void updateChecksum(byte[] log) {
        lock.lock();
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fileChannel.position(0);
            fileChannel.write(ByteBuffer.wrap(Parser.int2Byte(this.xChecksum)));
            fileChannel.force(false);
        } catch (IOException e) {
            ExceptionDealer.shutDown(e);
        } finally {
            lock.unlock();
        }
    }

    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            return log == null ? null : Arrays.copyOfRange(log, OFFSET_DATA, log.length);
        } finally {
            lock.unlock();
        }

    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fileChannel.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4L;
    }

    @Override
    public void close() {
        try {
            fileChannel.close();
            file.close();
        } catch (IOException e) {
            ExceptionDealer.shutDown(e);
        }

    }
}
