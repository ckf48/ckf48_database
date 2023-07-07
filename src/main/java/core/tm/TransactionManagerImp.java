package core.tm;

import core.utils.Parser;
import exceptionUtil.Error;
import exceptionUtil.ExceptionDealer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class TransactionManagerImp implements TransactionManager{
    static final int LEN_XID_HEADER_LENGTH = 8; // XID文件头长度
    private static final int XID_FILED_SIZE = 1; // 每个事务的占用长度

    private static final byte FIELD_TRAN_ACTIVE = 0; // 事务的三种状态
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;

    public static final long SUPER_XID = 0; // 超级事务，永远为committed状态

    static final String XID_SUFFIX = ".xid"; // XID 文件后缀

    private final RandomAccessFile file;

    private final FileChannel fileChannel;

    private long xidCounter;

    private final Lock counterLock;
    TransactionManagerImp(RandomAccessFile file, FileChannel fileChannel){
        this.file = file;
        this.fileChannel = fileChannel;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }
    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度
     */
    private void checkXIDCounter(){
        long fileLen = 0;
        try{
            fileLen = file.length();
        }catch (Exception e){
            ExceptionDealer.shutDown(e);
        }
        if(fileLen < LEN_XID_HEADER_LENGTH){
            ExceptionDealer.shutDown(Error.BadXIDFileException);
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fileChannel.position(0);
            fileChannel.read(byteBuffer);
        } catch (IOException e) {
            ExceptionDealer.shutDown(e);
        }

        this.xidCounter = Parser.parserLong(byteBuffer.array());
        long end = getXIDOffset(this.xidCounter + 1);
        if(end != fileLen){
            ExceptionDealer.shutDown(Error.BadXIDFileException);
        }
    }

    private long getXIDOffset(long xid){
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FILED_SIZE;
    }
    @Override
    public long begin() {
        counterLock.lock();
        try{
            long xid = xidCounter + 1;
            updateStatusXID(xid,FIELD_TRAN_ACTIVE);
            increaseXIDCounter();
            return xid;
        }finally {
            counterLock.unlock();
        }

    }

    private void updateStatusXID(long xid, byte status){
        long offset = getXIDOffset(xid);
        byte[] tmp = new byte[XID_FILED_SIZE];
        tmp[0] = status;
        ByteBuffer byteBuffer = ByteBuffer.wrap(tmp);
        try {
            fileChannel.position(offset);
            fileChannel.write(byteBuffer);
        }catch (IOException e){
            ExceptionDealer.shutDown(e);
        }

        try{
            fileChannel.force(false);
        }catch (IOException e){
            ExceptionDealer.shutDown(e);
        }
    }

    private void increaseXIDCounter(){
        xidCounter++;
        ByteBuffer byteBuffer = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fileChannel.position(0);
            fileChannel.write(byteBuffer);
        } catch (IOException e) {
            ExceptionDealer.shutDown(e);
        }

        try{
            fileChannel.force(false);
        }catch (IOException e){
            ExceptionDealer.shutDown(e);
        }
    }

    private boolean checkXID(long xid,byte status){
        long offset = getXIDOffset(xid);
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[XID_FILED_SIZE]);
        try {
            fileChannel.position(offset);
            fileChannel.read(byteBuffer);
        } catch (IOException e) {
            ExceptionDealer.shutDown(e);
        }

        return byteBuffer.array()[0] == status;
    }

    @Override
    public void commit(long xid) {
        updateStatusXID(xid,FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateStatusXID(xid,FIELD_TRAN_ABORTED);
    }

    @Override
    public boolean isActive(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid,FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if(xid == SUPER_XID) return true;
        return checkXID(xid,FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid,FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            file.close();
            fileChannel.close();
        } catch (IOException e) {
            ExceptionDealer.shutDown(e);
        }

    }
}
