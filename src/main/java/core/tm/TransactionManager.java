package core.tm;

import exceptionUtil.Error;
import exceptionUtil.ExceptionDealer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface TransactionManager {
    long begin();    //开启一个事务
    void commit(long xid);  //提交一个事务

    void abort(long xid);   // 取消一个事务

    boolean isActive(long xid);   // 查询一个事务的状态是否是正在进行

    boolean isCommitted(long xid);  // 查询一个事务的状态是否是已提交

    boolean isAborted(long xid);    // 查询一个事务的状态是否是已取消

    void close();   // 关闭TM


    public static TransactionManagerImp create(String path){
        File file = new File(path+TransactionManagerImp.XID_SUFFIX);
        try{
            if(!file.createNewFile()){
                ExceptionDealer.shutDown(Error.FileExistsException);
            }
        }catch (IOException e){
            ExceptionDealer.shutDown(e);
        }

        if(!file.canRead() || !file.canWrite()){
            ExceptionDealer.shutDown(Error.FileCannotRWException);
        }

        FileChannel fileChannel = null;
        RandomAccessFile randomAccessFile = null;

        try {
            randomAccessFile = new RandomAccessFile(file,"rw");
            fileChannel = randomAccessFile.getChannel();
        }catch (IOException e){
            ExceptionDealer.shutDown(e);
        }

        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[TransactionManagerImp.LEN_XID_HEADER_LENGTH]);
        try {
            fileChannel.position(0);
            fileChannel.write(byteBuffer);
        }catch (IOException e){
            ExceptionDealer.shutDown(e);
        }

        return new TransactionManagerImp(randomAccessFile,fileChannel);
    }

    public static TransactionManagerImp open(String path){
        File file = new File(path+TransactionManagerImp.XID_SUFFIX);
        if(!file.exists()){
            ExceptionDealer.shutDown(Error.FileNotExistsException);
        }

        if(!file.canRead() || !file.canWrite()){
            ExceptionDealer.shutDown(Error.FileCannotRWException);
        }

        FileChannel fileChannel = null;
        RandomAccessFile randomAccessFile = null;

        try {
            randomAccessFile = new RandomAccessFile(file,"rw");
            fileChannel = randomAccessFile.getChannel();
        }catch (IOException e){
            ExceptionDealer.shutDown(e);
        }

        return new TransactionManagerImp(randomAccessFile,fileChannel);

    }
}
