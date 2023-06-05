package core.utils;

import java.nio.ByteBuffer;

public class Parser {
    public static long parserLong(byte[] buff){
        ByteBuffer byteBuffer = ByteBuffer.wrap(buff,0,8);
        return byteBuffer.getLong();
    }

    public static byte[] long2Byte(long value){
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }
}
