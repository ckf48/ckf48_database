package core.utils;

import java.nio.ByteBuffer;

public class Parser {
    public static long parserLong(byte[] buff) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buff, 0, 8);
        return byteBuffer.getLong();
    }

    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }


    public static short parserShort(byte[] buff) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buff, 0, 2);
        return byteBuffer.getShort();
    }

    public static byte[] short2Byte(short value) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }

    public static int parserInt(byte[] buff) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buff, 0, 4);
        return byteBuffer.getInt();
    }

    public static byte[] int2Byte(int value) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }
}
