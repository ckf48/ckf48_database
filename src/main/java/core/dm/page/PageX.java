package core.dm.page;

import core.dm.pageCache.PageCache;
import core.utils.Parser;

import java.util.Arrays;

/**
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 */
public class PageX {
    private static final short OFFSET_FREE = 0;
    private static final short OFFSET_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OFFSET_DATA;

    public static byte[] initData() {
        byte[] data = new byte[PageCache.PAGE_SIZE];
        setFreeSpaceOffset(data, OFFSET_DATA);
        return data;
    }

    private static void setFreeSpaceOffset(byte[] data, short offset) {
        System.arraycopy(Parser.short2Byte(offset), 0, data, OFFSET_FREE, OFFSET_DATA);
    }

    public static short getFreeSpaceOffset(Page page) {
        return getFreeSpaceOffset(page.getData());
    }

    private static short getFreeSpaceOffset(byte[] data) {
        return Parser.parserShort(Arrays.copyOfRange(data, 0, 2));
    }

    // 将data插入pg中，返回插入位置
    public static short insert(Page page, byte[] data) {
        page.setDirty(true);
        short offset = getFreeSpaceOffset(page.getData());
        System.arraycopy(data, 0, page.getData(), offset, data.length);
        setFreeSpaceOffset(page.getData(), (short) (offset + data.length));
        return offset;
    }

    // 获取页面的空闲空间大小
    public static int getFreeSpace(Page page) {
        return PageCache.PAGE_SIZE - (int) getFreeSpaceOffset(page);
    }

    // 将data插入pg中的offset位置，并将pg的offset设置为较大的offset
    public static void recoverInsert(Page page, byte[] data, short offset) {
        page.setDirty(true);
        System.arraycopy(data, 0, page.getData(), offset, data.length);

        short FSO = getFreeSpaceOffset(page.getData());
        if (FSO < offset + data.length) {
            setFreeSpaceOffset(page.getData(), (short) (offset + data.length));
        }
    }

    // 将data插入pg中的offset位置，不更新update
    public static void recoverUpdate(Page page,byte[] data, short offset){
        page.setDirty(true);
        System.arraycopy(data,0,page.getData(),offset,data.length);
    }
}
