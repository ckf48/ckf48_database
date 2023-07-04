package core.dm.page;

import core.dm.pageCache.PageCache;
import core.utils.RandomDataCreator;

import java.util.Arrays;

/**
 * 特殊管理第一页
 * ValidCheck
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭
 */
public class PageOne {
    private static final int OFFSET_VALID_CHECK = 100;
    private static final int LEN_VALID_CHECK = 8;

    public static byte[] initData() {
        byte[] data = new byte[PageCache.PAGE_SIZE];
        setVCOpen(data);
        return data;
    }

    public static void setVCOpen(Page page) {
        page.setDirty(true);
        setVCOpen(page.getData());
    }

    private static void setVCOpen(byte[] data) {
        System.arraycopy(RandomDataCreator.randomBytes(LEN_VALID_CHECK), 0, data, OFFSET_VALID_CHECK, LEN_VALID_CHECK);
    }

    public static void setVCClose(Page page) {
        page.setDirty(true);
        setVCClose(page.getData());
    }

    private static void setVCClose(byte[] data) {
        System.arraycopy(data, OFFSET_VALID_CHECK, data, OFFSET_VALID_CHECK + LEN_VALID_CHECK, LEN_VALID_CHECK);
    }

    public static boolean checkVC(Page page){
        return checkVC(page.getData());
    }

    private static boolean checkVC(byte[] data) {
        return Arrays.equals(Arrays.copyOfRange(data, OFFSET_VALID_CHECK, OFFSET_VALID_CHECK + LEN_VALID_CHECK), Arrays.copyOfRange(data, OFFSET_VALID_CHECK + LEN_VALID_CHECK, OFFSET_VALID_CHECK + 2 * LEN_VALID_CHECK));
    }
}
