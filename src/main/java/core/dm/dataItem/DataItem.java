package core.dm.dataItem;

import com.google.common.primitives.Bytes;
import core.common.SubArray;
import core.dm.page.Page;
import core.utils.Parser;

public interface DataItem {
    SubArray data();

    void before();

    void unBefore();

    void after(long xid);

    void wlock();

    void wUnlock();

    void rlock();

    void rUnlock();

    Page page();

    long getUid();

    byte[] getOldRaw();

    SubArray getRaw();

    public static byte[] wrapDataItemRaw(byte[] data) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) data.length);
        return Bytes.concat(valid, size, data);
    }

    // 从页面的offset处解析处dataitem
//    public static DataItem parseDataItem(Page page, short offset,){
//
//    }

    public static void setDataItemRawInvalid(byte[] data) {
        data[DataItemImp.OFFSET_VALID] = (byte) 1;
    }
}
