package core.dm.dataItem;

import com.google.common.primitives.Bytes;
import core.common.SubArray;
import core.dm.DataManagerImp;
import core.dm.page.Page;
import core.utils.Parser;
import core.utils.UidGenerator;

import java.util.Arrays;

public interface DataItem {
    SubArray data();

    void before();

    void unBefore();

    void after(long xid);

    void release();

    void wlock();

    void wUnlock();

    void rlock();

    void rUnlock();

    Page page();

    long getUid();

    byte[] getOldRaw();

    SubArray getRaw();

    static byte[] wrapDataItemRaw(byte[] data) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) data.length);
        return Bytes.concat(valid, size, data);
    }

    //从页面的offset处解析处dataitem
    public static DataItem parseDataItem(Page page, short offset, DataManagerImp dm) {
        byte[] data = page.getData();
        short size = Parser.parserShort(Arrays.copyOfRange(data, offset + DataItemImp.OFFSET_SIZE, offset + DataItemImp.OFFSET_DATA));
        short length = (short) (size + DataItemImp.OFFSET_DATA);
        long uid = UidGenerator.addressToUid(page.getPageNumber(), offset);
        return new DataItemImp(new SubArray(data, offset, offset + length), new byte[length], page, uid, dm);
    }

    static void setDataItemRawInvalid(byte[] data) {
        data[DataItemImp.OFFSET_VALID] = (byte) 1;
    }
}
