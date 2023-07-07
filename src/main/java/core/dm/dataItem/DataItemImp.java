package core.dm.dataItem;

import core.common.SubArray;
import core.dm.page.Page;

public class DataItemImp implements DataItem {
    public static final int OFFSET_VALID = 0;
    public static final int OFFSET_SIZE = OFFSET_VALID + 1;
    public static final int OFFSET_DATA = OFFSET_SIZE + 2;

    @Override
    public SubArray data() {
        return null;
    }

    @Override
    public void before() {

    }

    @Override
    public void unBefore() {

    }

    @Override
    public void after(long xid) {

    }

    @Override
    public void wlock() {

    }

    @Override
    public void wUnlock() {

    }

    @Override
    public void rlock() {

    }

    @Override
    public void rUnlock() {

    }

    @Override
    public Page page() {
        return null;
    }

    @Override
    public long getUid() {
        return 0;
    }

    @Override
    public byte[] getOldRaw() {
        return new byte[0];
    }

    @Override
    public SubArray getRaw() {
        return null;
    }
}
