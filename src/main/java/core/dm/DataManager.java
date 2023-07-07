package core.dm;

import core.dm.dataItem.DataItem;

public interface DataManager {
    DataItem read(long uid) throws Exception;

    long insert(long xid, byte[] data) throws Exception;

    void close();
}
