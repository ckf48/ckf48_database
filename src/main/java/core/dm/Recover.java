package core.dm;

public class Recover {
    // updateLog:
// [LogType] [XID] [UID] [OldData] [NewData]
// insertLog:
// [LogType] [XID] [Pgno] [Offset] [Data]
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int UNDO = 0;
    private static final int REDO = 1;

    static class InsertLog {
        long xid;
        int pgno;
        short offset;
        byte[] data;
    }

    static class UpdateLog {
        long xid;
        int pgno;
        short offset;
        byte[] oldData;
        byte[] newData;
    }


}
