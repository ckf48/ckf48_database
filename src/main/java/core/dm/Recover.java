package core.dm;

import com.google.common.primitives.Bytes;
import core.common.SubArray;
import core.dm.dataItem.DataItem;
import core.dm.logger.Logger;
import core.dm.page.Page;
import core.dm.page.PageX;
import core.dm.pageCache.PageCache;
import core.tm.TransactionManager;
import core.utils.Parser;
import exceptionUtil.ExceptionDealer;

import java.util.*;
import java.util.Map.Entry;

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
        short freeSpaceOffset;
        byte[] data;
    }

    static class UpdateLog {
        long xid;
        int pgno;
        short offset;
        byte[] oldData;
        byte[] newData;
    }

    public static void recover(TransactionManager tm,Logger logger, PageCache pc){
        System.out.println("Recovering...");
        logger.rewind();
        int maxPgno = 0;
        while(true){
            byte[] log = logger.next();
            if(log == null){
                break;
            }
            int pgno;
            if(isInsertLog(log)){
                InsertLog insertLog = parseInsertLog(log);
                pgno = insertLog.pgno;
            }else{
                UpdateLog updateLog = parseUpdateLog(log);
                pgno = updateLog.pgno;
            }
            if(pgno > maxPgno){
                maxPgno = pgno;
            }
        }

        if(maxPgno == 0){
            maxPgno = 1;
        }

        pc.truncateByBgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        redoTransactions(tm,logger,pc);
        System.out.println("Redo Transactions Over.");

        undoTransactions(tm,logger,pc);
        System.out.println("Undo Transactions Over.");
        System.out.println("Recovery Over.");
    }

    private static void redoTransactions(TransactionManager tm, Logger logger, PageCache pageCache) {
        logger.rewind();
        while (true) {
            byte[] log = logger.next();
            if (log == null) {
                break;
            }
            if (isInsertLog(log)) {
                InsertLog insertLog = parseInsertLog(log);
                long xid = insertLog.xid;
                if (!tm.isActive(xid)) {
                    doInsertLog(pageCache, log, REDO);
                }
            } else {
                UpdateLog updateLog = parseUpdateLog(log);
                long xid = updateLog.xid;
                if (!tm.isActive(xid)) {
                    doUpdateLog(pageCache, log, REDO);
                }
            }
        }
    }

    private static void undoTransactions(TransactionManager tm, Logger logger, PageCache pageCache) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        logger.rewind();
        while (true) {
            byte[] log = logger.next();
            if (log == null) {
                break;
            }
            if (isInsertLog(log)) {
                InsertLog insertLog = parseInsertLog(log);
                long xid = insertLog.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    } else {
                        logCache.get(xid).add(log);
                    }
                }
            } else {
                UpdateLog updateLog = parseUpdateLog(log);
                long xid = updateLog.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    } else {
                        logCache.get(xid).add(log);
                    }
                }
            }
        }

        for(Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size()-1; i >= 0; i --) {
                byte[] log = logs.get(i);
                if(isInsertLog(log)) {
                    doInsertLog(pageCache, log, UNDO);
                } else {
                    doUpdateLog(pageCache, log, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    // [LogType] [XID] [Pgno] [Offset] [Data]

    private static final int OFFSET_TYPE = 0;
    private static final int OFFSET_XID = OFFSET_TYPE + 1;
    private static final int OFFSET_INSERT_PGNO = OFFSET_XID + 8;
    private static final int OFFSET_INSERT_FREESPACEOF = OFFSET_INSERT_PGNO + 4;
    private static final int OFFSET_INSERT_DATA = OFFSET_INSERT_FREESPACEOF + 2;

    public static byte[] wrapInsertLog(long xid, Page page, byte[] data) {
        byte[] logType = {LOG_TYPE_INSERT};
        byte[] xid_ = Parser.long2Byte(xid);
        byte[] pgno = Parser.int2Byte(page.getPageNumber());
        byte[] freeSpaceOffset = Parser.short2Byte(PageX.getFreeSpaceOffset(page));
        return Bytes.concat(logType, xid_, pgno, freeSpaceOffset, data);
    }

    private static InsertLog parseInsertLog(byte[] log) {
        InsertLog insertLog = new InsertLog();
        insertLog.xid = Parser.parserLong(Arrays.copyOfRange(log, OFFSET_XID, OFFSET_INSERT_PGNO));
        insertLog.pgno = Parser.parserInt(Arrays.copyOfRange(log, OFFSET_INSERT_PGNO, OFFSET_INSERT_FREESPACEOF));
        insertLog.freeSpaceOffset = Parser.parserShort(Arrays.copyOfRange(log, OFFSET_INSERT_FREESPACEOF, OFFSET_INSERT_DATA));
        insertLog.data = Arrays.copyOfRange(log, OFFSET_INSERT_DATA, log.length);

        return insertLog;
    }

    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLog insertLog = parseInsertLog(log);
        Page page = null;
        try {
            page = pc.getPage(insertLog.pgno);
        } catch (Exception e) {
            ExceptionDealer.shutDown(e);
        }
        assert page != null;
        try {
            if (flag == UNDO) {
                DataItem.setDataItemRawInvalid(insertLog.data);
            }
            PageX.recoverInsert(page, insertLog.data, insertLog.freeSpaceOffset);
        } finally {
            page.release();
        }
    }

    // [LogType] [XID] [UID] [OldData] [NewData]
    private static final int OFFSET_UPDATE_UID = OFFSET_XID + 8;
    private static final int OFFSET_UPDATE_DATA = OFFSET_UPDATE_UID + 8;

    public static byte[] wrapUpdateLog(long xid, DataItem dataItem) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xid_ = Parser.long2Byte(xid);
        byte[] uid = Parser.long2Byte(dataItem.getUid());
        byte[] oldData = dataItem.getOldRaw();
        SubArray data = dataItem.getRaw();
        byte[] newData = Arrays.copyOfRange(data.data, data.start, data.end);
        return Bytes.concat(logType, xid_, uid, oldData, newData);
    }

    private static UpdateLog parseUpdateLog(byte[] log) {
        UpdateLog updateLog = new UpdateLog();
        updateLog.xid = Parser.parserLong(Arrays.copyOfRange(log, OFFSET_XID, OFFSET_UPDATE_UID));
        long uid = Parser.parserLong(Arrays.copyOfRange(log, OFFSET_UPDATE_UID, OFFSET_UPDATE_DATA));
        updateLog.offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        updateLog.pgno = (int) (uid & ((1L << 32) - 1));

        int len = (log.length - OFFSET_UPDATE_DATA) / 2;

        updateLog.oldData = Arrays.copyOfRange(log, OFFSET_UPDATE_DATA, OFFSET_UPDATE_DATA + len);
        updateLog.newData = Arrays.copyOfRange(log, OFFSET_UPDATE_DATA + len, OFFSET_UPDATE_DATA + len * 2);
        return updateLog;

    }

    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pgno;
        short offset;
        byte[] data;
        UpdateLog updateLog = parseUpdateLog(log);
        pgno = updateLog.pgno;
        offset = updateLog.offset;
        if (flag == REDO) {
            data = updateLog.newData;
        } else {
            data = updateLog.oldData;
        }

        Page page = null;
        try {
            page = pc.getPage(pgno);
        } catch (Exception e) {
            ExceptionDealer.shutDown(e);
        }
        assert page != null;
        try {
            PageX.recoverUpdate(page, data, offset);
        } finally {
            page.release();
        }
    }

}
