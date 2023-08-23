package core.vm;

import com.google.common.primitives.Bytes;
import core.common.SubArray;
import core.dm.dataItem.DataItem;
import core.utils.Parser;

import java.util.Arrays;

/**
 * VM向上层抽象出entry
 * entry结构：
 * [XID_MIN] [XID_MAX] [data]
 */
public class Entry {
    private static final int OFFSET_XID_MIN = 0;
    private static final int OFFSET_XID_MAX = OFFSET_XID_MIN + 8;
    private static final int OFFSET_DATA = OFFSET_XID_MAX + 8;
    private long uid;
    private DataItem dataItem;
    private VersionManager vm;

    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        Entry entry = new Entry();
        entry.dataItem = dataItem;
        entry.uid = uid;
        entry.vm = vm;
        return entry;
    }

    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem di = ((VersionManagerImp) vm).dm.read(uid);
        return newEntry(vm, di, uid);
    }

    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xid_min = Parser.long2Byte(xid);
        byte[] xid_max = new byte[8];
        return Bytes.concat(xid_min, xid_max, data);
    }

    // 以拷贝的形式返回内容
    public byte[] data() {
        dataItem.rlock();
        try {
            SubArray subArray = dataItem.data();
            byte[] data = new byte[subArray.end - subArray.start - OFFSET_DATA];
            System.arraycopy(subArray.data, subArray.start + OFFSET_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnlock();
        }
    }

    public long getMin() {
        dataItem.rlock();
        try {
            SubArray subArray = dataItem.data();
            return Parser.parserLong(Arrays.copyOfRange(subArray.data, subArray.start + OFFSET_XID_MIN, subArray.start + OFFSET_XID_MAX));
        } finally {
            dataItem.rUnlock();
        }
    }

    public long getMax() {
        dataItem.rlock();
        try {
            SubArray subArray = dataItem.data();
            return Parser.parserLong(Arrays.copyOfRange(subArray.data, subArray.start + OFFSET_XID_MAX, subArray.end + OFFSET_DATA));
        } finally {
            dataItem.rUnlock();
        }
    }

    public void setMax(long xid){
        dataItem.before();
        try{
            SubArray subArray = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid),0,subArray.data,subArray.start + OFFSET_XID_MAX,8);
        }finally {
            dataItem.after(xid);
        }
    }


    public void remove() {
        dataItem.release();
    }

    public void release(){
        ((VersionManagerImp)vm).releaseEntry(this);
    }

    public long getUid(){
        return uid;
    }


}
