package core.dm;

import core.common.AbstractCache;
import core.dm.dataItem.DataItem;
import core.dm.dataItem.DataItemImp;
import core.dm.logger.Logger;
import core.dm.page.Page;
import core.dm.page.PageOne;
import core.dm.page.PageX;
import core.dm.pageCache.PageCache;
import core.dm.pageIndex.PageIndex;
import core.dm.pageIndex.PageInfo;
import core.tm.TransactionManager;
import core.utils.UidGenerator;
import exceptionUtil.Error;
import exceptionUtil.ExceptionDealer;

public class DataManagerImp extends AbstractCache<DataItem> implements DataManager {
    Logger logger;
    TransactionManager tm;
    PageCache pc;
    Page pageOne;
    PageIndex pageIndex;

    public DataManagerImp(PageCache pc, TransactionManager tm, Logger logger) {
        super(0);
        this.pc = pc;
        this.tm = tm;
        this.logger = logger;
        pageIndex = new PageIndex();
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImp di = (DataItemImp) super.get(uid);
        if (!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if (raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }
        PageInfo pageInfo = null;

        for (int i = 0; i < 5; i++) {
            pageInfo = pageIndex.select(raw.length);
            if (pageInfo != null) {
                break;
            } else {
                int newPgno = pc.newPage(PageX.initData());
                pageIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }

        if (pageInfo == null) {
            throw Error.DatabaseTooBusyException;
        }

        Page page = null;
        int freeSpace = 0;
        try {
            page = pc.getPage(pageInfo.pgno);
            byte[] log = Recover.wrapInsertLog(xid, page, raw);
            logger.log(log);

            short offset = PageX.insert(page, raw);
            page.release();
            return UidGenerator.addressToUid(pageInfo.pgno, offset);
        } finally {
            // 将取出的page重新插入pageIndex
            if (page == null) {
                pageIndex.add(pageInfo.pgno, freeSpace);
            } else {
                pageIndex.add(pageInfo.pgno, PageX.getFreeSpace(page));
            }
        }

    }

    @Override
    public void close() {
        super.close();
        logger.close();
        PageOne.setVCClose(pageOne);
        pageOne.release();
        pc.close();
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short) (uid & ((1L << 16) - 1));
        uid = uid >> 32;
        int pgno = (int) (uid & ((1L << 32) - 1));
        Page page = pc.getPage(pgno);

        return DataItem.parseDataItem(page,offset,this);
    }

    @Override
    protected void releaseForCache(DataItem dataItem) {
        dataItem.page().release();
    }

    // 在创建文件时初始化PageOne
    void initPageOne() {
        int pgno = pc.newPage(PageOne.initData());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            ExceptionDealer.shutDown(e);
        }

        pc.flushPage(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            ExceptionDealer.shutDown(e);
        }

        return PageOne.checkVC(pageOne);
    }

    // 初始化pageIndex
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for (int i = 2; i <= pageNumber; i++) {
            Page page = null;
            try {
                page = pc.getPage(i);
            } catch (Exception e) {
                ExceptionDealer.shutDown(e);
            }

            pageIndex.add(i, PageX.getFreeSpace(page));
            assert page != null;
            page.release();
        }
    }

    public void logDataItem(long xid, DataItem dataItem) {
        byte[] log = Recover.wrapUpdateLog(xid, dataItem);
        logger.log(log);
    }

    public void releaseDataItem(DataItem dataItem) {
        super.release(dataItem.getUid());
    }
}
