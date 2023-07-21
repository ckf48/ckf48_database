package core.dm;

import core.dm.dataItem.DataItem;
import core.dm.logger.Logger;
import core.dm.page.PageOne;
import core.dm.pageCache.PageCache;
import core.tm.TransactionManager;

public interface DataManager {
    DataItem read(long uid) throws Exception;

    long insert(long xid, byte[] data) throws Exception;

    void close();

    public static DataManager create(String path, long memory, TransactionManager tm){
        PageCache pageCache = PageCache.create(path,memory);
        Logger logger = Logger.create(path);
        DataManagerImp dm = new DataManagerImp(pageCache,tm,logger);

        dm.initPageOne();
        return dm;
    }

    public static DataManager open(String path,long memory,TransactionManager tm){
        PageCache pageCache = PageCache.open(path, memory);
        Logger logger = Logger.open(path);
        DataManagerImp dm = new DataManagerImp(pageCache,tm,logger);
        if(!dm.loadPageOne()){
            Recover.recover(tm,logger,pageCache);
        }
        dm.fillPageIndex();
        PageOne.setVCOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }
}
