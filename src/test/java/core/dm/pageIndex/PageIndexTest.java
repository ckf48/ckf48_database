package test.java.core.dm.pageIndex;

import core.dm.pageCache.PageCache;
import core.dm.pageIndex.PageIndex;
import core.dm.pageIndex.PageInfo;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PageIndexTest {
    @Test
    public void testPageIndex(){
        PageIndex pIndex = new PageIndex();
        int threshold = PageCache.PAGE_SIZE / 20;
        for(int i = 0; i < 20; i ++) {
            pIndex.add(i, i*threshold);
            pIndex.add(i, i*threshold);
            pIndex.add(i, i*threshold);
        }

        for(int k = 0; k < 3; k ++) {
            for(int i = 0; i < 19; i ++) {
                PageInfo pi = pIndex.select(i * threshold);
                assertNotNull(pi);
                assertEquals(pi.pgno,i + 1);
            }
        }
    }
}
