package test.java.core.common;

import core.common.AbstractCache;

public class MockCache extends AbstractCache<Long> {
    public MockCache() {
        super(50);
    }

    @Override
    protected Long getForCache(long key) throws Exception {
        return key;
    }

    @Override
    protected void releaseForCache(Long aLong) {

    }
}
