package test.java.core.dm.logger;

import core.dm.logger.Logger;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class LoggerTest {
    @Test
    public void loggerTest() {
        Logger logger = Logger.create("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/log/test/test");
        logger.log("ckf".getBytes());
        logger.log("zbq".getBytes());
        logger.log("ashdhask".getBytes());
        logger.log("aydsuh".getBytes());
        logger.log("rqrjqwkej".getBytes());
        logger.close();

        logger = Logger.open("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/log/test/test");
        logger.rewind();

        byte[] log = logger.next();
        assertNotNull(log);
        assertEquals("ckf", new String(log));

        log = logger.next();
        assertNotNull(log);
        assertEquals("zbq", new String(log));

        log = logger.next();
        assertNotNull(log);
        assertEquals("ashdhask", new String(log));

        log = logger.next();
        assertNotNull(log);
        assertEquals("aydsuh", new String(log));

        log = logger.next();
        assertNotNull(log);
        assertEquals("rqrjqwkej", new String(log));

        log = logger.next();
        assertNull(log);

        logger.close();

        assertTrue(new File("/Users/ckf/IdeaProjects/ckf48_database/src/fileStore/log/test/test.log").delete());


    }
}
