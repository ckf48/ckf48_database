package exceptionUtil;

public class Error {

    //common
    public static final Exception FileExistsException = new RuntimeException("File already exists!");
    public static final Exception FileNotExistsException = new RuntimeException("File does not exists!");
    public static final Exception FileCannotRWException = new RuntimeException("File cannot read or write!");
    public static final Exception CacheFullException = new RuntimeException("Cache is full!");
    //tm
    public static final Exception BadXIDFileException = new RuntimeException("Bad XID file!");
    //dm
    public static final Exception MemTooSmallException = new RuntimeException("Memory too small!");
    public static final Exception DataTooLargeException = new RuntimeException("Data too large!");
    public static final Exception DatabaseTooBusyException = new RuntimeException("Database too busy!");
    public static final Exception BadLogFileException = new RuntimeException("Bad log file!");
    //vm
    public static final Exception DeadLockException = new RuntimeException("Dead lock!");

}
