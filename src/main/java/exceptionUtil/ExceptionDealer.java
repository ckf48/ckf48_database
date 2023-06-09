package exceptionUtil;

public class ExceptionDealer {
    public static void shutDown(Exception e){
        e.printStackTrace();
        System.exit(1);
    }

}
