package core.utils;



import java.security.SecureRandom;
import java.util.Random;

public class RandomDataCreator {
    static Random random = new SecureRandom();
    public static byte[] randomBytes(int size){
        byte[] data = new byte[size];
        random.nextBytes(data);
        return data;
    }
}
