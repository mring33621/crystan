package xyz.mattring.crystan.util;

import java.nio.charset.StandardCharsets;

public class BytesConverter {
    public static byte[] utf8ToBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static String bytesToUtf8(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }
}
