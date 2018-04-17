package com.example.jutil;

/**
 * Created by yilong on 2018/2/24.
 */
public class DataTypeTransfer {
    public static byte[] longToByte(long l) {
        byte[] b = new byte[8];
        int size = 8;
        for (int i = 0; i < size; ++i) {
            b[i] = (byte) (l >> (size - i - 1 << 3));
        }
        return b;
    }

    public static long bytesToLong(byte[] b) {
        int size = 8;
        long l = 0;
//        for (int i = 0; i < size; ++i) {
//            l = l | (bts[i] << (size - i - 1 << 3));
//        }
        l = (b[7] & 0xFF | (b[6] & 0xFF) << 8 | (b[5] & 0xFF) << 16 | (b[4] & 0xFF) << 24 | (b[3] & 0xFF) << 32 | (b[2] & 0xFF) << 40 | (b[1] & 0xFF) << 48 | (b[0] & 0xFF) << 56);
        return l & 0xFFFFFFFFL;
    }
}
