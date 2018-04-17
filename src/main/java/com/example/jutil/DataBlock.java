package com.example.jutil;

import java.util.*;

/**
 * Created by yilong on 2018/2/22.
 */
public class DataBlock {
    public List<byte[]> buf = new ArrayList<byte[]>();
    public int cur = 0;
    public String lastKey;
    public String firstKey;

    public DataBlock(String firstKey) {
        this.firstKey = firstKey;
        this.lastKey = firstKey;
    }

    public boolean save(byte[] data) {
        if (Constant.BLOCK_SIZE < data.length + cur) {
            return false;
        }

        buf.add(data);
        cur += data.length;

        return true;
    }

    public int avaliableSize() {
        return (Constant.BLOCK_SIZE - cur);
    }

    public void updateLastKey(String k) {
        if (k.compareTo(lastKey) > 0) {
            lastKey = k;
        }
    }

    public String getLastKey() {
        return lastKey;
    }

    public List<byte[]>  getBufArray() {
        return buf;
    }

    public void  clean() {
        buf.clear();
    }
}
