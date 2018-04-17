package com.example.jutil;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by yilong on 2018/2/22.
 */
public class JFileFlusher {
    public static void saveIntoFile2(String filename, List<DataBlock> dataBlocks) {
        System.out.println();

        Path file = Paths.get(filename);
        if (!Files.exists(file)) {
            try {
                Files.createFile(file);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        DataOutputStream os = null;
        try {
            os = new DataOutputStream(new FileOutputStream(filename));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        }

        try {
            for (DataBlock dataBlock : dataBlocks) {
                List<byte[]> bts = dataBlock.getBufArray();
                for (byte[] bt : bts) {
                    os.write(bt);
                }

                if (dataBlock.avaliableSize() > 0) {
                    ByteBuffer bf = java.nio.ByteBuffer.allocate(dataBlock.avaliableSize());
                    for (int i=0; i<bf.array().length; i++) {
                        byte b = 0;
                        bf.put(i,b);
                    }
                    os.write(bf.array());
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            try {
                os.flush();
                os.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //
        System.out.println("MemTableFlusher, start saveIntoFile -> " + filename);
    }
}
