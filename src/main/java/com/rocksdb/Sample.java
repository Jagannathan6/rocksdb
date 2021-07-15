package com.rocksdb;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class Sample {

    public static void main(String args[]) {

        byte[] key = "State".getBytes();
        byte[] value = "Karnataka".getBytes();

        byte[] key1 = "TN".getBytes();
        byte[] value1 = "Chennai".getBytes();
        RocksDB.loadLibrary();

        try (final Options options = new Options().setCreateIfMissing(true)) {
            try (final RocksDB db = RocksDB.open(options, "/Users/jagannathan/Desktop/My Files/db/rocksdb")) {
                System.out.println("Value before adding : " + new String(db.get(key)));
                db.put(key, value);
                System.out.println("Value after adding : " + new String(db.get(key)));
                db.put(key1, value1);
                System.out.println("Value after adding : " + new String(db.get(key1)));

                List<byte[]> resultList = db.multiGetAsList( Arrays.asList(key, key1));
                for (byte[] result: resultList) {
                    System.out.println("Multi Get result " +new String(result));
                }

                db.delete(key);
                if (null == db.get(key)) {
                    System.out.println("Not present");
                    db.put(key, value);
                    System.out.println(new String(db.get(key)));
                }



            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}
