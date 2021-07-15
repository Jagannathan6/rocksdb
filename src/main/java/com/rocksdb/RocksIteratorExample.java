package com.rocksdb;

import org.rocksdb.*;

public class RocksIteratorExample {

    public static void main(String args[]) {

        RocksDB.loadLibrary();

        try (final Options options = new Options().setCreateIfMissing(true)) {
            try ( RocksDB db = TransactionDB.open(options, "/Users/jagannathan/Desktop/My Files/db/rocksdb")) {
                db.put("Jagan_1".getBytes(), "Rajagopalan".getBytes());
                db.put("Jagan_2".getBytes(), "Rajagopalan".getBytes());
                db.put("Jagan_3".getBytes(), "Rajagopalan".getBytes());
                db.put("Jagan_4".getBytes(), "Rajagopalan".getBytes());
                db.put("Jagan_5".getBytes(), "Rajagopalan".getBytes());



                ReadOptions readOptions = new ReadOptions();
                readOptions.setPrefixSameAsStart(true);
                try (RocksIterator rocksIterator = db.newIterator(readOptions)) {
                    for (rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()) {
                        System.out.println(new String(rocksIterator.key()) + " = " + new String(rocksIterator.value()));
                    }
                }
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }


    }
}

