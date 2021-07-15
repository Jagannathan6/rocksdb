package com.rocksdb;

import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class RocksColumnFamilyExample {

    public static void main(String[] args ) {
        try {
            byte[] INDIA = "India".getBytes(StandardCharsets.UTF_8);
            byte[] INDIA_CAPITAL = "Delhi".getBytes(StandardCharsets.UTF_8);
            byte[] US = "US".getBytes(StandardCharsets.UTF_8);
            byte[] US_CAPITAL = "Washington".getBytes(StandardCharsets.UTF_8);
            RocksDB.loadLibrary();
            ColumnFamilyOptions cf = new ColumnFamilyOptions();
            ColumnFamilyDescriptor defaultColumnFamilyDescriptor =
                    new ColumnFamilyDescriptor("default".getBytes(StandardCharsets.UTF_8), cf);
            ColumnFamilyDescriptor countryColumnFamilyDescriptor =
                    new ColumnFamilyDescriptor("country".getBytes(StandardCharsets.UTF_8), cf);
            DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
            final List<ColumnFamilyHandle> columnFamilyHandleList =
                    new ArrayList<>();
            RocksDB db = RocksDB.open(options, "/Users/jagannathan/Desktop/My Files/db/rocksdb",
                    Arrays.asList(defaultColumnFamilyDescriptor, countryColumnFamilyDescriptor), columnFamilyHandleList);

            log.info("Before adding India to Column Family: {}",new String(db.get(INDIA)) );
            db.put(INDIA, INDIA_CAPITAL);
            log.info("After adding India to Column Family: {}",new String(db.get(INDIA)) );
            db.put(US, US_CAPITAL);
            log.info("After adding US to Column Family :{}",new String(db.get(US)) );

            log.info("Fetching data from the column family DEFAULT :{}",new String(db.get("State".getBytes())) );
            db.close();
            for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
                columnFamilyHandle.close();
            }

        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}
