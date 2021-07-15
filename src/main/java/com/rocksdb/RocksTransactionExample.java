package com.rocksdb;

import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class RocksTransactionExample {
    public static void main(String[] args) {
        try {
            byte[]  INDIA = "Turkey".getBytes(StandardCharsets.UTF_8);
            byte[] INDIA_CURRENCY = "Lira".getBytes(StandardCharsets.UTF_8);
            RocksDB.loadLibrary();
            ColumnFamilyOptions cf = new ColumnFamilyOptions();
            ColumnFamilyDescriptor defaultColumnFamilyDescriptor =
                    new ColumnFamilyDescriptor("default".getBytes(StandardCharsets.UTF_8), cf);
            ColumnFamilyDescriptor currencyFileDescriptor =
                    new ColumnFamilyDescriptor("currency".getBytes(StandardCharsets.UTF_8), cf);
            ColumnFamilyDescriptor countryFileDescriptor =
                    new ColumnFamilyDescriptor("country".getBytes(StandardCharsets.UTF_8), cf);
            DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
            TransactionDBOptions transactionDBOptions = new TransactionDBOptions();
            final List<ColumnFamilyHandle> columnFamilyHandleList =
                    new ArrayList<>();
            TransactionDB db = TransactionDB.open(options, transactionDBOptions, "/Users/jagannathan/Desktop/My Files/db/rocksdb",
                    Arrays.asList(defaultColumnFamilyDescriptor, currencyFileDescriptor, countryFileDescriptor),
                    columnFamilyHandleList);


            WriteOptions writeOptions = new WriteOptions().setSync(true).setDisableWAL(false);

            db.delete(INDIA);

            if (null == db.get(INDIA)) {
                log.info("Data is deleted");
            } else {
                log.info("Before Transaction {} " + new String(db.get(INDIA)));
            }

            Transaction transaction = db.beginTransaction(writeOptions);

            try {
                transaction.put(INDIA, INDIA_CURRENCY);
                String [] a = new String [] {"Test"};
                log.info("" + a[100]);
                transaction.commit();
            } catch (Exception e) {
                log.error("Adding data to the Map failed");
                transaction.rollback();
            }

            transaction.close();

            if (null == db.get(INDIA)) {
                log.info("Data is not committed. Since an exception is raised, it is rolled back");
            } else {
                log.info("If commited {} " + new String(db.get(INDIA)));
            }

            transaction = db.beginTransaction(writeOptions);
            transaction.put(INDIA, INDIA_CURRENCY);
            transaction.commit();

            transaction.close();

            log.info("After Commit {} " + new String(db.get(INDIA)));

            db.close();
            for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
                columnFamilyHandle.close();
            }

        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}
