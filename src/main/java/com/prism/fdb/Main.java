package com.prism.fdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.tuple.Tuple;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

public class Main {

    private static String createDataSize(int msgSize) {
        // Java chars are 2 bytes
        msgSize = msgSize/2;
        msgSize = msgSize * 1024;
        StringBuilder sb = new StringBuilder(msgSize);
        for (int i=0; i<msgSize; i++) {
            sb.append('a');
        }
        return sb.toString();
    }

    public static void main(String[] args) {

        System.out.println("Starting.......");
        final String data = new String(new char[1024]);

        if(args.length != 3){
            System.out.println("Main.java threadNo batchSize totalInserts");
            System.exit(-1);
        }

        FDB fdb = FDB.selectAPIVersion(610);

        final AtomicLong total = new AtomicLong();

        long start = System.currentTimeMillis();
        try (Database db = fdb.open()) {

            Runnable runnable = () -> {
                // Run an operation on the database
                while (total.get() < Long.parseLong(args[2])) {
                    db.run(tr -> {
                        tr.options().setReadYourWritesDisable();
                        tr.options().setNextWriteNoWriteConflictRange();
                        for (int batch = 0; batch < Long.parseLong(args[1]); batch++) {
                            tr.set(Tuple.from(System.currentTimeMillis() + UUID.randomUUID().toString()).pack(), Tuple.from(data).pack());
                            //tr.set(Tuple.from(UUID.randomUUID()).pack(), Tuple.from(data).pack());
                            total.incrementAndGet();
                        }
                        System.out.println("Inserted " + total.get() + " records/s: " + ((float) total.get() * 1000) / (System.currentTimeMillis() - start));
                        return null;
                    });
                }
            };

            for(int t=1;t<Integer.parseInt(args[0]);t++){
                new Thread(runnable).start();
            }
            new Thread(runnable).run();
        }
    }
}