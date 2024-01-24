package com.tomfran.lsm.wal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.teeing;

public class WriteAheadLogTest {
    public static void main(String[] args) {
        var wal = new WriteAheadLog("test.wal");
        ExecutorService executor = Executors.newCachedThreadPool();
//        for (int i = 0; i < 100; i++) {
//            var data = ("Hello World " + i).getBytes();
//            bytes += data.length + 1;
//            wal.write(data).join();
//        }

//        var time = System.currentTimeMillis() - start;
//        System.out.println("Finished " + bytes + " in " + time + " ms");

//        wal.iterator().forEachRemaining(r -> System.out.println(r.seqNo() + " " + new String(r.data())));


        var start = System.currentTimeMillis();
        var bytes = 0;
        var completableFutureStream = Stream.iterate(0, i -> i + 1)
                .limit(100000)
                .map(i -> "Hello World " + i)
                .map(String::getBytes)
                .map(wal::write);
        var f = CompletableFuture.allOf(completableFutureStream.toArray(CompletableFuture[]::new));
        f.join();
        var time = System.currentTimeMillis() - start;
        System.out.println("Finished " + bytes + " in " + time + " ms");
        wal.close();
//                .map(executor.submit(wal::write));

    }

}
