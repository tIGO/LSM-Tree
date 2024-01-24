package com.tomfran.lsm.wal;

import com.tomfran.lsm.io.ExtendedInputStream;
import com.tomfran.lsm.io.ExtendedOutputStream;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class WriteAheadLog {

    private final String filename;
    private final ScheduledExecutorService es;
    private long seqNo;
    private ExtendedOutputStream os;

    private CompletableFuture<Void> pendingSync;

    public WriteAheadLog(String filename) {
        this.filename = filename;
        seqNo = 0;
        os = new ExtendedOutputStream(filename);
        es = newSingleThreadScheduledExecutor();
        es.scheduleAtFixedRate(this::flush, 1, 1, TimeUnit.SECONDS);
    }

    private void flush() {
        System.out.println("Flushing " + pendingSync);
        if (pendingSync != null) {
            var sync = pendingSync;
            pendingSync = null;
            try {
                os.flush();
                os.fsync();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            sync.complete(null);
            System.out.println("Flushing is done " + sync);
        }
    }

    synchronized public CompletableFuture<Void> write(byte[] data) {
        seqNo++;
        os.writeLong(seqNo);
        os.writeLong(data.length);
        os.write(data);
        if (pendingSync == null) {
            pendingSync = new CompletableFuture<>();
        }
        System.out.println("Write " + pendingSync);
        return pendingSync;
    }

    public WriteAheadLogIterator iterator() {
        return new WriteAheadLogIterator(filename);
    }

    public void close() {
        es.shutdownNow();
        os.close();
    }

    record WalRecord(long seqNo, byte[] data) {
    }

    public static class WriteAheadLogIterator implements Iterator<WalRecord>, AutoCloseable {

        private final ExtendedInputStream is;
        private WalRecord next;


        public WriteAheadLogIterator(String filename) {
            is = new ExtendedInputStream(filename);
            readNext();
        }

        @Override
        public boolean hasNext() {
            System.out.println("Has next " + next);
            return next != null;
        }

        @Override
        public WalRecord next() {
            var cur = next;
            readNext();
            return cur;
        }


        private void readNext() {
            System.out.println("Reading next");
            try {
                long seqNo = is.readLong();
                int length = (int) is.readLong();
                byte[] data = is.readNBytes(length);
                next = new WalRecord(seqNo, data);
            } catch (RuntimeException ignored) {
                next = null;
            }
        }

        @Override
        public void close() throws Exception {
            is.close();
        }
    }
}
