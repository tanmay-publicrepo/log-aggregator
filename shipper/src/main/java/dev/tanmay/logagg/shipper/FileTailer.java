/** About this class - Reads a log file and emits new lines as they are written to the file. It handles file rotation and truncation, and can start reading from the end of the file or from the beginning. It uses a consumer to process each line read from the file. File rotation and truncation are handled gracefully to ensure no data is lost. Consumer(function name is lineConsumer)is called for each line read. What are file rotation and truncation? File rotation is when a log file is renamed and a new file is created with the original name, often done to manage log file sizes. Truncation is when the contents of a log file are cleared or reduced in size, which can happen if the file is manually edited or if a logging system resets the file. The FileTailer class monitors the log file for these events and adjusts its reading position accordingly to continue emitting new lines without missing any data. */

package dev.tanmay.logagg.shipper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTailer {

    private static final Logger log = LoggerFactory.getLogger(FileTailer.class);
    private static final int BUFFER_SIZE = 64 * 1024;
    private static final long DEFAULT_POLL_INTERVAL_MS = 200;

    private final Path logFilePath;
    private final Consumer<String> lineConsumer;
    private final long pollIntervalMs;
    private final boolean startAtEnd;

    private final AtomicLong opens = new AtomicLong();
    private volatile boolean running = true;
    private FileChannel channel;
    private long position;
    private Object fileKey;
    private boolean firstOpen = true;
    private boolean waitingLogged = false;

    private final ByteArrayOutputStream pending = new ByteArrayOutputStream();
    private final ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);

    private final AtomicLong linesEmitted = new AtomicLong();
    private final AtomicLong bytesRead = new AtomicLong();
    private final AtomicLong rotations = new AtomicLong();
    private final AtomicLong truncations = new AtomicLong();
    private final AtomicLong callbackFailures = new AtomicLong();

    public FileTailer(Path logFilePath, Consumer<String> lineConsumer) {
        this(logFilePath, lineConsumer, DEFAULT_POLL_INTERVAL_MS, false);
    }

    public FileTailer(Path logFilePath, Consumer<String> lineConsumer, long pollIntervalMs, boolean startAtEnd) {
        this.logFilePath = Objects.requireNonNull(logFilePath, "logFilePath cannot be null");
        this.lineConsumer = Objects.requireNonNull(lineConsumer, "lineConsumer cannot be null");
        if (pollIntervalMs <= 0) {
            throw new IllegalArgumentException("pollIntervalMs must be positive");
        }
        this.pollIntervalMs = pollIntervalMs;
        this.startAtEnd = startAtEnd;
    }

    public void run() throws IOException {
        log.info("Tailing {} (poll every {} ms, startAtEnd={})", logFilePath, pollIntervalMs, startAtEnd);
        open();
        try {
            while (running) {
                try {
                    if (!openIfNeeded()) {
                        sleepQuietly();
                        continue;
                    }
                    if (readAvailable() <= 0) {
                        handleIdle();
                    }
                } catch (IOException e) {
                    log.error("I/O error while tailing {}; will retry", logFilePath, e);
                    closeQuietly();
                    sleepQuietly();
                }
            }
        } finally {
            flushPending("shutdown");
            closeQuietly();
            log.info("Tailer stopped after {} lines", linesEmitted.get());
        }
    }

    public void stop() {
        log.info("Stop requested for {}", logFilePath);
        running = false;
    }

    public long linesEmitted() {
        return linesEmitted.get();
    }

    public long bytesRead() {
        return bytesRead.get();
    }

    public long rotations() {
        return rotations.get();
    }

    public long truncations() {
        return truncations.get();
    }

    public long callbackFailures() {
        return callbackFailures.get();
    }

    public long opens() {
        return opens.get();
   }

    private void open() throws IOException {
        channel = FileChannel.open(logFilePath, StandardOpenOption.READ);
        opens.incrementAndGet();
        fileKey = readKey();
        if (firstOpen && startAtEnd) {
            position = channel.size();
            channel.position(position);
        } else {
            position = 0;
        }
        firstOpen = false;
        pending.reset();
        waitingLogged = false;
        log.info("Opened {} at position {}", logFilePath, position);
    }

    private boolean openIfNeeded() throws IOException {
        if (channel != null) {
            return true;
        }
        try {
            open();
            return true;
        } catch (NoSuchFileException e) {
            if (!waitingLogged) {
                log.warn("Waiting for {} to appear", logFilePath);
                waitingLogged = true;
            }
            return false;
        }
    }

    private int readAvailable() throws IOException {
        buffer.clear();
        int n = channel.read(buffer);
        if (n <= 0) {
            return n;
        }
        buffer.flip();
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
            if (b == '\n') {
                emitPending();
            } else {
                pending.write(b);
            }
        }
        position += n;
        bytesRead.addAndGet(n);
        return n;
    }

    private void emitPending() {
        byte[] bytes = pending.toByteArray();
        pending.reset();
        int length = bytes.length;
        if (length > 0 && bytes[length - 1] == '\r') {
            length--;
        }
        deliver(new String(bytes, 0, length, StandardCharsets.UTF_8));
    }

    private void deliver(String line) {
        try {
            lineConsumer.accept(line);
            linesEmitted.incrementAndGet();
        } catch (RuntimeException e) {
            callbackFailures.incrementAndGet();
            String preview = line.length() > 200 ? line.substring(0, 200) + "..." : line;
            log.error("Consumer failed for line: {}", preview, e);
        }
    }

    private Object readKey() throws IOException {
        return Files.readAttributes(logFilePath, BasicFileAttributes.class).fileKey();
    }

    private void closeQuietly() {
        if (channel == null) {
            return;
        }
        try {
            channel.close();
        } catch (IOException e) {
            log.warn("Failed to close {}", logFilePath, e);
        } finally {
            channel = null;
        }
    }

    private void flushPending(String reason) {
        if (pending.size() > 0) {
            log.warn("Emitting unterminated line of {} bytes at {}", pending.size(), reason);
            emitPending();
        }
    }

    private void discardPending(String reason) {
        if (pending.size() > 0) {
            log.warn("Discarding {} bytes of unterminated line after {}", pending.size(), reason);
            pending.reset();
        }
    }

    private void drain() throws IOException {
        int n;
        do {
            n = readAvailable();
        } while (n > 0);
    }

    private boolean checkTruncation() throws IOException {
        long size = channel.size();
        if (size < position) {
            log.warn("{} truncated (size {} < position {}); restarting from 0", logFilePath, size, position);
            truncations.incrementAndGet();
            discardPending("truncation");
            position = 0;
            channel.position(0);
            return true;
        }
        return false;
    }

    private boolean checkRotation() throws IOException {
        boolean rotated;
        try {
            rotated = !Objects.equals(readKey(), fileKey);
        } catch (NoSuchFileException e) {
            rotated = true;
        }
        if (!rotated) {
            return false;
        }
        log.info("Rotation detected for {}", logFilePath);
        rotations.incrementAndGet();
        drain();
        flushPending("rotation");
        closeQuietly();
        return true;
    }

    private void sleepQuietly() {
        try {
            Thread.sleep(pollIntervalMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running = false;
        }
    }

    private void handleIdle() throws IOException {
        if (checkTruncation()) {
            return;
        }
        if (checkRotation()) {
            return;
        }
        sleepQuietly();
    }
}