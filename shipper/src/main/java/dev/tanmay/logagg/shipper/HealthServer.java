/** About this class - Implements a simple HTTP server that exposes a health check endpoint. The server responds with "ok" if the application is healthy and "unhealthy" if it is not. It uses a BooleanSupplier to determine the health status and can be started and stopped as needed. */

package dev.tanmay.logagg.shipper;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

public class HealthServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(HealthServer.class);

    private final HttpServer server;
    private final BooleanSupplier healthy;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public HealthServer(int port, BooleanSupplier healthy) throws IOException {
        this.healthy = healthy;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.createContext("/health", this::handle);
    }

    public void start() {
        server.start();
        log.info("Health endpoint listening on port {}", port());
    }

    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        server.stop(0);
        log.info("Health endpoint stopped");
    }

    private void handle(HttpExchange exchange) throws IOException {
        boolean ok = healthy.getAsBoolean();
        byte[] body = (ok ? "ok" : "unhealthy").getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(ok ? 200 : 503, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }
}