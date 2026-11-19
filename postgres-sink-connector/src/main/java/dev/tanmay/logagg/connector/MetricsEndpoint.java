package dev.tanmay.logagg.connector;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class MetricsEndpoint implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsEndpoint.class);
    private static MetricsEndpoint shared;

    private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    private final HttpServer server;
        MetricsEndpoint(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", this::handle);
        server.start();
        LOG.info("metrics endpoint listening on port {}", port());
    }

    static synchronized MetricsEndpoint shared(int port) {
        if (shared == null) {
            try {
                shared = new MetricsEndpoint(port);
            } catch (IOException e) {
                throw new ConnectException("Cannot start the metrics endpoint on port " + port, e);
            }
        }
        return shared;
    }

    PrometheusMeterRegistry registry() { return registry; }
    int port() { return server.getAddress().getPort(); }

    private void handle(HttpExchange exchange) throws IOException {
        byte[] body = registry.scrape().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    @Override
    public void close() {
        server.stop(0);
        registry.close();
    }
}