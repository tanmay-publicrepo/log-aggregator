package dev.tanmay.logagg.shipper;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.junit.jupiter.api.Test;

class HealthServerTest {

    @Test
    void healthyReturns200() throws Exception {
        try (HealthServer server = new HealthServer(0, () -> true)) {
            server.start();

            HttpResponse<String> response = get(server.port());

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).isEqualTo("ok");
        }
    }

    @Test
    void unhealthyReturns503() throws Exception {
        try (HealthServer server = new HealthServer(0, () -> false)) {
            server.start();

            HttpResponse<String> response = get(server.port());

            assertThat(response.statusCode()).isEqualTo(503);
            assertThat(response.body()).isEqualTo("unhealthy");
        }
    }

    private static HttpResponse<String> get(int port) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/health"))
                .GET()
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}