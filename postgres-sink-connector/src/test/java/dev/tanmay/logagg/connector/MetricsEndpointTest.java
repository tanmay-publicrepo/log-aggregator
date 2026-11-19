package dev.tanmay.logagg.connector;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;

class MetricsEndpointTest {

    @Test
    void servesPrometheusTextOnMetrics() throws Exception {
        try (MetricsEndpoint endpoint = new MetricsEndpoint(0)) {
            Counter.builder("sink.rows.written").tag("table", "t").register(endpoint.registry()).increment(3);
            HttpResponse<String> response = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + endpoint.port() + "/metrics")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.headers().firstValue("Content-Type").orElse("")).contains("text/plain");
            assertThat(response.body()).contains("sink_rows_written_total{table=\"t\"}");
        }
    }
}