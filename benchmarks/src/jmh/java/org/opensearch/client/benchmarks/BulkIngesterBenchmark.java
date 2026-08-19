/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client.benchmarks;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.core5.http.HttpHost;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._helpers.bulk.AdaptiveOperationsController;
import org.opensearch.client.opensearch._helpers.bulk.BulkIngester;
import org.opensearch.client.opensearch._helpers.bulk.BulkListener;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5Transport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.opensearch.client.util.BinaryData;

/**
 * Benchmarks the {@link BulkIngester} end-to-end throughput.
 *
 * <p>Prerequisites: a running OpenSearch instance at localhost:9200 (default, no auth).
 * Override via system properties:
 * <ul>
 *   <li>{@code -Dbenchmark.host=my-host}</li>
 *   <li>{@code -Dbenchmark.port=9200}</li>
 *   <li>{@code -Dbenchmark.scheme=https} (default: {@code http})</li>
 *   <li>{@code -Dbenchmark.user=admin}</li>
 *   <li>{@code -Dbenchmark.password=secret}</li>
 *   <li>{@code -Dbenchmark.index=bench-index}</li>
 * </ul>
 *
 * <p>Parameterized axes:
 * <ul>
 *   <li>{@code maxConcurrentRequests} — in-flight bulk request limit</li>
 *   <li>{@code batchSize} — operations per bulk flush (used when {@code adaptive=false})</li>
 *   <li>{@code adaptive} — when {@code true}, enables {@link AdaptiveOperationsController};
 *       {@code batchSize} is ignored and the controller starts at 200 ops and tunes automatically</li>
 * </ul>
 *
 * <p>Run:
 * <pre>
 *   ./gradlew :benchmarks:jmh -Pbenchmark=BulkIngesterBenchmark
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class BulkIngesterBenchmark {

    // ---------- Benchmark parameters ----------

    /** Number of in-flight bulk requests allowed concurrently. */
    @Param({ "1", "2", "4", "8"})
    public int maxConcurrentRequests;

    /**
     * Operations per bulk flush (fixed mode).
     * Ignored when {@code adaptive=true} — the controller starts at 200 and tunes automatically.
     */
    @Param({ "1000","3000", "5000", "7000", "10000" })
    public int batchSize;

    /**
     * When {@code true}, enables {@link AdaptiveOperationsController}:
     * starts at {@code initialOps=200} and tunes {@code maxOperations} automatically.
     * When {@code false}, uses the fixed {@code batchSize} param.
     */
    @Param({ "false", "true" })
    public boolean adaptive;

    // ---------- Infrastructure ----------

    private ApacheHttpClient5Transport transport;
    private OpenSearchAsyncClient asyncClient;
    private OpenSearchClient syncClient;
    private BulkIngester<Void> ingester;
    private String indexName;
    private AdaptiveOperationsController adaptiveController;

    /** Pre-serialized document reused across all benchmark iterations (avoids measuring serialization). */
    private BinaryData preSerializedDoc;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Setup(Level.Trial)
    public void setup() throws Exception {
        final String host = System.getProperty("benchmark.host", "localhost");
        final int port = Integer.parseInt(System.getProperty("benchmark.port", "9200"));
        final String scheme = System.getProperty("benchmark.scheme", "http");
        final String user = System.getProperty("benchmark.user", "");
        final String password = System.getProperty("benchmark.password", "");
        final String indexBase = System.getProperty("benchmark.index", "bench-index");

        // Unique index name per trial run — avoids stale data from a previous run
        indexName = indexBase + "-" + System.currentTimeMillis();

        final int concurrency = maxConcurrentRequests;
        var mapper = new JacksonJsonpMapper(objectMapper);

        transport = ApacheHttpClient5TransportBuilder
            .builder(new HttpHost(scheme, host, port))
            .setMapper(mapper)
            .setHttpClientConfigCallback(httpClientBuilder -> {
                var cm = PoolingAsyncClientConnectionManagerBuilder.create()
                    .setMaxConnPerRoute(concurrency)
                    .setMaxConnTotal(concurrency)
                    .build();
                httpClientBuilder.setConnectionManager(cm);

                // Basic auth — only wired when credentials are provided
                if (!user.isEmpty()) {
                    var credentialsProvider = new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(
                        new AuthScope(host, port),
                        new UsernamePasswordCredentials(user, password.toCharArray())
                    );
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }

                return httpClientBuilder;
            })
            .build();

        asyncClient = new OpenSearchAsyncClient(transport);
        syncClient = new OpenSearchClient(transport);

        // Delete the index if it already exists, then create a fresh one
        try {
            syncClient.indices().delete(d -> d.index(indexName));
            System.out.println("[benchmark] deleted existing index: " + indexName);
        } catch (Exception ignored) {
            // index didn't exist — that's fine
        }
        syncClient.indices().create(c -> c.index(indexName));
        System.out.println("[benchmark] created index: " + indexName);

        // Pre-serialize a representative document once — avoids measuring Jackson overhead
        // in the throughput benchmark
        Map<String, Object> doc = buildSampleDoc();
        preSerializedDoc = BinaryData.of(doc, mapper);

        // Build the ingester
        final String finalIndex = indexName;
        final int initialOps = adaptive ? 200 : batchSize;
        adaptiveController = adaptive ? new AdaptiveOperationsController(initialOps) : null;

        ingester = BulkIngester.<Void>of(b -> {
            b.client(asyncClient)
                .maxOperations(initialOps)
                .maxSize(-1)
                .maxConcurrentRequests(maxConcurrentRequests)
                .globalSettings(gs -> gs.index(finalIndex));
            if (adaptiveController != null) {
                b.adaptiveOperations(adaptiveController);
            }
            b.listener(new BulkListener<>() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request, List<Void> contexts) {}

                @Override
                public void afterBulk(long executionId, BulkRequest request, List<Void> contexts, BulkResponse response) {
                    if (response.errors()) {
                        response.items().stream()
                            .filter(i -> i.error() != null)
                            .limit(3)
                            .forEach(i -> System.err.println("Bulk error: " + i.error().reason()));
                    }
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, List<Void> contexts, Throwable failure) {
                    System.err.println("Bulk failure: " + failure.getMessage());
                }
            });
            return b;
        });
    }

    @TearDown(Level.Trial)
    public void teardown() throws Exception {
        if (ingester != null) {
            ingester.close();
        }
        if (adaptiveController != null) {
            System.out.printf(
                "[adaptive] finalState: currentOps=%d bestOps=%d state=%s ewmaTput=%.2f ops/s increases=%d decreases=%d%n",
                adaptiveController.currentMaxOps(),
                adaptiveController.bestOps(),
                adaptiveController.state(),
                adaptiveController.ewmaThroughput(),
                adaptiveController.totalIncreases(),
                adaptiveController.totalDecreases()
            );
        }
        if (syncClient != null && indexName != null) {
            try {
                syncClient.indices().delete(d -> d.index(indexName));
                System.out.println("[benchmark] deleted index: " + indexName);
            } catch (Exception e) {
                System.err.println("[benchmark] failed to delete index " + indexName + ": " + e.getMessage());
            }
        }
        if (transport != null) {
            transport.close();
        }
    }

    // ---------- Benchmarks ----------

    /**
     * Throughput benchmark: how many index operations per second can the ingester sustain
     * under concurrent producer load.
     *
     * <p>{@code @Threads(Threads.MAX)} spawns one producer thread per available CPU, matching
     * the default {@code maxConcurrentRequests} value ({@code availableProcessors}) so the
     * ingester always has work queued and stays in steady-state rather than alternating
     * between idle and blocked phases. This reduces variance across measurement iterations.
     */
    @Benchmark
    @Threads(Threads.MAX)
    public void bulkIndexThroughput() {
        ingester.add(op -> op.index(i -> i
            .document(preSerializedDoc)
        ));
    }

    // ---------- Helpers ----------

    private static Map<String, Object> buildSampleDoc() {
        Map<String, Object> doc = new HashMap<>();
        doc.put("title", "OpenSearch Java Client Benchmark Document");
        doc.put("content", "The quick brown fox jumps over the lazy dog. ".repeat(10));
        doc.put("timestamp", System.currentTimeMillis());
        doc.put("tags", List.of("benchmark", "opensearch", "java-client"));
        doc.put("score", 42.0);
        Map<String, Object> nested = new HashMap<>();
        nested.put("field1", "value1");
        nested.put("field2", 123);
        doc.put("metadata", nested);
        return doc;
    }
}
