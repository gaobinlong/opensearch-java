/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client.samples;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.opensearch._helpers.bulk.AdaptiveOperationsController;
import org.opensearch.client.opensearch._helpers.bulk.BulkIngester;
import org.opensearch.client.opensearch._helpers.bulk.BulkListener;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;

/**
 * Demonstrates adaptive {@code maxOperations} tuning in {@link BulkIngester}.
 *
 * <p>The {@link AdaptiveOperationsController} observes server-side indexing latency
 * ({@code BulkResponse.took()}) after every bulk response and automatically adjusts
 * {@code maxOperations} using hill climbing (increase) and AIMD (decrease on overload).
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :samples:run -Dsamples.mainClass=AdaptiveBulkIngesterSample
 * </pre>
 *
 * <p>Configure the target cluster via environment variables (same as other samples):
 * <pre>
 *   HOST=my-cluster.example.com PORT=443 HTTPS=true USERNAME=admin PASSWORD=secret \
 *     ./gradlew :samples:run -Dsamples.mainClass=AdaptiveBulkIngesterSample
 * </pre>
 */
public class AdaptiveBulkIngesterSample {

    private static final Logger LOGGER = LogManager.getLogger(AdaptiveBulkIngesterSample.class);
    private static final String INDEX_NAME = "adaptive-bulk-sample";
    private static final int TOTAL_DOCS = 100_000;

    public static void main(String[] args) {
        try {
            var client = SampleClient.create();

            var version = client.info().version();
            LOGGER.info("Connected to {}@{}", version.distribution(), version.number());

            // Create the index (delete first if it already exists)
            if (client.indices().exists(r -> r.index(INDEX_NAME)).value()) {
                LOGGER.info("Deleting existing index '{}'", INDEX_NAME);
                client.indices().delete(r -> r.index(INDEX_NAME));
            }
            LOGGER.info("Creating index '{}'", INDEX_NAME);
            client.indices().create(r -> r.index(INDEX_NAME));

            // ---------------------------------------------------------------
            // Build the AdaptiveOperationsController.
            //
            // Parameters:
            //   initialOps=200     — start small, ramp up safely
            //   minOps=10          — hard floor
            //   maxOps=10_000      — hard ceiling
            //   initialStep=200    — coarse initial step for fast ramp-up
            //   minStep=25         — fine-grained step near the optimum
            //   minImprovement=0.005 — stop increasing when gain < 0.5%
            //   dropThreshold=0.10   — 10% throughput drop = regression
            //   decreaseFactor=0.75  — AIMD: reduce by 25% on overload
            //   ewmaAlpha=0.3        — EWMA smoothing coefficient
            // ---------------------------------------------------------------
            AdaptiveOperationsController controller = new AdaptiveOperationsController(
                200,     // initialOps
                10,      // minOps
                10_000,  // maxOps
                200,     // initialStep
                25,      // minStep
                0.005,   // minImprovementFraction
                0.10,    // dropThreshold
                0.75,    // overloadDecreaseFactor
                0.3      // ewmaAlpha
            );

            // ---------------------------------------------------------------
            // Build BulkIngester with adaptive control enabled.
            //
            // - maxOperations(200)     sets the *initial* value; the controller
            //                          takes over from the first response onward.
            // - adaptiveOperations(controller) wires the controller in.
            // - flushInterval is omitted: flush triggers solely on maxOperations.
            // ---------------------------------------------------------------
            BulkIngester<Void> ingester = BulkIngester.<Void>of(b -> b
                .client(client)
                .maxOperations(200)
                .adaptiveOperations(controller)
                .listener(new BulkListener<>() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request, java.util.List<Void> contexts) {
                        LOGGER.debug(
                            "Sending bulk #{}: {} ops (maxOps={}, state={})",
                            executionId,
                            request.operations().size(),
                            controller.currentMaxOps(),
                            controller.state()
                        );
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, java.util.List<Void> contexts, BulkResponse response) {
                        LOGGER.info(
                            "Bulk #{} completed: {} ops, took={}ms | adaptive → maxOps={}, bestOps={}, state={}, ewmaTput={} ops/s",
                            executionId,
                            request.operations().size(),
                            response.took(),
                            controller.currentMaxOps(),
                            controller.bestOps(),
                            controller.state(),
                            String.format("%.2f", controller.ewmaThroughput())
                        );
                        if (response.errors()) {
                            response.items().stream()
                                .filter(i -> i.error() != null)
                                .limit(3)
                                .forEach(i -> LOGGER.warn("Item error: {}", i.error().reason()));
                        }
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, java.util.List<Void> contexts, Throwable failure) {
                        LOGGER.error("Bulk #{} failed: {}", executionId, failure.getMessage());
                    }
                })
            );

            // ---------------------------------------------------------------
            // Index TOTAL_DOCS documents. The ingester flushes automatically
            // when the buffer reaches maxOperations; the adaptive controller
            // adjusts maxOperations based on observed server latency.
            // ---------------------------------------------------------------
            LOGGER.info("Indexing {} documents with adaptive maxOperations...", TOTAL_DOCS);
            long start = System.currentTimeMillis();

            for (int i = 0; i < TOTAL_DOCS; i++) {
                final int docId = i;
                ingester.add(op -> op.index(idx -> idx
                    .index(INDEX_NAME)
                    .document(buildDoc(docId))
                ));
            }

            // Flush remaining buffered operations and wait for all in-flight requests
            ingester.close();

            long elapsed = System.currentTimeMillis() - start;
            LOGGER.info(
                "Indexing complete: {} docs in {}ms ({} docs/s)",
                TOTAL_DOCS,
                elapsed,
                String.format("%.0f", TOTAL_DOCS * 1000.0 / elapsed)
            );

            // ---------------------------------------------------------------
            // Print final controller state
            // ---------------------------------------------------------------
            LOGGER.info("=== Adaptive controller final state ===");
            LOGGER.info("  currentMaxOps  : {}", controller.currentMaxOps());
            LOGGER.info("  bestOps        : {}", controller.bestOps());
            LOGGER.info("  state          : {}", controller.state());
            LOGGER.info("  ewmaThroughput : {} ops/s", String.format("%.4f", controller.ewmaThroughput()));
            LOGGER.info("  totalIncreases : {}", controller.totalIncreases());
            LOGGER.info("  totalDecreases : {}", controller.totalDecreases());

            // Cleanup
            LOGGER.info("Deleting index '{}'", INDEX_NAME);
            client.indices().delete(r -> r.index(INDEX_NAME));

        } catch (Exception e) {
            LOGGER.error("Unexpected exception", e);
        }
    }

    private static Map<String, Object> buildDoc(int index) {
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
